package laila

import (
	"encoding/binary"
	"net/url"
	"strconv"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/codec.py -- pluggable
// wire codec for RPC carriers (``json`` / ``msgpack``).
//
// Every carrier serialises JSON-RPC message dicts to ``bytes`` and back.
// Two codecs are supported and share *identical* semantics for
// laila-specific objects (most importantly the ``__laila_future__``
// tagging that lets the receiver promote a returned future into a
// RemoteFuture):
//
//   - ``"json"`` -- delegates to Encode / Decode (the canonical,
//     human-debuggable format used by the TCP/IP transport) and UTF-8
//     encodes the result.
//   - ``"msgpack"`` -- a compact binary format for bandwidth-constrained
//     links. Uses the same object-flattening rules as LailaJSONEncoder via
//     a shared ``default`` hook.
//
// The module also provides length-prefixed *framing* helpers so stream
// transports (which see an undelimited byte river) can recover message
// boundaries: Frame prepends a 4-byte big-endian length, and Read_frame
// reads exactly one such frame from a py.StreamReader.
//
// Stream-lane frames: besides RPC messages, carriers multiplex opaque
// *stream lanes* on the same connection. A stream frame is distinguished
// from an RPC payload by its **first byte**: RPC payloads start with
// ``{`` (JSON, ``0x7B``) or a msgpack map marker (``0x80``-``0x8F``,
// ``0xDE``, ``0xDF``), whereas every byte below ``0x20`` is reserved for
// binary control. The header layout and helpers live in ``wire``
// (STREAM_MARKER, STREAM_HEADER, FLAG_START, FLAG_END, Is_reserved_frame,
// Pack_stream_frame, Unpack_stream_header).

// CODECS are the supported codec tokens.
var CODECS = []string{"json", "msgpack"}

// LENGTH_PREFIX_SIZE_ is ``struct.Struct(">I").size``.
const LENGTH_PREFIX_SIZE_ = 4

// CODEC_MAX_FRAME_BYTES is the hard cap on a single frame (256 MiB) to
// bound memory on a hostile peer. This is the *outer* framing cap, not
// the stream-message cap -- see ``max_stream_frame_bytes`` on the carriers
// for the latter. (Python: ``codec.MAX_FRAME_BYTES``; renamed here because
// the connection module's ``MAX_FRAME_BYTES`` shares the flat package.)
const CODEC_MAX_FRAME_BYTES = 256 * 1024 * 1024

// Msgpack_default_ flattens a laila object for msgpack using the JSON
// encoder's rules.
//
// Reuses LailaJSONEncoder's default so futures, pydantic models and
// identity objects serialise identically regardless of codec.
func Msgpack_default_(o any) (any, error) { return Laila_json_default(o) }

// Codec_encode serialises *obj* (a JSON-RPC message dict) to bytes with
// *codec*, one of CODECS. Raises ValueError if *codec* is not a
// recognised token.
func Codec_encode(obj any, codec string) ([]byte, error) {
	if codec == "json" {
		s, err := Encode(obj)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
	if codec == "msgpack" {
		return py.Msgpack_packb(obj, py.MsgpackOpts{Default: Msgpack_default_})
	}
	return nil, py.ValueError.Newf("Unknown codec %s; expected one of %s.", py.Repr(codec), py.Repr(py.Tuple(py.Iter(CODECS))))
}

// Codec_decode deserialises bytes produced by Codec_encode back to a
// dict. Raises ValueError if *codec* is not a recognised token.
func Codec_decode(data []byte, codec string) (any, error) {
	if codec == "json" {
		return Decode(string(data))
	}
	if codec == "msgpack" {
		return py.Msgpack_unpackb(data)
	}
	return nil, py.ValueError.Newf("Unknown codec %s; expected one of %s.", py.Repr(codec), py.Repr(py.Tuple(py.Iter(CODECS))))
}

// Frame prepends a 4-byte big-endian length prefix to *payload*.
//
// Used by stream carriers to delimit messages on an undelimited byte
// stream.
func Frame(payload []byte) []byte {
	out := make([]byte, LENGTH_PREFIX_SIZE_+len(payload))
	binary.BigEndian.PutUint32(out[:LENGTH_PREFIX_SIZE_], uint32(len(payload)))
	copy(out[LENGTH_PREFIX_SIZE_:], payload)
	return out
}

// Encode_frame is Codec_encode then Frame.
func Encode_frame(obj any, codec string) ([]byte, error) {
	b, err := Codec_encode(obj, codec)
	if err != nil {
		return nil, err
	}
	return Frame(b), nil
}

// Read_frame reads exactly one length-prefixed frame from *reader*.
//
// Returns the payload bytes (without the prefix), or nil when the stream
// reaches EOF cleanly between frames. Raises ValueError if the
// advertised length exceeds CODEC_MAX_FRAME_BYTES.
func Read_frame(reader *py.StreamReader) ([]byte, error) {
	header, err := reader.Readexactly(LENGTH_PREFIX_SIZE_)
	if err != nil {
		if py.IsInstance(err, py.IncompleteReadError) {
			return nil, nil
		}
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(header))
	if length > CODEC_MAX_FRAME_BYTES {
		return nil, py.ValueError.Newf("Frame length %d exceeds cap %d.", length, CODEC_MAX_FRAME_BYTES)
	}
	if length == 0 {
		return []byte{}, nil
	}
	payload, err := reader.Readexactly(length)
	if err != nil {
		if py.IsInstance(err, py.IncompleteReadError) {
			return nil, nil
		}
		return nil, err
	}
	return payload, nil
}

// ---------------------------------------------------------------------------
// policy/central/communication/protocols/_carriers/uri.py -- small URI
// parsing helpers shared by concrete transports.
//
// Transports peer via ``add_peer(uri, secret)``; these helpers turn the
// common URI shapes (``scheme://host:port``, ``scheme:///path``) into the
// address components a driver needs, without each transport re-deriving
// the same ``urllib.parse`` boilerplate.
// ---------------------------------------------------------------------------

// Split_host_port returns ``(host, port)`` from a ``scheme://host:port``
// URI, falling back to *default_host* / *default_port* (Python defaults
// ``"127.0.0.1"`` / ``0``).
func Split_host_port(uri string, defaults ...any) (string, int, error) {
	default_host, default_port := "127.0.0.1", 0
	if len(defaults) > 0 && !py.IsNone(defaults[0]) {
		default_host = py.Str(defaults[0])
	}
	if len(defaults) > 1 && !py.IsNone(defaults[1]) {
		n, err := py.Int(defaults[1])
		if err != nil {
			return "", 0, err
		}
		default_port = n
	}
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", 0, py.ValueError.New(err.Error())
	}
	host := parsed.Hostname()
	if host == "" {
		host = default_host
	} else {
		host = strings.ToLower(host)
	}
	port := default_port
	if p := parsed.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			return "", 0, py.ValueError.Newf("Port could not be cast to integer value as %s", py.Repr(p))
		}
		port = n
	}
	return host, port, nil
}

// urlsplit_ is ``urllib.parse.urlparse(uri)`` reduced to ``(netloc,
// path)``: the netloc is everything after ``//`` up to the next ``/``,
// ``?`` or ``#``; unlike net/url it never rejects an authority such as
// ``LAILA:POLICY:<uuid>`` (loopback / broker URIs name policies, not
// hosts).
func urlsplit_(uri string) (netloc string, path string) {
	rest := uri
	if i := strings.Index(rest, ":"); i > 0 && strings.HasPrefix(rest[i:], "://") {
		rest = rest[i+1:]
	} else if i := strings.Index(rest, ":"); i > 0 && isSchemeChars_(rest[:i]) {
		rest = rest[i+1:]
	}
	if strings.HasPrefix(rest, "//") {
		rest = rest[2:]
		end := len(rest)
		for j, c := range rest {
			if c == '/' || c == '?' || c == '#' {
				end = j
				break
			}
		}
		netloc, rest = rest[:end], rest[end:]
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	return netloc, rest
}

func isSchemeChars_(s string) bool {
	for i, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'))
		if !ok {
			return false
		}
	}
	return s != ""
}

// Uri_path returns the path component of a ``scheme:///path`` URI.
func Uri_path(uri string) string {
	netloc, path := urlsplit_(uri)
	// netloc-less URIs (unix:///tmp/x) keep the leading slash in path.
	if path != "" {
		return path
	}
	return netloc
}

// Uri_authority returns the authority (everything after ``scheme://``)
// verbatim.
func Uri_authority(uri string) string {
	netloc, path := urlsplit_(uri)
	if netloc != "" {
		return netloc
	}
	return strings.TrimLeft(path, "/")
}
