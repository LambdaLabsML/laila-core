package laila

import (
	"os"
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/local/unixsocket.py -- Unix
// domain socket communication transport.
//
// Same-host inter-policy RPC over a py.Start_unix_server stream -- the
// fast, file-permission-secured local channel (no TCP port exposed).
// Built on the reliable StreamRPCProtocol_ carrier.
//
//   - ``protocol_name`` ``"unix"`` (aliases ``uds`` / ``unixsocket``)
//   - URI scheme ``unix:///path/to/socket``

// LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_ is the Unix-domain-socket
// peer-to-peer transport.
//
// Fields:
//
//   - Path (optional) -- filesystem path of the listening socket. When
//     empty a stable path is derived under the system temp dir from this
//     protocol's uuid, so Bound_path is meaningful even with no config.
type LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_ struct {
	StreamRPCProtocol_

	Path string `py:"path" default:""`

	Bound_path_ *string `py:"_bound_path"`
}

// LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class is the class
// descriptor (``protocol_name = "unix"``).
var LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_)(nil),
	StreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "unix").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("unix", "uds", "unixsocket")).
	Set_classvar("matches_token", Token_aliases_classmethod_("unix", "uds", "unixsocket")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("unix://"))

// New_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Bound_path is the path the server is (or would be) listening on.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Bound_path() string {
	if p.Bound_path_ != nil && *p.Bound_path_ != "" {
		return *p.Bound_path_
	}
	return p.Effective_path_()
}

// Effective_path_ is the configured path, or a stable per-uuid temp path.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Effective_path_() string {
	if p.Path != "" {
		return p.Path
	}
	digest := py.Sha1_hex([]byte(p.Uuid()))[:16]
	return filepath.Join(os.TempDir(), "laila_unix_"+digest+".sock")
}

// Matches_token accepts ``"unix"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class, token)
}

// Can_handle_uri claims ``unix://`` URIs.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_class, uri)
}

// Serve_ binds the listening Unix socket (unlinking a stale file first).
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Serve_() (any, error) {
	path := p.Effective_path_()
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return nil, py.OSError.New(err.Error()).From(err)
		}
	}
	server, err := py.Start_unix_server(p.Handle_inbound_stream_, path)
	if err != nil {
		return nil, err
	}
	p.Bound_path_ = &path
	return server, nil
}

// Open_connection_ dials the socket named by the ``unix://`` URI.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Open_connection_(uri string) (*py.StreamReader, *py.StreamWriter, error) {
	return py.Open_unix_connection(Uri_path(uri))
}

// Close_server_ closes the server and unlinks the socket file.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Close_server_(server any) error {
	if err := p.StreamRPCProtocol_.Close_server_(server); err != nil {
		return err
	}
	if p.Bound_path_ != nil && *p.Bound_path_ != "" {
		if _, err := os.Stat(*p.Bound_path_); err == nil {
			_ = os.Remove(*p.Bound_path_)
		}
	}
	p.Bound_path_ = nil
	return nil
}

// Connect_unix is the convenience wrapper building the ``unix://`` URI
// for Connect.
func (p *LAILA_IDENTIFIABLE_UNIXSOCKET_COMM_PROTOCOL_) Connect_unix(path string, secret string) (string, error) {
	return p.Connect("unix://"+path, secret)
}
