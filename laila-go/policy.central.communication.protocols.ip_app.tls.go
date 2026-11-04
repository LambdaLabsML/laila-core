package laila

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/tls.py -- TLS
// communication transport (TCP + SSL).
//
// Subclasses the raw-TCP transport and wraps both the listener and the
// outbound connection in a TLS context built from the configured
// certificate material. Working but **config-required**: a server needs
// ``certfile`` (+ ``keyfile``); there is no auto-generated certificate,
// so a missing cert raises a clear error at Start.
//
//   - ``protocol_name`` ``"tls"`` (aliases ``tcps`` / ``ssl`` /
//     ``tls/tcp``)
//   - URI scheme ``tls://host:port`` (also ``tcps://``)

// LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_ is the TLS-secured peer-to-peer
// transport.
//
// Fields:
//
//   - Certfile (optional) -- PEM certificate for the listener (required
//     to serve).
//   - Keyfile (optional) -- PEM private key for Certfile.
//   - Cafile (optional) -- CA bundle used to verify the peer. When
//     omitted on the client side, verification is disabled
//     (``check_hostname=False``, ``verify_mode=CERT_NONE``) so
//     self-signed test certs work.
//   - Server_hostname (optional) -- name presented/verified during the
//     client TLS handshake.
type LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_

	Certfile        *string `py:"certfile"`
	Keyfile         *string `py:"keyfile"`
	Cafile          *string `py:"cafile"`
	Server_hostname *string `py:"server_hostname"`
}

// LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "tls"``).
var LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_TCP_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "tls").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("tls", "tcps", "ssl", "tls/tcp")).
	Set_classvar("matches_token", Token_aliases_classmethod_("tls", "tcps", "ssl", "tls/tcp")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("tls://", "tcps://"))

// New_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func str_or_empty_(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Server_ssl_context_ builds the listener-side TLS config.
func (p *LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_) Server_ssl_context_() (*tls.Config, error) {
	if str_or_empty_(p.Certfile) == "" {
		return nil, py.RuntimeError.New(
			"TLS transport requires `certfile` (and `keyfile`) to serve. Set them on the protocol or via laila.args.")
	}
	keyfile := str_or_empty_(p.Keyfile)
	if keyfile == "" {
		keyfile = *p.Certfile
	}
	cert, err := tls.LoadX509KeyPair(*p.Certfile, keyfile)
	if err != nil {
		return nil, py.FromGo(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

// Client_ssl_context_ builds the dial-side TLS config.
func (p *LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_) Client_ssl_context_() (*tls.Config, error) {
	ctx := &tls.Config{}
	if cafile := str_or_empty_(p.Cafile); cafile != "" {
		pem, err := os.ReadFile(cafile)
		if err != nil {
			return nil, py.FromGo(err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, py.ValueError.Newf("no certificates found in cafile %s", py.Repr(cafile))
		}
		ctx.RootCAs = pool
	} else {
		ctx.InsecureSkipVerify = true
	}
	return ctx, nil
}

// Serve_ binds the TLS listener; with no certificate this is a
// client-only endpoint (can peer out, cannot accept inbound). Avoids
// forcing a cert on pure clients.
func (p *LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_) Serve_() (any, error) {
	if str_or_empty_(p.Certfile) == "" {
		return nil, nil
	}
	ctx, err := p.Server_ssl_context_()
	if err != nil {
		return nil, err
	}
	server, err := py.Start_server(p.Handle_inbound_stream_, p.Host, p.Port, py.Kw{"ssl": ctx})
	if err != nil {
		return nil, err
	}
	port := server.Sockets()[0].Getsockname().Port
	p.Bound_port_ = &port
	return server, nil
}

// Open_connection_ dials ``host:port`` over TLS.
func (p *LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_) Open_connection_(uri string) (*py.StreamReader, *py.StreamWriter, error) {
	host, port, err := Split_host_port(uri)
	if err != nil {
		return nil, nil, err
	}
	ctx, err := p.Client_ssl_context_()
	if err != nil {
		return nil, nil, err
	}
	server_hostname := str_or_empty_(p.Server_hostname)
	if server_hostname == "" {
		server_hostname = host
	}
	return py.Open_connection(host, port, py.Kw{"ssl": ctx, "server_hostname": server_hostname})
}

// Connect_tls is the convenience wrapper building the ``tls://`` URI for
// Connect.
func (p *LAILA_IDENTIFIABLE_TLS_COMM_PROTOCOL_) Connect_tls(host string, port int, secret string) (string, error) {
	return p.Connect(fmt.Sprintf("tls://%s:%d", host, port), secret)
}
