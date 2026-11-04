package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/connection.py -- WebSocket connection
// management for the TCP/IP protocol.
//
// This module factors the websocket plumbing out of
// LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_ so the protocol class itself
// can stay focused on lifecycle and the public RPC API. Three phases are
// implemented here:
//
//  1. **Server bring-up** (Start_server) -- bind a websockets server to
//     ``proto.host:proto.port`` and stash the handle on *proto*. The
//     server's per-connection callback is Handle_inbound_.
//  2. **Handshake** (Handle_inbound_ / Connect_outbound) -- the first
//     frame on a freshly opened socket must be a ``peer.connect``
//     JSON-RPC request carrying the originating policy's ``global_id``
//     and the shared secret. On success both sides register the peer
//     with their local protocol/communication and enter the shared
//     receive loop.
//  3. **Receive loop** (Receive_loop_) -- one-per-connection task that
//     decodes inbound frames and routes them to either the request
//     handler (Handle_rpc_request_) or the response handler
//     (Handle_rpc_response_).
//
// All functions are async and are expected to run inside the protocol
// instance's dedicated event loop (the daemon thread spun up by
// LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_.Start).

var connection_log_ = py.Get_logger("laila.policy.central.communication.connection")

// COMM_PING_PATH_ is the reserved dotted path of the liveness ping (same
// as the carriers').
var COMM_PING_PATH_ = []string{"__comm_ping__"}

// MAX_FRAME_BYTES is the largest WebSocket frame accepted or sent on a
// peer link -- the same 64 MiB the stream carriers use
// (``max_stream_frame_bytes``). The ``websockets`` default is 1 MiB,
// which a single base64-serialized entry can exceed; a peer that receives
// a bigger frame closes the socket with 1009, so the limit is enforced
// symmetrically here and in LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_.Send_
// (which refuses to send an oversize frame instead of killing the link).
//
// A ``var`` (not a const) so tests can patch it exactly like
// ``mock.patch.object(connection, "MAX_FRAME_BYTES", ...)``.
var MAX_FRAME_BYTES = 64 * 1024 * 1024

// Start_server starts the WebSocket listener and stores the server handle
// on *proto* (the TCP/IP protocol instance whose ``host`` / ``port`` to
// bind).
func Start_server(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) error {
	server, err := py.Serve(
		func(ws *py.WebSocket) error { return Handle_inbound_(proto, ws) },
		proto.Host,
		proto.Port,
		py.Kw{"max_size": MAX_FRAME_BYTES},
	)
	if err != nil {
		return err
	}
	bound_port := server.Sockets()[0].Getsockname().Port
	proto.Bound_port_ = &bound_port
	proto.Server_ = server
	connection_log_.Info("Communication server listening on %s:%s", proto.Host, py.Str(proto.Bound_port()))
	return nil
}

// Handle_inbound_ handles a freshly accepted inbound WebSocket connection.
//
// Expects the first message to be a ``peer.connect`` JSON-RPC request. On
// success, registers the peer bidirectionally and enters the shared
// receive loop.
func Handle_inbound_(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, ws *py.WebSocket) error {
	rawv, err := py.Wait_for(func() (any, error) { return ws.Recv() }, 10.0)
	if err != nil {
		if py.IsInstance(err, py.TimeoutError) || py.IsInstance(err, py.ConnectionClosed) {
			return nil
		}
		return err
	}
	raw := rawv.([]byte)

	msg, err := Decode_msg(raw)
	if err != nil {
		return err
	}

	if !Is_request(msg) || py.Str(msg.GetDefault("method", nil)) != "peer.connect" {
		resp := Make_error(
			msg.GetDefault("id", nil),
			ERR_INVALID_REQUEST,
			"First message must be a peer.connect request.",
			nil,
		)
		if err := ws_send_encoded_(ws, resp); err != nil {
			return err
		}
		return ws.Close()
	}

	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	secret := params.GetDefault("secret", nil)

	if !py.Eq(secret, proto.Peer_secret_key) {
		resp := Make_error(
			msg.GetDefault("id", nil),
			ERR_AUTH_FAILED,
			"Invalid peer secret key.",
			nil,
		)
		if err := ws_send_encoded_(ws, resp); err != nil {
			return err
		}
		return ws.Close()
	}

	policy_id := proto.Policy_id_()
	resp := Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", policy_id))
	if err := ws_send_encoded_(ws, resp); err != nil {
		return err
	}

	pid := py.Str(peer_id)
	proto.Register_peer_(pid, ws)
	connection_log_.Info("Accepted inbound peer %s", pid)

	return Receive_loop_(proto, ws, pid)
}

// Connect_outbound initiates an outbound peering connection.
//
// Connects to *uri* (e.g. ``ws://host:port``), performs the
// ``peer.connect`` handshake with the remote policy's ``peer_secret_key``
// *secret*, registers the peer on both sides, and starts the shared
// receive loop. Returns the remote policy's ``global_id``.
//
// Raises ConnectionError if the handshake is rejected or times out.
func Connect_outbound(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, uri string, secret string) (string, error) {
	ws, err := py.Connect(uri, py.Kw{"max_size": MAX_FRAME_BYTES})
	if err != nil {
		return "", err
	}

	policy_id := proto.Policy_id_()
	req := Make_request(
		"peer.connect",
		py.DictOf(
			"from_id", policy_id,
			"secret", secret,
		),
		"",
	)
	if err := ws_send_encoded_(ws, req); err != nil {
		return "", err
	}

	rawv, err := py.Wait_for(func() (any, error) { return ws.Recv() }, 10.0)
	if err != nil {
		if py.IsInstance(err, py.TimeoutError) || py.IsInstance(err, py.ConnectionClosed) {
			_ = ws.Close()
			return "", py.ConnectionError.New("Peer handshake timed out or connection lost.").From(err)
		}
		return "", err
	}

	msg, err := Decode_msg(rawv.([]byte))
	if err != nil {
		return "", err
	}

	if e, found := msg.Get("error"); found {
		_ = ws.Close()
		return "", py.ConnectionError.Newf("Peer rejected connection: %s", py.Str(error_message_(e)))
	}

	peer_id := dict_or_empty_(msg.GetDefault("result", nil)).GetDefault("peer_id", nil)
	if py.IsNone(peer_id) {
		_ = ws.Close()
		return "", py.ConnectionError.New("Peer response missing peer_id.")
	}

	pid := py.Str(peer_id)
	proto.Register_peer_(pid, ws)
	connection_log_.Info("Connected to outbound peer %s at %s", pid, uri)

	if _, err := py.Ensure_future(func() (any, error) { return nil, Receive_loop_(proto, ws, pid) }); err != nil {
		return "", err
	}

	return pid, nil
}

// Receive_loop_ reads messages from *ws* and dispatches requests /
// responses.
//
// This loop runs identically on both the initiator and acceptor side of
// a peered connection.
func Receive_loop_(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, ws *py.WebSocket, peer_id string) error {
	defer proto.Unregister_peer_(peer_id)
	err := ws.Iter(func(raw []byte) error {
		msg, err := Decode_msg(raw)
		if err != nil {
			return err
		}

		if Is_request(msg) {
			if py.Str(msg.GetDefault("method", nil)) == "peer.disconnect" {
				// graceful goodbye from the peer: end this connection
				return py.StopIteration.New()
			}
			// Handle off the receive loop so a blocking policy method (a
			// remember that waits on a pool) neither stalls the socket
			// nor serialises every call from this peer.
			_, err := py.Ensure_future(func() (any, error) { return nil, Handle_rpc_request_(proto, ws, msg) })
			return err
		} else if Is_response(msg) {
			Handle_rpc_response_(proto, msg)
		} else {
			connection_log_.Warning("Unrecognised message from %s: %s", peer_id, py.Repr(truncate_bytes_(raw, 200)))
		}
		return nil
	})
	if err != nil {
		if py.IsInstance(err, py.StopIteration) {
			return nil
		}
		if py.IsInstance(err, py.ConnectionClosed) {
			connection_log_.Info("Connection to peer %s closed.", peer_id)
			return nil
		}
		return err
	}
	return nil
}

// Handle_rpc_request_ executes an inbound ``rpc.call`` and sends the
// result back on *ws*.
func Handle_rpc_request_(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, ws *py.WebSocket, msg *py.Dict) error {
	request_id := msg.GetDefault("id", nil)
	method := msg.GetDefault("method", nil)

	if py.Str(method) != "rpc.call" {
		resp := Make_error(
			request_id,
			ERR_METHOD_NOT_FOUND,
			fmt.Sprintf("Unknown method: %s", py.Str(method)),
			nil,
		)
		return ws_send_encoded_(ws, resp)
	}

	params := dict_or_empty_(msg.GetDefault("params", nil))
	path := Str_list_(params.GetDefault("path", py.NewList()))
	args := py.Iter(params.GetDefault("args", py.NewList()))
	kwargs := Kw_of_(params.GetDefault("kwargs", py.NewDict()))

	// Liveness control frame: answered here, never reaches the policy.
	if Str_list_eq_(path, COMM_PING_PATH_) {
		return ws_send_encoded_(ws, Make_result(request_id, "pong"))
	}

	var resp *py.Dict
	// The policy method may block (pool reads, future waits): run it on a
	// worker thread, never on the socket's event loop.
	comm := proto.Communication()
	result, err := py.To_thread(func() (any, error) {
		if comm == nil {
			return nil, py.AttributeError.New("'NoneType' object has no attribute '_execute_rpc'")
		}
		return Execute_rpc_via_(comm, path, args, kwargs)
	})
	if err != nil {
		resp = Make_error(
			request_id,
			ERR_EXECUTION,
			fmt.Sprintf("%s: %s", py.Typename(err), err.Error()),
			nil,
		)
	} else {
		resp = Make_result(request_id, result)
	}

	if err := ws_send_encoded_(ws, resp); err != nil {
		if py.IsInstance(err, py.ConnectionClosed) {
			connection_log_.Debug("peer went away before the reply to %s could be sent", py.Str(request_id))
			return nil
		}
		return err
	}
	return nil
}

// Handle_rpc_response_ resolves a pending outbound RPC call with the
// received JSON-RPC response.
func Handle_rpc_response_(proto *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, msg *py.Dict) {
	request_id := msg.GetDefault("id", nil)
	if py.IsNone(request_id) {
		return
	}

	pending := proto.Pending_rpc_(py.Str(request_id))
	if pending == nil {
		connection_log_.Warning("Received response for unknown request %s", py.Str(request_id))
		return
	}

	if e, found := msg.Get("error"); found {
		pending.Set_error(e)
	} else {
		pending.Set_result(msg.GetDefault("result", nil))
	}

	pending.Event.Set()
}

// ---------------------------------------------------------------------------
// helpers shared with the carriers
// ---------------------------------------------------------------------------

// ws_send_encoded_ is ``await ws.send(protocol.encode(obj))``.
func ws_send_encoded_(ws *py.WebSocket, obj any) error {
	frame, err := Encode(obj)
	if err != nil {
		return err
	}
	return ws.Send(frame)
}

// dict_or_empty_ views v as a dict (``msg.get("params", {})``).
func dict_or_empty_(v any) *py.Dict {
	if d, ok := v.(*py.Dict); ok {
		return d
	}
	return py.NewDict()
}

// error_message_ is ``err.get("message", err)``.
func error_message_(e any) any {
	if d, ok := e.(*py.Dict); ok {
		return d.GetDefault("message", d)
	}
	return e
}

// Str_list_ converts a decoded JSON list into []string.
func Str_list_(v any) []string {
	items := py.Iter(v)
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, py.Str(it))
	}
	return out
}

// Str_list_eq_ is ``a == b`` for two string lists.
func Str_list_eq_(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Kw_of_ views a decoded JSON object as keyword arguments.
func Kw_of_(v any) py.Kw {
	out := py.Kw{}
	if d, ok := v.(*py.Dict); ok {
		for _, it := range d.Items() {
			out[py.Str(it.Key)] = it.Value
		}
	}
	return out
}

// truncate_bytes_ is ``raw[:n]``.
func truncate_bytes_(raw []byte, n int) []byte {
	if len(raw) > n {
		return raw[:n]
	}
	return raw
}
