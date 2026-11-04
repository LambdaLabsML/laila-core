package py

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ``websockets`` (the asyncio client/server library) for the loop-lock
// model, over github.com/coder/websocket.
//
// Surface used by laila: ``serve(handler, host, port, max_size=)``,
// ``connect(uri, max_size=, ssl=)``, ``ws.recv()``, ``ws.send(data)``,
// ``ws.close()``, ``async for raw in ws`` (``Iter``), ``ws.wait_closed()``,
// ``server.sockets[0].getsockname()``, ``server.close()`` /
// ``server.wait_closed()`` and the ``ConnectionClosed`` family.

// ConnectionClosed is websockets.ConnectionClosed (``.rcvd`` / ``.sent``
// carry the close frames as (code, reason) when known).
var ConnectionClosed = NewType("ConnectionClosed", Exception)

// ConnectionClosedOK is websockets.ConnectionClosedOK (clean 1000/1001).
var ConnectionClosedOK = NewType("ConnectionClosedOK", ConnectionClosed)

// ConnectionClosedError is websockets.ConnectionClosedError (any other
// close code, or the transport dying without a close frame).
var ConnectionClosedError = NewType("ConnectionClosedError", ConnectionClosed)

// InvalidURI is websockets.InvalidURI.
var InvalidURI = NewType("InvalidURI", Exception)

// InvalidHandshake is websockets.InvalidHandshake.
var InvalidHandshake = NewType("InvalidHandshake", Exception)

func closedError(err error) error {
	if err == nil {
		return nil
	}
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		e := ConnectionClosedError
		if ce.Code == websocket.StatusNormalClosure || ce.Code == websocket.StatusGoingAway {
			e = ConnectionClosedOK
		}
		return e.Newf("received %d (%s) %s", int(ce.Code), closeName(ce.Code), ce.Reason).
			With("code", int(ce.Code)).With("reason", ce.Reason).From(err)
	}
	return ConnectionClosedError.New("no close frame received or sent").With("code", 1006).With("reason", "").From(err)
}

func closeName(c websocket.StatusCode) string {
	switch c {
	case websocket.StatusNormalClosure:
		return "OK"
	case websocket.StatusGoingAway:
		return "going away"
	case websocket.StatusMessageTooBig:
		return "message too big"
	case websocket.StatusProtocolError:
		return "protocol error"
	}
	return "unknown"
}

// WebSocket is a ``websockets`` connection (ClientConnection or
// ServerConnection): full-duplex, every blocking method a leaf await.
type WebSocket struct {
	c        *websocket.Conn
	nc       net.Conn
	loop     *Loop
	once     sync.Once
	closed   chan struct{}
	closeErr error
	mu       sync.Mutex
	local    Addr
	remote   Addr
	Max_size int
}

func newWebSocket(c *websocket.Conn, nc net.Conn, l *Loop, max_size int) *WebSocket {
	ws := &WebSocket{c: c, nc: nc, loop: l, closed: make(chan struct{}), Max_size: max_size}
	if nc != nil {
		ws.local = Addr_of(nc.LocalAddr())
		ws.remote = Addr_of(nc.RemoteAddr())
	}
	if max_size > 0 {
		c.SetReadLimit(int64(max_size))
	} else {
		c.SetReadLimit(-1)
	}
	if l != nil {
		l.Register_closer(wsCloser{ws})
	}
	return ws
}

// wsCloser adapts WebSocket to io.Closer for Loop.Register_closer.
type wsCloser struct{ ws *WebSocket }

func (c wsCloser) Close() error { return c.ws.CloseNow() }

// Local_address is ws.local_address.
func (ws *WebSocket) Local_address() Addr { return ws.local }

// Remote_address is ws.remote_address.
func (ws *WebSocket) Remote_address() Addr { return ws.remote }

// Recv is ``await ws.recv()``: the next message as bytes (text frames
// come back as their UTF-8 bytes; ``decode`` in laila's protocol accepts
// both). ConnectionClosed when the socket is gone.
func (ws *WebSocket) Recv() ([]byte, error) {
	var (
		data []byte
		err  error
	)
	e := leafAwait(func(cancel <-chan struct{}) error {
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, data, err = ws.c.Read(ctx)
		}()
		select {
		case <-done:
			return nil
		case <-cancel:
			stop()
			<-done
			return CancelledError.New()
		}
	})
	if e != nil {
		return nil, e
	}
	if err != nil {
		ws.markClosed(err)
		return nil, closedError(err)
	}
	return data, nil
}

// Iter is ``async for raw in ws``: yields messages until the connection
// closes (a clean close ends the iteration without error, like
// ``websockets``' ``__aiter__``; an unclean one raises
// ConnectionClosedError).
func (ws *WebSocket) Iter(fn func(raw []byte) error) error {
	for {
		raw, err := ws.Recv()
		if err != nil {
			if IsInstance(err, ConnectionClosedOK) {
				return nil
			}
			return err
		}
		if err := fn(raw); err != nil {
			return err
		}
	}
}

// Send is ``await ws.send(data)``: str sends a text frame, bytes a
// binary frame.
func (ws *WebSocket) Send(data any) error {
	var (
		typ websocket.MessageType
		p   []byte
	)
	switch v := data.(type) {
	case string:
		typ, p = websocket.MessageText, []byte(v)
	case []byte:
		typ, p = websocket.MessageBinary, v
	default:
		return TypeError.Newf("data is a %s but should be a str or bytes", Pytypename(data))
	}
	if ws.Max_size > 0 && len(p) > ws.Max_size {
		return ConnectionClosedError.Newf("frame of %d bytes exceeds max_size %d", len(p), ws.Max_size).With("code", 1009).With("reason", "message too big")
	}
	var err error
	e := Blocking(func() error {
		err = ws.c.Write(context.Background(), typ, p)
		return nil
	})
	if e != nil {
		return e
	}
	if err != nil {
		ws.markClosed(err)
		return closedError(err)
	}
	return nil
}

// Ping is ``await ws.ping()``; resolves when the pong arrives.
func (ws *WebSocket) Ping() error {
	var err error
	e := Blocking(func() error {
		ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		err = ws.c.Ping(ctx)
		return nil
	})
	if e != nil {
		return e
	}
	if err != nil {
		return closedError(err)
	}
	return nil
}

func (ws *WebSocket) markClosed(err error) {
	ws.once.Do(func() {
		ws.mu.Lock()
		ws.closeErr = err
		ws.mu.Unlock()
		close(ws.closed)
		if ws.loop != nil {
			ws.loop.Unregister_closer(wsCloser{ws})
		}
	})
}

// Close is ``await ws.close(code=1000, reason="")``: performs the closing
// handshake (bounded) and releases the transport. Idempotent.
func (ws *WebSocket) Close(kw ...Kw) error {
	k := MergeKw(kw...)
	code := websocket.StatusNormalClosure
	if c, ok := k["code"]; ok {
		n, _ := Int(c)
		code = websocket.StatusCode(n)
	}
	reason, _ := k["reason"].(string)
	return Blocking(func() error {
		_ = ws.c.Close(code, reason)
		ws.markClosed(nil)
		return nil
	})
}

// CloseNow is io.Closer for loop.close(): drops the transport without a
// close handshake.
func (ws *WebSocket) CloseNow() error {
	_ = ws.c.CloseNow()
	ws.markClosed(nil)
	return nil
}

// Wait_closed is ``await ws.wait_closed()``.
func (ws *WebSocket) Wait_closed() error { return Await_chan(ws.closed) }

// Closed reports whether the connection has been torn down.
func (ws *WebSocket) Closed() bool {
	select {
	case <-ws.closed:
		return true
	default:
		return false
	}
}

// Close_code is ws.close_code (nil until closed; 1000 after a local
// close, the peer's code otherwise, 1006 when no frame was exchanged).
func (ws *WebSocket) Close_code() any {
	if !ws.Closed() {
		return nil
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closeErr == nil {
		return 1000
	}
	var ce websocket.CloseError
	if errors.As(ws.closeErr, &ce) {
		return int(ce.Code)
	}
	return 1006
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

// WebSocketServer is the object returned by ``serve``.
type WebSocketServer struct {
	ln     net.Listener
	srv    *http.Server
	loop   *Loop
	closed chan struct{}
	done   chan struct{}
	once   sync.Once
	connMu sync.Mutex
	conns  map[*WebSocket]struct{}
}

// Sockets is server.sockets.
func (s *WebSocketServer) Sockets() []*Socket { return []*Socket{{ln: s.ln}} }

// Is_serving is server.is_serving().
func (s *WebSocketServer) Is_serving() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}

// Close is server.close(): stops accepting and closes every connection.
func (s *WebSocketServer) Close() {
	s.once.Do(func() {
		close(s.closed)
		_ = s.ln.Close()
		if s.loop != nil {
			s.loop.Unregister_closer(s.ln)
		}
		s.connMu.Lock()
		conns := make([]*WebSocket, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.connMu.Unlock()
		for _, c := range conns {
			_ = c.c.Close(websocket.StatusGoingAway, "")
			c.markClosed(nil)
		}
		go func() {
			ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_ = s.srv.Shutdown(ctx)
		}()
	})
}

// Wait_closed is ``await server.wait_closed()``.
func (s *WebSocketServer) Wait_closed() error {
	s.Close()
	return Await_chan(s.done)
}

// Serve is ``await websockets.serve(handler, host, port, max_size=, ssl=)``.
// handler runs as a task on the current loop for every accepted
// connection; the connection is closed when the handler returns.
func Serve(handler func(ws *WebSocket) error, host string, port int, kw ...Kw) (*WebSocketServer, error) {
	k := MergeKw(kw...)
	max_size := 1 << 20
	if v, ok := k["max_size"]; ok {
		if v == nil {
			max_size = 0
		} else {
			max_size, _ = Int(v)
		}
	}
	var tlsCfg *tls.Config
	if c, ok := k["ssl"].(*tls.Config); ok {
		tlsCfg = c
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var (
		ln  net.Listener
		err error
	)
	if e := Blocking(func() error { ln, err = net.Listen("tcp", addr); return nil }); e != nil {
		return nil, e
	}
	if err != nil {
		return nil, FromGo(err)
	}
	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}
	l := Current_loop()
	s := &WebSocketServer{ln: ln, loop: l, closed: make(chan struct{}), done: make(chan struct{}), conns: map[*WebSocket]struct{}{}}
	if l != nil {
		l.Register_closer(ln)
	}
	type connKey struct{}
	s.srv = &http.Server{
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, c)
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, CompressionMode: websocket.CompressionDisabled})
			if err != nil {
				return
			}
			nc, _ := r.Context().Value(connKey{}).(net.Conn)
			ws := newWebSocket(c, nc, l, max_size)
			s.connMu.Lock()
			s.conns[ws] = struct{}{}
			s.connMu.Unlock()
			run := func() (any, error) {
				defer func() {
					_ = c.Close(websocket.StatusNormalClosure, "")
					ws.markClosed(nil)
					s.connMu.Lock()
					delete(s.conns, ws)
					s.connMu.Unlock()
				}()
				return nil, handler(ws)
			}
			var t *Task
			if l != nil {
				t = l.Create_task(run)
			} else {
				go func() { _, _ = run() }()
			}
			// Hijacked connection: keep this goroutine alive until the
			// handler is done so net/http does not reclaim anything early.
			if t != nil {
				<-t.Chan()
			} else {
				<-ws.closed
			}
		}),
	}
	go func() {
		defer close(s.done)
		_ = s.srv.Serve(ln)
	}()
	return s, nil
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

// Connect is ``await websockets.connect(uri, max_size=, ssl=,
// open_timeout=)``. OSError when the TCP connect fails, InvalidHandshake
// when the server rejects the upgrade.
func Connect(uri string, kw ...Kw) (*WebSocket, error) {
	k := MergeKw(kw...)
	max_size := 1 << 20
	if v, ok := k["max_size"]; ok {
		if v == nil {
			max_size = 0
		} else {
			max_size, _ = Int(v)
		}
	}
	open_timeout := 10.0
	if v, ok := k["open_timeout"]; ok && v != nil {
		open_timeout, _ = Float(v)
	}
	var tlsCfg *tls.Config
	if c, ok := k["ssl"].(*tls.Config); ok {
		tlsCfg = c
	}
	var (
		c    *websocket.Conn
		nc   net.Conn
		err  error
		resp *http.Response
	)
	e := Blocking(func() error {
		d, _ := Duration(open_timeout)
		ctx, stop := context.WithTimeout(context.Background(), d)
		defer stop()
		var mu sync.Mutex
		tr := &http.Transport{
			TLSClientConfig: tlsCfg,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err == nil {
					mu.Lock()
					nc = conn
					mu.Unlock()
				}
				return conn, err
			},
		}
		c, resp, err = websocket.Dial(ctx, uri, &websocket.DialOptions{
			HTTPClient:      &http.Client{Transport: tr},
			CompressionMode: websocket.CompressionDisabled,
		})
		return nil
	})
	if e != nil {
		return nil, e
	}
	if err != nil {
		if resp != nil {
			return nil, InvalidHandshake.Newf("server rejected WebSocket connection: HTTP %d", resp.StatusCode).From(err)
		}
		var ne net.Error
		if errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded) {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, TimeoutError.New("timed out during opening handshake").From(err)
			}
			return nil, OSError.Newf("%v", err).From(err)
		}
		return nil, FromGo(err)
	}
	return newWebSocket(c, nc, Current_loop(), max_size), nil
}
