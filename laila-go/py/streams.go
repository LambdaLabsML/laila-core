package py

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// asyncio streams / sockets for the loop-lock model.
//
// ``asyncio.StreamReader`` / ``StreamWriter``, ``start_server`` /
// ``open_connection`` (TCP, TLS, Unix domain) and
// ``create_datagram_endpoint`` on top of ``net``. Every blocking point is
// a leaf await, so a coroutine reading a frame releases the loop lock
// exactly like CPython's reader does at ``await reader.readexactly``.
//
// Each accepted / opened connection gets one reader goroutine pumping
// ``net.Conn`` bytes into the StreamReader (the selector callback in
// CPython) and one writer goroutine flushing the StreamWriter's buffer
// (the transport's write side); ``write()`` is therefore non-blocking and
// ``await drain()`` honours the write-buffer high-water mark.

// IncompleteReadError is asyncio.IncompleteReadError.
var IncompleteReadError = NewType("IncompleteReadError", EOFError)

// Addr is a socket address tuple ``(host, port)`` -- comparable, so it can
// key dicts and be compared with ``==`` like the Python tuple.
type Addr struct {
	Host string
	Port int
}

// Repr is ``('127.0.0.1', 1234)``.
func (a Addr) Repr() string { return fmt.Sprintf("(%s, %d)", Repr_str(a.Host), a.Port) }

// String is the ``host:port`` form net.Dial accepts.
func (a Addr) String() string { return net.JoinHostPort(a.Host, strconv.Itoa(a.Port)) }

// Addr_of converts a net.Addr to an Addr (Unix sockets keep the path in
// Host with Port 0).
func Addr_of(a net.Addr) Addr {
	switch x := a.(type) {
	case *net.TCPAddr:
		return Addr{Host: x.IP.String(), Port: x.Port}
	case *net.UDPAddr:
		return Addr{Host: x.IP.String(), Port: x.Port}
	case *net.UnixAddr:
		return Addr{Host: x.Name}
	}
	if a == nil {
		return Addr{}
	}
	h, p, err := net.SplitHostPort(a.String())
	if err != nil {
		return Addr{Host: a.String()}
	}
	port, _ := strconv.Atoi(p)
	return Addr{Host: h, Port: port}
}

// ---------------------------------------------------------------------------
// StreamReader
// ---------------------------------------------------------------------------

// StreamReader is asyncio.StreamReader: a byte buffer fed by the transport
// and drained by leaf-awaiting coroutines.
//
// Flow control mirrors asyncio: the transport pump (Wrap_conn /
// Stream_pair) pauses reading once more than ``2 * limit`` bytes are
// buffered and resumes when the consumer has drained it to ``limit``
// (``_maybe_pause_transport`` / ``_maybe_resume_transport``). Without it a
// slow consumer would make the buffer grow without bound and inbound
// control traffic would queue behind megabytes of bulk data.
type StreamReader struct {
	mu     sync.Mutex
	buf    []byte
	eof    bool
	err    error
	notify chan struct{}
	limit  int
	paused bool // asyncio ``_paused``: the transport pump is blocked in Wait_resume_
	resume chan struct{}
}

// DEFAULT_STREAM_LIMIT is asyncio's ``_DEFAULT_LIMIT`` (64 KiB).
const DEFAULT_STREAM_LIMIT = 64 * 1024

// New_StreamReader is asyncio.StreamReader(limit=_DEFAULT_LIMIT).
func New_StreamReader() *StreamReader {
	return &StreamReader{notify: make(chan struct{}), limit: DEFAULT_STREAM_LIMIT, resume: make(chan struct{})}
}

// Set_limit is the ``limit=`` constructor argument of
// asyncio.StreamReader: the high-water mark used for flow control
// (pause at ``2 * limit``, resume at ``limit``). Call it before the
// transport starts feeding data.
func (r *StreamReader) Set_limit(limit int) {
	r.mu.Lock()
	r.limit = max(1, limit)
	r.mu.Unlock()
}

// Limit is ``reader._limit``.
func (r *StreamReader) Limit() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limit
}

func (r *StreamReader) wake() {
	close(r.notify)
	r.notify = make(chan struct{})
}

// consumed_ is ``_maybe_resume_transport`` (called under mu after bytes
// leave the buffer): wakes a paused transport pump once the buffer is
// back under ``limit``.
func (r *StreamReader) consumed_() {
	if r.paused && len(r.buf) <= r.limit {
		r.paused = false
		r.wake_resume_()
	}
}

// Wait_resume_ blocks the transport pump while the reader is paused
// (asyncio ``pause_reading`` -- set by Feed_data once more than
// ``2 * limit`` bytes are buffered); returns at once otherwise or once
// the reader hit EOF / an exception.
func (r *StreamReader) Wait_resume_() {
	for {
		r.mu.Lock()
		if !r.paused || r.eof || r.err != nil {
			r.mu.Unlock()
			return
		}
		ch := r.resume
		r.mu.Unlock()
		<-ch
	}
}

// Feed_data is reader.feed_data(data), including ``_maybe_pause_transport``.
func (r *StreamReader) Feed_data(data []byte) {
	r.mu.Lock()
	r.buf = append(r.buf, data...)
	r.wake()
	if !r.paused && len(r.buf) > 2*r.limit {
		r.paused = true
	}
	r.mu.Unlock()
}

// Feed_eof is reader.feed_eof().
func (r *StreamReader) Feed_eof() {
	r.mu.Lock()
	r.eof = true
	r.wake()
	r.wake_resume_()
	r.mu.Unlock()
}

func (r *StreamReader) wake_resume_() {
	close(r.resume)
	r.resume = make(chan struct{})
}

// Set_exception is reader.set_exception(exc).
func (r *StreamReader) Set_exception(err error) {
	r.mu.Lock()
	r.err = err
	r.wake()
	r.wake_resume_()
	r.mu.Unlock()
}

// At_eof is reader.at_eof().
func (r *StreamReader) At_eof() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.eof && len(r.buf) == 0
}

// waitData blocks (leaf await) until the buffer holds at least n bytes,
// EOF or an exception (asyncio ``_wait_for_data``).
func (r *StreamReader) waitData(n int) error {
	for {
		r.mu.Lock()
		if len(r.buf) >= n || r.eof || r.err != nil {
			r.mu.Unlock()
			return nil
		}
		// Waiting for data while paused would deadlock -- essential for
		// readexactly(n) when n > limit (asyncio resumes the transport
		// here too).
		if r.paused {
			r.paused = false
			r.wake_resume_()
		}
		ch := r.notify
		r.mu.Unlock()
		if err := Await_chan(ch); err != nil {
			return err
		}
	}
}

// Readexactly is ``await reader.readexactly(n)``: IncompleteReadError when
// EOF arrives first (``.partial`` carried as the first arg).
func (r *StreamReader) Readexactly(n int) ([]byte, error) {
	if n < 0 {
		return nil, ValueError.New("readexactly size can not be less than zero")
	}
	if err := r.waitData(n); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil && len(r.buf) < n {
		return nil, r.err
	}
	if len(r.buf) < n {
		partial := append([]byte(nil), r.buf...)
		r.buf = nil
		r.consumed_()
		return nil, IncompleteReadError.Newf("%d bytes read on a total of %d expected bytes", len(partial), n)
	}
	out := Bytes_clone(r.buf[:n])
	r.buf = r.buf[n:]
	r.consumed_()
	return out, nil
}

// Read is ``await reader.read(n)``: up to n bytes (n < 0: until EOF); an
// empty slice means EOF.
func (r *StreamReader) Read(n int) ([]byte, error) {
	if n < 0 {
		for {
			r.mu.Lock()
			if r.eof || r.err != nil {
				out := r.buf
				r.buf = nil
				r.consumed_()
				err := r.err
				r.mu.Unlock()
				if err != nil && len(out) == 0 {
					return nil, err
				}
				return out, nil
			}
			ch := r.notify
			r.mu.Unlock()
			if err := Await_chan(ch); err != nil {
				return nil, err
			}
		}
	}
	if err := r.waitData(1); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		if r.err != nil {
			return nil, r.err
		}
		return []byte{}, nil
	}
	if n > len(r.buf) {
		n = len(r.buf)
	}
	out := Bytes_clone(r.buf[:n])
	r.buf = r.buf[n:]
	r.consumed_()
	return out, nil
}

// ---------------------------------------------------------------------------
// StreamWriter
// ---------------------------------------------------------------------------

// Transport is the write side's ``writer.transport``: carries the
// write-buffer limits and the socket extras.
type Transport struct {
	w      *StreamWriter
	high   int
	low    int
	extras map[string]any
}

// Set_write_buffer_limits is transport.set_write_buffer_limits(high, low).
func (t *Transport) Set_write_buffer_limits(high, low int) {
	t.w.mu.Lock()
	t.high, t.low = high, low
	t.w.mu.Unlock()
}

// Get_write_buffer_size is transport.get_write_buffer_size().
func (t *Transport) Get_write_buffer_size() int {
	t.w.mu.Lock()
	defer t.w.mu.Unlock()
	return len(t.w.buf)
}

// Get_extra_info is transport.get_extra_info(name).
func (t *Transport) Get_extra_info(name string) any { return t.extras[name] }

// Is_closing is transport.is_closing().
func (t *Transport) Is_closing() bool { return t.w.Is_closing() }

// StreamWriter is asyncio.StreamWriter over an io.WriteCloser (net.Conn,
// *os.File, a pipe...). ``Write`` buffers; a writer goroutine flushes;
// ``Drain`` leaf-awaits until the buffer is below the high-water mark.
type StreamWriter struct {
	mu        sync.Mutex
	buf       []byte
	err       error
	closing   bool
	closed    chan struct{}
	kick      chan struct{}
	notify    chan struct{}
	dst       io.WriteCloser
	closeDst  func()
	transport *Transport
	reader    *StreamReader
}

// New_StreamWriter wraps dst. reader (optional) is the paired reader
// (``writer.transport`` closes both, like asyncio's duplex transport).
// extras are the ``get_extra_info`` keys (``socket``, ``peername``,
// ``sockname``, ...).
func New_StreamWriter(dst io.WriteCloser, reader *StreamReader, extras map[string]any) *StreamWriter {
	w := &StreamWriter{
		dst:    dst,
		closed: make(chan struct{}),
		kick:   make(chan struct{}, 1),
		notify: make(chan struct{}),
		reader: reader,
	}
	if extras == nil {
		extras = map[string]any{}
	}
	w.transport = &Transport{w: w, high: 64 * 1024, low: 16 * 1024, extras: extras}
	go w.pump()
	return w
}

func (w *StreamWriter) wake() {
	close(w.notify)
	w.notify = make(chan struct{})
}

func (w *StreamWriter) pump() {
	defer close(w.closed)
	for {
		w.mu.Lock()
		for len(w.buf) == 0 {
			if w.closing || w.err != nil {
				w.mu.Unlock()
				w.finishClose()
				return
			}
			w.mu.Unlock()
			<-w.kick
			w.mu.Lock()
		}
		chunk := w.buf
		w.buf = nil
		w.mu.Unlock()
		_, err := w.dst.Write(chunk)
		w.mu.Lock()
		if err != nil {
			w.err = err
		}
		w.wake()
		w.mu.Unlock()
		if err != nil {
			w.finishClose()
			return
		}
	}
}

func (w *StreamWriter) finishClose() {
	if w.closeDst != nil {
		w.closeDst()
	} else {
		_ = w.dst.Close()
	}
}

// Transport is writer.transport.
func (w *StreamWriter) Transport() *Transport { return w.transport }

// Get_extra_info is writer.get_extra_info(name).
func (w *StreamWriter) Get_extra_info(name string) any { return w.transport.extras[name] }

// Write is writer.write(data): buffers without blocking (a write after
// close is dropped, as asyncio logs and ignores it).
func (w *StreamWriter) Write(data []byte) {
	w.mu.Lock()
	if w.closing || w.err != nil {
		w.mu.Unlock()
		return
	}
	w.buf = append(w.buf, data...)
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// Drain is ``await writer.drain()``: waits until the buffered bytes fall
// below the high-water mark; raises the transport error, if any.
func (w *StreamWriter) Drain() error {
	for {
		w.mu.Lock()
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return ConnectionError.New(err.Error()).From(err)
		}
		if len(w.buf) <= w.transport.high || w.closing {
			w.mu.Unlock()
			return nil
		}
		ch := w.notify
		w.mu.Unlock()
		if err := Await_chan(ch); err != nil {
			return err
		}
	}
}

// Close is writer.close(): flushes what is buffered, then closes the
// underlying stream (and feeds EOF to the paired reader).
func (w *StreamWriter) Close() {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return
	}
	w.closing = true
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
	if w.reader != nil {
		w.reader.Feed_eof()
	}
}

// Is_closing is writer.is_closing().
func (w *StreamWriter) Is_closing() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closing || w.err != nil
}

// Wait_closed is ``await writer.wait_closed()``.
func (w *StreamWriter) Wait_closed() error { return Await_chan(w.closed) }

// Closed reports whether the underlying stream has been closed.
func (w *StreamWriter) Closed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}

// Abort is transport.abort(): close without flushing.
func (w *StreamWriter) Abort() {
	w.mu.Lock()
	w.buf = nil
	w.mu.Unlock()
	w.Close()
}

// ---------------------------------------------------------------------------
// connection plumbing
// ---------------------------------------------------------------------------

// Socket is the ``get_extra_info("socket")`` object: the raw connection.
type Socket struct {
	Conn net.Conn
	ln   net.Listener
}

// Getsockname is socket.getsockname().
func (s *Socket) Getsockname() Addr {
	if s.ln != nil {
		return Addr_of(s.ln.Addr())
	}
	return Addr_of(s.Conn.LocalAddr())
}

// Getpeername is socket.getpeername().
func (s *Socket) Getpeername() Addr {
	if s.Conn == nil {
		return Addr{}
	}
	return Addr_of(s.Conn.RemoteAddr())
}

// Setsockopt_nodelay is ``sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)``.
func (s *Socket) Setsockopt_nodelay(on bool) error {
	if tc, ok := s.Conn.(*net.TCPConn); ok {
		return tc.SetNoDelay(on)
	}
	if tc, ok := s.Conn.(*tls.Conn); ok {
		if inner, ok := tc.NetConn().(*net.TCPConn); ok {
			return inner.SetNoDelay(on)
		}
	}
	return OSError.New("not a TCP socket")
}

// Wrap_conn turns a connected net.Conn into an asyncio stream pair with a
// reader goroutine pumping inbound bytes. The optional loop registers the
// connection so ``loop.close()`` tears it down.
func Wrap_conn(conn net.Conn, l *Loop) (*StreamReader, *StreamWriter) {
	reader := New_StreamReader()
	sock := &Socket{Conn: conn}
	extras := map[string]any{
		"socket":   sock,
		"peername": Addr_of(conn.RemoteAddr()),
		"sockname": Addr_of(conn.LocalAddr()),
	}
	writer := New_StreamWriter(conn, reader, extras)
	if l != nil {
		l.Register_closer(conn)
		writer.closeDst = func() {
			_ = conn.Close()
			l.Unregister_closer(conn)
		}
	}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				reader.Feed_data(buf[:n])
				reader.Wait_resume_() // asyncio pause_reading / resume_reading
			}
			if err != nil {
				reader.Feed_eof()
				return
			}
		}
	}()
	return reader, writer
}

// Stream_pair wraps an arbitrary (reader, writer) pair -- a serial port, a
// pty, a pipe -- as asyncio streams (``connect_read_pipe`` +
// ``connect_write_pipe``). closeFn (optional) runs when the writer closes.
//
// ``limit`` is the optional ``asyncio.StreamReader(limit=...)`` high-water
// mark (default 64 KiB).
func Stream_pair(r io.Reader, w io.WriteCloser, closeFn func(), limit ...int) (*StreamReader, *StreamWriter) {
	reader := New_StreamReader()
	if len(limit) > 0 {
		reader.Set_limit(limit[0])
	}
	writer := New_StreamWriter(w, reader, nil)
	if closeFn != nil {
		writer.closeDst = closeFn
	}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				reader.Feed_data(buf[:n])
				reader.Wait_resume_() // asyncio pause_reading / resume_reading
			}
			if err != nil {
				reader.Feed_eof()
				return
			}
		}
	}()
	return reader, writer
}

// Server is the object returned by asyncio.start_server.
type Server struct {
	ln      net.Listener
	loop    *Loop
	closed  chan struct{}
	done    chan struct{}
	once    sync.Once
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
	path    string
}

// Sockets is server.sockets.
func (s *Server) Sockets() []*Socket { return []*Socket{{ln: s.ln}} }

// Is_serving is server.is_serving().
func (s *Server) Is_serving() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}

// Close is server.close(): stops accepting and closes active connections.
func (s *Server) Close() {
	s.once.Do(func() {
		close(s.closed)
		_ = s.ln.Close()
		if s.loop != nil {
			s.loop.Unregister_closer(s.ln)
		}
		s.connsMu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.connsMu.Unlock()
	})
}

// Wait_closed is ``await server.wait_closed()``.
func (s *Server) Wait_closed() error {
	s.Close()
	return Await_chan(s.done)
}

func serve(ln net.Listener, l *Loop, cb func(*StreamReader, *StreamWriter) error, tlsCfg *tls.Config, path string) *Server {
	s := &Server{ln: ln, loop: l, closed: make(chan struct{}), done: make(chan struct{}), conns: map[net.Conn]struct{}{}, path: path}
	if l != nil {
		l.Register_closer(ln)
	}
	go func() {
		defer close(s.done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.closed:
				default:
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						continue
					}
				}
				return
			}
			if tlsCfg != nil {
				conn = tls.Server(conn, tlsCfg)
			}
			s.connsMu.Lock()
			s.conns[conn] = struct{}{}
			s.connsMu.Unlock()
			reader, writer := Wrap_conn(conn, l)
			prev := writer.closeDst
			writer.closeDst = func() {
				if prev != nil {
					prev()
				} else {
					_ = conn.Close()
				}
				s.connsMu.Lock()
				delete(s.conns, conn)
				s.connsMu.Unlock()
			}
			handler := func() (any, error) {
				defer writer.Close()
				return nil, cb(reader, writer)
			}
			if l != nil {
				l.Create_task(handler)
			} else {
				go func() { _, _ = handler() }()
			}
		}
	}()
	return s
}

// Start_server is ``await asyncio.start_server(cb, host, port, ssl=...)``.
// Returns the Server; ``server.sockets[0].getsockname()`` has the bound
// port. kw: ``ssl`` (*tls.Config), ``reuse_address`` (ignored).
func Start_server(cb func(*StreamReader, *StreamWriter) error, host string, port int, kw ...Kw) (*Server, error) {
	k := MergeKw(kw...)
	var tlsCfg *tls.Config
	if c, ok := k["ssl"].(*tls.Config); ok {
		tlsCfg = c
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var (
		ln  net.Listener
		err error
	)
	e := Blocking(func() error {
		ln, err = net.Listen("tcp", addr)
		return nil
	})
	if e != nil {
		return nil, e
	}
	if err != nil {
		return nil, FromGo(err)
	}
	return serve(ln, Current_loop(), cb, tlsCfg, ""), nil
}

// Start_unix_server is ``await asyncio.start_unix_server(cb, path=...)``.
func Start_unix_server(cb func(*StreamReader, *StreamWriter) error, path string) (*Server, error) {
	var (
		ln  net.Listener
		err error
	)
	e := Blocking(func() error {
		ln, err = net.Listen("unix", path)
		return nil
	})
	if e != nil {
		return nil, e
	}
	if err != nil {
		return nil, FromGo(err)
	}
	if ul, ok := ln.(*net.UnixListener); ok {
		// asyncio leaves the socket file for the caller to manage.
		ul.SetUnlinkOnClose(false)
	}
	return serve(ln, Current_loop(), cb, nil, path), nil
}

// Open_connection is ``await asyncio.open_connection(host, port, ssl=...,
// server_hostname=...)``. The dial runs as a leaf await.
func Open_connection(host string, port int, kw ...Kw) (*StreamReader, *StreamWriter, error) {
	k := MergeKw(kw...)
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var (
		conn net.Conn
		err  error
	)
	timeout := 30.0
	if t, ok := k["timeout"].(float64); ok {
		timeout = t
	}
	e := Blocking(func() error {
		d, _ := Duration(timeout)
		conn, err = net.DialTimeout("tcp", addr, d)
		if err != nil {
			return nil
		}
		if cfg, ok := k["ssl"].(*tls.Config); ok && cfg != nil {
			cfg = cfg.Clone()
			if sh, ok := k["server_hostname"].(string); ok && sh != "" {
				cfg.ServerName = sh
			} else if cfg.ServerName == "" {
				cfg.ServerName = host
			}
			tc := tls.Client(conn, cfg)
			if herr := tc.Handshake(); herr != nil {
				_ = conn.Close()
				err = herr
				return nil
			}
			conn = tc
		}
		return nil
	})
	if e != nil {
		return nil, nil, e
	}
	if err != nil {
		return nil, nil, FromGo(err)
	}
	r, w := Wrap_conn(conn, Current_loop())
	return r, w, nil
}

// Open_unix_connection is ``await asyncio.open_unix_connection(path)``.
func Open_unix_connection(path string) (*StreamReader, *StreamWriter, error) {
	var (
		conn net.Conn
		err  error
	)
	e := Blocking(func() error {
		conn, err = net.DialTimeout("unix", path, 30*time.Second)
		return nil
	})
	if e != nil {
		return nil, nil, e
	}
	if err != nil {
		return nil, nil, FromGo(err)
	}
	r, w := Wrap_conn(conn, Current_loop())
	return r, w, nil
}

// ---------------------------------------------------------------------------
// datagrams
// ---------------------------------------------------------------------------

// DatagramProtocol is asyncio.DatagramProtocol.
type DatagramProtocol interface {
	Connection_made(transport *DatagramTransport)
	Datagram_received(data []byte, addr Addr)
	Error_received(err error)
}

// DatagramTransport is asyncio.DatagramTransport over a UDP socket.
type DatagramTransport struct {
	conn   *net.UDPConn
	loop   *Loop
	once   sync.Once
	closed chan struct{}
	extras map[string]any
}

// Sendto is transport.sendto(data, addr).
func (t *DatagramTransport) Sendto(data []byte, addr Addr) error {
	ua, err := net.ResolveUDPAddr("udp", addr.String())
	if err != nil {
		return FromGo(err)
	}
	if _, err := t.conn.WriteToUDP(data, ua); err != nil {
		return FromGo(err)
	}
	return nil
}

// Close is transport.close().
func (t *DatagramTransport) Close() {
	t.once.Do(func() {
		close(t.closed)
		_ = t.conn.Close()
		if t.loop != nil {
			t.loop.Unregister_closer(t.conn)
		}
	})
}

// Is_closing is transport.is_closing().
func (t *DatagramTransport) Is_closing() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}

// Get_extra_info is transport.get_extra_info(name) (``sockname``).
func (t *DatagramTransport) Get_extra_info(name string) any { return t.extras[name] }

// Create_datagram_endpoint is ``await loop.create_datagram_endpoint(factory,
// local_addr=(host, port))``: binds the UDP socket and pumps datagrams to
// ``protocol.datagram_received`` from a reader goroutine (holding the loop
// lock, as the selector callback would).
func Create_datagram_endpoint(factory func() DatagramProtocol, local_addr Addr) (*DatagramTransport, DatagramProtocol, error) {
	var (
		conn *net.UDPConn
		err  error
	)
	e := Blocking(func() error {
		var ua *net.UDPAddr
		ua, err = net.ResolveUDPAddr("udp", local_addr.String())
		if err != nil {
			return nil
		}
		conn, err = net.ListenUDP("udp", ua)
		return nil
	})
	if e != nil {
		return nil, nil, e
	}
	if err != nil {
		return nil, nil, FromGo(err)
	}
	l := Current_loop()
	t := &DatagramTransport{conn: conn, loop: l, closed: make(chan struct{}), extras: map[string]any{"sockname": Addr_of(conn.LocalAddr())}}
	if l != nil {
		l.Register_closer(conn)
	}
	proto := factory()
	proto.Connection_made(t)
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, rerr := conn.ReadFromUDP(buf)
			if rerr != nil {
				select {
				case <-t.closed:
					return
				default:
				}
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					continue
				}
				if l != nil && !l.Is_closed() {
					_, _ = l.Call_soon_threadsafe(func() { proto.Error_received(FromGo(rerr)) })
				}
				return
			}
			data := Bytes_clone(buf[:n])
			addr := Addr_of(from)
			if l != nil && !l.Is_closed() {
				_, _ = l.Call_soon_threadsafe(func() { proto.Datagram_received(data, addr) })
			} else {
				proto.Datagram_received(data, addr)
			}
		}
	}()
	return t, proto, nil
}

// Set_blocking is os.set_blocking(fd, flag) for an *os.File (no-op; Go
// files are already driven by goroutines).
func Set_blocking(f *os.File, flag bool) error { return nil }
