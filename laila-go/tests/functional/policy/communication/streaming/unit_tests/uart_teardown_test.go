// UART teardown: no ``Fatal read error on pipe transport`` / EBADF on shutdown.
//
// Regression tests for the serial carrier's close order. The read side of
// ``DefaultUARTProtocol`` is a reader pump registered on the *original*
// serial fd. If the device is closed while the pump is still registered,
// the pump wakes on a dead fd and the loop's default exception handler
// reports ``OSError: [Errno 9] Bad file descriptor`` on every clean
// shutdown.
//
// Everything here runs hardware-free on an ``openpty()`` pair with the
// scripted fake peers from helpers.go. Each test installs a custom
// exception handler on the carrier's loop *and* captures the ``asyncio``
// logger at ``ERROR``, then asserts nothing was reported. Where the
// platform would not surface the EBADF itself, the carrier's read
// registration is checked directly: after ``stop()`` the serial fd must
// not be registered (``_read_transport is None``).
//
// Port of tests/functional/policy/communication/streaming/unit_tests/test_uart_teardown.py.
package unit_tests

import (
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// errorProbeHandler_ is the ``_H(logging.Handler)`` capturing ERROR
// records on the ``asyncio`` logger.
type errorProbeHandler_ struct {
	py.BaseHandler
	probe *asyncioErrorProbe_
}

func (h *errorProbeHandler_) Handle(r *py.LogRecord) {
	if r.Levelno < h.Level() {
		return
	}
	h.probe.mu.Lock()
	h.probe.records = append(h.probe.records, r)
	h.probe.mu.Unlock()
}

// asyncioErrorProbe_ is ``_AsyncioErrorProbe``: capture everything asyncio
// would report during a teardown.
//
//   - a custom exception handler on *loop* (replaces the default one,
//     which is what prints ``Fatal read error on pipe transport``);
//   - a logging handler at ``ERROR`` on the ``asyncio`` logger, in case
//     anything bypasses the loop handler.
type asyncioErrorProbe_ struct {
	loop     *py.Loop
	mu       sync.Mutex
	contexts []*py.Dict
	records  []*py.LogRecord
	handler_ *errorProbeHandler_
}

func new_asyncioErrorProbe_(loop *py.Loop) *asyncioErrorProbe_ {
	p := &asyncioErrorProbe_{loop: loop}
	p.handler_ = &errorProbeHandler_{probe: p}
	p.handler_.Set_level(py.ERROR)
	return p
}

// Enter is ``__enter__``.
func (p *asyncioErrorProbe_) Enter(t *testing.T) *asyncioErrorProbe_ {
	t.Helper()
	installed := py.NewEvent()
	_, err := p.loop.Call_soon_threadsafe(func() {
		p.loop.Set_exception_handler(func(ctx *py.Dict) {
			p.mu.Lock()
			p.contexts = append(p.contexts, ctx)
			p.mu.Unlock()
		})
		installed.Set()
	})
	must(0, err)
	assertTrue(t, installed.Wait(5.0), "could not install loop exception handler")
	py.Get_logger("asyncio").Add_handler(p.handler_)
	return p
}

// Exit is ``__exit__``.
func (p *asyncioErrorProbe_) Exit() {
	py.Get_logger("asyncio").Remove_handler(p.handler_)
}

// Report is ``report()``.
func (p *asyncioErrorProbe_) Report() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []string{}
	for _, c := range p.contexts {
		out = append(out, fmt.Sprintf("%s: %s", py.Str(c.GetDefault("message", nil)), py.Repr(c.GetDefault("exception", nil))))
	}
	for _, r := range p.records {
		out = append(out, r.Get_message())
	}
	return out
}

// selector_registered_ is the Go analogue of ``fd in _selector_fds(loop)``:
// the carrier keeps the serial read pump in ``_read_transport`` while it
// is registered.
func selector_registered_(proto *laila.LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_) bool {
	return proto.Read_transport_ != nil
}

// assert_released_ is ``_assert_released``: the serial fd is closed,
// unregistered, and the loop thread is gone.
func assert_released_(t *testing.T, proto *laila.LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_, fd int) {
	t.Helper()
	assertTrue(t, !selector_registered_(proto), "serial fd still registered with the selector")
	var st syscall.Stat_t
	assertTrue(t, syscall.Fstat(fd, &st) != nil, "serial fd still open (fstat succeeded)")
	assertTrue(t, proto.Loop_state_().Loop_thread_ == nil, "_loop_thread is not None")
	assertTrue(t, proto.Serial_ == nil, "_serial is not None")
	assertTrue(t, proto.Read_transport_ == nil, "_read_transport is not None")
	assertTrue(t, !proto.Started_, "_started is still True")
}

func enter_uart_(t *testing.T, up *UartPtyPair) *UartPtyPair {
	t.Helper()
	if _, err := up.Enter(); err != nil {
		up.Exit()
		t.Fatal(err)
	}
	t.Cleanup(up.Exit)
	return up
}

func assertNoReport(t *testing.T, probe *asyncioErrorProbe_) {
	t.Helper()
	if rep := probe.Report(); len(rep) != 0 {
		t.Fatalf("asyncio reported errors during teardown: %v", rep)
	}
}

// ----------------------------------------------------------------------
// TestUartTeardownIsSilent -- 1 + 2: no asyncio error on any teardown
// path; the fd is really released.
// ----------------------------------------------------------------------

func uart_teardown_run_(t *testing.T, teardown func(up *UartPtyPair)) {
	t.Helper()
	up := enter_uart_(t, New_UartPtyPair(Rpc_only_peer_))
	gid := up.Peer_gid
	assertTrue(t, up.Proto.Ping(gid), "ping")
	fd := up.Proto.Serial_.Fileno()
	loop := up.Proto.Loop()
	assertTrue(t, selector_registered_(up.Proto), "test precondition: read side registered")
	probe := new_asyncioErrorProbe_(loop).Enter(t)
	func() {
		defer probe.Exit()
		teardown(up)
		sleep(0.2) // give a stray _read_ready a chance to fire
	}()
	assertNoReport(t, probe)
	assert_released_(t, up.Proto, fd)
	assertNoErrors(t, up.Rpc_peer())
}

func TestUartTeardownIsSilent(t *testing.T) {
	t.Run("communication_stop", func(t *testing.T) {
		uart_teardown_run_(t, func(up *UartPtyPair) { must(0, up.Comm_().Stop()) })
	})

	t.Run("laila_terminate", func(t *testing.T) {
		original := must(laila.Get_active_policy())
		t.Cleanup(func() { laila.Activate_policy(original) })
		uart_teardown_run_(t, func(up *UartPtyPair) {
			laila.Activate_policy(up.Policy)
			errs := laila.Terminate()
			assertTrue(t, len(errs) == 0, fmt.Sprintf("terminate errors: %v", errs))
			assertTrue(t, !laila.Local_policies_.Contains(up.Policy.Global_id()), "policy still in _local_policies")
		})
	})

	t.Run("remove_connection", func(t *testing.T) {
		uart_teardown_run_(t, func(up *UartPtyPair) {
			must(0, up.Comm_().Remove_connection(up.Proto))
			assertTrue(t, !up.Comm_().Connections.Contains(up.Proto.Global_id()), "protocol still in connections")
			// stop() still runs on the (now removed) protocol: idempotent
			must(0, up.Proto.Stop())
		})
	})
}

// ----------------------------------------------------------------------
// TestUartRestart -- 3: ``start()`` after ``stop()`` on the same instance
// peers again.
// ----------------------------------------------------------------------

func TestUartRestart(t *testing.T) {
	up := enter_uart_(t, New_UartPtyPair(Rpc_only_peer_))
	gid := up.Peer_gid
	assertTrue(t, up.Proto.Ping(gid), "ping")
	fd1 := up.Proto.Serial_.Fileno()
	loop1 := up.Proto.Loop()
	probe := new_asyncioErrorProbe_(loop1).Enter(t)
	func() {
		defer probe.Exit()
		must(0, up.Comm_().Stop())
	}()
	assertNoReport(t, probe)
	assert_released_(t, up.Proto, fd1)
	assertTrue(t, !up.Comm_().Peers.Contains(gid), "peer still registered after stop")

	// second lifecycle on the very same protocol instance
	must(0, up.Proto.Start())
	assertTrue(t, up.Proto.Started_, "_started")
	assertTrue(t, up.Proto.Read_transport_ != nil, "_read_transport is None")
	assertTrue(t, up.Proto.Loop_state_().Loop_thread_ != nil, "_loop_thread is None")
	fd2 := up.Proto.Serial_.Fileno()
	loop2 := up.Proto.Loop()
	assertTrue(t, selector_registered_(up.Proto), "read side registered after restart")
	gid2 := must(up.Comm_().Add_peer("serial://"+up.Proto.Port, up.Proto.Peer_secret_key))
	assertEq(t, gid2, gid)
	assertEq(t, up.Rpc_peer().Handshakes_(), 2)
	assertTrue(t, up.Proto.Ping(gid), "ping after restart")
	probe = new_asyncioErrorProbe_(loop2).Enter(t)
	func() {
		defer probe.Exit()
		must(0, up.Comm_().Stop())
	}()
	assertNoReport(t, probe)
	assert_released_(t, up.Proto, fd2)
	assertNoErrors(t, up.Rpc_peer())
}

// ----------------------------------------------------------------------
// TestUartTeardownMidFrame -- 4: stop while ``read_frame()`` is blocked
// on a partial frame.
// ----------------------------------------------------------------------

func TestUartTeardownMidFrame(t *testing.T) {
	up := enter_uart_(t, New_UartPtyPair(Rpc_only_peer_))
	gid := up.Peer_gid
	assertTrue(t, up.Proto.Ping(gid), "ping")
	// A 4-byte header announcing 256 bytes that never arrive: laila's
	// receive loop is now parked inside readexactly(256).
	header := []byte{0, 0, 1, 0}
	peer := up.Rpc_peer()
	_, err := py.Run_coroutine_threadsafe(func() (any, error) { return nil, peer.write_raw_(header) }, peer.Loop).Result(5)
	must(0, err)
	sleep(0.2)
	fd := up.Proto.Serial_.Fileno()
	loop := up.Proto.Loop()
	recv_task := up.Proto.Recv_task_
	assertTrue(t, !recv_task.Done(), "recv task finished early")
	probe := new_asyncioErrorProbe_(loop).Enter(t)
	var elapsed float64
	func() {
		defer probe.Exit()
		t0 := time.Now()
		must(0, up.Comm_().Stop())
		elapsed = time.Since(t0).Seconds()
		sleep(0.2)
	}()
	assertTrue(t, elapsed < 2.0, fmt.Sprintf("stop() took %.2fs", elapsed))
	assertNoReport(t, probe)
	assertTrue(t, recv_task.Done(), "recv task still running")
	assert_released_(t, up.Proto, fd)
	assertNoErrors(t, up.Rpc_peer())
}

// ----------------------------------------------------------------------
// TestUartTeardownUnderStream -- 5: stop while the peer keeps pushing
// frames on a static lane.
// ----------------------------------------------------------------------

func TestUartTeardownUnderStream(t *testing.T) {
	original := must(laila.Get_active_policy())
	t.Cleanup(func() { laila.Activate_policy(original) })
	up := enter_uart_(t, New_UartPtyPair(Lane_peer_, py.Kw{
		"uart_kwargs": py.Kw{"static_lanes": py.DictOf("video", 1)},
		"peer_kwargs": py.Kw{"lane": 1, "chunk_size": 1024},
	}))
	gid := up.Peer_gid
	ch := channel_(t, up.Comm_(), gid, "video")
	laila.Activate_policy(up.Policy)
	relay := must(laila.Get_relay(gid, "video", py.Kw{"timeout": 10}))

	stop := py.NewEvent()
	var pushed_mu sync.Mutex
	pushed := 0
	payload := make([]byte, 0, 3072) // bytes(range(256)) * 12 -> 3 KB -> 3 chunks
	for i := 0; i < 12; i++ {
		for b := 0; b < 256; b++ {
			payload = append(payload, byte(b))
		}
	}
	peer := up.Lane_peer()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Is_set() {
			_, err := py.Run_coroutine_threadsafe(func() (any, error) { return peer.Emit(payload) }, peer.Loop).Result(0.5)
			if err != nil {
				// pty buffer full once nobody reads the slave, or peer loop
				// shutting down: keep hammering until told to stop
				sleep(0.01)
				continue
			}
			pushed_mu.Lock()
			pushed++
			pushed_mu.Unlock()
		}
	}()
	var fd int
	var probe *asyncioErrorProbe_
	func() {
		defer func() {
			stop.Set()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}()
		// frames are really flowing before we pull the plug
		assertEq(t, len(bytes_of_(t, next_(t, relay))), 3072)
		assertTrue(t, Wait_until(func() bool {
			pushed_mu.Lock()
			defer pushed_mu.Unlock()
			return pushed >= 5
		}, 5), "producer never pushed 5 messages")
		fd = up.Proto.Serial_.Fileno()
		loop := up.Proto.Loop()
		probe = new_asyncioErrorProbe_(loop).Enter(t)
		var elapsed float64
		func() {
			defer probe.Exit()
			t0 := time.Now()
			must(0, up.Comm_().Stop())
			elapsed = time.Since(t0).Seconds()
			sleep(0.2)
		}()
		assertTrue(t, elapsed < 5.0, fmt.Sprintf("stop() took %.2fs", elapsed))
		assertNoReport(t, probe)
	}()

	assertTrue(t, ch.Closed(), "channel not closed")
	// buffered messages drain, then the relay ends
	drained := 0
	for {
		_, err := relay.Next()
		if err != nil {
			assertRaises(t, py.StopIteration, err)
			break
		}
		drained++
	}
	assertTrue(t, drained < 10_000, "too many drained messages")
	_, err := relay.Next()
	assertRaises(t, py.StopIteration, err)
	assertRaises(t, py.ConnectionError, ch.Send([]byte("after stop")))
	assert_released_(t, up.Proto, fd)
	assertNoErrors(t, up.Rpc_peer())
}

// assertNoErrors is ``self.assertEqual(up.peer.errors, [])``.
func assertNoErrors(t *testing.T, peer *FakeRpcOnlyPeer) {
	t.Helper()
	if errs := peer.Errors_(); len(errs) != 0 {
		t.Fatalf("fake peer recorded errors: %v", errs)
	}
}
