package laila

import (
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/loopthread.py --
// shared event-loop-thread lifecycle for transports.
//
// Every wire-backed transport (stream, datagram, point-to-point, broker,
// register carriers and the WebSocket ``tcpip`` protocol) owns a private
// asyncio event loop on a daemon thread. The boot and teardown sequence
// is identical for all of them and subtle enough that it should exist
// exactly once:
//
// ``start``
//
//	Create the loop, run the transport's ``_async_start(ready)`` on the
//	thread, block the caller until *ready* is set (or the boot raised),
//	then keep the loop running with ``run_forever``. A boot failure joins
//	the thread and **closes the loop** so a failed ``start()`` leaks
//	nothing.
//
// ``stop``
//
//	Run the transport's ``shutdown()`` coroutine on the loop (it is
//	expected to close its endpoints and call Cancel_pending_tasks), stop
//	the loop, join the thread and then **close the loop**. Not closing the
//	loop leaks its selector fd and self-pipe pair on every lifecycle,
//	which is how long-running processes that peer/unpeer repeatedly run
//	out of descriptors.
//
// After ``stop`` the transport's ``_event_loop`` is ``None``; sends must
// go through CarrierRPCProtocol_.Loop_call_, which turns that into a
// clear ConnectionError.

var loopthread_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.loopthread")

// Loop_thread_state_ holds the ``_event_loop`` / ``_loop_thread`` private
// attributes every loop-owning transport declares.
//
// Python reads ``proto._event_loop`` from any thread (a plain attribute);
// in Go the executor workers that answer RPCs and the user thread that
// calls ``stop()`` touch it concurrently, so the reference is guarded by
// ``mu_`` and read through Loop(). The exported field stays for
// Python-style reflection (``_event_loop``) and is written only by the
// setter.
type Loop_thread_state_ struct {
	Event_loop_  *py.Loop   `py:"_event_loop"`
	Loop_thread_ *py.Thread `py:"_loop_thread"`
	mu_          sync.Mutex
}

// Loop_state_ exposes the state to the helpers below.
func (s *Loop_thread_state_) Loop_state_() *Loop_thread_state_ { return s }

// Loop is ``self._event_loop`` (nil once the transport is stopped).
func (s *Loop_thread_state_) Loop() *py.Loop {
	s.mu_.Lock()
	defer s.mu_.Unlock()
	return s.Event_loop_
}

// set_loop_ is ``self._event_loop = loop``.
func (s *Loop_thread_state_) set_loop_(loop *py.Loop) {
	s.mu_.Lock()
	s.Event_loop_ = loop
	s.mu_.Unlock()
}

// Loop_thread_owner_ is any transport that owns a private loop thread.
type Loop_thread_owner_ interface {
	Loop_state_() *Loop_thread_state_
}

// loop_owner_name_ is ``type(proto).__name__``.
func loop_owner_name_(proto any) string {
	if c, ok := proto.(interface{ Class_() *py.Class }); ok && c.Class_() != nil {
		return c.Class_().Name
	}
	return py.Pytypename(proto)
}

// Start_loop_thread boots *proto*'s private loop thread and blocks until
// it is ready.
//
// Sets ``proto._event_loop`` / ``proto._loop_thread``. Re-raises the
// exception from *async_start* in the caller's thread (with the loop
// already closed) if the boot failed.
func Start_loop_thread(proto Loop_thread_owner_, async_start func(ready *py.Event) error, ready_timeout float64) error {
	st := proto.Loop_state_()
	loop := py.New_event_loop()
	st.set_loop_(loop)
	ready := py.NewEvent()
	var boot_error error
	var boot_set bool

	run_loop := func() {
		py.Set_event_loop(loop)
		_, err := loop.Run_until_complete(func() (any, error) { return nil, async_start(ready) })
		if err != nil {
			boot_error = err
			boot_set = true
			ready.Set()
			return
		}
		_ = loop.Run_forever()
	}

	thread := py.New_Thread(py.Kw{"target": run_loop, "daemon": true, "name": loop_owner_name_(proto) + "-loop"})
	st.Loop_thread_ = thread
	if err := thread.Start(); err != nil {
		return err
	}
	ready.Wait(ready_timeout)
	if boot_set {
		_ = thread.Join(ready_timeout)
		st.Loop_thread_ = nil
		Close_loop(proto)
		return boot_error
	}
	return nil
}

// Stop_loop_thread runs *shutdown* on *proto*'s loop, stops it, joins the
// thread, closes the loop.
//
// Idempotent and best-effort: a shutdown coroutine that raises or
// overruns *timeout* is logged at debug and the loop is still stopped.
func Stop_loop_thread(proto Loop_thread_owner_, shutdown func() error, timeout ...float64) {
	t := 5.0
	if len(timeout) > 0 {
		t = timeout[0]
	}
	st := proto.Loop_state_()
	loop := st.Loop()
	thread := st.Loop_thread_
	if loop != nil && !loop.Is_closed() && loop.Is_running() {
		if shutdown != nil {
			fut := py.Run_coroutine_threadsafe(func() (any, error) { return nil, shutdown() }, loop)
			if _, err := fut.Result(t); err != nil {
				loopthread_log_.Debug("%s shutdown coroutine failed", loop_owner_name_(proto), py.Kw{"exc_info": err})
			}
		}
		_, _ = loop.Call_soon_threadsafe(loop.Stop)
	}
	if thread != nil {
		_ = thread.Join(t)
	}
	st.Loop_thread_ = nil
	Close_loop(proto)
}

// Close_loop closes *proto*'s loop if its thread has exited; always drops
// the reference.
func Close_loop(proto Loop_thread_owner_) {
	st := proto.Loop_state_()
	loop := st.Loop()
	st.set_loop_(nil)
	if loop == nil || loop.Is_closed() {
		return
	}
	if loop.Is_running() {
		// The loop thread did not exit within the join timeout. Closing a
		// running loop raises; leave it to die with its daemon thread.
		loopthread_log_.Warning("%s event loop still running at close; leaking it", loop_owner_name_(proto))
		return
	}
	if err := loop.Close(); err != nil {
		loopthread_log_.Debug("closing %s event loop failed", loop_owner_name_(proto), py.Kw{"exc_info": err})
	}
}

// Cancel_pending_tasks cancels every other task on the current loop and
// waits for them to finish.
//
// Call at the end of a transport's shutdown coroutine so no receive loop,
// writer task or retransmit timer survives into ``loop.stop()`` (a task
// destroyed while pending logs an asyncio error on close).
func Cancel_pending_tasks() error {
	loop, err := py.Get_running_loop()
	if err != nil {
		return err
	}
	current := py.Current_task()
	var pending []*py.Task
	for _, task := range loop.All_tasks() {
		if task != current {
			pending = append(pending, task)
		}
	}
	for _, task := range pending {
		task.Cancel()
	}
	if len(pending) > 0 {
		fns := make([]func() (any, error), len(pending))
		for i, task := range pending {
			task := task
			fns[i] = task.Await
		}
		_, err := py.Gather(fns, py.Kw{"return_exceptions": true})
		return err
	}
	return nil
}
