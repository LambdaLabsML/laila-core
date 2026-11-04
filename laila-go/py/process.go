package py

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// concurrent.futures.process.ProcessPoolExecutor + the multiprocessing
// "spawn" start method.
//
// Python ships a *pickled* ``(task, args, kwargs)`` to a worker process and
// resolves ``task`` by its ``module.qualname`` on the other side; that is
// what "top-level picklable callable" means. Go cannot pickle a function
// pointer, so the port keeps an explicit registry instead:
//
//	var pid_task = py.Register_process_task("tests._pid", pid)
//
// A registered task is callable in-process (``pid_task.Call(...)``), so the
// same object may be handed to a thread-pool or a process-pool taskforce,
// and it is what ``pickle.dumps`` would have produced for the function.
// Arguments, keyword arguments and results travel as msgpack (the port's
// pickle for plain data); anything msgpack cannot encode raises the same
// TypeError Python raises for an unpicklable task.
//
// Workers are the current executable re-executed with
// ``LAILA_PROCESS_WORKER=1`` (the spawn start method re-imports ``__main__``
// the same way). ``main()`` / ``TestMain`` must call Process_worker_main()
// before doing anything else -- the Go analogue of the
// ``if __name__ == "__main__":`` guard that spawn requires.

// ProcessWorkerEnv is the environment variable marking a worker process.
const ProcessWorkerEnv = "LAILA_PROCESS_WORKER"

// ProcessTask is a function registered for execution in worker processes.
// It is the Go stand-in for a module-level Python function: it has a
// stable qualified name both sides agree on.
type ProcessTask struct {
	Name string
	Fn   any
}

// Call runs the task in-process: ``task(*args, **kwargs)``.
func (t *ProcessTask) Call(args []any, kw Kw) (any, error) { return Call(t.Fn, args, kw) }

// Qualname is ``task.__qualname__``.
func (t *ProcessTask) Qualname() string { return t.Name }

// Repr is ``<function name at 0x...>``.
func (t *ProcessTask) Repr() string { return fmt.Sprintf("<function %s>", t.Name) }

var (
	process_tasks_mu    sync.RWMutex
	process_tasks       = map[string]*ProcessTask{}
	process_tasks_by_fn = map[uintptr]*ProcessTask{}
)

// Register_process_task registers fn under name (its ``module.qualname``)
// and returns the callable handle. Registering the same name twice with a
// different function is a ValueError-level programming error and panics,
// exactly as two different top-level objects cannot share one qualname.
func Register_process_task(name string, fn any) *ProcessTask {
	if name == "" {
		panic(ValueError.New("process task name must not be empty"))
	}
	if !Callable(fn) {
		panic(TypeError.Newf("'%s' object is not callable", Pytypename(fn)))
	}
	process_tasks_mu.Lock()
	defer process_tasks_mu.Unlock()
	if prev, ok := process_tasks[name]; ok {
		if fnPointer(prev.Fn) == fnPointer(fn) {
			return prev
		}
		panic(ValueError.Newf("process task %q is already registered with a different function", name))
	}
	t := &ProcessTask{Name: name, Fn: fn}
	process_tasks[name] = t
	if p := fnPointer(fn); p != 0 {
		process_tasks_by_fn[p] = t
	}
	return t
}

// fnPointer returns the code pointer of a Go func value (0 otherwise).
func fnPointer(fn any) uintptr {
	if fn == nil {
		return 0
	}
	rv := reflect.ValueOf(fn)
	if rv.Kind() != reflect.Func {
		return 0
	}
	return rv.Pointer()
}

// Lookup_process_task resolves the registered task behind a callable:
// a *ProcessTask, its registered name, a registered Go func value, or a
// Partial wrapping any of those. ok is False for anything else (a lambda
// / closure -- Python's "Can't pickle local object").
func Lookup_process_task(task any) (t *ProcessTask, ok bool) {
	switch x := task.(type) {
	case *ProcessTask:
		return x, true
	case string:
		process_tasks_mu.RLock()
		defer process_tasks_mu.RUnlock()
		t, ok = process_tasks[x]
		return t, ok
	case *PartialFunc:
		return Lookup_process_task(x.Func)
	}
	if p := fnPointer(task); p != 0 {
		process_tasks_mu.RLock()
		defer process_tasks_mu.RUnlock()
		t, ok = process_tasks_by_fn[p]
		return t, ok
	}
	return nil, false
}

// Process_task_by_name is the worker-side lookup (KeyError when unknown).
func Process_task_by_name(name string) (*ProcessTask, error) {
	process_tasks_mu.RLock()
	defer process_tasks_mu.RUnlock()
	if t, ok := process_tasks[name]; ok {
		return t, nil
	}
	return nil, KeyError.New(name)
}

// ---------------------------------------------------------------------------
// functools.partial as an introspectable object
// ---------------------------------------------------------------------------

// PartialFunc is a functools.partial object: ``Func``, ``Args`` and
// ``Keywords`` are the ``.func`` / ``.args`` / ``.keywords`` attributes.
type PartialFunc struct {
	Func     any
	Args     []any
	Keywords Kw
}

// Call is ``partial(*more, **kw)``.
func (p *PartialFunc) Call(more []any, kw Kw) (any, error) {
	args := append(append([]any(nil), p.Args...), more...)
	var merged Kw
	if len(p.Keywords) > 0 || len(kw) > 0 {
		merged = Kw{}
		for k, v := range p.Keywords {
			merged[k] = v
		}
		for k, v := range kw {
			merged[k] = v
		}
	}
	return Call(p.Func, args, merged)
}

// Repr is ``functools.partial(<func>, *args)``.
func (p *PartialFunc) Repr() string {
	return fmt.Sprintf("functools.partial(%s, %s)", Repr(p.Func), Repr(Tuple(p.Args)))
}

// ---------------------------------------------------------------------------
// wire frames
// ---------------------------------------------------------------------------

func writeFrame(w io.Writer, payload any) error {
	body, err := Msgpack_packb(payload)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

func readFrame(r io.Reader) (*Dict, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	v, err := Msgpack_unpackb(body)
	if err != nil {
		return nil, err
	}
	d, ok := v.(*Dict)
	if !ok {
		return nil, ValueError.Newf("malformed process-pool frame: %s", Pytypename(v))
	}
	return d, nil
}

// Encode_process_error flattens an exception for the wire.
func Encode_process_error(err error) *Dict {
	d := NewDict()
	d.Set("type", Typename(err))
	d.Set("message", err.Error())
	return d
}

// Decode_process_error rebuilds an exception of the same class (falling
// back to Exception for classes this process does not know).
func Decode_process_error(d *Dict) error {
	name, _ := d.GetDefault("type", "Exception").(string)
	msg, _ := d.GetDefault("message", "").(string)
	t := Type_by_name(name)
	if t == nil {
		t = Exception
	}
	return t.New(msg)
}

// ---------------------------------------------------------------------------
// worker side
// ---------------------------------------------------------------------------

// Process_worker_main serves tasks when this process was spawned as a pool
// worker; it never returns in that case (the worker exits with the pipe).
// It is a no-op in a normal process, so it is safe to call unconditionally
// at the top of main() / TestMain.
func Process_worker_main() {
	if os.Getenv(ProcessWorkerEnv) != "1" {
		return
	}
	in := os.NewFile(3, "laila-process-worker-in")
	out := os.NewFile(4, "laila-process-worker-out")
	if in == nil || out == nil {
		fmt.Fprintln(os.Stderr, "laila process worker: missing task pipes (fd 3/4)")
		os.Exit(2)
	}
	for {
		req, err := readFrame(in)
		if err != nil {
			// Parent closed the pipe (shutdown) or died: exit quietly,
			// like a multiprocessing worker reading the sentinel.
			os.Exit(0)
		}
		resp := NewDict()
		resp.Set("id", req.GetDefault("id", nil))
		result, err := runProcessTask(req)
		if err != nil {
			resp.Set("ok", false)
			resp.Set("error", Encode_process_error(err))
		} else {
			resp.Set("ok", true)
			resp.Set("result", result)
		}
		if err := writeFrame(out, resp); err != nil {
			if errors.Is(err, TypeError) {
				// Unpicklable result: Python fails that future only.
				resp = NewDict()
				resp.Set("id", req.GetDefault("id", nil))
				resp.Set("ok", false)
				resp.Set("error", Encode_process_error(err))
				if writeFrame(out, resp) == nil {
					continue
				}
			}
			os.Exit(0)
		}
	}
}

// runProcessTask is _process_runner(task, args, kwargs) with the panic ->
// exception conversion Python gets for free.
func runProcessTask(req *Dict) (result any, err error) {
	defer Recover(&err)
	name, _ := req.GetDefault("task", "").(string)
	task, err := Process_task_by_name(name)
	if err != nil {
		return nil, err
	}
	args := Iter(req.GetDefault("args", nil))
	var kw Kw
	if d, ok := req.GetDefault("kwargs", nil).(*Dict); ok && d.Len() > 0 {
		kw = Kw{}
		for _, it := range d.Items() {
			kw[Str(it.Key)] = it.Value
		}
	}
	return task.Call(args, kw)
}

// ---------------------------------------------------------------------------
// parent side
// ---------------------------------------------------------------------------

// processJob is a pending work item.
type processJob struct {
	id     int64
	fut    *CFuture
	task   *ProcessTask
	args   []any
	kwargs Kw
}

// processWorker is one spawned worker process.
type processWorker struct {
	cmd  *exec.Cmd
	in   *os.File // we write requests here
	out  *os.File // we read responses here
	pid  int
	done chan struct{}
}

// ProcessPoolExecutor runs registered tasks in spawned worker processes.
type ProcessPoolExecutor struct {
	mu         sync.Mutex
	cv         *sync.Cond
	maxWorkers int
	mpContext  string
	workers    map[*processWorker]struct{}
	idle       int
	pending    []*processJob
	inflight   map[*processWorker]*processJob
	broken     error
	shutdown   bool
	nextID     int64
	wg         sync.WaitGroup
}

// New_ProcessPoolExecutor is ProcessPoolExecutor(max_workers=..., mp_context=...).
// max_workers <= 0 means os.cpu_count().
func New_ProcessPoolExecutor(max_workers int, kw ...Kw) *ProcessPoolExecutor {
	k := MergeKw(kw...)
	if max_workers <= 0 {
		max_workers = runtime.NumCPU()
	}
	ctx, _ := k.Get("mp_context", "spawn").(string)
	e := &ProcessPoolExecutor{
		maxWorkers: max_workers,
		mpContext:  ctx,
		workers:    map[*processWorker]struct{}{},
		inflight:   map[*processWorker]*processJob{},
	}
	e.cv = sync.NewCond(&e.mu)
	return e
}

// Max_workers is ``executor._max_workers``.
func (e *ProcessPoolExecutor) Max_workers() int { return e.maxWorkers }

// Broken reports whether the executor is in the BrokenProcessPool state.
func (e *ProcessPoolExecutor) Broken() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.broken != nil
}

// Worker_pids lists the live worker process ids (``executor._processes``).
func (e *ProcessPoolExecutor) Worker_pids() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]int, 0, len(e.workers))
	for w := range e.workers {
		out = append(out, w.pid)
	}
	return out
}

// Submit is ``executor.submit(task, *args, **kwargs)`` for a registered
// task (resolved with Lookup_process_task). Raises BrokenProcessPool once
// a worker died, RuntimeError after shutdown, and TypeError when the task
// is not registered or its arguments are not serialisable.
func (e *ProcessPoolExecutor) Submit(task any, args []any, kwargs Kw) (*CFuture, error) {
	t, ok := Lookup_process_task(task)
	if !ok {
		return nil, TypeError.Newf("cannot pickle %s: it is not a registered process task", Repr(task))
	}
	// Partial-bound arguments come first, like partial(fn, *bound)(*args).
	if p, isPartial := task.(*PartialFunc); isPartial {
		args = append(append([]any(nil), p.Args...), args...)
		if len(p.Keywords) > 0 {
			merged := Kw{}
			for k, v := range p.Keywords {
				merged[k] = v
			}
			for k, v := range kwargs {
				merged[k] = v
			}
			kwargs = merged
		}
	}
	if _, err := Msgpack_packb(Tuple(args)); err != nil {
		return nil, TypeError.Newf("cannot pickle task arguments: %s", err.Error())
	}
	if _, err := Msgpack_packb(kwargs); err != nil {
		return nil, TypeError.Newf("cannot pickle task keyword arguments: %s", err.Error())
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.broken != nil {
		return nil, e.broken
	}
	if e.shutdown {
		return nil, RuntimeError.New("cannot schedule new futures after shutdown")
	}
	e.nextID++
	job := &processJob{id: e.nextID, fut: NewCFuture(), task: t, args: args, kwargs: kwargs}
	e.pending = append(e.pending, job)
	// Spawn on demand (Python >= 3.9: no idle worker -> start one, up to
	// max_workers).
	if e.idle == 0 && len(e.workers) < e.maxWorkers {
		if err := e.spawnLocked(); err != nil {
			e.pending = e.pending[:len(e.pending)-1]
			return nil, err
		}
	}
	e.cv.Broadcast()
	return job.fut, nil
}

// spawnLocked starts one worker process; e.mu must be held.
func (e *ProcessPoolExecutor) spawnLocked() error {
	reqR, reqW, err := os.Pipe()
	if err != nil {
		return OSError.New(err.Error())
	}
	respR, respW, err := os.Pipe()
	if err != nil {
		reqR.Close()
		reqW.Close()
		return OSError.New(err.Error())
	}
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), ProcessWorkerEnv+"=1")
	cmd.Stdin = nil
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{reqR, respW}
	setPdeathsig(cmd)
	if err := cmd.Start(); err != nil {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
		return OSError.New(err.Error())
	}
	// The child holds its own copies now.
	reqR.Close()
	respW.Close()
	w := &processWorker{cmd: cmd, in: reqW, out: respR, pid: cmd.Process.Pid, done: make(chan struct{})}
	e.workers[w] = struct{}{}
	e.idle++
	e.wg.Add(1)
	go e.serve(w)
	return nil
}

// serve is the per-worker feeder: it moves pending jobs to its process
// one at a time and publishes the responses.
func (e *ProcessPoolExecutor) serve(w *processWorker) {
	defer e.wg.Done()
	defer close(w.done)
	for {
		e.mu.Lock()
		for len(e.pending) == 0 && !e.shutdown && e.broken == nil {
			e.cv.Wait()
		}
		if e.broken != nil || (e.shutdown && len(e.pending) == 0) {
			e.retireLocked(w)
			e.mu.Unlock()
			return
		}
		job := e.pending[0]
		e.pending = e.pending[1:]
		if !job.fut.Set_running_or_notify_cancel() {
			e.mu.Unlock()
			continue
		}
		e.idle--
		e.inflight[w] = job
		e.mu.Unlock()

		req := NewDict()
		req.Set("id", job.id)
		req.Set("task", job.task.Name)
		req.Set("args", Tuple(job.args))
		req.Set("kwargs", job.kwargs)
		var resp *Dict
		err := writeFrame(w.in, req)
		if err == nil {
			resp, err = readFrame(w.out)
		}
		if err != nil {
			// The worker vanished mid-task: the whole pool is broken.
			e.mu.Lock()
			e.breakLocked(w, err)
			e.mu.Unlock()
			return
		}

		if Truthy(resp.GetDefault("ok", false)) {
			_ = job.fut.Set_result(resp.GetDefault("result", nil))
		} else {
			errd, _ := resp.GetDefault("error", nil).(*Dict)
			if errd == nil {
				errd = NewDict()
			}
			_ = job.fut.Set_exception(Decode_process_error(errd))
		}
		e.mu.Lock()
		delete(e.inflight, w)
		e.idle++
		e.mu.Unlock()
	}
}

// retireLocked closes a worker's pipes (the child exits on EOF) and reaps
// it; e.mu must be held.
func (e *ProcessPoolExecutor) retireLocked(w *processWorker) {
	if _, ok := e.workers[w]; !ok {
		return
	}
	delete(e.workers, w)
	if _, busy := e.inflight[w]; !busy {
		e.idle--
	}
	delete(e.inflight, w)
	w.in.Close()
	w.out.Close()
	go func() { _ = w.cmd.Wait() }()
}

// breakLocked flips the executor to the broken state: every in-flight and
// pending future fails with BrokenProcessPool and all workers are killed,
// as concurrent.futures does when a worker terminates abruptly.
func (e *ProcessPoolExecutor) breakLocked(dead *processWorker, cause error) {
	if e.broken == nil {
		e.broken = BrokenProcessPool.New(
			"A process in the process pool was terminated abruptly while the future was running or pending.",
		).From(cause)
	}
	bpe := e.broken
	for w, job := range e.inflight {
		_ = job.fut.Set_exception(bpe)
		delete(e.inflight, w)
	}
	for _, job := range e.pending {
		_ = job.fut.Set_exception(bpe)
	}
	e.pending = nil
	for w := range e.workers {
		delete(e.workers, w)
		w.in.Close()
		w.out.Close()
		if w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
		go func(w *processWorker) { _ = w.cmd.Wait() }(w)
	}
	e.idle = 0
	e.cv.Broadcast()
}

// Shutdown is ``executor.shutdown(wait=..., cancel_futures=...)``.
func (e *ProcessPoolExecutor) Shutdown(wait bool, kw ...Kw) {
	k := MergeKw(kw...)
	cancel := Truthy(k.Get("cancel_futures", false))
	e.mu.Lock()
	e.shutdown = true
	if cancel {
		for _, job := range e.pending {
			job.fut.Cancel()
		}
		e.pending = nil
	}
	e.cv.Broadcast()
	e.mu.Unlock()
	if wait {
		e.wg.Wait()
	}
}

// Join_workers blocks until every worker goroutine has exited (test helper).
func (e *ProcessPoolExecutor) Join_workers(timeout float64) bool {
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	if timeout < 0 {
		<-done
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(time.Duration(timeout * float64(time.Second))):
		return false
	}
}

// Getpid is os.getpid().
func Getpid() int { return os.Getpid() }

// Exit_ is os._exit(code): terminate immediately, no deferred functions.
func Exit_(code int) { os.Exit(code) }
