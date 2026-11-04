package py

import (
	"bytes"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// subprocess -- the slice of Python's subprocess module the storage
// backends use to manage a private server process (``redis-server``,
// ``postgres``, ``mongod``).

// TimeoutExpired is ``subprocess.TimeoutExpired``.
var TimeoutExpired = NewType("TimeoutExpired", Exception)

// CalledProcessError is ``subprocess.CalledProcessError``.
var CalledProcessError = NewType("CalledProcessError", Exception)

// Popen is ``subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, text=True)``:
// both pipes are drained into buffers so the child never blocks on a
// full pipe and ``stdout.read()`` is available after exit.
type Popen struct {
	Args   []string
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
	outMu  sync.Mutex

	done     chan struct{}
	waitErr  error
	exitCode int
	finished bool
	mu       sync.Mutex
}

// Popen_ starts the command. FileNotFoundError when the executable is
// missing (``[Errno 2] No such file or directory: 'cmd'``).
func Popen_(args []string) (*Popen, error) {
	if len(args) == 0 {
		return nil, ValueError.New("Popen requires a non-empty command")
	}
	p := &Popen{Args: args, done: make(chan struct{})}
	p.cmd = exec.Command(args[0], args[1:]...)
	p.cmd.Stdout = &lockedWriter{buf: &p.stdout, mu: &p.outMu}
	p.cmd.Stderr = &lockedWriter{buf: &p.stderr, mu: &p.outMu}
	if err := p.cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, FileNotFoundError.Newf("[Errno 2] No such file or directory: %s", Repr(args[0])).With("errno", 2).With("filename", args[0])
		}
		return nil, FromGo(err)
	}
	go func() {
		err := p.cmd.Wait()
		p.mu.Lock()
		p.finished = true
		p.waitErr = err
		if p.cmd.ProcessState != nil {
			p.exitCode = p.cmd.ProcessState.ExitCode()
			if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				// Python reports a signal death as -signum.
				p.exitCode = -int(ws.Signal())
			}
		}
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(b)
}

// Pid is ``proc.pid``.
func (p *Popen) Pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// Poll is ``proc.poll()``: the exit code once the child has exited, nil
// while it is still running.
func (p *Popen) Poll() *int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.finished {
		return nil
	}
	code := p.exitCode
	return &code
}

// Wait is ``proc.wait(timeout)``; TimeoutExpired when the child is still
// running after *timeout* seconds (timeout < 0 waits forever).
func (p *Popen) Wait(timeout float64) (int, error) {
	if timeout < 0 {
		<-p.done
	} else {
		d, _ := Duration(timeout)
		select {
		case <-p.done:
		case <-time.After(d):
			return 0, TimeoutExpired.Newf("Command %s timed out after %v seconds", Repr(p.Args), timeout)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitCode, nil
}

// Terminate is ``proc.terminate()`` (SIGTERM).
func (p *Popen) Terminate() error {
	if p.Poll() != nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return FromGo(err)
	}
	return nil
}

// Kill is ``proc.kill()`` (SIGKILL).
func (p *Popen) Kill() error {
	if p.Poll() != nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return FromGo(err)
	}
	return nil
}

// Stdout_read is ``proc.stdout.read()`` (everything captured so far).
func (p *Popen) Stdout_read() string {
	p.outMu.Lock()
	defer p.outMu.Unlock()
	return p.stdout.String()
}

// Stderr_read is ``proc.stderr.read()`` (everything captured so far).
func (p *Popen) Stderr_read() string {
	p.outMu.Lock()
	defer p.outMu.Unlock()
	return p.stderr.String()
}

// Subprocess_run is ``subprocess.run(cmd, check=True, capture_output=True,
// text=True)``: (stdout, stderr, nil) on success; FileNotFoundError when
// the executable is missing; CalledProcessError (with ``Attr("stdout")``,
// ``Attr("stderr")``, ``Attr("returncode")``) on a non-zero exit.
func Subprocess_run(args []string) (string, string, error) {
	p, err := Popen_(args)
	if err != nil {
		return "", "", err
	}
	code, _ := p.Wait(-1)
	stdout, stderr := p.Stdout_read(), p.Stderr_read()
	if code != 0 {
		return stdout, stderr, CalledProcessError.Newf(
			"Command %s returned non-zero exit status %d.", Repr(args), code).
			With("returncode", code).With("stdout", stdout).With("stderr", stderr)
	}
	return stdout, stderr, nil
}

// ---------------------------------------------------------------------------
// atexit
// ---------------------------------------------------------------------------

var (
	atexit_mu  sync.Mutex
	atexit_fns []func()
)

// Atexit_register is ``atexit.register(fn)``. Go has no interpreter
// shutdown hook, so the callbacks run from Atexit_run, which
// ``laila.Terminate`` and ``Process_worker_main`` invoke; a ``main`` that
// exits through neither should ``defer py.Atexit_run()``.
func Atexit_register(fn func()) {
	atexit_mu.Lock()
	atexit_fns = append(atexit_fns, fn)
	atexit_mu.Unlock()
}

// Atexit_run runs the registered callbacks in LIFO order (each once).
func Atexit_run() {
	atexit_mu.Lock()
	fns := atexit_fns
	atexit_fns = nil
	atexit_mu.Unlock()
	for i := len(fns) - 1; i >= 0; i-- {
		func() {
			defer func() { _ = recover() }()
			fns[i]()
		}()
	}
}
