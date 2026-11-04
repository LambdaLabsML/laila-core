//go:build workload

// Port of tests/workloads/_wl.py: helpers shared by the workload suite (no
// test functions here). Regular package file so every *_test.go in the
// package can use it, like the Python modules do with ``from _wl import``.
package workloads

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// SCALE is ``float(os.environ.get("LAILA_WORKLOAD_SCALE", "1.0"))``.
var SCALE = func() float64 {
	if v := os.Getenv("LAILA_WORKLOAD_SCALE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 1.0
}()

// N_scaled is ``n_scaled(n, floor=1)``.
func N_scaled(n int, floor ...int) int {
	f := 1
	if len(floor) > 0 {
		f = floor[0]
	}
	return max(f, int(float64(n)*SCALE))
}

// ---------------------------------------------------------------------------
// timing
// ---------------------------------------------------------------------------

// Stopwatch is ``with Stopwatch() as sw: ...; sw.elapsed``.
type Stopwatch struct {
	t0      float64
	Elapsed float64
}

// Enter is ``__enter__``.
func (s *Stopwatch) Enter() *Stopwatch {
	s.t0 = py.Perf_counter()
	return s
}

// Exit is ``__exit__``.
func (s *Stopwatch) Exit() { s.Elapsed = py.Perf_counter() - s.t0 }

// Timed runs fn inside a Stopwatch and returns it.
func Timed(fn func()) *Stopwatch {
	sw := (&Stopwatch{}).Enter()
	fn()
	sw.Exit()
	return sw
}

// Wait_until is ``wait_until(pred, timeout=10.0, step=0.01)``.
func Wait_until(pred func() bool, timeout_step ...float64) bool {
	timeout, step := 10.0, 0.01
	if len(timeout_step) > 0 {
		timeout = timeout_step[0]
	}
	if len(timeout_step) > 1 {
		step = timeout_step[1]
	}
	deadline := py.Monotonic() + timeout
	for py.Monotonic() < deadline {
		if pred() {
			return true
		}
		py.Sleep_sync(step)
	}
	return pred()
}

// Assert_subquadratic asserts that growing the input ``ratio``x did not
// grow time ~ratio**2. A linear algorithm gives ``large_t ~= ratio *
// small_t``; a quadratic one ``ratio**2``. We allow ``slack`` times the
// linear expectation.
func Assert_subquadratic(small_t, large_t, ratio float64, slack ...float64) error {
	sl := 4.0
	if len(slack) > 0 {
		sl = slack[0]
	}
	small_t = max(small_t, 1e-3)
	if large_t > small_t*ratio*sl {
		return py.AssertionError.Newf(
			"scaling looks super-linear: %.3fs -> %.3fs for %gx input (linear bound %.3fs)",
			small_t, large_t, ratio, small_t*ratio*sl)
	}
	return nil
}

// ---------------------------------------------------------------------------
// payloads
// ---------------------------------------------------------------------------

// Rand_bytes is ``rand_bytes(n, seed)``: n deterministic bytes.
func Rand_bytes(n int, seed ...int) []byte {
	s := int64(0)
	if len(seed) > 0 {
		s = int64(seed[0])
	}
	rng := rand.New(rand.NewSource(s))
	out := make([]byte, n)
	rng.Read(out)
	return out
}

// Rand_array is ``rand_array(n_bytes, dtype=np.float64, seed)``: a 1-D
// float64 (or float32 when itemsize 4) array of standard normals.
func Rand_array(n_bytes int, opts ...any) *py.NDArray {
	itemsize, seed := 8, int64(0)
	for _, o := range opts {
		switch v := o.(type) {
		case string:
			if v == "float32" || v == "<f4" {
				itemsize = 4
			}
		case int:
			seed = int64(v)
		}
	}
	rng := rand.New(rand.NewSource(seed))
	count := max(1, n_bytes/itemsize)
	if itemsize == 4 {
		data := make([]float32, count)
		for i := range data {
			data[i] = float32(rng.NormFloat64())
		}
		return py.NDArray_float32([]int{count}, data)
	}
	data := make([]float64, count)
	for i := range data {
		data[i] = rng.NormFloat64()
	}
	return py.NDArray_float64([]int{count}, data)
}

// Big_dict is ``big_dict(n_keys, value_len=8)``.
func Big_dict(n_keys int, value_len ...int) *py.Dict {
	vl := 8
	if len(value_len) > 0 {
		vl = value_len[0]
	}
	v := strings.Repeat("v", vl)
	d := py.NewDict()
	for i := 0; i < n_keys; i++ {
		d.Set("k"+strconv.Itoa(i), v)
	}
	return d
}

// Nested is ``nested(depth, leaf=1, kind="dict")``.
func Nested(depth int, leaf any, kind string) any {
	obj := leaf
	for i := 0; i < depth; i++ {
		if kind == "dict" {
			obj = py.DictOf("n", obj)
		} else {
			obj = py.NewList(obj)
		}
	}
	return obj
}

// Nested_depth is ``nested_depth(obj)``.
func Nested_depth(obj any) int {
	d := 0
	for {
		switch x := obj.(type) {
		case *py.Dict:
			if x.Len() == 0 {
				return d
			}
			obj = x.GetDefault("n", nil)
		case *py.List:
			if x.Len() == 0 {
				return d
			}
			obj, _ = x.Get(0)
		default:
			return d
		}
		d++
	}
}

// Compressible_text is ``compressible_text(n)``.
func Compressible_text(n int) string {
	return strings.Repeat("lorem ipsum dolor sit amet ", n/27+1)[:n]
}

// ---------------------------------------------------------------------------
// futures
// ---------------------------------------------------------------------------

// Release_all is ``release_all(*futs)``.
func Release_all(futs ...any) {
	for _, f := range futs {
		if py.IsNone(f) {
			continue
		}
		if r, ok := f.(interface{ Release() }); ok {
			func() {
				defer func() { _ = recover() }()
				r.Release()
			}()
		}
	}
}

// Waiter is anything with ``wait(timeout)``.
type Waiter interface {
	Wait(timeout ...any) (any, error)
}

// Wait is ``f.wait(timeout)`` for any future-like value.
func Wait(f any, timeout float64) (any, error) {
	w, ok := f.(Waiter)
	if !ok {
		return nil, py.TypeError.Newf("%s has no wait()", py.Pytypename(f))
	}
	return w.Wait(timeout)
}

// Wait_all is ``wait_all(futs, timeout=120.0)``.
func Wait_all(futs []any, timeout ...float64) ([]any, error) {
	to := 120.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	out := make([]any, 0, len(futs))
	for _, f := range futs {
		v, err := Wait(f, to)
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Quiesce is ``quiesce(policy=None, timeout=120.0)``: wait for every
// future still registered in the policy's bank.
//
// A ``GroupFuture.wait()`` raises as soon as one child fails while the
// siblings keep running; closing a pool underneath them crashes the
// interpreter (see FINDINGS F-06), so tests drain the bank first.
func Quiesce(policy any, timeout ...float64) {
	to := 120.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	if py.IsNone(policy) {
		policy = py.Must(laila.Get_active_policy())
	}
	deadline := py.Monotonic() + to
	bank := As_policy(policy).Future_bank
	for _, fut := range bank.Values() {
		remaining := max(0.1, deadline-py.Monotonic())
		if w, ok := fut.(Waiter); ok {
			func() {
				defer func() { _ = recover() }()
				_, _ = w.Wait(remaining)
			}()
		}
	}
}

// Unwrap is ``unwrap(v)``: the raw payload of an Entry (or pass through
// non-entries).
func Unwrap(v any) any {
	for {
		e, ok := v.(*laila.Entry)
		if !ok {
			return v
		}
		v = py.Must(e.Data())
	}
}

// ---------------------------------------------------------------------------
// threads
// ---------------------------------------------------------------------------

// Run_threads is ``run_threads(fn, n_threads)``: run ``fn(i)`` on
// ``n_threads`` threads simultaneously and return results. The first
// error (or panic) is returned.
func Run_threads(fn func(i int) (any, error), n_threads int) ([]any, error) {
	results := make([]any, n_threads)
	var mu sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(n_threads)
	for i := 0; i < n_threads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					errs = append(errs, py.PanicToError(r))
					mu.Unlock()
				}
			}()
			ready.Done()
			<-start // threading.Barrier
			v, err := fn(i)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			results[i] = v
		}(i)
	}
	ready.Wait()
	close(start)
	wg.Wait()
	if len(errs) > 0 {
		return results, errs[0]
	}
	return results, nil
}

// Go_threads is Run_threads for workers that only report errors.
func Go_threads(fn func(i int) error, n_threads int) error {
	_, err := Run_threads(func(i int) (any, error) { return nil, fn(i) }, n_threads)
	return err
}

// Pool_map is ``pool_map(fn, items, workers=8)``.
func Pool_map(fn func(item any) (any, error), items []any, workers ...int) ([]any, error) {
	w := 8
	if len(workers) > 0 {
		w = workers[0]
	}
	ex := py.New_ThreadPoolExecutor(w)
	defer ex.Shutdown(true)
	futs := make([]*py.CFuture, len(items))
	for i, it := range items {
		f, err := ex.Submit(func() (any, error) { return fn(it) })
		if err != nil {
			return nil, err
		}
		futs[i] = f
	}
	out := make([]any, len(items))
	for i, f := range futs {
		v, err := f.Result(-1)
		if err != nil {
			return out, err
		}
		out[i] = v
	}
	return out, nil
}

// Open_fds is ``open_fds()``: the set of open descriptor names.
func Open_fds() map[string]struct{} {
	out := map[string]struct{}{}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return out
	}
	for _, e := range entries {
		out[e.Name()] = struct{}{}
	}
	return out
}

// Live_thread_names is ``live_thread_names()``: names of the py.Thread
// objects alive (Go has no enumeration of goroutines by name).
func Live_thread_names() []string {
	var names []string
	for _, t := range py.Enumerate() {
		names = append(names, t.Name())
	}
	return py.Sorted_strings(names)
}

// ---------------------------------------------------------------------------
// pools
// ---------------------------------------------------------------------------

// Make_pool is ``make_pool(kind, directory, **kw)``: a backend pool rooted
// under ``directory``.
func Make_pool(kind string, directory string, kw ...py.Kw) (any, error) {
	tag := py.Uuid4().Hex()[:8]
	opts := py.MergeKw(kw...)
	switch kind {
	case "mem":
			return laila.New_DefaultPool(opts)
	case "sqlite":
		o := opts.Copy()
		o["file_path"] = filepath.Join(directory, tag+".sqlite")
		return laila.New_SQLitePool(o)
	case "duckdb":
		o := opts.Copy()
		o["file_path"] = filepath.Join(directory, tag+".duckdb")
		return laila.New_DuckDBPool(o)
	case "hdf5":
		o := opts.Copy()
		o["file_path"] = filepath.Join(directory, tag+".h5")
		return laila.New_HDF5Pool(o)
	}
	return nil, py.ValueError.New(kind)
}

// Close_pool is ``close_pool(pool)``.
func Close_pool(pool any) {
	if c, ok := pool.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// Has_hdf5 is ``has_hdf5()``: ``import h5py`` succeeds -- here, the hdf5
// driver module is linked (drivers.go blank import, cgo builds only).
func Has_hdf5() bool { return laila.Driver_available("h5py") }

// Has_duckdb is the same gate for the duckdb driver (Python always has
// ``duckdb`` installed with the ``[duckdb]`` extra; the Go driver is a cgo
// module, so the kind is absent without it like a missing ``h5py``).
func Has_duckdb() bool { return laila.Driver_available("duckdb") }

// PERSISTENT_KINDS / ALL_KINDS mirror the Python lists: ``["sqlite",
// "duckdb"] + (["hdf5"] if has_hdf5() else [])``.
var PERSISTENT_KINDS = func() []string {
	out := []string{"sqlite"}
	if Has_duckdb() {
		out = append(out, "duckdb")
	}
	if Has_hdf5() {
		out = append(out, "hdf5")
	}
	return out
}()

var ALL_KINDS = append([]string{"mem"}, PERSISTENT_KINDS...)

// ---------------------------------------------------------------------------
// two in-process policies peered over one transport
// ---------------------------------------------------------------------------

// TwoPolicies is two in-process ``DefaultPolicy`` instances peered over
// ``kind`` in {"loopback", "tcp", "udp", "unix", "ws"}.
type TwoPolicies struct {
	Kind              string
	Proto_kwargs      py.Kw
	Liveness_interval *float64
	Activate          string // "a", "b" or ""
	A, B              *laila.LAILA_IDENTIFIABLE_POLICY_
	Pa, Pb            laila.Comm_protocol_
	Aid, Bid          string
	prev_active_      any
}

// New_TwoPolicies is ``TwoPolicies(kind="tcp", *, liveness_interval=None,
// activate="a", **proto_kwargs)``.
func New_TwoPolicies(kind string, kw ...py.Kw) *TwoPolicies {
	opts := py.MergeKw(kw...)
	t := &TwoPolicies{Kind: kind, Proto_kwargs: py.Kw{}, Activate: "a"}
	for k, v := range opts {
		switch k {
		case "liveness_interval":
			if !py.IsNone(v) {
				f := py.Must(py.Float(v))
				t.Liveness_interval = &f
			}
		case "activate":
			if py.IsNone(v) {
				t.Activate = ""
			} else {
				t.Activate = py.Str(v)
			}
		default:
			t.Proto_kwargs[k] = v
		}
	}
	return t
}

func (t *TwoPolicies) factory_() (func(kw ...py.Kw) (laila.Comm_protocol_, error), error) {
	switch t.Kind {
	case "loopback":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultLoopbackProtocol(kw...) }, nil
	case "tcp":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPProtocol(kw...) }, nil
	case "udp":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUDPProtocol(kw...) }, nil
	case "unix":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultUnixSocketProtocol(kw...) }, nil
	case "ws":
		return func(kw ...py.Kw) (laila.Comm_protocol_, error) { return laila.New_DefaultTCPIPProtocol(kw...) }, nil
	}
	return nil, py.KeyError.New(t.Kind)
}

// Uri_for is ``uri_for(policy, proto)``.
func (t *TwoPolicies) Uri_for(policy *laila.LAILA_IDENTIFIABLE_POLICY_, proto laila.Comm_protocol_) (string, error) {
	switch t.Kind {
	case "loopback":
		return "loopback://" + policy.Global_id(), nil
	case "tcp":
		return fmt.Sprintf("tcp://127.0.0.1:%d", Bound_port(proto)), nil
	case "udp":
		return fmt.Sprintf("udp://127.0.0.1:%d", Bound_port(proto)), nil
	case "unix":
		return "unix://" + proto.(interface{ Bound_path() string }).Bound_path(), nil
	case "ws":
		return fmt.Sprintf("ws://127.0.0.1:%d", Bound_port(proto)), nil
	}
	return "", py.ValueError.New(t.Kind)
}

// Bound_port is ``proto.bound_port`` for any IP transport.
func Bound_port(proto any) int { return proto.(interface{ Bound_port() int }).Bound_port() }

// Secret_of is ``proto.peer_secret_key``.
func Secret_of(proto any) string {
	if c := laila.As_carrier_(proto); c != nil {
		return c.Peer_secret_key
	}
	// the ``ws`` kind (DefaultTCPIPProtocol) is not a carrier
	return py.Str(py.Must(py.Getattr(proto, "peer_secret_key")))
}

// Comm is ``policy.central.communication``.
func Comm(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return laila.As_communication_(p.Central.Communication)
}

// Mem is ``policy.central.memory``.
func Mem(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ {
	return laila.As_central_memory_(p.Central.Memory)
}

// Router is ``policy.central.memory.pool_router``.
func Router(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_POOL_ROUTER_ {
	return Mem(p).Pool_router.(interface {
		Pool_router_base_() *laila.LAILA_IDENTIFIABLE_POOL_ROUTER_
	}).Pool_router_base_()
}

// Cmd is ``policy.central.command``.
func Cmd(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	return p.Central.Command.(interface {
		Central_command_base_() *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	}).Central_command_base_()
}

// Enter is ``__enter__``.
func (t *TwoPolicies) Enter() (*TwoPolicies, error) {
	factory, err := t.factory_()
	if err != nil {
		return nil, err
	}
	kw := t.Proto_kwargs.Copy()
	if t.Kind == "tcp" || t.Kind == "udp" || t.Kind == "ws" {
		if _, ok := kw["host"]; !ok {
			kw["host"] = "127.0.0.1"
		}
	}
	if t.Pa, err = factory(kw); err != nil {
		return nil, err
	}
	if t.Pb, err = factory(kw); err != nil {
		return nil, err
	}
	t.prev_active_, _ = laila.Get_active_policy()
	a, err := laila.New_DefaultPolicy()
	if err != nil {
		return nil, err
	}
	b, err := laila.New_DefaultPolicy()
	if err != nil {
		return nil, err
	}
	t.A, t.B = As_policy(a), As_policy(b)
	if t.Liveness_interval != nil {
		Comm(t.A).Liveness_interval = *t.Liveness_interval
		Comm(t.B).Liveness_interval = *t.Liveness_interval
	}
	if err := Comm(t.A).Add_connection(t.Pa); err != nil {
		return nil, err
	}
	if err := Comm(t.B).Add_connection(t.Pb); err != nil {
		return nil, err
	}
	t.Aid = t.A.Global_id()
	uri, err := t.Uri_for(t.B, t.Pb)
	if err != nil {
		return nil, err
	}
	if t.Bid, err = Comm(t.A).Add_peer(uri, Secret_of(t.Pb)); err != nil {
		return nil, err
	}
	if t.Bid != t.B.Global_id() {
		return nil, py.AssertionError.Newf("%s != %s", t.Bid, t.B.Global_id())
	}
	if !Wait_until(func() bool { return Comm(t.B).Peers.Contains(t.Aid) }, 15.0) {
		return nil, py.RuntimeError.New("peer B never registered A")
	}
	switch t.Activate {
	case "a":
		laila.Activate_policy(t.A)
	case "b":
		laila.Activate_policy(t.B)
	}
	return t, nil
}

// Exit is ``__exit__``.
func (t *TwoPolicies) Exit() {
	for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{t.A, t.B} {
		if pol != nil {
			_ = Comm(pol).Stop()
		}
	}
	for _, pol := range []*laila.LAILA_IDENTIFIABLE_POLICY_{t.A, t.B} {
		if pol != nil {
			_ = Cmd(pol).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
			laila.Local_policies_.Pop(pol.Global_id())
		}
	}
	if !py.IsNone(t.prev_active_) {
		laila.Activate_policy(t.prev_active_)
	}
}

// Ca is ``self.a.central.communication``.
func (t *TwoPolicies) Ca() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(t.A) }

// Cb is ``self.b.central.communication``.
func (t *TwoPolicies) Cb() *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ { return Comm(t.B) }

// Peer is ``self.ca.peers[self.bid]``.
func (t *TwoPolicies) Peer() *laila.PeerProxy {
	return py.Must(t.Ca().Peers.Getitem(t.Bid)).(*laila.PeerProxy)
}

// Extend_b is ``extend_b(nickname="store", pool=None)``.
func (t *TwoPolicies) Extend_b(nickname string, pool ...any) (any, error) {
	return extend_(t.B, nickname, pool...)
}

// Extend_a is ``extend_a(nickname="store", pool=None)``.
func (t *TwoPolicies) Extend_a(nickname string, pool ...any) (any, error) {
	return extend_(t.A, nickname, pool...)
}

func extend_(p *laila.LAILA_IDENTIFIABLE_POLICY_, nickname string, pool ...any) (any, error) {
	var pl any
	if len(pool) > 0 && !py.IsNone(pool[0]) {
		pl = pool[0]
	} else {
		var err error
		if pl, err = laila.New_DefaultPool(); err != nil {
			return nil, err
		}
	}
	if err := Mem(p).Extend(pl, py.Kw{"pool_nickname": nickname}); err != nil {
		return nil, err
	}
	return pl, nil
}

// ---------------------------------------------------------------------------
// subprocess peers
// ---------------------------------------------------------------------------

// CHILD_ENV is the environment variable that turns the test binary into a
// child peer (the Python suite runs ``python -c <_CHILD_TEMPLATE>``; the
// Go suite re-executes itself -- see TestMain in conftest_test.go).
const CHILD_ENV = "LAILA_WORKLOAD_CHILD"

// Child_extras is the port of the ``extra`` code string a Python ChildPeer
// executes after its transport is up: named Go funcs registered by the
// test files (``Child_extras["w21"] = func() error {...}``), selected with
// ``ChildPeer.Extra``.
var Child_extras = map[string]func(args map[string]string) error{}

// ChildPeer is a laila policy running in a child process, reachable over
// tcp/ws.
type ChildPeer struct {
	Nickname     string
	Kind         string
	Home         string
	Liveness     float64
	Extra        string            // name in Child_extras
	Extra_args   map[string]string // forwarded to the extra through the environment
	Proto_kwargs py.Kw
	Proc         *exec.Cmd
	Info         map[string]string
	stdin        *bufio.Writer
	stdout       *bufio.Reader
	stderr       strings.Builder
	done         chan struct{}
	exit_err     error
}

// New_ChildPeer is ``ChildPeer(nickname, *, kind="tcp", home, liveness=15.0,
// extra="", proto_kwargs=None)``.
func New_ChildPeer(nickname string, kw ...py.Kw) *ChildPeer {
	opts := py.MergeKw(kw...)
	c := &ChildPeer{
		Nickname:     nickname,
		Kind:         py.Str(opts.Get("kind", "tcp")),
		Home:         py.Str(opts.Get("home", "")),
		Liveness:     py.Must(py.Float(opts.Get("liveness", 15.0))),
		Extra:        py.Str(opts.Get("extra", "")),
		Proto_kwargs: py.Kw{},
		Info:         map[string]string{},
		Extra_args:   map[string]string{},
	}
	if pk, ok := opts["proto_kwargs"]; ok && !py.IsNone(pk) {
		c.Proto_kwargs = py.MergeKw(pk.(py.Kw))
	}
	if ea, ok := opts["extra_args"]; ok && !py.IsNone(ea) {
		c.Extra_args = ea.(map[string]string)
	}
	return c
}

// Enter is ``__enter__``: spawn the child and wait for ``READY``.
func (c *ChildPeer) Enter() (*ChildPeer, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, py.FromGo(err)
	}
	cmd := exec.Command(exe, "-test.run", "^$")
	env := append(os.Environ(),
		CHILD_ENV+"="+c.Nickname,
		CHILD_ENV+"_KIND="+c.Kind,
		CHILD_ENV+"_HOME="+filepath.Join(c.Home, "child-"+c.Nickname),
		CHILD_ENV+"_LIVENESS="+strconv.FormatFloat(c.Liveness, 'g', -1, 64),
		CHILD_ENV+"_EXTRA="+c.Extra,
		CHILD_ENV+"_PROTO_KWARGS="+py.Must(py.Json_dumps(kw_dict_(c.Proto_kwargs))),
	)
	for k, v := range c.Extra_args {
		env = append(env, CHILD_ENV+"_ARG_"+k+"="+v)
	}
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, py.FromGo(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, py.FromGo(err)
	}
	cmd.Stderr = &c.stderr
	if err := cmd.Start(); err != nil {
		return nil, py.FromGo(err)
	}
	c.Proc = cmd
	c.stdin = bufio.NewWriter(stdin)
	c.stdout = bufio.NewReader(stdout)
	c.done = make(chan struct{})
	go func() {
		c.exit_err = cmd.Wait()
		close(c.done)
	}()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		line, err := c.stdout.ReadString('\n')
		if err != nil {
			tail := c.stderr.String()
			if len(tail) > 2000 {
				tail = tail[len(tail)-2000:]
			}
			return nil, py.RuntimeError.Newf("child peer died during boot: %s", tail)
		}
		line = strings.TrimSpace(line)
		if line == "READY" {
			return c, nil
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			c.Info[k] = v
		}
	}
	return nil, py.RuntimeError.New("child peer boot timed out")
}

// Port is ``int(self.info["PORT"])``.
func (c *ChildPeer) Port() int { return py.Must(strconv.Atoi(c.Info["PORT"])) }

// Secret is ``self.info["SECRET"]``.
func (c *ChildPeer) Secret() string { return c.Info["SECRET"] }

// Gid is ``self.info["POLICY_ID"]``.
func (c *ChildPeer) Gid() string { return c.Info["POLICY_ID"] }

// Uri is ``"{scheme}://127.0.0.1:{port}"``.
func (c *ChildPeer) Uri() string {
	scheme := "tcp"
	if c.Kind == "ws" {
		scheme = "ws"
	}
	return fmt.Sprintf("%s://127.0.0.1:%d", scheme, c.Port())
}

// Poll is ``proc.poll()``: nil while running, else the exit error.
func (c *ChildPeer) Running() bool {
	if c.Proc == nil {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

// Kill is ``kill()``.
func (c *ChildPeer) Kill() {
	if c.Running() {
		_ = c.Proc.Process.Kill()
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
		}
	}
}

// Quit is ``quit(timeout=20.0)``: ask the child to terminate; returns the
// exit code.
func (c *ChildPeer) Quit(timeout ...float64) int {
	to := 20.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	if c.Proc == nil {
		return 0
	}
	if c.Running() {
		_, _ = c.stdin.WriteString("QUIT\n")
		_ = c.stdin.Flush()
		select {
		case <-c.done:
		case <-time.After(time.Duration(to * float64(time.Second))):
			c.Kill()
		}
	}
	if c.exit_err == nil {
		return 0
	}
	if ee, ok := c.exit_err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return -1
}

// Exit is ``__exit__``.
func (c *ChildPeer) Exit() { c.Quit() }

func kw_dict_(kw py.Kw) *py.Dict {
	d := py.NewDict()
	for k, v := range kw {
		d.Set(k, v)
	}
	return d
}

// As_policy is ``policy`` as its base struct (DefaultPolicy returns ``any``).
func As_policy(v any) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	return v.(interface {
		Policy_base_() *laila.LAILA_IDENTIFIABLE_POLICY_
	}).Policy_base_()
}

// Child_main is the body of ``_CHILD_TEMPLATE``: runs in the re-executed
// test binary when CHILD_ENV is set. Returns the process exit code.
func Child_main() int {
	nickname := os.Getenv(CHILD_ENV)
	kind := os.Getenv(CHILD_ENV + "_KIND")
	home := os.Getenv(CHILD_ENV + "_HOME")
	liveness, _ := strconv.ParseFloat(os.Getenv(CHILD_ENV+"_LIVENESS"), 64)
	extra := os.Getenv(CHILD_ENV + "_EXTRA")
	proto_kwargs := py.Kw{}
	if raw := os.Getenv(CHILD_ENV + "_PROTO_KWARGS"); raw != "" {
		if d, err := py.Json_loads(raw); err == nil {
			for _, it := range d.(*py.Dict).Items() {
				proto_kwargs[py.Str(it.Key)] = it.Value
			}
		}
	}
	args := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, CHILD_ENV+"_ARG_") {
			args[strings.TrimPrefix(k, CHILD_ENV+"_ARG_")] = v
		}
	}
	fail := func(err error) int {
		fmt.Fprintln(os.Stderr, "child peer:", err)
		return 1
	}
	laila.Set_default_directory(home)
	policy, err := laila.New_DefaultPolicy(py.Kw{"nickname": nickname})
	if err != nil {
		return fail(err)
	}
	laila.Activate_policy(policy)
	store, err := laila.New_DefaultPool()
	if err != nil {
		return fail(err)
	}
	if err := Mem(As_policy(policy)).Extend(store, py.Kw{"pool_nickname": "store"}); err != nil {
		return fail(err)
	}
	kw := proto_kwargs.Copy()
	kw["host"], kw["port"], kw["peer_secret_key"] = "127.0.0.1", 0, py.Uuid4().Hex()
	var proto laila.Comm_protocol_
	if kind == "ws" {
		proto, err = laila.New_DefaultTCPIPProtocol(kw)
	} else {
		proto, err = laila.New_DefaultTCPProtocol(kw)
	}
	if err != nil {
		return fail(err)
	}
	comm := Comm(As_policy(policy))
	comm.Liveness_interval = liveness
	if err := comm.Add_connection(proto); err != nil {
		return fail(err)
	}
	if extra != "" {
		fn, ok := Child_extras[extra]
		if !ok {
			return fail(py.KeyError.New(extra))
		}
		if err := fn(args); err != nil {
			return fail(err)
		}
	}
	fmt.Printf("PORT=%d\n", Bound_port(proto))
	fmt.Printf("SECRET=%s\n", Secret_of(proto))
	fmt.Printf("POLICY_ID=%s\n", As_policy(policy).Global_id())
	fmt.Println("READY")
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "QUIT" {
			break
		}
	}
	laila.Terminate()
	return 0
}

// ---------------------------------------------------------------------------
// subprocess scripts (``subprocess.run([sys.executable, "-c", code])``)
// ---------------------------------------------------------------------------

// SCRIPT_ENV selects a Child_scripts entry when the test binary is
// re-executed as a plain script child (no peer policy, no transport).
const SCRIPT_ENV = "LAILA_WORKLOAD_SCRIPT"

// Child_scripts are the Go twins of the ``textwrap.dedent("""...""")`` code
// strings the Python suite runs with ``python -c``: named funcs registered
// by the test files, run in a fresh process with ``args`` from the
// environment. The return value is the process exit code.
var Child_scripts = map[string]func(args map[string]string) int{}

// ScriptResult is the ``subprocess.CompletedProcess`` of a child script.
type ScriptResult struct {
	Returncode int
	Stdout     string
	Stderr     string
}

// script_cmd_ builds the re-exec command for a child script.
func script_cmd_(name string, args map[string]string) (*exec.Cmd, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, py.FromGo(err)
	}
	cmd := exec.Command(exe, "-test.run", "^$")
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, SCRIPT_ENV) && !strings.HasPrefix(kv, CHILD_ENV) {
			env = append(env, kv)
		}
	}
	env = append(env, SCRIPT_ENV+"="+name)
	for k, v := range args {
		env = append(env, SCRIPT_ENV+"_ARG_"+k+"="+v)
	}
	cmd.Env = env
	return cmd, nil
}

// Run_script is ``subprocess.run([sys.executable, "-c", code],
// capture_output=True, text=True, timeout=timeout)`` for the named child
// script. A timeout kills the child and returns ``TimeoutExpired``.
func Run_script(name string, args map[string]string, timeout ...float64) (*ScriptResult, error) {
	to := 120.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	cmd, err := script_cmd_(name, args)
	if err != nil {
		return nil, err
	}
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Start(); err != nil {
		return nil, py.FromGo(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case werr := <-done:
		res := &ScriptResult{Stdout: out.String(), Stderr: errb.String()}
		if werr != nil {
			if ee, ok := werr.(*exec.ExitError); ok {
				res.Returncode = ee.ExitCode()
			} else {
				return nil, py.FromGo(werr)
			}
		}
		return res, nil
	case <-time.After(time.Duration(to * float64(time.Second))):
		_ = cmd.Process.Kill()
		<-done
		return nil, py.TimeoutError.New(fmt.Sprintf("child script %q timed out after %.0fs", name, to))
	}
}

// Script_proc is a ``subprocess.Popen`` child script: started now,
// collected with Communicate.
type Script_proc struct {
	Cmd  *exec.Cmd
	out  strings.Builder
	errb strings.Builder
	done chan error
}

// Popen_script is ``subprocess.Popen([sys.executable, "-c", code], ...)``.
func Popen_script(name string, args map[string]string) (*Script_proc, error) {
	cmd, err := script_cmd_(name, args)
	if err != nil {
		return nil, err
	}
	p := &Script_proc{Cmd: cmd, done: make(chan error, 1)}
	cmd.Stdout, cmd.Stderr = &p.out, &p.errb
	if err := cmd.Start(); err != nil {
		return nil, py.FromGo(err)
	}
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

// Communicate is ``proc.communicate(timeout=...)``.
func (p *Script_proc) Communicate(timeout ...float64) (*ScriptResult, error) {
	to := 300.0
	if len(timeout) > 0 {
		to = timeout[0]
	}
	select {
	case werr := <-p.done:
		res := &ScriptResult{Stdout: p.out.String(), Stderr: p.errb.String()}
		if werr != nil {
			if ee, ok := werr.(*exec.ExitError); ok {
				res.Returncode = ee.ExitCode()
			} else {
				return nil, py.FromGo(werr)
			}
		}
		return res, nil
	case <-time.After(time.Duration(to * float64(time.Second))):
		_ = p.Cmd.Process.Kill()
		<-p.done
		return nil, py.TimeoutError.New("child script timed out")
	}
}

// Script_main runs the Child_scripts entry named by SCRIPT_ENV; the body
// of ``python -c`` in the re-executed binary.
func Script_main() int {
	name := os.Getenv(SCRIPT_ENV)
	fn, ok := Child_scripts[name]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown child script:", name)
		return 1
	}
	args := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, SCRIPT_ENV+"_ARG_") {
			args[strings.TrimPrefix(k, SCRIPT_ENV+"_ARG_")] = v
		}
	}
	return fn(args)
}

// Tail is ``s[-n:]``.
func Tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// ---------------------------------------------------------------------------
// misc
// ---------------------------------------------------------------------------

// Shuffled is ``shuffled(seq, seed=0)``.
func Shuffled[T any](seq []T, seed ...int) []T {
	s := int64(0)
	if len(seed) > 0 {
		s = int64(seed[0])
	}
	out := append([]T(nil), seq...)
	rand.New(rand.NewSource(s)).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// Range is ``list(range(n))`` as ints.
func Range(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// Range_list is ``list(range(n))`` as a *py.List.
func Range_list(n int) *py.List {
	l := py.NewList()
	for i := 0; i < n; i++ {
		l.Append(i)
	}
	return l
}
