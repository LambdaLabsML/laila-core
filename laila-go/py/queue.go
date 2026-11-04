package py

import (
	"container/heap"
	"sync"
	"time"
)

// queue.PriorityQueue -- a thread-safe min-heap ordered with Python's
// ``<`` (Lt) over the stored items (tuples compare lexicographically, so
// ``(-affinity, global_id)`` sorts highest-affinity first).

type pqItems []any

func (h pqItems) Len() int { return len(h) }
func (h pqItems) Less(i, j int) bool {
	lt, err := Lt(h[i], h[j])
	if err != nil {
		panic(err)
	}
	return lt
}
func (h pqItems) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pqItems) Push(x any)   { *h = append(*h, x) }
func (h *pqItems) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
func (h pqItems) Peek() (any, bool) {
	if len(h) == 0 {
		return nil, false
	}
	return h[0], true
}

// PriorityQueue is queue.PriorityQueue(maxsize=0).
type PriorityQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items pqItems
}

// New_PriorityQueue creates an unbounded priority queue.
func New_PriorityQueue() *PriorityQueue {
	q := &PriorityQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Put is q.put(item).
func (q *PriorityQueue) Put(item any) {
	q.mu.Lock()
	heap.Push(&q.items, item)
	q.cond.Signal()
	q.mu.Unlock()
}

// Get is q.get(): blocks until an item is available.
func (q *PriorityQueue) Get() any {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 {
		q.cond.Wait()
	}
	return heap.Pop(&q.items)
}

// Get_nowait is q.get_nowait(): queue.Empty (as a LookupError) when empty.
func (q *PriorityQueue) Get_nowait() (any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, Queue_Empty.New()
	}
	return heap.Pop(&q.items), nil
}

// Qsize is q.qsize().
func (q *PriorityQueue) Qsize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Empty is q.empty().
func (q *PriorityQueue) Empty() bool { return q.Qsize() == 0 }

// Items is a sorted snapshot (smallest first) -- a port-only inspection
// helper (“sorted(q.queue)“).
func (q *PriorityQueue) Items() []any {
	q.mu.Lock()
	defer q.mu.Unlock()
	cp := append(pqItems(nil), q.items...)
	out := make([]any, 0, len(cp))
	for len(cp) > 0 {
		out = append(out, heap.Pop(&cp))
	}
	return out
}

// Queue_Empty is queue.Empty.
var Queue_Empty = NewType("Empty", Exception)

// Queue_Full is queue.Full.
var Queue_Full = NewType("Full", Exception)

// Queue is queue.Queue: a thread-safe FIFO with an optional bound and
// blocking “get“/“put“ (both honour a timeout in seconds; “<0“
// waits forever).
type Queue struct {
	mu         sync.Mutex
	notEmpty   *sync.Cond
	notFull    *sync.Cond
	items      []any
	maxsize    int
	unfinished int
	allDone    *sync.Cond
}

// New_Queue is queue.Queue(maxsize); “maxsize <= 0“ means unbounded.
func New_Queue(maxsize int) *Queue {
	q := &Queue{maxsize: maxsize}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	q.allDone = sync.NewCond(&q.mu)
	return q
}

// Qsize is q.qsize().
func (q *Queue) Qsize() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }

// Empty is q.empty().
func (q *Queue) Empty() bool { return q.Qsize() == 0 }

// Full is q.full().
func (q *Queue) Full() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxsize > 0 && len(q.items) >= q.maxsize
}

// Maxsize is q.maxsize.
func (q *Queue) Maxsize() int { return q.maxsize }

// Put_nowait is q.put_nowait(item): queue.Full when bounded and full.
func (q *Queue) Put_nowait(item any) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.maxsize > 0 && len(q.items) >= q.maxsize {
		return Queue_Full.New()
	}
	q.items = append(q.items, item)
	q.unfinished++
	q.notEmpty.Signal()
	return nil
}

// Put is q.put(item, timeout=): blocks while full; queue.Full on timeout.
func (q *Queue) Put(item any, timeout float64) error {
	if q.maxsize <= 0 {
		return q.Put_nowait(item)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var deadline time.Time
	if timeout >= 0 {
		d, _ := Duration(timeout)
		deadline = time.Now().Add(d)
	}
	for len(q.items) >= q.maxsize {
		if timeout >= 0 {
			rem := time.Until(deadline)
			if rem <= 0 {
				return Queue_Full.New()
			}
			timedWait(q.notFull, &q.mu, rem)
		} else {
			q.notFull.Wait()
		}
	}
	q.items = append(q.items, item)
	q.unfinished++
	q.notEmpty.Signal()
	return nil
}

// Get_nowait is q.get_nowait(): queue.Empty when nothing is queued.
func (q *Queue) Get_nowait() (any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, Queue_Empty.New()
	}
	item := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	q.notFull.Signal()
	return item, nil
}

// Get is q.get(timeout=): blocks until an item arrives; queue.Empty on
// timeout (“timeout < 0“ waits forever).
func (q *Queue) Get(timeout float64) (any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var deadline time.Time
	if timeout >= 0 {
		d, _ := Duration(timeout)
		deadline = time.Now().Add(d)
	}
	for len(q.items) == 0 {
		if timeout >= 0 {
			rem := time.Until(deadline)
			if rem <= 0 {
				return nil, Queue_Empty.New()
			}
			timedWait(q.notEmpty, &q.mu, rem)
		} else {
			q.notEmpty.Wait()
		}
	}
	item := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	q.notFull.Signal()
	return item, nil
}

// Task_done is q.task_done().
func (q *Queue) Task_done() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.unfinished <= 0 {
		return ValueError.New("task_done() called too many times")
	}
	q.unfinished--
	if q.unfinished == 0 {
		q.allDone.Broadcast()
	}
	return nil
}

// Join is q.join().
func (q *Queue) Join() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.unfinished > 0 {
		q.allDone.Wait()
	}
}

// Items is a snapshot of the queued items (“list(q.queue)“).
func (q *Queue) Items() []any {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]any(nil), q.items...)
}

// timedWait waits on cond for at most d (the mutex is held on entry and
// exit, as with cond.Wait).
func timedWait(cond *sync.Cond, mu *sync.Mutex, d time.Duration) {
	tm := time.AfterFunc(d, func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	})
	cond.Wait()
	tm.Stop()
}
