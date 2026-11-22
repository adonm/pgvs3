package syncx

import "sync"

// Result holds the outcome of a singleflight call.
type Result struct {
	Val any
	Err error
}

type call struct {
	done chan struct{}
	res  Result
}

// Group deduplicates concurrent calls for the same key.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) DoChan(key string, fn func() (any, error)) <-chan Result {
	out := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		go func() {
			<-c.done
			out <- c.res
		}()
		return out
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		defer func() {
			g.mu.Lock()
			delete(g.m, key)
			g.mu.Unlock()
			close(c.done)
			out <- c.res
		}()
		c.res.Val, c.res.Err = fn()
	}()
	return out
}

// Weighted is a non-blocking weighted semaphore.
type Weighted struct {
	mu  sync.Mutex
	cur int64
	max int64
}

func NewWeighted(n int64) *Weighted {
	return &Weighted{max: n}
}

func (w *Weighted) TryAcquire(n int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur+n > w.max {
		return false
	}
	w.cur += n
	return true
}

func (w *Weighted) Release(n int64) {
	w.mu.Lock()
	w.cur -= n
	if w.cur < 0 {
		w.cur = 0
	}
	w.mu.Unlock()
}
