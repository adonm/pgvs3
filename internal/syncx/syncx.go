package syncx

import (
	"context"
	"sync"
)

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

// ErrGroup runs goroutines with context cancellation and optional concurrency limit.
type ErrGroup struct {
	cancel  func()
	wg      sync.WaitGroup
	sem     chan struct{}
	errOnce sync.Once
	err     error
}

func WithContext(ctx context.Context) (*ErrGroup, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &ErrGroup{cancel: cancel}, ctx
}

func (g *ErrGroup) SetLimit(n int) {
	if n > 0 {
		g.sem = make(chan struct{}, n)
	}
}

func (g *ErrGroup) Go(fn func() error) {
	if g.sem != nil {
		g.sem <- struct{}{}
	}
	g.wg.Go(func() {
		if g.sem != nil {
			defer func() { <-g.sem }()
		}
		if err := fn(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel()
				}
			})
		}
	})
}

func (g *ErrGroup) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
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
