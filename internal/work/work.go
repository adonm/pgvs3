package work

import (
	"context"
	"log/slog"
	"math"
	"time"
	"uuid"
)

type PeriodicOptions struct {
	Name     string
	Interval time.Duration
	Timeout  time.Duration

	Drain bool
	Log   *slog.Logger
}

func Periodic(ctx context.Context, opts PeriodicOptions, fn func(context.Context) (bool, error)) {
	interval := opts.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		for ctx.Err() == nil {
			runCtx := ctx
			cancel := func() {}
			if opts.Timeout > 0 {
				runCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
			}
			more, err := fn(runCtx)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					log.Error(opts.Name, "err", err)
				}
				break
			}
			if !opts.Drain || !more {
				break
			}
		}
		timer.Reset(interval)
	}
}

type Backoff struct {
	Initial time.Duration
	Factor  float64
	Max     time.Duration
}

func (b Backoff) Delay(attempt int) time.Duration {
	if b.Initial <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}
	delay := float64(b.Initial) * math.Pow(factor, float64(attempt-1))
	if b.Max > 0 && delay > float64(b.Max) {
		delay = float64(b.Max)
	}
	if delay > float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(math.Round(delay))
}

func NewID() string { return uuid.NewV7().String() }
