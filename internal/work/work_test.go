package work

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Initial: time.Second, Factor: 2, Max: 10 * time.Second}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 10 * time.Second},
		{99, 10 * time.Second},
	}
	for _, tc := range tests {
		if got := b.Delay(tc.attempt); got != tc.want {
			t.Fatalf("Delay(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
	if got := (Backoff{}).Delay(1); got != 0 {
		t.Fatalf("zero backoff = %s, want 0", got)
	}
}

func TestNewIDIsUnique(t *testing.T) {
	seen := make(map[string]bool, 100)
	for range 100 {
		id := NewID()
		if id == "" {
			t.Fatal("empty id")
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestPeriodicDrainsAndStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		Periodic(ctx, PeriodicOptions{
			Name:     "test",
			Interval: time.Millisecond,
			Timeout:  time.Second,
			Drain:    true,
		}, func(context.Context) (bool, error) {
			calls++
			if calls == 3 {
				cancel()
			}
			return calls < 3, nil
		})
		if calls != 3 {
			t.Fatalf("calls = %d, want 3", calls)
		}
	})
}
