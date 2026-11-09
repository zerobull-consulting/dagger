package exporter

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSleepCtxCause(t *testing.T) {
	cause := errors.New("stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, cause) {
		t.Fatalf("sleepCtx = %v, want the cause %v", err, cause)
	}
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("sleepCtx = %v", err)
	}
}
