package s3util

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func respErr(status int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      errors.New("s3 failed"),
	}
}

func TestHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"non-HTTP", errors.New("boom"), 0},
		{"response error", respErr(http.StatusNotFound), http.StatusNotFound},
		{"wrapped", fmt.Errorf("get: %w", respErr(http.StatusPreconditionFailed)), http.StatusPreconditionFailed},
		{"wrapped twice", fmt.Errorf("a: %w", fmt.Errorf("b: %w", respErr(http.StatusConflict))), http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := HTTPStatus(tc.err); got != tc.want {
				t.Errorf("HTTPStatus = %d, want %d", got, tc.want)
			}
			if tc.want != 0 && !HasStatus(tc.err, tc.want) {
				t.Errorf("HasStatus(%d) = false", tc.want)
			}
			if HasStatus(tc.err, http.StatusTeapot) {
				t.Errorf("HasStatus(418) = true")
			}
		})
	}
}

func TestSleepCtx(t *testing.T) {
	if err := SleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("SleepCtx = %v", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("cause"))
	// ctx.Err(), not the cause: the exporter adapts it (exporter.TestSleepCtxCause).
	if err := SleepCtx(ctx, time.Hour); err != ctx.Err() {
		t.Fatalf("SleepCtx on a cancelled context = %v, want %v", err, ctx.Err())
	}
}
