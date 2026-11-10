package s3util

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/smithy-go"
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

// TestSleepCtxCanceled checks that a context that has already ended wins
// over a timer that has fired, every time.
func TestSleepCtxCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, d := range []time.Duration{0, 1} {
		for range 1000 {
			if err := SleepCtx(ctx, d); !errors.Is(err, context.Canceled) {
				t.Fatalf("SleepCtx(canceled, %v) = %v, want context.Canceled", d, err)
			}
		}
	}
}

// apiErr is an S3 error as the SDK returns it: a status and a code.
func apiErr(status int, code string) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      &smithy.GenericAPIError{Code: code, Message: code},
	}
}

// ErrorCode reads the code through wrapping; Denied is a 403, or the code
// AccessDenied with any status.
func TestErrorCodeAndDenied(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		code   string
		denied bool
	}{
		{"nil", nil, "", false},
		{"non-S3", errors.New("boom"), "", false},
		{"403 AccessDenied", fmt.Errorf("get k: %w", apiErr(http.StatusForbidden, "AccessDenied")), "AccessDenied", true},
		{"403, another code", apiErr(http.StatusForbidden, "SignatureDoesNotMatch"), "SignatureDoesNotMatch", true},
		{"403, no code", respErr(http.StatusForbidden), "", true},
		{"AccessDenied, no status", &smithy.GenericAPIError{Code: "AccessDenied"}, "AccessDenied", true},
		{"500", apiErr(http.StatusInternalServerError, "InternalError"), "InternalError", false},
		{"404", apiErr(http.StatusNotFound, "NoSuchKey"), "NoSuchKey", false},
		{"401", apiErr(http.StatusUnauthorized, "Unauthorized"), "Unauthorized", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorCode(tc.err); got != tc.code {
				t.Errorf("ErrorCode = %q, want %q", got, tc.code)
			}
			if got := Denied(tc.err); got != tc.denied {
				t.Errorf("Denied = %v, want %v", got, tc.denied)
			}
		})
	}
}
