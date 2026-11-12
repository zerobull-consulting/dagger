// Package s3util holds the small helpers the cache service's S3 call sites
// share: reading an error's HTTP status, and sleeping until a context ends.
package s3util

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// HTTPStatus is the HTTP status of an S3 error, or 0 when err is nil or
// carries no HTTP response.
func HTTPStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// ErrorCode is an S3 error's code, such as AccessDenied, or "" when err
// carries none.
func ErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// Denied reports an error of a key that isn't allowed the request: a 403,
// or an AccessDenied error code.
func Denied(err error) bool {
	return HTTPStatus(err) == http.StatusForbidden || ErrorCode(err) == "AccessDenied"
}

// HasStatus reports whether err carries an HTTP response with status.
func HasStatus(err error, status int) bool {
	return status != 0 && HTTPStatus(err) == status
}

// SleepCtx waits for d, or until ctx ends, in which case it returns ctx.Err().
// A context that has already ended always returns its error: the select
// below would pick at random between it and a timer that has already fired.
func SleepCtx(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
