// Package s3util holds the small helpers the cache service's S3 call sites
// share: reading an error's HTTP status, and sleeping until a context ends.
package s3util

import (
	"context"
	"errors"
	"time"

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

// HasStatus reports whether err carries an HTTP response with status.
func HasStatus(err error, status int) bool {
	return status != 0 && HTTPStatus(err) == status
}

// SleepCtx waits for d, or until ctx ends, in which case it returns ctx.Err().
func SleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
