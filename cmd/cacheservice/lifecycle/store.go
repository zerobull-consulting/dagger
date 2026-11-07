package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// CallTimeout bounds each request to the store.
const CallTimeout = 30 * time.Second

// Config is a bucket's lifecycle configuration.
type Config struct {
	Rules []types.LifecycleRule
	// TransitionDefaultMinimumObjectSize is written back as it was read.
	TransitionDefaultMinimumObjectSize types.TransitionDefaultMinimumObjectSize
}

// Store is what the command needs of the bucket.
type Store interface {
	// GetLifecycle reads the bucket's configuration: no rules when it has
	// none.
	GetLifecycle(ctx context.Context) (Config, error)
	// PutLifecycle replaces it; with no rules, it deletes it.
	PutLifecycle(ctx context.Context, cfg Config) error
	// ReadMarker reads the prefix's ownership marker (ownership.ReadMarker).
	ReadMarker(ctx context.Context, prefix string) (ownership.Marker, error)
}

// S3Store is a Store in S3. Every request is bounded by CallTimeout.
type S3Store struct {
	Client *s3.Client
	Bucket string
}

var _ Store = (*S3Store)(nil)

func (s *S3Store) GetLifecycle(ctx context.Context) (Config, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	out, err := s.Client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: &s.Bucket})
	if err != nil {
		if errorCode(err) == "NoSuchLifecycleConfiguration" {
			return Config{}, nil
		}
		return Config{}, err
	}
	return Config{Rules: out.Rules, TransitionDefaultMinimumObjectSize: out.TransitionDefaultMinimumObjectSize}, nil
}

func (s *S3Store) PutLifecycle(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	if len(cfg.Rules) == 0 {
		_, err := s.Client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: &s.Bucket})
		return err
	}
	_, err := s.Client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket:                             &s.Bucket,
		LifecycleConfiguration:             &types.BucketLifecycleConfiguration{Rules: cfg.Rules},
		TransitionDefaultMinimumObjectSize: cfg.TransitionDefaultMinimumObjectSize,
	})
	return err
}

func (s *S3Store) ReadMarker(ctx context.Context, prefix string) (ownership.Marker, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	return ownership.ReadMarker(ctx, &metastore.S3Objects{Client: s.Client, Bucket: s.Bucket}, prefix)
}

// errorCode is an S3 error's code, such as AccessDenied.
func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// httpStatus is an S3 error's HTTP status, or 0.
func httpStatus(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// denied reports an error of a key that isn't allowed the request.
func denied(err error) bool {
	return httpStatus(err) == http.StatusForbidden || errorCode(err) == "AccessDenied"
}

// explainRead is an error reading the configuration, with what to do.
func explainRead(err error) error {
	if denied(err) {
		return fmt.Errorf("can't read the bucket's lifecycle configuration: the key isn't allowed to (%w). "+
			"Use a key with s3:GetLifecycleConfiguration; apply needs the admin key, which also reads it", err)
	}
	return fmt.Errorf("can't read the bucket's lifecycle configuration: %w", err)
}

// explainWrite is an error writing the configuration, with what to do.
func explainWrite(err error, abortRule bool) error {
	switch {
	case denied(err):
		return fmt.Errorf("the store refused to set the lifecycle configuration (%w). "+
			"Setting it takes the admin key (on DigitalOcean Spaces, a full-access key): a Read or Read/Write/Delete key gets 403. "+
			"Run apply with it, interactively; never put it in the service's or a job's secrets", err)
	case abortRule && httpStatus(err) == http.StatusBadRequest:
		return fmt.Errorf("the store refused the configuration (%w). "+
			"Some stores refuse a rule that aborts incomplete multipart uploads (MinIO does, and drops stale uploads itself after 24h): "+
			"run again with -abort-rule=false on such a store", err)
	}
	return fmt.Errorf("can't set the bucket's lifecycle configuration: %w", err)
}
