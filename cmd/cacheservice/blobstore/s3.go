package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/opencontainers/go-digest"
)

const (
	// DefaultPutExpiry is how long a presigned PUT works. Short: the engine
	// starts its uploads as soon as it gets the URLs.
	DefaultPutExpiry = 15 * time.Minute
	// DefaultGetExpiry is how long a presigned GET works. The engine asks for
	// fresh ones (renew) when they run out.
	DefaultGetExpiry = time.Hour
	// maxPresignExpiry is SigV4's limit on a presigned URL's lifetime.
	maxPresignExpiry = 7 * 24 * time.Hour
)

// S3Config configures an S3Store.
type S3Config struct {
	// Endpoint is the S3 API's base URL, such as http://localhost:9000.
	// Empty means AWS's own endpoint for Region.
	Endpoint string
	// Region is the bucket's region. Presigned URLs are signed for it.
	Region string
	// Bucket holds the blobs.
	Bucket string
	// Prefix is prepended to every key, with a slash: blobs are stored at
	// <Prefix>/blobs/<algorithm>/<hex>. Empty stores them at the bucket's top.
	Prefix string
	// PathStyle addresses the bucket in the URL's path, not its host name.
	// MinIO and most other S3-compatible stores need it.
	PathStyle bool
	// PutExpiry is how long a presigned PUT works. Zero means
	// DefaultPutExpiry.
	PutExpiry time.Duration
	// GetExpiry is how long a presigned GET works. Zero means
	// DefaultGetExpiry.
	GetExpiry time.Duration
	// Credentials signs requests and presigned URLs. Nil means the AWS SDK's
	// default chain (environment, shared config, instance role, ...).
	Credentials aws.CredentialsProvider
	// HTTPClient sends the store's own requests (HeadObject). Nil means the
	// SDK's default.
	HTTPClient aws.HTTPClient
}

// Environment variables read by S3ConfigFromEnv. They match what
// hack/remote-cache/dev.sh prints.
const (
	EnvEndpoint        = "CACHESERVICE_S3_ENDPOINT"
	EnvRegion          = "CACHESERVICE_S3_REGION"
	EnvBucket          = "CACHESERVICE_S3_BUCKET"
	EnvPrefix          = "CACHESERVICE_S3_PREFIX"
	EnvPathStyle       = "CACHESERVICE_S3_PATH_STYLE"
	EnvAccessKeyID     = "CACHESERVICE_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey = "CACHESERVICE_S3_SECRET_ACCESS_KEY"
	EnvPutExpiry       = "CACHESERVICE_S3_PUT_EXPIRY"
	EnvGetExpiry       = "CACHESERVICE_S3_GET_EXPIRY"
)

// S3ConfigFromEnv reads an S3Config from the CACHESERVICE_S3_* variables.
// Expiries are Go durations (15m, 1h). Static credentials are used when both
// the access key ID and the secret are set; otherwise Credentials is nil, and
// the SDK's default chain applies.
func S3ConfigFromEnv() (S3Config, error) {
	cfg := S3Config{
		Endpoint: os.Getenv(EnvEndpoint),
		Region:   os.Getenv(EnvRegion),
		Bucket:   os.Getenv(EnvBucket),
		Prefix:   os.Getenv(EnvPrefix),
	}
	if v := os.Getenv(EnvPathStyle); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return S3Config{}, fmt.Errorf("%s: %w", EnvPathStyle, err)
		}
		cfg.PathStyle = b
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{{EnvPutExpiry, &cfg.PutExpiry}, {EnvGetExpiry, &cfg.GetExpiry}} {
		if v := os.Getenv(d.name); v != "" {
			dur, err := time.ParseDuration(v)
			if err != nil {
				return S3Config{}, fmt.Errorf("%s: %w", d.name, err)
			}
			*d.dst = dur
		}
	}
	id, secret := os.Getenv(EnvAccessKeyID), os.Getenv(EnvSecretAccessKey)
	switch {
	case id != "" && secret != "":
		cfg.Credentials = credentials.NewStaticCredentialsProvider(id, secret, "")
	case id != "" || secret != "":
		return S3Config{}, fmt.Errorf("set both %s and %s, or neither", EnvAccessKeyID, EnvSecretAccessKey)
	}
	return cfg, nil
}

// ErrNotPositiveSize is returned for a PUT of an empty blob, which a presigned
// URL can't bound.
var ErrNotPositiveSize = errors.New("a presigned PUT needs a positive size")

// S3Store is a Store in an S3 bucket.
type S3Store struct {
	client    *s3.Client
	presign   *s3.PresignClient
	bucket    string
	prefix    string
	putExpiry time.Duration
	getExpiry time.Duration
	now       func() time.Time
}

var _ Store = (*S3Store)(nil)

// NewS3 returns an S3Store for cfg. It makes no requests.
func NewS3(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 blob store: no bucket")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 blob store: no region")
	}
	if cfg.PutExpiry == 0 {
		cfg.PutExpiry = DefaultPutExpiry
	}
	if cfg.GetExpiry == 0 {
		cfg.GetExpiry = DefaultGetExpiry
	}
	for _, e := range []struct {
		name string
		exp  time.Duration
	}{{"PUT", cfg.PutExpiry}, {"GET", cfg.GetExpiry}} {
		if e.exp < time.Second || e.exp > maxPresignExpiry {
			return nil, fmt.Errorf("s3 blob store: %s expiry %s is outside [1s, %s]", e.name, e.exp, maxPresignExpiry)
		}
	}

	creds := cfg.Credentials
	if creds == nil {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
		if err != nil {
			return nil, fmt.Errorf("s3 blob store: load AWS config: %w", err)
		}
		creds = awsCfg.Credentials
	}
	opts := s3.Options{
		Region:       cfg.Region,
		Credentials:  creds,
		UsePathStyle: cfg.PathStyle,
		HTTPClient:   cfg.HTTPClient,
		// Without this, the SDK adds checksum headers to presigned PUTs, and
		// the engine's upload sends none of them.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	client := s3.New(opts)
	return &S3Store{
		client:    client,
		presign:   s3.NewPresignClient(client),
		bucket:    cfg.Bucket,
		prefix:    strings.Trim(cfg.Prefix, "/"),
		putExpiry: cfg.PutExpiry,
		getExpiry: cfg.GetExpiry,
		now:       time.Now,
	}, nil
}

// Key returns the object key of a blob: <prefix>/blobs/<algorithm>/<hex>.
func (s *S3Store) Key(dgst digest.Digest) (string, error) {
	return blobKey(s.prefix, dgst)
}

func blobKey(prefix string, dgst digest.Digest) (string, error) {
	if err := dgst.Validate(); err != nil {
		return "", fmt.Errorf("blob key: %w", err)
	}
	key := "blobs/" + dgst.Algorithm().String() + "/" + dgst.Encoded()
	if prefix != "" {
		key = prefix + "/" + key
	}
	return key, nil
}

// Has reports whether the blob is stored, with HeadObject.
func (s *S3Store) Has(ctx context.Context, dgst digest.Digest) (bool, error) {
	key, err := s.Key(dgst)
	if err != nil {
		return false, err
	}
	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("head %s: %w", key, err)
}

// isNotFound reports a 404 and nothing else: a HEAD response has no body, so
// the status is all S3 says.
func isNotFound(err error) bool {
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
}

// PresignPut returns a presigned PUT of exactly size bytes. Content-Length is
// signed, so S3 refuses a body of any other size. The store does not check
// the body against dgst.
//
// size must be positive: SigV4 never signs a Content-Length of 0, so a PUT
// presigned for an empty blob would take a body of any size. Layer blobs are
// compressed archives and never empty.
func (s *S3Store) PresignPut(ctx context.Context, dgst digest.Digest, size int64) (Address, error) {
	if size <= 0 {
		return Address{}, fmt.Errorf("presign PUT %s: size %d: %w", dgst, size, ErrNotPositiveSize)
	}
	key, err := s.Key(dgst)
	if err != nil {
		return Address{}, err
	}
	// Taken before signing, so the URL works at least until ExpiresAt.
	signedAt := s.now()
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &key,
		ContentLength: aws.Int64(size),
	}, s3.WithPresignExpires(s.putExpiry))
	if err != nil {
		return Address{}, fmt.Errorf("presign PUT %s: %w", key, err)
	}
	return Address{URL: req.URL, ExpiresAt: s.expiresAt(ctx, signedAt, s.putExpiry)}, nil
}

// PresignGet returns a presigned GET of the blob. It does not check that the
// blob exists.
func (s *S3Store) PresignGet(ctx context.Context, dgst digest.Digest) (Address, error) {
	key, err := s.Key(dgst)
	if err != nil {
		return Address{}, err
	}
	// Taken before signing, so the URL works at least until ExpiresAt.
	signedAt := s.now()
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}, s3.WithPresignExpires(s.getExpiry))
	if err != nil {
		return Address{}, fmt.Errorf("presign GET %s: %w", key, err)
	}
	return Address{URL: req.URL, ExpiresAt: s.expiresAt(ctx, signedAt, s.getExpiry)}, nil
}

// expiresAt is when a URL signed at signedAt for expiry stops working: at the
// end of its expiry, or earlier when the credentials that signed it are
// temporary (STS, an instance role) and expire first.
func (s *S3Store) expiresAt(ctx context.Context, signedAt time.Time, expiry time.Duration) time.Time {
	at := signedAt.Add(expiry)
	// Presigning has just retrieved these, so the provider's cache answers.
	creds, err := s.client.Options().Credentials.Retrieve(ctx)
	if err == nil && creds.CanExpire && creds.Expires.Before(at) {
		return creds.Expires
	}
	return at
}
