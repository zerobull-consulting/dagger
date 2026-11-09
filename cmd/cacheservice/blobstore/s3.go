package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencontainers/go-digest"

	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
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
	// DefaultMaxPromotions bounds the promotions in flight per store,
	// across its namespaces.
	DefaultMaxPromotions = 8
	// maxPartCopy is S3's largest UploadPartCopy: 5 GiB. A blob up to this
	// size is promoted with one part, which the store hashes as it copies.
	maxPartCopy = 5 << 30
	// defaultCopyPartSize is the part size of a blob over maxPartCopy,
	// which the service hashes itself. S3 allows 10,000 parts.
	defaultCopyPartSize = 1 << 30
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
	// Prefix is the service's prefix: a namespace's blobs are stored at
	// <Prefix>/ns/<namespace>/blobs/<algorithm>/<hex> (see package layout).
	// Empty puts ns/ at the bucket's top. The service owns everything under
	// it (see the README, "Bucket ownership").
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
	// HTTPClient sends the store's own requests (HeadObject, the
	// promotion's copies and reads). Nil means the SDK's default.
	HTTPClient aws.HTTPClient
	// MaxPromotions bounds the promotions in flight at once, across every
	// namespace. Zero means DefaultMaxPromotions.
	MaxPromotions int
	// HashInService makes the service hash every staged blob by reading it,
	// instead of having the store hash it as it copies. Use it for a store
	// whose part copies return a SHA-256 checksum that can't be trusted.
	// A store that returns none is detected, and hashed by the service.
	HashInService bool
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
	EnvMaxPromotions   = "CACHESERVICE_S3_MAX_PROMOTIONS"
	EnvHashInService   = "CACHESERVICE_S3_HASH_IN_SERVICE"
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
	for _, b := range []struct {
		name string
		dst  *bool
	}{{EnvPathStyle, &cfg.PathStyle}, {EnvHashInService, &cfg.HashInService}} {
		if v := os.Getenv(b.name); v != "" {
			val, err := strconv.ParseBool(v)
			if err != nil {
				return S3Config{}, fmt.Errorf("%s: %w", b.name, err)
			}
			*b.dst = val
		}
	}
	if v := os.Getenv(EnvMaxPromotions); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return S3Config{}, fmt.Errorf("%s: %q is not a positive number", EnvMaxPromotions, v)
		}
		cfg.MaxPromotions = n
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
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
	// ns is the namespace of a view (Namespace). The store itself, with
	// none, names no keys: every blob is a namespace's.
	ns        string
	putExpiry time.Duration
	getExpiry time.Duration
	now       func() time.Time

	// promotions bounds the promotions in flight, across every namespace
	// view of the store, which share it.
	promotions chan struct{}
	// hashInService is S3Config.HashInService. noStoreHash is set, for
	// every view, once a part copy returned no checksum: the store doesn't
	// hash, and the service does from then on.
	hashInService bool
	noStoreHash   *atomic.Bool
	// maxPartCopy and copyPartSize are maxPartCopy and defaultCopyPartSize;
	// tests lower them to exercise many-part copies.
	maxPartCopy  int64
	copyPartSize int64
}

var (
	_ Store      = (*S3Store)(nil)
	_ Namespacer = (*S3Store)(nil)
)

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
	if cfg.MaxPromotions < 0 {
		return nil, fmt.Errorf("s3 blob store: max promotions %d is negative", cfg.MaxPromotions)
	}
	if cfg.MaxPromotions == 0 {
		cfg.MaxPromotions = DefaultMaxPromotions
	}
	for _, e := range []struct {
		name string
		exp  time.Duration
	}{{"PUT", cfg.PutExpiry}, {"GET", cfg.GetExpiry}} {
		if e.exp < time.Second || e.exp > maxPresignExpiry {
			return nil, fmt.Errorf("s3 blob store: %s expiry %s is outside [1s, %s]", e.name, e.exp, maxPresignExpiry)
		}
	}

	client, err := NewS3Client(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("s3 blob store: %w", err)
	}
	return &S3Store{
		client:    client,
		presign:   s3.NewPresignClient(client),
		bucket:    cfg.Bucket,
		prefix:    strings.Trim(cfg.Prefix, "/"),
		putExpiry: cfg.PutExpiry,
		getExpiry: cfg.GetExpiry,
		now:       time.Now,

		promotions:    make(chan struct{}, cfg.MaxPromotions),
		hashInService: cfg.HashInService,
		noStoreHash:   &atomic.Bool{},
		maxPartCopy:   maxPartCopy,
		copyPartSize:  defaultCopyPartSize,
	}, nil
}

// Client is the store's S3 client. Other stores of the same bucket can share
// it (metastore.NewS3WithClient).
func (s *S3Store) Client() *s3.Client { return s.client }

// NewS3Client returns an S3 client for cfg's endpoint, region, addressing,
// credentials and HTTP client. It ignores the bucket, prefix and expiries, and
// makes no requests. The metadata store shares it, so both stores reach S3 the
// same way.
func NewS3Client(ctx context.Context, cfg S3Config) (*s3.Client, error) {
	if cfg.Region == "" {
		return nil, errors.New("no region")
	}
	creds := cfg.Credentials
	if creds == nil {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
		if err != nil {
			return nil, fmt.Errorf("load AWS config: %w", err)
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
	return s3.New(opts), nil
}

// Namespace returns the store of one namespace's blobs: the same bucket and
// expiries, with keys under <prefix>/ns/<ns>/, so its blobs are at
// <prefix>/ns/<ns>/blobs/<algorithm>/<hex>, next to the namespace's metadata.
func (s *S3Store) Namespace(ns string) (Store, error) {
	if err := layout.ValidateNamespace(ns); err != nil {
		return nil, err
	}
	c := *s
	c.ns = ns
	return &c, nil
}

// errNoNamespace is a key asked of the store itself, not of a namespace's
// view: blobs are kept only per namespace.
var errNoNamespace = errors.New("s3 blob store: no namespace; blobs are kept per namespace (Namespace)")

// nsPrefix is the prefix of the view's namespace: <prefix>/ns/<ns>/.
func (s *S3Store) nsPrefix() (string, error) {
	if s.ns == "" {
		return "", errNoNamespace
	}
	return layout.NamespacePrefix(s.prefix, s.ns)
}

// Key returns the object key of a blob: <prefix>/ns/<ns>/blobs/<algorithm>/<hex>.
func (s *S3Store) Key(dgst digest.Digest) (string, error) {
	if err := dgst.Validate(); err != nil {
		return "", fmt.Errorf("blob key: %w", err)
	}
	p, err := s.nsPrefix()
	if err != nil {
		return "", err
	}
	return p + layout.BlobRel(dgst), nil
}

// StagingKey returns the object key a blob is uploaded to under stage:
// <prefix>/ns/<ns>/staging/<stage>/<algorithm>/<hex>.
func (s *S3Store) StagingKey(stage Stage, dgst digest.Digest) (string, error) {
	if err := stage.Validate(); err != nil {
		return "", err
	}
	if err := dgst.Validate(); err != nil {
		return "", fmt.Errorf("staging key: %w", err)
	}
	p, err := s.nsPrefix()
	if err != nil {
		return "", err
	}
	return p + layout.StagingRel(string(stage), dgst), nil
}

// StagingPrefix is the prefix of every staged upload of the namespace's
// view: <prefix>/ns/<ns>/staging/. The bucket's lifecycle rule expires
// what is under it. It is "" for the store itself, which has none.
func (s *S3Store) StagingPrefix() string {
	p, err := s.nsPrefix()
	if err != nil {
		return ""
	}
	return p + layout.StagingDir
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
	return s3util.HasStatus(err, http.StatusNotFound)
}

// isConditionFailed reports a conditional write that lost: 412, or 409 for
// one that raced another write of the same key.
func isConditionFailed(err error) bool {
	return s3util.HasStatus(err, http.StatusPreconditionFailed) || s3util.HasStatus(err, http.StatusConflict)
}

// PresignPut returns a presigned PUT of exactly size bytes to the blob's
// staging key under stage. Content-Length is signed, so S3 refuses a body of
// any other size. S3 doesn't check the body against dgst: Promote does.
//
// size must be positive: SigV4 never signs a Content-Length of 0, so a PUT
// presigned for an empty blob would take a body of any size. Layer blobs are
// compressed archives and never empty.
func (s *S3Store) PresignPut(ctx context.Context, stage Stage, dgst digest.Digest, size int64) (Address, error) {
	if size <= 0 {
		return Address{}, fmt.Errorf("presign PUT %s: size %d: %w", dgst, size, ErrNotPositiveSize)
	}
	key, err := s.StagingKey(stage, dgst)
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
