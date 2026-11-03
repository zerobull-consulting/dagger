package e2e

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
)

// Upstream is what the S3 proxy needs to forward to an endpoint that routes on
// its own host name, such as an HTTPS store (Resigning).
//
// A store checks a signature against the Host it receives, and engines reach
// the proxy at its own address, so the URLs they are given are signed for it.
// MinIO takes that Host. A hosted store doesn't: it needs its own name, for
// which the proxy's signature is wrong. So the proxy checks the signature it
// received, as the store would, and signs the request again for the store,
// with the same signed headers, time and expiry. What the store refuses is
// unchanged: a presigned URL past its expiry, or a body of another size than
// signed, still fails. The services always address the proxy path-style; it
// forwards in the addressing PathStyle says.
type Upstream struct {
	// Endpoint is the store's URL, such as https://<region>.digitaloceanspaces.com.
	Endpoint string
	Region   string
	Bucket   string
	// PathStyle addresses the bucket in the path; otherwise, in the host.
	PathStyle   bool
	Credentials aws.CredentialsProvider
	// Transport sends to the store. Nil is http.DefaultTransport.
	Transport http.RoundTripper
}

// newUpstream is the Upstream of cfg's store. Its credentials are client's:
// cfg's are nil when they come from the SDK's default chain, which client has
// resolved.
func newUpstream(cfg blobstore.S3Config, client *s3.Client) *Upstream {
	return &Upstream{Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket, PathStyle: cfg.PathStyle, Credentials: client.Options().Credentials}
}

// amzQuery are the query parameters of a presigned URL's signature.
var amzQuery = []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Security-Token"}

const amzDate = "20060102T150405Z"

// signed is a request's SigV4 signature, as it arrived.
type signed struct {
	presigned bool
	signature string
	headers   []string
	when      time.Time
	expires   string
	payload   string
}

// parseSignature reads r's signature, from its query or Authorization.
func parseSignature(r *http.Request) (signed, error) {
	var s signed
	q := r.URL.Query()
	var list, date string
	if sig := q.Get("X-Amz-Signature"); sig != "" {
		s.presigned = true
		s.signature, list, date, s.expires = sig, q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Date"), q.Get("X-Amz-Expires")
		s.payload = "UNSIGNED-PAYLOAD"
	} else {
		auth, ok := strings.CutPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ")
		if !ok {
			return s, errors.New("no SigV4 signature")
		}
		for _, field := range strings.Split(auth, ",") {
			name, value, _ := strings.Cut(strings.TrimSpace(field), "=")
			switch name {
			case "SignedHeaders":
				list = value
			case "Signature":
				s.signature = value
			}
		}
		date = r.Header.Get("X-Amz-Date")
		s.payload = r.Header.Get("X-Amz-Content-Sha256")
		if s.payload == "" {
			return s, errors.New("no X-Amz-Content-Sha256")
		}
	}
	if s.signature == "" || list == "" {
		return s, errors.New("an incomplete signature")
	}
	s.headers = strings.Split(list, ";")
	when, err := time.Parse(amzDate, date)
	if err != nil {
		return s, fmt.Errorf("signature date %q: %w", date, err)
	}
	s.when = when
	return s, nil
}

// sign signs r's method and signed headers for host and target, and returns
// the signature, and what to send: the query with it, or the header.
func (u *Upstream) sign(ctx context.Context, r *http.Request, s signed, host string, target *url.URL) (signature, rawQuery, authorization string, err error) {
	creds, err := u.Credentials.Retrieve(ctx)
	if err != nil {
		return "", "", "", fmt.Errorf("credentials: %w", err)
	}
	q := target.Query()
	for _, name := range amzQuery {
		q.Del(name)
	}
	if s.presigned {
		q.Set("X-Amz-Expires", s.expires)
	}
	to := *target
	to.RawQuery = q.Encode()
	req := &http.Request{Method: r.Method, URL: &to, Host: host, Header: http.Header{}}
	for _, name := range s.headers {
		switch name {
		case "host":
		case "content-length":
			req.ContentLength = r.ContentLength
		default:
			if vs, ok := r.Header[http.CanonicalHeaderKey(name)]; ok {
				req.Header[http.CanonicalHeaderKey(name)] = vs
			}
		}
	}
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if s.presigned {
		out, _, err := signer.PresignHTTP(ctx, creds, req, s.payload, "s3", u.Region, s.when)
		if err != nil {
			return "", "", "", err
		}
		res, err := url.Parse(out)
		if err != nil {
			return "", "", "", err
		}
		return res.Query().Get("X-Amz-Signature"), res.RawQuery, "", nil
	}
	if err := signer.SignHTTP(ctx, creds, req, s.payload, "s3", u.Region, s.when); err != nil {
		return "", "", "", err
	}
	auth := req.Header.Get("Authorization")
	_, sig, ok := strings.Cut(auth, "Signature=")
	if !ok {
		return "", "", "", errors.New("no signature from the signer")
	}
	return sig, to.RawQuery, auth, nil
}

// forward makes r a request for the store: it checks the signature r arrived
// with, moves r to the store's host, and signs it for that.
func (u *Upstream) forward(r *http.Request, store *url.URL) error {
	s, err := parseSignature(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	got, _, _, err := u.sign(ctx, r, s, r.Host, r.URL)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.signature)) != 1 {
		return errors.New("the signature doesn't match the request")
	}

	path := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	key, inBucket := strings.CutPrefix(path, url.PathEscape(u.Bucket)+"/")
	if !inBucket {
		if path != url.PathEscape(u.Bucket) {
			return fmt.Errorf("path %s isn't in bucket %s", r.URL.Path, u.Bucket)
		}
		key = "" // a request for the bucket itself
	}
	target := *store
	host := store.Host
	escaped := "/" + path
	if !u.PathStyle {
		host = u.Bucket + "." + store.Host
		escaped = "/" + key
	}
	decoded, err := url.PathUnescape(escaped)
	if err != nil {
		return err
	}
	target.Host, target.Path, target.RawPath, target.RawQuery = host, decoded, escaped, r.URL.RawQuery

	_, rawQuery, auth, err := u.sign(ctx, r, s, host, &target)
	if err != nil {
		return err
	}
	target.RawQuery = rawQuery
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	r.URL, r.Host = &target, host
	return nil
}
