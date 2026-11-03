package gc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Bucket is a Bucket in S3. The service's credentials need, besides what
// the blob and metadata stores need: s3:ListBucket and
// s3:ListBucketMultipartUploads on the bucket, s3:DeleteObject on keys
// under the prefix (staging keys already need it), s3:AbortMultipartUpload
// (promotion already needs it), and, to report namespaces without a staging
// rule, s3:GetLifecycleConfiguration.
type S3Bucket struct {
	Client *s3.Client
	Bucket string
}

var _ Bucket = (*S3Bucket)(nil)

// ListPrefixes lists with the delimiter "/": one ListObjectsV2 per 1,000
// prefixes.
func (b *S3Bucket) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	pages := s3.NewListObjectsV2Paginator(b.Client, &s3.ListObjectsV2Input{
		Bucket:    &b.Bucket,
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	var out []string
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, p := range page.CommonPrefixes {
			out = append(out, aws.ToString(p.Prefix))
		}
	}
	return out, nil
}

// List lists every object under prefix: one ListObjectsV2 per 1,000 keys.
func (b *S3Bucket) List(ctx context.Context, prefix string, fn func([]Object) error) error {
	pages := s3.NewListObjectsV2Paginator(b.Client, &s3.ListObjectsV2Input{
		Bucket: &b.Bucket,
		Prefix: aws.String(prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}
		objs := make([]Object, 0, len(page.Contents))
		for _, o := range page.Contents {
			objs = append(objs, Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), Modified: aws.ToTime(o.LastModified)})
		}
		if err := fn(objs); err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes keys with one DeleteObjects, and fails if any key failed.
func (b *S3Bucket) Delete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) > MaxDeleteBatch {
		return fmt.Errorf("delete: %d keys, over %d", len(keys), MaxDeleteBatch)
	}
	ids := make([]types.ObjectIdentifier, len(keys))
	for i := range keys {
		ids[i] = types.ObjectIdentifier{Key: aws.String(keys[i])}
	}
	out, err := b.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &b.Bucket,
		Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("delete %d objects: %w", len(keys), err)
	}
	if len(out.Errors) > 0 {
		e := out.Errors[0]
		return fmt.Errorf("delete %d objects: %d failed, such as %s: %s", len(keys), len(out.Errors), aws.ToString(e.Key), aws.ToString(e.Message))
	}
	return nil
}

// ListUploads lists incomplete multipart uploads: one
// ListMultipartUploads per 1,000.
func (b *S3Bucket) ListUploads(ctx context.Context, prefix string) ([]Upload, error) {
	in := &s3.ListMultipartUploadsInput{Bucket: &b.Bucket, Prefix: aws.String(prefix)}
	var out []Upload
	for {
		page, err := b.Client.ListMultipartUploads(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list uploads %s: %w", prefix, err)
		}
		for _, u := range page.Uploads {
			out = append(out, Upload{Key: aws.ToString(u.Key), ID: aws.ToString(u.UploadId), Initiated: aws.ToTime(u.Initiated)})
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, nil
		}
		in.KeyMarker, in.UploadIdMarker = page.NextKeyMarker, page.NextUploadIdMarker
		if in.KeyMarker == nil && in.UploadIdMarker == nil {
			return out, nil
		}
	}
}

// AbortUpload aborts an upload; one already gone (404) is not an error.
func (b *S3Bucket) AbortUpload(ctx context.Context, u Upload) error {
	_, err := b.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &b.Bucket, Key: &u.Key, UploadId: &u.ID})
	if err != nil && status(err) != http.StatusNotFound {
		return fmt.Errorf("abort upload of %s: %w", u.Key, err)
	}
	return nil
}

// ExpirationPrefixes reads the bucket's lifecycle configuration: the
// prefixes of its enabled rules that expire current objects, filtered by
// prefix alone (a rule that also filters by tag or size doesn't expire
// every object under its prefix).
func (b *S3Bucket) ExpirationPrefixes(ctx context.Context) ([]string, error) {
	out, err := b.Client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: &b.Bucket})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NoSuchLifecycleConfiguration" {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %v", ErrRulesUnknown, err)
	}
	var prefixes []string
	for _, r := range out.Rules {
		if r.Status != types.ExpirationStatusEnabled || r.Expiration == nil {
			continue
		}
		if r.Expiration.Days == nil && r.Expiration.Date == nil {
			continue
		}
		p, ok := rulePrefix(r)
		if ok {
			prefixes = append(prefixes, p)
		}
	}
	return prefixes, nil
}

// rulePrefix is the prefix a rule applies to, and false when it filters by
// anything else too.
func rulePrefix(r types.LifecycleRule) (string, bool) {
	f := r.Filter
	if f == nil {
		return aws.ToString(r.Prefix), true
	}
	if f.Tag != nil || f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil {
		return "", false
	}
	if a := f.And; a != nil {
		if len(a.Tags) > 0 || a.ObjectSizeGreaterThan != nil || a.ObjectSizeLessThan != nil {
			return "", false
		}
		return aws.ToString(a.Prefix), true
	}
	return strings.Clone(aws.ToString(f.Prefix)), true
}

func status(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}
