package s3test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DeletePrefix against a real store: it deletes what is under the prefix and
// aborts its uploads, whatever the store lists by prefix, and leaves a sibling
// whose key merely starts the same. A test's cleanup after a failure is this.
func TestMinIODeletePrefix(t *testing.T) {
	endpoint := os.Getenv("CACHESERVICE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CACHESERVICE_S3_ENDPOINT is not set; start MinIO with hack/remote-cache/dev.sh up")
	}
	ctx := context.Background()
	bucket := os.Getenv("CACHESERVICE_S3_BUCKET")
	if bucket == "" {
		bucket = "dagger-cache"
	}
	region := os.Getenv("CACHESERVICE_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	scope := NewScope(t, bucket)
	c := s3.New(s3.Options{
		Region: region, BaseEndpoint: aws.String(endpoint), UsePathStyle: os.Getenv("CACHESERVICE_S3_PATH_STYLE") == "true",
		Credentials:                credentials.NewStaticCredentialsProvider(os.Getenv("CACHESERVICE_S3_ACCESS_KEY_ID"), os.Getenv("CACHESERVICE_S3_SECRET_ACCESS_KEY"), ""),
		HTTPClient:                 scope.Guard,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	inside, sibling := scope.Prefix+"/in", scope.Prefix+"/in-sibling"
	scope.DeleteOnCleanup(t, c)
	for _, k := range []string{inside + "/a", inside + "/deep/b", sibling + "/c"} {
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: aws.String(k), Body: strings.NewReader("x")}); err != nil {
			t.Fatal(err)
		}
	}
	mine, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: aws.String(inside + "/upload")})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: aws.String(sibling + "/upload")})
	if err != nil {
		t.Fatal(err)
	}

	if err := DeletePrefix(ctx, c, bucket, inside); err != nil {
		t.Fatal(err)
	}

	list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: aws.String(scope.Prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || aws.ToString(list.Contents[0].Key) != sibling+"/c" {
		t.Fatalf("left %d objects, want only the sibling's", len(list.Contents))
	}
	ups, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, u := range ups.Uploads {
		found[aws.ToString(u.UploadId)] = true
	}
	if found[aws.ToString(mine.UploadId)] || !found[aws.ToString(other.UploadId)] {
		t.Fatalf("after the delete: its upload left %v, the sibling's left %v", found[aws.ToString(mine.UploadId)], found[aws.ToString(other.UploadId)])
	}
}
