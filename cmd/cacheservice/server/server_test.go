package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
)

// EnableS3 refuses an incomplete configuration before it reaches a store,
// and leaves both paths off.
func TestEnableS3RefusesBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  blobstore.S3Config
		want string
	}{
		{"no bucket", blobstore.S3Config{Region: "us-east-1"}, "no bucket"},
		{"no region", blobstore.S3Config{Bucket: "b"}, "no region"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := New(slog.New(slog.DiscardHandler), "")
			err := svc.EnableS3(context.Background(), tc.cfg, S3Options{Scope: testScope})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error with %q", err, tc.want)
			}
			if svc.export != nil || svc.imports != nil {
				t.Fatal("a path is on after a refused configuration")
			}
		})
	}
}
