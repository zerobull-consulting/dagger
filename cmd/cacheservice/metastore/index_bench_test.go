package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/dagger/dagger/cmd/cacheservice/blobstore"
	"github.com/dagger/dagger/dagql"
)

// seedIndex writes an index of n entries, all roots of one engine cache, as a
// busy scope's would be, and returns its size in bytes.
func seedIndex(b *testing.B, s *Store, scope string, n int) int {
	b.Helper()
	idx := NewIndex()
	cache := &EngineCache{Generation: 5, UpdatedAtUnix: 1, Numbers: map[uint64]Mapping{}}
	idx.Caches["cache-a"] = cache
	for i := 1; i <= n; i++ {
		num := uint64(i)
		idx.Entries[num] = &Entry{
			Bundle:  fmt.Sprintf("%064x", i/50),
			Ordinal: dagql.TransferOrdinal(i%50 + 1),
			Source:  Source{CacheID: "cache-a", Generation: 5, Number: num},
			Deps:    []uint64{num/2 + 1, num/3 + 1},
			Parts:   []string{`{"part":"fs"}`},
		}
		idx.Roots[num] = Root{RecordedAtUnix: 1}
		cache.Numbers[num] = Mapping{Number: num, Generation: 5}
	}
	idx.NextNumber = uint64(n) + 1
	body, err := json.Marshal(idx)
	if err != nil {
		b.Fatal(err)
	}
	key, err := s.IndexKey(scope)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := s.Objects().Put(context.Background(), key, body, Condition{}); err != nil {
		b.Fatal(err)
	}
	return len(body)
}

// BenchmarkCheckGenerationRead measures what the exporter's early stale check
// (exporter.checkGeneration) costs on each export try: reading and decoding
// the scope's index, then Index.CheckGeneration. Run by hand:
//
//	go test ./cmd/cacheservice/metastore -run '^$' -bench CheckGenerationRead -benchtime 20x
//
// With CACHESERVICE_S3_ENDPOINT set (hack/remote-cache/dev.sh up), it also
// runs against the MinIO bucket. The default run uses the in-memory store.
func BenchmarkCheckGenerationRead(b *testing.B) {
	stores := map[string]func(*testing.B) *Store{
		"mem": func(b *testing.B) *Store {
			s, err := New(Config{Objects: NewMemObjects(), Prefix: "/team/"})
			if err != nil {
				b.Fatal(err)
			}
			return s
		},
	}
	if os.Getenv(blobstore.EnvEndpoint) != "" {
		stores["minio"] = func(b *testing.B) *Store {
			cfg, err := blobstore.S3ConfigFromEnv()
			if err != nil {
				b.Fatal(err)
			}
			if cfg.Region == "" {
				cfg.Region = "us-east-1"
			}
			if cfg.Bucket == "" {
				cfg.Bucket = "dagger-cache"
			}
			cfg.Prefix = fmt.Sprintf("bench-checkgeneration-%d", os.Getpid())
			s, err := NewS3(context.Background(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			return s
		}
	}
	for name, mk := range stores {
		for _, n := range []int{10_000, 100_000} {
			b.Run(fmt.Sprintf("%s/entries=%d", name, n), func(b *testing.B) {
				s := mk(b)
				size := seedIndex(b, s, "scope", n)
				ctx := context.Background()
				b.ResetTimer()
				for range b.N {
					idx, err := s.Index(ctx, "scope")
					if err != nil {
						b.Fatal(err)
					}
					if err := idx.CheckGeneration(&dagql.ValueBundle{}, "cache-a", 5); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(size)/1e6, "index-MB")
			})
		}
	}
}
