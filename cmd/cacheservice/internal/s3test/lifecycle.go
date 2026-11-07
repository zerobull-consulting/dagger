package s3test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

// lockTimeout bounds the wait for LockLifecycle: longer than any one test
// holds it.
const lockTimeout = 10 * time.Minute

// LockLifecycle holds, until t ends, a lock on the bucket's lifecycle
// configuration across this machine's test processes: go test runs packages
// in parallel, and a test that replaces the configuration (TestMinIOGC)
// would otherwise race one that merges into it (the lifecycle command's).
// It doesn't cover other machines on the same bucket: see the README's "In
// CI".
func LockLifecycle(t testing.TB, bucket string) {
	t.Helper()
	sum := sha256.Sum256([]byte(os.Getenv(EnvEndpoint) + "\x00" + bucket))
	l := flock.New(filepath.Join(os.TempDir(), "cacheservice-test-lifecycle-"+hex.EncodeToString(sum[:8])+".lock"))
	ctx, cancel := context.WithTimeout(context.Background(), lockTimeout)
	defer cancel()
	if ok, err := l.TryLockContext(ctx, 100*time.Millisecond); !ok || err != nil {
		t.Fatalf("s3test: lock the bucket's lifecycle configuration (%s, at most %s): %v", l.Path(), lockTimeout, err)
	}
	t.Cleanup(func() { _ = l.Unlock() })
}
