package metastore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/dagger/dagger/cmd/cacheservice/metastore"
	"github.com/dagger/dagger/dagql"
	"github.com/dagger/dagger/engine"
	"github.com/dagger/dagger/engine/snapshots/config"
)

type versionTestRoot struct{}

func (*versionTestRoot) Type() *ast.Type { return &ast.Type{NamedType: "Query", NonNull: true} }

// The service's bundle version is its own constant, not the engine's: this
// test pins the two together by exporting a value through dagql's real
// capture path (Cache.WithExportedValues, what the engine runs for an
// export) and checking the version the bundle carries. When a merge from
// upstream bumps the engine's bundle version, it fails in that merge.
func TestBundleVersionMatchesEngineCapture(t *testing.T) {
	ctx := engine.ContextWithClientMetadata(t.Context(), &engine.ClientMetadata{ClientID: "bundle-version-test", SessionID: "test-session"})
	cache, err := dagql.NewCache(ctx, filepath.Join(t.TempDir(), "cache.db"), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.CloseDiscardingPersistence()) })
	srv, err := dagql.NewServer(ctx, &versionTestRoot{})
	require.NoError(t, err)
	ctx = dagql.ContextWithCache(ctx, cache)

	value := dagql.String("bundle version")
	frame := &dagql.ResultCall{Kind: dagql.ResultCallKindField, Field: "bundleVersion", Type: dagql.NewResultCallType(value.Type())}
	root, err := cache.GetOrInitCall(ctx, "test-session", srv, &dagql.CallRequest{ResultCall: frame, IsPersistable: true}, func(context.Context) (dagql.AnyResult, error) {
		return dagql.NewResultForCall(value, frame)
	})
	require.NoError(t, err)

	var bundle dagql.ValueBundle
	require.NoError(t, cache.WithExportedValues(ctx, dagql.ValueSelection{Roots: []dagql.AnyResult{root}}, config.RefConfig{}, func(_ context.Context, exported *dagql.ExportedValues) error {
		bundle = exported.Bundle
		return nil
	}))
	require.NotEmpty(t, bundle.Values, "the capture exported nothing, so it proves nothing about the version")
	require.Equal(t, metastore.BundleVersion, bundle.Version,
		"the engine's dagql now writes ValueBundle version %d and the service stores only version %d. "+
			"Check what changed in the bundle format (dagql/cache_transfer_types.go and its capture and import), "+
			"update the service for it (metastore, importer, exporter, fakeengine), "+
			"then set metastore.BundleVersion to %d",
		bundle.Version, metastore.BundleVersion, bundle.Version)
	require.NoError(t, metastore.ValidateBundle(&bundle), "the service refuses the bundle the engine's capture writes")
}
