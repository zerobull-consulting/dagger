package gc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metrics"
	"github.com/dagger/dagger/cmd/cacheservice/ownership"
)

// RootScope labels what GC finds outside any namespace, in reports and
// metrics. A namespace name starts with a letter or digit, so it can't be
// one.
const RootScope = "_root"

// MaxUnexpectedLogged bounds the unexpected keys a run logs and reports per
// namespace (or for RootScope).
const MaxUnexpectedLogged = 10

// ErrNotOwned is a run that did nothing because the prefix isn't the
// service's as far as GC can tell: no marker of this layout, keys of no
// known shape outside the namespaces, or uploads it couldn't list.
var ErrNotOwned = errors.New("gc: the prefix's ownership can't be confirmed, so the run does nothing")

// unexpected counts keys of no known shape, and keeps the first few.
type unexpected struct {
	n    int
	keys []string
}

func (u *unexpected) add(key string) {
	u.n++
	if len(u.keys) < MaxUnexpectedLogged {
		u.keys = append(u.keys, key)
	}
}

// survey is what GC found of the prefix before collecting anything.
type survey struct {
	// namespaces are the valid names under ns/ that hold objects.
	namespaces []string
	// uploads are the incomplete multipart uploads at blob and staging
	// keys (layout.IsUploadRel), by
	// namespace; badUploads those in a namespace at keys of no known
	// shape.
	uploads    map[string][]Upload
	badUploads map[string]*unexpected
	// root are the keys of no known shape outside any namespace.
	root unexpected
}

// checkMarker confirms the prefix is the service's, of this layout.
func (c *Collector) checkMarker(ctx context.Context) error {
	_, err := ownership.ReadMarker(ctx, c.cfg.Meta.Objects(), c.cfg.Meta.Prefix())
	var other *ownership.ErrOtherLayout
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ownership.ErrNoMarker):
		c.cfg.Metrics.GCRefused(metrics.GCRefusedNoMarker)
		return fmt.Errorf("%w: no ownership marker (%s) under the prefix", ErrNotOwned, layout.MarkerKey(c.cfg.Meta.Prefix()))
	case errors.As(err, &other):
		c.cfg.Metrics.GCRefused(metrics.GCRefusedOtherLayout)
		return fmt.Errorf("%w: %v", ErrNotOwned, err)
	default:
		c.cfg.Metrics.GCRefused(metrics.GCRefusedMarkerUnreadable)
		return fmt.Errorf("%w: %v", ErrNotOwned, err)
	}
}

// survey lists the prefix's top level, the namespaces' level, and every
// incomplete multipart upload under the prefix, and sorts what it finds
// into namespaces and violations. It reads only: two delimited listings
// (one request per 1,000 entries), a bounded listing of each unexpected
// prefix, and the upload listing.
func (c *Collector) survey(ctx context.Context) (*survey, error) {
	prefix := c.cfg.Meta.Prefix()
	root := layout.Root(prefix)
	nsTop := layout.NamespacesPrefix(prefix)
	s := &survey{uploads: map[string][]Upload{}, badUploads: map[string]*unexpected{}}

	dirs, objs, err := c.cfg.Bucket.ListLevel(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", root, err)
	}
	for _, o := range objs {
		if o.Key != layout.MarkerKey(prefix) {
			s.root.add(o.Key)
		}
	}
	for _, d := range dirs {
		if d != nsTop {
			if err := c.sampleUnder(ctx, d, &s.root); err != nil {
				return nil, err
			}
		}
	}
	dirs, objs, err = c.cfg.Bucket.ListLevel(ctx, nsTop)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", nsTop, err)
	}
	for _, o := range objs {
		s.root.add(o.Key)
	}
	for _, d := range dirs {
		name := strings.TrimSuffix(strings.TrimPrefix(d, nsTop), "/")
		if layout.ValidateNamespace(name) != nil {
			if err := c.sampleUnder(ctx, d, &s.root); err != nil {
				return nil, err
			}
			continue
		}
		s.namespaces = append(s.namespaces, name)
	}
	slices.Sort(s.namespaces)

	uploads, err := c.cfg.Bucket.ListUploads(ctx, root)
	if err != nil {
		// Without the listing, an upload of another writer's would go
		// unseen: the run can't confirm the prefix is the service's.
		c.cfg.Metrics.GCRefused(metrics.GCRefusedUploadsUnlisted)
		return nil, fmt.Errorf("%w: list multipart uploads: %v", ErrNotOwned, err)
	}
	for _, u := range uploads {
		place, ns, rel := layout.Locate(prefix, u.Key)
		switch {
		case place == layout.NotUnder:
			// A store that filtered loosely: not under the prefix, so
			// neither the service's nor its concern.
		case place != layout.InNamespace:
			s.root.add("upload:" + u.Key)
		case !layout.IsUploadRel(rel):
			bad := s.badUploads[ns]
			if bad == nil {
				bad = &unexpected{}
				s.badUploads[ns] = bad
			}
			bad.add("upload:" + u.Key)
		default:
			s.uploads[ns] = append(s.uploads[ns], u)
		}
	}
	return s, nil
}

// errSampled ends a bounded listing.
var errSampled = errors.New("sampled")

// sampleUnder adds the keys under an unexpected prefix to u: its first page
// only, so a large foreign tree costs one request, and once u holds
// MaxUnexpectedLogged keys, none: the prefix itself is counted as one
// (the run stops anyway). The count is then a lower bound.
func (c *Collector) sampleUnder(ctx context.Context, prefix string, u *unexpected) error {
	if len(u.keys) >= MaxUnexpectedLogged {
		u.add(prefix)
		return nil
	}
	err := c.cfg.Bucket.List(ctx, prefix, func(objs []Object) error {
		for _, o := range objs {
			u.add(o.Key)
		}
		return errSampled
	})
	if err != nil && !errors.Is(err, errSampled) {
		return fmt.Errorf("list %q: %w", prefix, err)
	}
	return nil
}

// refuseRoot reports keys of no known shape outside the namespaces: the
// run collects nothing.
func (c *Collector) refuseRoot(s *survey) (Report, error) {
	c.cfg.Metrics.GCUnexpected(RootScope, s.root.n)
	c.cfg.Metrics.GCRefused(metrics.GCRefusedRootViolation)
	c.log.Error("gc: keys of no known shape under the prefix, outside its namespaces: the prefix isn't the service's alone, so GC does nothing. "+
		"Nothing else may write under the service's prefix (see README, Bucket ownership)",
		"count", s.root.n, "keys", s.root.keys)
	rep := Report{Namespace: RootScope, DryRun: c.cfg.DryRun, Unexpected: s.root.keys, UnexpectedCount: s.root.n,
		Skipped: "keys of no known shape outside the namespaces"}
	return rep, fmt.Errorf("%w: %d keys of no known shape outside the namespaces, such as %s", ErrNotOwned, s.root.n, s.root.keys[0])
}
