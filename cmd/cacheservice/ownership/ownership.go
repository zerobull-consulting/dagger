// Package ownership is the cache service's claim to its prefix: the
// ownership marker, <prefix>/cacheservice.json, and the check every
// instance makes at startup (Check) and GC makes before each run
// (ReadMarker).
//
// The service owns everything under its prefix (the whole bucket when the
// prefix is empty): nothing else may write there. Rather than tell its keys
// apart from other applications', it asserts that nothing else is there,
// and stops when that's false. The marker, holding the layout version
// (layout.Version), is what makes a prefix the service's:
//
//   - present, with layout.Version: the prefix is the service's;
//   - present, with another version: it is a store of another layout, which
//     this service can't read (ErrOtherLayout);
//   - absent, with nothing under the prefix: no store exists there yet. A
//     writer claims it by creating the marker, with If-None-Match: * so two
//     writers starting together both proceed, under one marker; a read-only
//     instance starts with nothing to import, and writes nothing;
//   - absent, with keys under the prefix: they aren't the service's, or are
//     an older layout's; either way the service refuses it (ErrNotOwned).
//
// Nothing here deletes or rewrites anything.
package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/dagger/dagger/cmd/cacheservice/internal/s3util"
	"github.com/dagger/dagger/cmd/cacheservice/layout"
	"github.com/dagger/dagger/cmd/cacheservice/metastore"
)

// MaxListed is how many keys a refusal lists, and so how many the startup
// check's one listing asks for (one more, to tell "10" from "more").
const MaxListed = 10

// Store is what the check needs of the object store: whole-object reads
// and conditional writes (metastore.Objects), and a bounded listing.
type Store interface {
	Get(ctx context.Context, key string) (body []byte, etag string, err error)
	Put(ctx context.Context, key string, body []byte, cond metastore.Condition) (etag string, err error)
	// ListKeys returns up to max keys under prefix, in key order: one
	// request.
	ListKeys(ctx context.Context, prefix string, max int) ([]string, error)
	// ListUploadKeys returns the keys of up to max incomplete multipart
	// uploads under prefix.
	ListUploadKeys(ctx context.Context, prefix string, max int) ([]string, error)
}

// ServiceVersion is the version recorded in a marker this binary creates.
// The image stamps its revision here (-ldflags -X); otherwise it is the
// build's VCS revision, or "dev".
var ServiceVersion = ""

// Revision is the revision this binary reports, in its markers and its
// identity (compat.Identity): ServiceVersion, else the build's VCS
// revision, else "dev".
func Revision() string { return serviceVersion() }

func serviceVersion() string {
	if ServiceVersion != "" {
		return ServiceVersion
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				return s.Value
			}
		}
	}
	return "dev"
}

// Marker is the ownership marker's content.
type Marker struct {
	// Layout is the layout version of everything under the prefix.
	Layout int `json:"layout"`
	// Created is when the marker was created, and ServiceVersion by which
	// version of the service.
	Created        time.Time `json:"created"`
	ServiceVersion string    `json:"serviceVersion"`
}

// ErrNoMarker is a prefix without a marker: no store of this service's.
var ErrNoMarker = errors.New("no ownership marker")

// ErrBadMarker is a marker of this layout that the service didn't write:
// a field it always writes is missing.
var ErrBadMarker = errors.New("not an ownership marker of the service's")

// ErrOtherLayout is a marker of another layout version.
type ErrOtherLayout struct {
	Key   string
	Found int
}

func (e *ErrOtherLayout) Error() string {
	return fmt.Sprintf("%s records layout version %d, and this service uses layout %d: it can't use this store. "+
		"Wipe the prefix (then re-run the cache-warming job), or set another CACHESERVICE_S3_PREFIX (see the README, \"Changing layouts\")",
		e.Key, e.Found, layout.Version)
}

// ErrNotOwned is a prefix with keys under it and no marker.
type ErrNotOwned struct {
	Prefix string
	// Keys are some of the keys found, at most MaxListed; More is set when
	// there were others.
	Keys []string
	More bool
}

func (e *ErrNotOwned) Error() string {
	where := "the bucket"
	if e.Prefix != "" {
		where = "prefix " + e.Prefix + "/"
	}
	keys := strings.Join(e.Keys, ", ")
	if e.More {
		keys += ", ..."
	}
	return fmt.Sprintf("%s holds keys and no ownership marker (%s), so it isn't this service's: %s. "+
		"The service needs a prefix of its own, which nothing else writes to: wipe it, or set another CACHESERVICE_S3_PREFIX "+
		"(see the README, \"Bucket ownership\")", where, layout.MarkerName, keys)
}

// State is what Check found.
type State int

const (
	// Owned: the marker was there, of this layout.
	Owned State = iota
	// Claimed: the prefix was empty, and this writer created the marker.
	Claimed
	// ClaimedByOther: the prefix was empty, and another writer created the
	// marker first; it is of this layout.
	ClaimedByOther
	// Empty: the prefix is empty, and this read-only instance left it so.
	Empty
)

func (s State) String() string {
	return [...]string{"owned", "claimed", "claimed by another writer", "empty"}[s]
}

// ReadMarker reads the marker under prefix: ErrNoMarker when there is none,
// and ErrOtherLayout when it is of another layout.
func ReadMarker(ctx context.Context, st interface {
	Get(context.Context, string) ([]byte, string, error)
}, prefix string) (Marker, error) {
	key := layout.MarkerKey(prefix)
	body, _, err := st.Get(ctx, key)
	if errors.Is(err, metastore.ErrNotFound) {
		return Marker{}, ErrNoMarker
	}
	if err != nil {
		return Marker{}, fmt.Errorf("read ownership marker: %w", err)
	}
	var m Marker
	if err := json.Unmarshal(body, &m); err != nil {
		return Marker{}, fmt.Errorf("ownership marker %s: %w", key, err)
	}
	if m.Layout != layout.Version {
		return m, &ErrOtherLayout{Key: key, Found: m.Layout}
	}
	// The service writes every field. One missing is a marker the service
	// didn't write, so it claims nothing.
	if m.Created.IsZero() || m.ServiceVersion == "" {
		return m, fmt.Errorf("%w: %s lacks its creation time or service version, so the service didn't write it", ErrBadMarker, key)
	}
	return m, nil
}

// Check is the startup check: it returns the state of the prefix, or why
// the instance must not use it. A writer creates the marker on an empty
// prefix; a read-only instance (writer false) writes nothing.
//
// It costs one GET when the marker is there. Otherwise, one bounded list
// (MaxListed+1 keys); on an empty prefix one GET per ancestor prefix (the
// bucket's top included), and for a writer one listing of multipart
// uploads and one conditional PUT, plus one GET if another writer created
// the marker first. A listing
// that finds keys is followed by one more GET of the marker: it may be a
// writer that claimed the prefix since the first one.
func Check(ctx context.Context, st Store, prefix string, writer bool, now time.Time) (State, Marker, error) {
	m, err := ReadMarker(ctx, st, prefix)
	if err == nil {
		return Owned, m, nil
	}
	if !errors.Is(err, ErrNoMarker) {
		return 0, m, err
	}
	keys, err := st.ListKeys(ctx, layout.Root(prefix), MaxListed+1)
	if err != nil {
		return 0, Marker{}, fmt.Errorf("list %s: %w", where(prefix), err)
	}
	if len(keys) > 0 {
		// A writer racing this one may have created the marker, and
		// written under it, between the read and the listing.
		if m, err := ReadMarker(ctx, st, prefix); !errors.Is(err, ErrNoMarker) {
			if err != nil {
				return 0, m, err
			}
			return Owned, m, nil
		}
		e := &ErrNotOwned{Prefix: strings.Trim(prefix, "/"), Keys: keys}
		if len(keys) > MaxListed {
			e.Keys, e.More = keys[:MaxListed], true
		}
		return 0, Marker{}, e
	}
	// Not empty, if an ancestor prefix is a store's: everything under it
	// is that service's, this prefix included.
	if err := checkAncestors(ctx, st, prefix); err != nil {
		return 0, Marker{}, err
	}
	if !writer {
		return Empty, Marker{}, nil
	}
	// An upload in progress is a writer too, which the object listing
	// doesn't show. Only a writer, which claims the prefix, asks: a
	// read-only instance never claims anything.
	ups, err := st.ListUploadKeys(ctx, layout.Root(prefix), MaxListed+1)
	if err != nil {
		return 0, Marker{}, fmt.Errorf("list multipart uploads under %s: %w", where(prefix), err)
	}
	if len(ups) > 0 {
		// A writer racing this one may have claimed the prefix since the
		// listing, and started a promotion's upload under it.
		if m, err := ReadMarker(ctx, st, prefix); !errors.Is(err, ErrNoMarker) {
			if err != nil {
				return 0, m, err
			}
			return Owned, m, nil
		}
		e := &ErrNotOwned{Prefix: strings.Trim(prefix, "/")}
		for _, k := range ups[:min(len(ups), MaxListed)] {
			e.Keys = append(e.Keys, "upload:"+k)
		}
		e.More = len(ups) > MaxListed
		return 0, Marker{}, e
	}
	m = Marker{Layout: layout.Version, Created: now.UTC(), ServiceVersion: serviceVersion()}
	body, err := json.Marshal(m)
	if err != nil {
		return 0, Marker{}, err
	}
	_, err = st.Put(ctx, layout.MarkerKey(prefix), body, metastore.Condition{IfNoneMatch: true})
	if errors.Is(err, metastore.ErrPreconditionFailed) {
		// Another writer created it first: it must be of this layout too.
		m, err := ReadMarker(ctx, st, prefix)
		if err != nil {
			return 0, m, err
		}
		return ClaimedByOther, m, nil
	}
	if err != nil {
		return 0, Marker{}, fmt.Errorf("create ownership marker: %w", err)
	}
	return Claimed, m, nil
}

// ErrNestedPrefix is a prefix under another that holds a marker: the
// service there owns this prefix too.
type ErrNestedPrefix struct {
	Prefix, Ancestor string
}

func (e *ErrNestedPrefix) Error() string {
	anc := "the bucket's top"
	if e.Ancestor != "" {
		anc = "prefix " + e.Ancestor + "/"
	}
	return fmt.Sprintf("prefix %s/ is inside %s, which holds an ownership marker (%s): the service there owns everything under it. "+
		"Set a CACHESERVICE_S3_PREFIX outside it (see the README, \"Bucket ownership\")", e.Prefix, anc, layout.MarkerKey(e.Ancestor))
}

// ErrAncestorUnread is an ancestor prefix's marker position that the
// nested-prefix check couldn't read, for an error other than not found or
// access denied: a 5xx, a timeout, a dropped connection. The check can't
// tell whether a service owns the ancestor, so it refuses.
type ErrAncestorUnread struct {
	Prefix, Key string
	Err         error
}

func (e *ErrAncestorUnread) Error() string {
	return fmt.Sprintf("can't read %s, an ancestor's ownership marker position, to check that prefix %s/ isn't inside another service's: %v. "+
		"Try again once the store answers (see the README, \"Bucket ownership\")", e.Key, e.Prefix, e.Err)
}

func (e *ErrAncestorUnread) Unwrap() error { return e.Err }

// checkAncestors refuses a prefix whose ancestors hold a marker, of any
// content: one GET per ancestor, made only when the prefix has none of its
// own. An ancestor whose marker is not found, or that the key may not read
// (a key allowed only its own prefix gets 403 there), is skipped, since a
// nested claim costs no data, only the ancestor's GC, which then stops on
// this prefix's keys. Any other error (a 5xx, a timeout) refuses, with
// ErrAncestorUnread: the store didn't say, so the check doesn't guess. The
// store's client retries; this adds no retries of its own.
func checkAncestors(ctx context.Context, st Store, prefix string) error {
	p := strings.Trim(prefix, "/")
	if p == "" {
		return nil
	}
	segs := strings.Split(p, "/")
	for i := range segs {
		anc := strings.Join(segs[:i], "/")
		key := layout.MarkerKey(anc)
		_, _, err := st.Get(ctx, key)
		switch {
		case err == nil:
			return &ErrNestedPrefix{Prefix: p, Ancestor: anc}
		case errors.Is(err, metastore.ErrNotFound), s3util.Denied(err):
			continue
		default:
			return &ErrAncestorUnread{Prefix: p, Key: key, Err: err}
		}
	}
	return nil
}

func where(prefix string) string {
	if p := strings.Trim(prefix, "/"); p != "" {
		return "prefix " + p + "/"
	}
	return "the bucket"
}
