package e2e

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// LogRecord is one record a service logged, with its attributes as strings,
// including those added with Logger.With (such as "conn" and "component").
type LogRecord struct {
	Time  time.Time
	Level slog.Level
	Msg   string
	Attrs map[string]string
}

// Attr returns an attribute, or "".
func (r LogRecord) Attr(key string) string { return r.Attrs[key] }

// LogRecorder keeps every record a service logs, at every level, and also
// writes them to another handler (the service's log file).
type LogRecorder struct {
	mu   sync.Mutex
	recs []LogRecord
}

// Handler returns a slog handler that records into r and writes to out.
func (r *LogRecorder) Handler(out slog.Handler) slog.Handler {
	return &recordingHandler{rec: r, out: out}
}

// Mark returns a position in the log: Since(mark, ...) reads what comes
// after it.
func (r *LogRecorder) Mark() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recs)
}

// Since returns the records after mark with message msg (any message when
// empty) for which match returns true (every one when nil).
func (r *LogRecorder) Since(mark int, msg string, match func(LogRecord) bool) []LogRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []LogRecord
	for _, rec := range r.recs[min(mark, len(r.recs)):] {
		if msg != "" && rec.Msg != msg {
			continue
		}
		if match != nil && !match(rec) {
			continue
		}
		out = append(out, rec)
	}
	return out
}

// CacheSpans returns the "cache span" records after mark from an engine
// cache, whose span name starts with name (any name when empty).
func (r *LogRecorder) CacheSpans(mark int, cacheID, name string) []LogRecord {
	return r.Since(mark, "cache span", func(rec LogRecord) bool {
		return strings.HasPrefix(rec.Attr("cache"), cacheID+"/") && strings.HasPrefix(rec.Attr("name"), name)
	})
}

type recordingHandler struct {
	rec   *LogRecorder
	out   slog.Handler
	attrs []slog.Attr
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(ctx context.Context, r slog.Record) error {
	rec := LogRecord{Time: r.Time, Level: r.Level, Msg: r.Message, Attrs: map[string]string{}}
	for _, a := range h.attrs {
		rec.Attrs[a.Key] = a.Value.Resolve().String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.Resolve().String()
		return true
	})
	h.rec.mu.Lock()
	h.rec.recs = append(h.rec.recs, rec)
	h.rec.mu.Unlock()
	if h.out != nil && h.out.Enabled(ctx, r.Level) {
		return h.out.Handle(ctx, r)
	}
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &recordingHandler{rec: h.rec, attrs: append(append([]slog.Attr{}, h.attrs...), attrs...)}
	if h.out != nil {
		next.out = h.out.WithAttrs(attrs)
	}
	return next
}

// WithGroup keeps the handler as it is: the service logs no groups.
func (h *recordingHandler) WithGroup(string) slog.Handler { return h }
