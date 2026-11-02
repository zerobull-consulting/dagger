// Package metricstest reads metrics back in tests.
package metricstest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Sample is one metric's reading: a counter's or gauge's value, or a
// histogram's count and sum.
type Sample struct {
	Value float64
	Count uint64
	Sum   float64
	// Found is false when no metric had the name and labels.
	Found bool
}

// Read gathers g and returns the metric named name whose labels include
// every one of labels.
func Read(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) Sample {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !hasLabels(m, labels) {
				continue
			}
			s := Sample{Found: true}
			switch {
			case m.GetCounter() != nil:
				s.Value = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				s.Value = m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				s.Count = m.GetHistogram().GetSampleCount()
				s.Sum = m.GetHistogram().GetSampleSum()
			}
			return s
		}
	}
	return Sample{}
}

// Value is Read's counter or gauge value, 0 when not found.
func Value(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	return Read(t, g, name, labels).Value
}

func hasLabels(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, l := range m.GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
