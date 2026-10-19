package proxystats

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

func TestWorkerStatsInflightTotal(t *testing.T) {
	var nilStats *WorkerStats
	if got := nilStats.InflightTotal(); got != 0 {
		t.Fatalf("nil worker inflight=%d, want 0", got)
	}
	stats := NewWorkerStats()
	if got := stats.InflightTotal(); got != 0 {
		t.Fatalf("initial inflight=%d, want 0", got)
	}
	flow := stats.BeginParking("sb-inflight", proxy.ConnectServiceForward)
	if got := stats.InflightTotal(); got != 1 {
		t.Fatalf("parking inflight=%d, want 1", got)
	}
	flow.Close()
	if got := stats.InflightTotal(); got != 0 {
		t.Fatalf("closed inflight=%d, want 0", got)
	}
}
