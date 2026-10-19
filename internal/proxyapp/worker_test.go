package proxyapp

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
)

// TestPrepareWorkerRejectsUnusableDrainDescriptor proves worker preparation
// fails fast on both unusable drain shapes: an absent descriptor and a
// descriptor number that is not open in this process.
func TestPrepareWorkerRejectsUnusableDrainDescriptor(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	cfg := testProxyConfig(t)
	cfg.ShmPath = filepath.Join(t.TempDir(), "worker-routes.shm")
	table, err := proxyshm.Create(cfg.ShmPath, cfg.RouteCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	effective, err := FreezeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := proxyadmission.NewMaster(cfg.RouteCapacity, cfg.Workers)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()
	for name, tc := range map[string]struct {
		drainFD int
		wantErr string
	}{
		"missing descriptor": {-1, "missing drain descriptor"},
		"closed descriptor":  {9999, "for proxy-drain"},
	} {
		t.Run(name, func(t *testing.T) {
			admissionFile, err := admission.DupFile()
			if err != nil {
				t.Fatal(err)
			}
			wakeRead, wakeWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer wakeRead.Close()
			defer wakeWrite.Close()
			notifyRead, notifyWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer notifyRead.Close()
			defer notifyWrite.Close()
			fds := workerFDMapping{
				Data: -1, MMDS: -1,
				Wake: int(wakeWrite.Fd()), Notify: int(notifyRead.Fd()),
				Stats: -1, MMDSRPC: -1, Drain: tc.drainFD,
				Admission: int(admissionFile.Fd()),
			}
			bootstrap := &WorkerBootstrap{
				process:     Process{Role: RoleWorker, WorkerID: "proxy-0", WorkerEpoch: 1},
				effective:   effective,
				fds:         fds,
				workerIndex: 0,
			}
			if _, err := PrepareWorker(bootstrap); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("PrepareWorker(drain=%d) error=%v, want containing %q", tc.drainFD, err, tc.wantErr)
			}
		})
	}
}
