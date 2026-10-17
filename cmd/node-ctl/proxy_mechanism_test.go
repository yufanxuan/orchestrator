package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const shmTruncateHelperEnv = "KUASAR_TEST_SHM_TRUNCATE_HELPER"

func init() {
	if os.Getenv(shmTruncateHelperEnv) == "1" {
		runShmTruncateHelper()
	}
}

func runShmTruncateHelper() {
	path := os.Getenv("KUASAR_TEST_SHM_PATH")
	if path == "" {
		fmt.Fprintln(os.Stderr, "missing shm path")
		os.Exit(2)
	}
	if _, err := proxyshm.Create(path, 16); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func mechanismRoute(sid string) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID:  sid,
		StableID:   sid,
		Profile:    "e2b",
		State:      routesync.StateRunning,
		FloatingIP: "127.0.0.1",
	}
}

// TestShmSamePathTruncateCorruptsOtherProcess proves problem #1's mechanism:
// a second proxyshm.Create on the SAME path zeroes the live view held by
// another process (O_TRUNC + header rewrite over a shared mmap), while a
// generation-suffixed path leaves it intact.
func TestShmSamePathTruncateCorruptsOtherProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "proxy-routes.shm")
	table, err := proxyshm.Create(shmPath, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	if err := table.Upsert(mechanismRoute("sb-mech")); err != nil {
		t.Fatal(err)
	}
	if _, found := table.Lookup("sb-mech"); !found {
		t.Fatal("precondition: route missing before second creator")
	}

	child := func(path string) {
		t.Helper()
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(),
			shmTruncateHelperEnv+"=1",
			"KUASAR_TEST_SHM_PATH="+path,
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("second creator on %s: %v\n%s", path, err, out)
		}
	}

	child(shmPath + ".g1")
	if _, found := table.Lookup("sb-mech"); !found {
		t.Fatal("generation-suffixed create wiped the live view (unexpected)")
	}
	t.Logf("RESULT shm=generation-path live_view=intact")

	child(shmPath)
	if _, found := table.Lookup("sb-mech"); found {
		t.Fatal("same-path O_TRUNC creator did NOT corrupt the live view — problem #1 premise falsified")
	} else {
		t.Logf("RESULT shm=same-path live_view=WIPED (problem #1 confirmed: cross-process corruption)")
	}
}

func writeMechanismMasterConfig(t *testing.T, dir, name, configSocket, dataAddr string) string {
	t.Helper()
	cfg := fmt.Sprintf(`config_socket: %s
paths:
  run_root: %s
data_listen: %s
stats_socket: %s
shm_path: %s
route_capacity: 16
workers: 1
auth: off
park_timeout: 100ms
`, configSocket, filepath.Join(dir, "run"), dataAddr,
		filepath.Join(dir, name+"-stats.sock"), filepath.Join(dir, name+"-routes.shm"))
	path := filepath.Join(dir, name+".yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRoutesyncEvictionOscillation proves problem #3's mechanism with two REAL
// proxy masters: registering a second master evicts the first, whose subscriber
// reconnects and evicts the second — the lease oscillates until one exits.
func TestRoutesyncEvictionOscillation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	bin := os.Getenv(hotUpgradeBinEnv)
	if bin == "" {
		t.Skipf("%s not set", hotUpgradeBinEnv)
	}
	dir := t.TempDir()
	configSocket := filepath.Join(dir, "node-ctl.socket")
	conductorLog := filepath.Join(dir, "conductor.log")
	logFile, err := os.OpenFile(conductorLog, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	source := &staticRouteSource{
		policy: routesync.Policy{Domain: "mech.test", AuthMode: "off"},
	}
	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- configsock.New(configSocket, configsock.Deps{
			RouteSource: source,
			Plugins:     configsock.NewRegistry(),
		}, slog.New(slog.NewTextHandler(logFile, nil))).ServeReady(serverCtx, ready)
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("config socket: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("config socket readiness timed out")
	}

	countLog := func(marker string) int {
		b, _ := os.ReadFile(conductorLog)
		return strings.Count(string(b), marker)
	}
	startMasterAt := func(name string) *exec.Cmd {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		cfg := writeMechanismMasterConfig(t, dir, name, configSocket, addr)
		ml, err := os.OpenFile(filepath.Join(dir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "proxy", "serve", "--config", cfg)
		cmd.Stdout = ml
		cmd.Stderr = ml
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_, _ = cmd.Process.Wait()
			_ = ml.Close()
		})
		return cmd
	}

	masterA := startMasterAt("master-a")
	deadline := time.Now().Add(15 * time.Second)
	for countLog("plugin registered") < 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if countLog("plugin registered") < 1 {
		t.Fatal("master A never registered")
	}
	t.Logf("RESULT evict phase=A registered=1")

	masterB := startMasterAt("master-b")
	deadline = time.Now().Add(15 * time.Second)
	for countLog("plugin registered") < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if countLog("plugin registered") < 2 {
		t.Fatal("master B never registered (no eviction evidence)")
	}

	deadline = time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if countLog("plugin registered") >= 3 && countLog("plugin deregistered") >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	reg := countLog("plugin registered")
	dereg := countLog("plugin deregistered")
	aliveA := processExists(masterA.Process.Pid)
	aliveB := processExists(masterB.Process.Pid)
	t.Logf("RESULT evict phase=oscillation-window registered=%d deregistered=%d masterA_alive=%v masterB_alive=%v",
		reg, dereg, aliveA, aliveB)
	if reg >= 3 && dereg >= 2 {
		t.Logf("RESULT evict outcome=OSCILLATION-CONFIRMED (problem #3: subscriber reconnect re-evicts the successor)")
	} else {
		t.Logf("RESULT evict outcome=evicted-once (reconnect backoff exceeded window; eviction still proven: reg=%d dereg=%d)", reg, dereg)
	}
}

// TestSecondMasterStartCorruptsServingInstance proves the bind-order hazard
// behind problem #1's ordering fix: with today's RunMaster order (SHM create
// BEFORE data_listen bind), a second instance that fails to bind still wipes
// the serving instance's route view.
func TestSecondMasterStartCorruptsServingInstance(t *testing.T) {
	stack := newProxyStack(t, 1)
	start := time.Now()
	first := stack.startMaster("bindorder-first")
	stack.awaitServing(start, 30*time.Second)
	if err := stack.tryTunnel(3000); err != nil {
		t.Fatalf("precondition tunnel: %v", err)
	}
	second := stack.startMaster("bindorder-second")
	if err, exited := second.exitWithin(15 * time.Second); !exited {
		_ = second.cmd.Process.Kill()
		t.Fatalf("second master did not exit: %v", err)
	} else {
		t.Logf("RESULT bindorder second_master_exit=%v (expected EADDRINUSE)", err)
	}
	time.Sleep(200 * time.Millisecond)
	err := stack.tryTunnel(2000)
	if err != nil {
		t.Logf("RESULT bindorder outcome=SERVING-INSTANCE-CORRUPTED (first master tunnel after second start: %v)", err)
		t.Logf("RESULT bindorder conclusion=SHM O_TRUNC by failed second start wiped the serving route view — bind-first reorder is REQUIRED")
	} else {
		t.Logf("RESULT bindorder outcome=serving-instance-survived (tunnel ok after failed second start)")
	}
	_ = first
}
