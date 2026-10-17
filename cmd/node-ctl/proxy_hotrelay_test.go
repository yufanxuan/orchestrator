package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const hotRelayComponentEnv = "HOTUPGRADE_COMPONENT_BIN"

// probeStatsEmptyBatch posts an empty traffic batch to the stats socket and
// returns the HTTP status. The readiness gate (synced + workers available)
// answers 503 before the successor is ready; an empty batch past the gate is
// rejected with 400. The 503→400 transition is the readiness signal.
func probeStatsEmptyBatch(statsPath string) (int, error) {
	connection, err := net.DialTimeout("unix", statsPath, 200*time.Millisecond)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	request := "POST /v1/traffic:batchGet HTTP/1.1\r\nHost: proxy-stats\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]"
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	if _, err := connection.Write([]byte(request)); err != nil {
		return 0, err
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = response.Body.Read(make([]byte, 1))
	return response.StatusCode, nil
}

// TestProxyHotUpgradeNodeCtlRelay validates the zero-framework-change fork
// path: the "old master" (this test process) exec's node-ctl with inherited
// listener descriptors; node-ctl dispatches through its existing
// componentexec chain to the on-disk component binary (custom-proxy standing
// in for xproxy), which becomes the successor and restores the inherited
// listener. Readiness is observed through the existing stats 503 gate.
func TestProxyHotUpgradeNodeCtlRelay(t *testing.T) {
	componentBin := os.Getenv(hotRelayComponentEnv)
	if componentBin == "" {
		t.Skipf("%s not set; build examples/custom-proxy to run the relay verification", hotRelayComponentEnv)
	}
	stack := newProxyStackCfg(t, 1, "drain_timeout: 1s\n")
	listener, err := net.Listen("tcp", stack.proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listenerFile, err := listener.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer listenerFile.Close()

	relayConfig := filepath.Join(stack.dir, "relay-proxy.yaml")
	relayBody := fmt.Sprintf(`config_socket: %s
paths:
  run_root: %s
  proxy_executable: %s
data_listen: %s
workers: 1
auth: off
park_timeout: 2s
drain_timeout: 1s
`, filepath.Join(stack.dir, "node-ctl.socket"), filepath.Join(stack.dir, "run"), componentBin, stack.proxyAddr)
	if err := os.WriteFile(relayConfig, []byte(relayBody), 0o600); err != nil {
		t.Fatal(err)
	}
	statsPath := filepath.Join(stack.dir, "proxy-stats.sock")

	logPath := filepath.Join(stack.dir, "relay-master.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(stack.bin, "proxy", "serve", "--config", relayConfig)
	cmd.Env = append(os.Environ(),
		"KUASAR_PROXY_HOT_UPGRADE=1",
		"KUASAR_PROXY_HOT_UPGRADE_DATA_FD=3",
	)
	cmd.ExtraFiles = []*os.File{listenerFile}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	waitDone := make(chan struct{})
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait(); close(waitDone) }()
	t.Cleanup(func() {
		select {
		case <-waitDone:
		default:
			_ = cmd.Process.Kill()
			<-waitDone
		}
		_ = logFile.Close()
	})

	spawnAt := time.Now()
	var sawUnavailable, sawReady bool
	var readyAfter time.Duration
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := probeStatsEmptyBatch(statsPath)
		if err == nil {
			switch {
			case status == http.StatusServiceUnavailable:
				sawUnavailable = true
			case status == http.StatusBadRequest || status == http.StatusOK:
				sawReady = true
				readyAfter = time.Since(spawnAt)
			}
			if sawReady {
				break
			}
		}
		select {
		case err := <-exited:
			contents, _ := os.ReadFile(logPath)
			t.Fatalf("relay master exited before readiness: %v\n%s", err, contents)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !sawReady {
		contents, _ := os.ReadFile(logPath)
		t.Fatalf("stats probe never passed the readiness gate\n%s", contents)
	}
	t.Logf("RESULT relay stats_probe=503-then-non503 saw_unavailable=%v ready_ms=%d", sawUnavailable, readyAfter.Milliseconds())

	tunnel := stack.openLiveTunnel("relay-inherited")
	stack.awaitTick(tunnel, 3)
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "data listener restored from inherited descriptor") {
		t.Fatalf("relay log lacks inherited-descriptor line:\n%s", contents)
	}

	termAt := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		t.Logf("RESULT relay master_exit_ms=%d exit_err=%v", time.Since(termAt).Milliseconds(), err)
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("relay master did not exit after SIGTERM")
	}
	breakErr, broke := tunnel.awaitBreak(5 * time.Second)
	if !broke {
		t.Fatal("tunnel survived relay master teardown")
	}
	t.Logf("RESULT relay two_hop=env-fd-passthrough-ok break_ms=%d break_err=%v",
		time.Since(termAt).Milliseconds(), breakErr)
}
