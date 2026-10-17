package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

const routerBinEnv = "HOTUPGRADE_CLUSTER_CTL_BIN"

type routerProc struct {
	cmd     *exec.Cmd
	done    chan error
	mu      sync.Mutex
	exitErr error
	exited  bool
}

func startRouter(t *testing.T, bin, cfgPath, logPath string) *routerProc {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "router", "--config", cfgPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	r := &routerProc{cmd: cmd, done: make(chan error, 1)}
	go func() { r.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if _, exited := r.exitWithin(0); !exited {
			_ = cmd.Process.Kill()
			r.exitWithin(2 * time.Second)
		}
		_ = logFile.Close()
	})
	return r
}

func (r *routerProc) exitWithin(timeout time.Duration) (error, bool) {
	r.mu.Lock()
	exited, err := r.exited, r.exitErr
	r.mu.Unlock()
	if exited {
		return err, true
	}
	select {
	case err := <-r.done:
		r.mu.Lock()
		r.exited, r.exitErr = true, err
		r.mu.Unlock()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func awaitRouterPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("router did not listen on %s within %s", addr, timeout)
}

func openIdleConns(t *testing.T, addr string, n int) []net.Conn {
	t.Helper()
	conns := make([]net.Conn, 0, n)
	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})
	for i := 0; i < n; i++ {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	return conns
}

func countBroken(conns []net.Conn) int {
	broken := 0
	for i, conn := range conns {
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			if isConnBroken(err) {
				broken++
				conns[i] = nil
			}
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}
	}
	return broken
}

func isConnBroken(err error) bool {
	type timeout interface{ Timeout() bool }
	if te, ok := err.(timeout); ok && te.Timeout() {
		return false
	}
	return true
}

func TestRouterReusePortUpgrade(t *testing.T) {
	bin := os.Getenv(routerBinEnv)
	if bin == "" {
		t.Skipf("%s not set; build cluster-ctl for linux to run the docker verification", routerBinEnv)
	}
	if runtime.GOOS != "linux" {
		t.Skip("router verification runs on linux")
	}
	dir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	cfgPath := filepath.Join(dir, "router.yaml")
	cfg := fmt.Sprintf("domain: hotup.test\ningress:\n  listen: %s\nregistry:\n  bootstrap: 127.0.0.1:7700\n", addr)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	first := startRouter(t, bin, cfgPath, filepath.Join(dir, "router-first.log"))
	awaitRouterPort(t, addr, 30*time.Second)
	second := startRouter(t, bin, cfgPath, filepath.Join(dir, "router-second.log"))
	select {
	case err := <-second.done:
		t.Fatalf("second router process could not share the port: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	conns := openIdleConns(t, addr, 10)
	termAt := time.Now()
	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exitErr, exited := first.exitWithin(10 * time.Second)
	if !exited {
		_ = first.cmd.Process.Kill()
		t.Fatal("first router did not exit after SIGTERM")
	}
	exitMS := time.Since(termAt).Milliseconds()
	broken := countBroken(conns)
	alive := 0
	for _, conn := range conns {
		if conn != nil {
			alive++
		}
	}
	postConns := openIdleConns(t, addr, 3)
	t.Logf("RESULT phase=router-reuseport dual_instance=accepted exit_ms=%d exit_err=%v conns_broken=%d conns_survived=%d post_upgrade_dials=%d",
		exitMS, exitErr, broken, alive, len(postConns))
	if err := second.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
}
