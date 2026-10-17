package main

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestProxyHotUpgradeListenerInheritance drives the full PR2 path with real
// processes: the test process binds the data listener (standing in for the old
// master), hot-spawns a node-ctl proxy master with the inherited descriptor,
// and proves the successor serves traffic through the inherited listen socket
// while the original holder keeps its descriptor.
func TestProxyHotUpgradeListenerInheritance(t *testing.T) {
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

	logPath := filepath.Join(stack.dir, "hotfd-master.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(stack.bin, "proxy", "serve", "--config", stack.cfgPath)
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
	exited := make(chan error, 1)
	waitDone := make(chan struct{})
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

	start := time.Now()
	stack.awaitServing(start, 30*time.Second)
	tunnel := stack.openLiveTunnel("inherited-listener")
	stack.awaitTick(tunnel, 3)
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "data listener restored from inherited descriptor") {
		t.Fatalf("master log lacks restore line:\n%s", contents)
	}

	termAt := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		t.Logf("RESULT hotfd master_exit_ms=%d exit_err=%v", time.Since(termAt).Milliseconds(), err)
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("hot master did not exit after SIGTERM")
	}
	breakErr, broke := tunnel.awaitBreak(5 * time.Second)
	if !broke {
		t.Fatal("tunnel survived master teardown")
	}
	t.Logf("RESULT hotfd inherited_listener=tunnel-served break_ms=%d break_err=%v",
		time.Since(termAt).Milliseconds(), breakErr)
}
