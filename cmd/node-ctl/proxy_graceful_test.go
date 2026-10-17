package main

import (
	"syscall"
	"testing"
	"time"
)

// TestProxyGracefulShutdownDrainsLongConnections proves the framework's
// graceful-exit capability on SIGTERM: phase one freezes the route view and
// suspends supervision; phase two stops worker accept while existing long
// connections keep relaying until the configured drain bound, then workers
// are killed and the master exits.
func TestProxyGracefulShutdownDrainsLongConnections(t *testing.T) {
	stack := newProxyStackCfg(t, 1, "drain_timeout: 3s\n")
	start := time.Now()
	master := stack.startMaster("graceful-drain")
	stack.awaitServing(start, 30*time.Second)
	tunnel := stack.openLiveTunnel("pre-shutdown")
	stack.awaitTick(tunnel, 3)

	termAt := time.Now()
	if err := master.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if _, broke := tunnel.awaitBreak(1500 * time.Millisecond); broke {
		t.Fatalf("long connection broke during the drain window: %v", tunnel)
	}
	if err, exited := master.exitWithin(1500 * time.Millisecond); exited {
		t.Fatalf("master exited before the drain bound (drain_timeout=3s): %v", err)
	}
	breakErr, broke := tunnel.awaitBreak(5 * time.Second)
	if !broke {
		t.Fatal("long connection survived past the drain bound (force-kill failed)")
	}
	breakAfter := time.Since(termAt)
	if err, exited := master.exitWithin(5 * time.Second); !exited {
		_ = master.cmd.Process.Kill()
		t.Fatalf("master did not exit after drain: %v", err)
	}
	if breakAfter < 2500*time.Millisecond {
		t.Fatalf("connection broke at %v, want near the 3s drain bound", breakAfter)
	}
	if err := assertAddressReusable(stack.proxyAddr); err != nil {
		t.Fatalf("data listener was not released: %v", err)
	}
	t.Logf("RESULT graceful phase=drain-bound break_ms=%d break_err=%v", breakAfter.Milliseconds(), breakErr)
}

// TestProxyGracefulShutdownInflightZeroExitsEarly proves the drain completes
// as soon as in-flight relays finish: closing the only client connection lets
// the worker observe inflight zero and exit well before the configured bound.
func TestProxyGracefulShutdownInflightZeroExitsEarly(t *testing.T) {
	stack := newProxyStackCfg(t, 1, "drain_timeout: 30s\n")
	start := time.Now()
	master := stack.startMaster("graceful-early")
	stack.awaitServing(start, 30*time.Second)
	tunnel := stack.openLiveTunnel("pre-shutdown")
	stack.awaitTick(tunnel, 3)

	if err := master.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exitStart := time.Now()
	tunnel.close()
	if err, exited := master.exitWithin(10 * time.Second); !exited {
		_ = master.cmd.Process.Kill()
		t.Fatalf("master did not exit early on inflight zero (drain_timeout=30s): %v", err)
	} else {
		t.Logf("RESULT graceful phase=inflight-zero exit_ms=%d (bound 30s)", time.Since(exitStart).Milliseconds())
	}
	if err := assertAddressReusable(stack.proxyAddr); err != nil {
		t.Fatalf("data listener was not released: %v", err)
	}
}
