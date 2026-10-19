package proxyapp

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRestartGateWaitPassesWhileNotSuspended(t *testing.T) {
	gate := newRestartGate()
	if !gate.wait(context.Background()) {
		t.Fatal("wait failed while not suspended")
	}
	gate.resume() // no-op transition keeps the gate open
	if !gate.wait(context.Background()) {
		t.Fatal("wait failed after redundant resume")
	}
}

func TestRestartGateWaitBlocksUntilResume(t *testing.T) {
	gate := newRestartGate()
	gate.suspend()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan bool, 1)
	go func() { result <- gate.wait(ctx) }()
	select {
	case <-result:
		t.Fatal("wait returned while suspended")
	case <-time.After(50 * time.Millisecond):
	}
	gate.resume()
	select {
	case allowed := <-result:
		if !allowed {
			t.Fatal("wait returned false after resume")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not return after resume")
	}
}

func TestRestartGateWaitReturnsFalseOnCancellation(t *testing.T) {
	gate := newRestartGate()
	gate.suspend()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan bool, 1)
	go func() { result <- gate.wait(ctx) }()
	cancel()
	select {
	case allowed := <-result:
		if allowed {
			t.Fatal("wait returned true after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not observe cancellation")
	}
}

func TestSubscriberManagerStartStopCycle(t *testing.T) {
	var launches atomic.Int32
	started := make(chan struct{}, 4)
	stopped := make(chan struct{}, 4)
	manager := &subscriberManager{launch: func(ctx context.Context) {
		launches.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
	}}
	manager.stop() // stopping before any start is a no-op
	manager.start()
	manager.start() // a second start must not launch twice
	<-started
	if got := launches.Load(); got != 1 {
		t.Fatalf("launches after double start=%d, want 1", got)
	}
	manager.stop()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not cancel the running subscriber")
	}
	manager.join()
	manager.start()
	<-started
	if got := launches.Load(); got != 2 {
		t.Fatalf("launches after restart=%d, want 2", got)
	}
	manager.stop()
}

func TestSubscriberManagerRestartWaitsForUnwind(t *testing.T) {
	release := make(chan struct{})
	var unwound sync.Once
	manager := &subscriberManager{launch: func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(150 * time.Millisecond) // a slow session unwind
		unwound.Do(func() { close(release) })
	}}
	manager.start()
	manager.stop()
	manager.stop() // a second stop must not panic or block
	start := time.Now()
	manager.start() // relaunch joins the previous unwind first
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("restart waited only %v for the previous unwind", elapsed)
	}
	select {
	case <-release:
	default:
		t.Fatal("previous subscriber had not unwound before relaunch")
	}
	manager.stop()
	manager.join()
}

func TestWorkerRegistryRegisterRemoveSnapshot(t *testing.T) {
	registry := newWorkerRegistry()
	first := &workerHandle{done: make(chan struct{})}
	second := &workerHandle{done: make(chan struct{})}
	registry.register(1, first)
	registry.register(2, second)
	registry.remove(1, &workerHandle{}) // a mismatched handle must not unregister
	if handles := registry.snapshot(); len(handles) != 2 {
		t.Fatalf("handles after mismatched remove=%d, want 2", len(handles))
	}
	registry.remove(1, first)
	registry.remove(2, second)
	if handles := registry.snapshot(); len(handles) != 0 {
		t.Fatalf("handles after removals=%d, want 0", len(handles))
	}
}

func TestMetricsControlCloseAndReopen(t *testing.T) {
	served := make(chan net.Listener, 4)
	control := &metricsControl{
		addr:  "127.0.0.1:0",
		log:   testDiscardLogger(),
		serve: func(listener net.Listener) { served <- listener },
	}
	control.reopen()
	listener := <-served
	control.reopen() // already serving; must not bind again
	select {
	case extra := <-served:
		_ = extra.Close()
		t.Fatal("reopen served a second listener while already serving")
	default:
	}
	control.close()
	if _, err := listener.Accept(); err == nil {
		t.Fatal("listener still accepting after close")
	}
	control.reopen()
	rebound := <-served
	defer rebound.Close()
	if rebound == listener {
		t.Fatal("reopen reused the closed listener")
	}
}

func TestMetricsControlNilAndUnservedGuards(t *testing.T) {
	var absent *metricsControl
	absent.close()
	absent.reopen()
	unserved := &metricsControl{addr: "127.0.0.1:0", log: testDiscardLogger()}
	unserved.reopen() // nil serve must not bind
	unserved.mu.Lock()
	listener := unserved.listener
	unserved.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
		t.Fatal("reopen bound a listener without a serve function")
	}
}

func TestMetricsControlReopenListenFailureKeepsState(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	control := &metricsControl{
		addr:  occupied.Addr().String(),
		log:   testDiscardLogger(),
		serve: func(net.Listener) {},
	}
	control.reopen()
	control.mu.Lock()
	listener := control.listener
	control.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
		t.Fatal("reopen kept a listener after a failed bind")
	}
}

func TestMasterControlPhaseOneAndRollback(t *testing.T) {
	var launches atomic.Int32
	started := make(chan struct{}, 4)
	stopped := make(chan struct{}, 4)
	served := make(chan net.Listener, 4)
	control := &MasterControl{
		logger: testDiscardLogger(),
		sub: &subscriberManager{launch: func(ctx context.Context) {
			launches.Add(1)
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
		}},
		gate:    newRestartGate(),
		workers: newWorkerRegistry(),
		metrics: &metricsControl{
			addr:  "127.0.0.1:0",
			log:   testDiscardLogger(),
			serve: func(listener net.Listener) { served <- listener },
		},
	}
	control.metrics.reopen()
	bound := <-served
	control.sub.start() // the running master starts the subscriber before serving
	<-started

	control.GracefulShutdown()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not cancelled by phase one")
	}
	gateCtx, cancelGate := context.WithCancel(context.Background())
	cancelGate()
	if control.gate.wait(gateCtx) {
		t.Fatal("supervision gate not suspended by phase one")
	}
	if _, err := bound.Accept(); err == nil {
		t.Fatal("metrics listener still serving after phase one")
	}

	control.RollbackPhaseOne()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber was not restarted by rollback")
	}
	if !control.gate.wait(context.Background()) {
		t.Fatal("supervision gate not resumed by rollback")
	}
	rebound := <-served
	_ = rebound.Close()
	if got := launches.Load(); got != 2 {
		t.Fatalf("launches after rollback=%d, want 2", got)
	}

	control.GracefulShutdown()
	control.GracefulShutdown() // idempotent: only one stop reaches the subscriber
	<-stopped
	control.RollbackPhaseOne()
	<-started
	if got := launches.Load(); got != 3 {
		t.Fatalf("launches after repeated cycle=%d, want 3", got)
	}
	control.RollbackPhaseOne() // no phase one pending: must not restart
	select {
	case <-started:
		t.Fatal("rollback without phase one restarted the subscriber")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMasterControlDrainWorkersWithoutHandles(t *testing.T) {
	control := &MasterControl{logger: testDiscardLogger(), workers: newWorkerRegistry()}
	finished := make(chan struct{})
	go func() {
		control.DrainWorkers(time.Minute)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("drain blocked without registered workers")
	}
}

func TestMasterControlDrainWorkersWaitsForGracefulExits(t *testing.T) {
	control := &MasterControl{logger: testDiscardLogger(), workers: newWorkerRegistry()}
	for index := 0; index < 2; index++ {
		readEnd, writeEnd, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer readEnd.Close()
		handle := &workerHandle{drain: writeEnd, done: make(chan struct{})}
		go func() {
			buffer := make([]byte, 1)
			if _, err := readEnd.Read(buffer); err != nil {
				return
			}
			close(handle.done)
		}()
		control.workers.register(index, handle)
	}
	start := time.Now()
	control.DrainWorkers(10 * time.Second)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("drain waited %v for prompt workers", elapsed)
	}
}

func TestMasterControlDrainWorkersKillsStragglers(t *testing.T) {
	control := &MasterControl{logger: testDiscardLogger(), workers: newWorkerRegistry()}
	promptDone := make(chan struct{})
	promptRead, promptWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer promptRead.Close()
	prompt := &workerHandle{drain: promptWrite, done: promptDone}
	go func() {
		buffer := make([]byte, 1)
		if _, err := promptRead.Read(buffer); err != nil {
			return
		}
		close(promptDone)
	}()
	stragglerDone := make(chan struct{})
	straggler := &workerHandle{
		drain:  nil, // no pipe: the drain write is skipped entirely
		cancel: func() { close(stragglerDone) },
		done:   stragglerDone,
	}
	stragglerTwoDone := make(chan struct{})
	stragglerTwo := &workerHandle{
		drain:  nil,
		cancel: func() { close(stragglerTwoDone) },
		done:   stragglerTwoDone,
	}
	control.workers.register(0, prompt)
	control.workers.register(1, straggler)
	control.workers.register(2, stragglerTwo)
	start := time.Now()
	control.DrainWorkers(200 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("drain returned in %v before the straggler bound", elapsed)
	}
	select {
	case <-stragglerDone:
	default:
		t.Fatal("straggler was not killed after the drain bound")
	}
	select {
	case <-stragglerTwoDone:
	default:
		t.Fatal("second straggler was not killed past the drain bound")
	}
}
