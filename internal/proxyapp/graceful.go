package proxyapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// restartGate suspends and resumes the superviseWorker restart loop without
// touching the running worker processes. While suspended, a crashed worker is
// not replaced; resume continues with the next epoch.
type restartGate struct {
	mu        sync.Mutex
	suspended bool
	changed   chan struct{}
}

func newRestartGate() *restartGate {
	return &restartGate{changed: make(chan struct{})}
}

func (g *restartGate) suspend() {
	g.transition(true)
}

func (g *restartGate) resume() {
	g.transition(false)
}

func (g *restartGate) transition(suspended bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.suspended != suspended {
		g.suspended = suspended
		close(g.changed)
		g.changed = make(chan struct{})
	}
}

// wait blocks while the gate is suspended and reports whether restarting is
// allowed again. It returns false once ctx is done.
func (g *restartGate) wait(ctx context.Context) bool {
	for {
		g.mu.Lock()
		suspended, changed := g.suspended, g.changed
		g.mu.Unlock()
		if !suspended {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

// subscriberManager owns the routesync subscriber lifecycle so the graceful
// path can cancel it (freezing the route view) and a rollback can restart it.
type subscriberManager struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	launch  func(context.Context)
	startMu sync.Mutex // serializes launches so two subscribers never overlap
}

func (m *subscriberManager) start() {
	m.startMu.Lock()
	defer m.startMu.Unlock()
	m.mu.Lock()
	done, cancel := m.done, m.cancel
	m.mu.Unlock()
	if done != nil {
		if cancel != nil {
			return // already running
		}
		// A stopped instance must fully unwind before a rollback relaunches;
		// otherwise two subscribers briefly race for the plugin lease.
		<-done
	}
	ctx, cancel2 := context.WithCancel(context.Background())
	newDone := make(chan struct{})
	m.mu.Lock()
	m.cancel, m.done = cancel2, newDone
	m.mu.Unlock()
	go func() {
		defer close(newDone)
		m.launch(ctx)
	}()
}

// stop cancels the running subscriber without waiting. The cancelled session
// never reconnects, so a registering successor cannot be evicted afterwards.
// Use join before unmapping state that the unwind path still touches.
func (m *subscriberManager) stop() {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// join blocks until the current or last cancelled subscriber instance has
// fully exited. RunMaster's teardown calls it before closing the shared
// table: the subscriber's unwind path still writes the sync header.
func (m *subscriberManager) join() {
	m.mu.Lock()
	done := m.done
	m.mu.Unlock()
	if done != nil {
		<-done
	}
}

// workerHandle is the master-side control surface of one running worker
// process: the drain pipe, the process kill switch, and its exit signal.
type workerHandle struct {
	drain  *os.File
	cancel context.CancelFunc
	done   chan struct{}
}

type workerRegistry struct {
	mu      sync.Mutex
	handles map[int]*workerHandle
}

func newWorkerRegistry() *workerRegistry {
	return &workerRegistry{handles: map[int]*workerHandle{}}
}

func (r *workerRegistry) register(index int, handle *workerHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handles[index] = handle
}

func (r *workerRegistry) remove(index int, handle *workerHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handles[index] == handle {
		delete(r.handles, index)
	}
}

func (r *workerRegistry) snapshot() []*workerHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	handles := make([]*workerHandle, 0, len(r.handles))
	for _, handle := range r.handles {
		handles = append(handles, handle)
	}
	return handles
}

// metricsControl owns the optional metrics listener so the graceful path can
// stop serving and a rollback can re-bind it.
type metricsControl struct {
	addr string
	log  *slog.Logger
	// serve starts one serving loop for a freshly bound listener; it is the
	// master's task launcher so reopened servers stay tracked by masterGroup.
	serve func(net.Listener)

	mu       sync.Mutex
	listener net.Listener
}

func (m *metricsControl) close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listener != nil {
		_ = m.listener.Close()
		m.listener = nil
	}
}

func (m *metricsControl) reopen() {
	if m == nil || m.serve == nil {
		return
	}
	m.mu.Lock()
	if m.listener != nil {
		m.mu.Unlock()
		return
	}
	listener, err := net.Listen("tcp", m.addr)
	if err != nil {
		m.mu.Unlock()
		m.log.Warn("proxy metrics reopen", "addr", m.addr, "err", err)
		return
	}
	m.listener = listener
	m.mu.Unlock()
	m.serve(listener)
}

// MasterControl exposes the graceful-exit atomic capability of one running
// proxy master for hot-upgrade orchestration. Phase one (GracefulShutdown) is
// reversible via RollbackPhaseOne; phase two (DrainWorkers) ends worker
// serving and is used only when this master is exiting or has been replaced.
type MasterControl struct {
	logger  *slog.Logger
	sub     *subscriberManager
	gate    *restartGate
	workers *workerRegistry
	metrics *metricsControl

	listenerMu sync.Mutex
	data       net.Listener
	mmds       net.Listener

	mu       sync.Mutex
	phaseOne bool
}

// setListeners records the master's serving listeners so a hot-upgrade
// orchestration can capture their descriptors for the forked successor.
func (c *MasterControl) setListeners(data, mmds net.Listener) {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()
	c.data, c.mmds = data, mmds
}

// CaptureListenerFDs returns duplicated listener files for the data listener
// and, when MMDS serving is enabled, the MMDS listener. The caller owns the
// returned files and passes them to the forked successor as extra files
// together with the hot-upgrade descriptor environment.
func (c *MasterControl) CaptureListenerFDs() (data, mmds *os.File, err error) {
	c.listenerMu.Lock()
	defer c.listenerMu.Unlock()
	if c.data == nil {
		return nil, nil, fmt.Errorf("proxy: master listeners are not serving yet")
	}
	dataFile, err := listenerFile(c.data)
	if err != nil {
		return nil, nil, fmt.Errorf("proxy: capture data listener: %w", err)
	}
	mmdsFile, err := listenerFile(c.mmds)
	if err != nil {
		_ = dataFile.Close()
		return nil, nil, fmt.Errorf("proxy: capture mmds listener: %w", err)
	}
	return dataFile, mmdsFile, nil
}

// GracefulShutdown runs phase one: cancel the routesync subscriber (the route
// view freezes; no reconnection, so a registering successor cannot be evicted
// by this master), suspend worker supervision, and stop serving metrics.
// Existing connections and workers are untouched. Idempotent.
func (c *MasterControl) GracefulShutdown() {
	c.mu.Lock()
	if c.phaseOne {
		c.mu.Unlock()
		return
	}
	c.phaseOne = true
	c.mu.Unlock()
	c.sub.stop()
	c.gate.suspend()
	c.metrics.close()
	c.logger.Info("proxy master graceful shutdown: phase one",
		"subscriber", "cancelled", "supervision", "suspended", "metrics", "stopped")
}

// DrainWorkers runs phase two: every worker stops accepting and exits once
// its in-flight relays finish, bounded by timeout; stragglers are killed. It
// blocks until all workers are gone.
func (c *MasterControl) DrainWorkers(timeout time.Duration) {
	handles := c.workers.snapshot()
	for _, handle := range handles {
		if handle.drain != nil {
			_, _ = handle.drain.Write([]byte{1})
		}
	}
	if len(handles) == 0 {
		return
	}
	deadline := time.Now().Add(timeout)
	var stragglers []*workerHandle
	for _, handle := range handles {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			stragglers = append(stragglers, handle)
			continue
		}
		select {
		case <-handle.done:
		case <-time.After(remaining):
			stragglers = append(stragglers, handle)
		}
	}
	for _, handle := range stragglers {
		c.logger.Warn("proxy worker drain timeout; killing", "worker", handle)
		if handle.cancel != nil {
			handle.cancel()
		}
		<-handle.done
	}
	c.logger.Info("proxy master graceful shutdown: phase two complete",
		"workers", len(handles), "killed", len(stragglers))
}

// RollbackPhaseOne reverses GracefulShutdown after a failed hot-upgrade
// attempt: the routesync subscriber restarts, worker supervision resumes, and
// metrics serving is re-established. Workers that already drained exit are
// replaced by the resumed supervisor with a fresh epoch.
func (c *MasterControl) RollbackPhaseOne() {
	c.mu.Lock()
	if !c.phaseOne {
		c.mu.Unlock()
		return
	}
	c.phaseOne = false
	c.mu.Unlock()
	c.sub.start()
	c.gate.resume()
	c.metrics.reopen()
	c.logger.Info("proxy master graceful shutdown: phase one rolled back")
}
