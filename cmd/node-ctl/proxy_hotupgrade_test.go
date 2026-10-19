package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const hotUpgradeBinEnv = "HOTUPGRADE_NODE_CTL_BIN"

type staticRouteSource struct {
	policy routesync.Policy
	route  routesync.RouteEntry
}

func (s *staticRouteSource) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	return fn(s.route)
}

func (s *staticRouteSource) Subscribe() (<-chan routesync.Event, func()) {
	ch := make(chan routesync.Event)
	return ch, func() {}
}

func (s *staticRouteSource) OnWake(context.Context, string) {}

func (s *staticRouteSource) Policy() routesync.Policy { return s.policy }

type echoBackend struct {
	ln net.Listener
}

func startEchoBackend(t *testing.T) *echoBackend {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &echoBackend{ln: ln}
	go b.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

func (b *echoBackend) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handle(conn)
	}
}

func (b *echoBackend) handle(conn net.Conn) {
	defer conn.Close()
	if _, err := conn.Write([]byte("hello backend\n")); err != nil {
		return
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		br := bufio.NewReader(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(conn, "echo %s", line); err != nil {
				return
			}
		}
	}()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-readerDone:
			return
		case <-tick.C:
			n++
			if _, err := fmt.Fprintf(conn, "tick %d\n", n); err != nil {
				return
			}
		}
	}
}

type tunnelClient struct {
	conn     net.Conn
	br       *bufio.Reader
	broken   chan struct{}
	mu       sync.Mutex
	breakErr error
	lastLine string
}

func openTunnel(proxyAddr, sid string, port, budgetMS int) (*tunnelClient, error) {
	deadline := time.Now().Add(time.Duration(budgetMS) * time.Millisecond)
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Until(deadline))
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}
	fail := func(err error) (*tunnelClient, error) {
		_ = conn.Close()
		return nil, err
	}
	req := fmt.Sprintf("CONNECT sandbox:%d HTTP/1.1\r\nHost: sandbox:%d\r\nE2b-Sandbox-Id: %s\r\nE2b-Sandbox-Port: %d\r\n\r\n", port, port, sid, port)
	if err := conn.SetDeadline(deadline); err != nil {
		return fail(err)
	}
	if _, err := conn.Write([]byte(req)); err != nil {
		return fail(fmt.Errorf("send connect: %w", err))
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return fail(fmt.Errorf("read connect response: %w", err))
	}
	if resp.StatusCode != http.StatusOK {
		return fail(fmt.Errorf("connect status %d", resp.StatusCode))
	}
	hello, err := br.ReadString('\n')
	if err != nil {
		return fail(fmt.Errorf("read backend hello: %w", err))
	}
	if !strings.Contains(hello, "hello backend") {
		return fail(fmt.Errorf("unexpected backend hello %q", hello))
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return fail(err)
	}
	tc := &tunnelClient{conn: conn, br: br, broken: make(chan struct{})}
	go tc.readLoop()
	return tc, nil
}

func (tc *tunnelClient) readLoop() {
	for {
		line, err := tc.br.ReadString('\n')
		if err != nil {
			tc.mu.Lock()
			tc.breakErr = err
			tc.mu.Unlock()
			close(tc.broken)
			_ = tc.conn.Close()
			return
		}
		tc.mu.Lock()
		tc.lastLine = strings.TrimRight(line, "\n")
		tc.mu.Unlock()
	}
}

func (tc *tunnelClient) awaitBreak(timeout time.Duration) (error, bool) {
	select {
	case <-tc.broken:
		tc.mu.Lock()
		defer tc.mu.Unlock()
		return tc.breakErr, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (tc *tunnelClient) close() {
	_ = tc.conn.Close()
	<-tc.broken
}

type masterProc struct {
	cmd     *exec.Cmd
	done    chan error
	mu      sync.Mutex
	exitErr error
	exited  bool
}

func startProxyMaster(t *testing.T, bin, cfgPath, logPath string) *masterProc {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "proxy", "serve", "--config", cfgPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	m := &masterProc{cmd: cmd, done: make(chan error, 1)}
	go func() { m.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if _, exited := m.exitWithin(0); !exited {
			_ = cmd.Process.Kill()
			m.exitWithin(2 * time.Second)
		}
		_ = logFile.Close()
	})
	return m
}

func (m *masterProc) exitWithin(timeout time.Duration) (error, bool) {
	m.mu.Lock()
	exited, err := m.exited, m.exitErr
	m.mu.Unlock()
	if exited {
		return err, true
	}
	select {
	case err := <-m.done:
		m.mu.Lock()
		m.exited, m.exitErr = true, err
		m.mu.Unlock()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func childPIDs(parent int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == parent {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if err != nil {
			continue
		}
		text := string(stat)
		idx := strings.LastIndex(text, ")")
		if idx < 0 || idx+2 >= len(text) {
			continue
		}
		fields := strings.Fields(text[idx+2:])
		if len(fields) < 2 {
			continue
		}
		if ppid, err := strconv.Atoi(fields[1]); err == nil && ppid == parent {
			pids = append(pids, pid)
		}
	}
	return pids
}

type proxyStack struct {
	t           *testing.T
	dir         string
	cfgPath     string
	bin         string
	lastLog     string
	backend     *echoBackend
	serverCtx   context.Context
	stopServer  context.CancelFunc
	proxyAddr   string
	sid         string
	backendPort int
}

func newProxyStack(t *testing.T, workers int) *proxyStack {
	return newProxyStackCfg(t, workers, "")
}

func newProxyStackCfg(t *testing.T, workers int, extra string) *proxyStack {
	return newProxyStackPolicy(t, workers, extra, routesync.Policy{
		Domain: "hotup.test", AuthMode: "off", ParkTimeoutMS: 2000,
	})
}

func newProxyStackPolicy(t *testing.T, workers int, extra string, policy routesync.Policy) *proxyStack {
	t.Helper()
	bin := os.Getenv(hotUpgradeBinEnv)
	if bin == "" {
		t.Skipf("%s not set; build node-ctl and the test binary for linux to run the docker verification", hotUpgradeBinEnv)
	}
	if runtime.GOOS != "linux" {
		t.Skip("proxy master/worker reexec requires linux")
	}
	dir := t.TempDir()
	backend := startEchoBackend(t)
	backendPort := backend.ln.Addr().(*net.TCPAddr).Port
	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	configSocket := filepath.Join(dir, "node-ctl.socket")
	source := &staticRouteSource{
		policy: policy,
		route: routesync.RouteEntry{
			SandboxID:  "sb-hotup",
			StableID:   "sb-hotup",
			Profile:    "e2b",
			State:      routesync.StateRunning,
			FloatingIP: "127.0.0.1",
		},
	}
	serverReady := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- configsock.New(configSocket, configsock.Deps{
			RouteSource: source,
			Plugins:     configsock.NewRegistry(),
		}, slog.New(slog.NewTextHandler(os.Stderr, nil))).ServeReady(serverCtx, serverReady)
	}()
	select {
	case <-serverReady:
	case err := <-serverDone:
		t.Fatalf("config socket exited before readiness: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("config socket readiness timed out")
	}
	t.Cleanup(func() {
		select {
		case err := <-serverDone:
			if err != nil {
				t.Logf("config socket server error: %v", err)
			}
		case <-time.After(2 * time.Second):
		}
	})
	dataAddr := reserveLoopbackAddress(t)
	cfg := fmt.Sprintf(`config_socket: %s
paths:
  run_root: %s
data_listen: %s
workers: %d
auth: off
park_timeout: 2s
`, configSocket, filepath.Join(dir, "run"), dataAddr, workers) + extra
	cfgPath := filepath.Join(dir, "proxy.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return &proxyStack{
		t: t, dir: dir, cfgPath: cfgPath, bin: bin, backend: backend,
		serverCtx: serverCtx, stopServer: stopServer, proxyAddr: dataAddr,
		sid: "sb-hotup", backendPort: backendPort,
	}
}

func (s *proxyStack) tryTunnel(budgetMS int) error {
	tc, err := openTunnel(s.proxyAddr, s.sid, s.backendPort, budgetMS)
	if err != nil {
		return err
	}
	tc.close()
	return nil
}

func (s *proxyStack) awaitServing(start time.Time, timeout time.Duration) (bindDelay, serveDelay time.Duration) {
	s.t.Helper()
	bound := time.Time{}
	lastErr := error(nil)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if bound.IsZero() {
			conn, err := net.DialTimeout("tcp", s.proxyAddr, 50*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				bound = time.Now()
			}
		}
		err := s.tryTunnel(200)
		if err == nil {
			if !bound.IsZero() {
				bindDelay = bound.Sub(start)
			}
			serveDelay = time.Since(start)
			return bindDelay, serveDelay
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	s.t.Logf("proxy master log tail:\n%s", readTail(s.lastLog))
	s.t.Fatalf("proxy did not become serving within %s (bound=%v last_err=%v)", timeout, !bound.IsZero(), lastErr)
	return 0, 0
}

func (s *proxyStack) startMaster(name string) *masterProc {
	s.lastLog = filepath.Join(s.dir, name+".log")
	return startProxyMaster(s.t, s.bin, s.cfgPath, s.lastLog)
}

func (s *proxyStack) openLiveTunnel(name string) *tunnelClient {
	s.t.Helper()
	tc, err := openTunnel(s.proxyAddr, s.sid, s.backendPort, 5000)
	if err != nil {
		s.t.Fatalf("%s: open tunnel: %v", name, err)
	}
	return tc
}

func (s *proxyStack) awaitTick(tc *tunnelClient, want int) {
	s.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tc.mu.Lock()
		last := tc.lastLine
		tc.mu.Unlock()
		if strings.HasPrefix(last, "tick ") {
			if n, err := strconv.Atoi(strings.TrimPrefix(last, "tick ")); err == nil && n >= want {
				return
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("heartbeat stalled at %q", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProxyConcurrentStartRejected(t *testing.T) {
	stack := newProxyStack(t, 1)
	start := time.Now()
	_ = stack.startMaster("concurrent-base")
	stack.awaitServing(start, 30*time.Second)
	second := stack.startMaster("concurrent-second")
	if err, exited := second.exitWithin(10 * time.Second); !exited {
		_ = second.cmd.Process.Kill()
		t.Fatal("second proxy master instance kept running alongside the first")
	} else if err == nil {
		t.Fatal("second proxy master instance exited cleanly while first still holds the listeners")
	} else {
		log := readTail(filepath.Join(stack.dir, "concurrent-second.log"))
		t.Logf("RESULT phase=concurrent-start outcome=rejected err=%v log_tail=%q", err, log)
	}
}

func TestProxyWorkerCrashLongConnection(t *testing.T) {
	stack := newProxyStack(t, 1)
	start := time.Now()
	master := stack.startMaster("worker-crash")
	stack.awaitServing(start, 30*time.Second)
	tc := stack.openLiveTunnel("baseline")
	stack.awaitTick(tc, 3)
	pids := childPIDs(master.cmd.Process.Pid)
	if len(pids) != 1 {
		t.Fatalf("expected exactly one worker child, got %v", pids)
	}
	worker := pids[0]
	killAt := time.Now()
	if err := syscall.Kill(worker, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	breakErr, broke := tc.awaitBreak(5 * time.Second)
	if !broke {
		t.Fatal("long connection survived worker SIGKILL")
	}
	breakMS := time.Since(killAt).Milliseconds()
	recoverStart := time.Now()
	stack.awaitServing(recoverStart, 30*time.Second)
	recoverMS := time.Since(recoverStart).Milliseconds()
	var replacement []int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		current := childPIDs(master.cmd.Process.Pid)
		if len(current) == 1 && current[0] != worker {
			replacement = current
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if replacement == nil {
		t.Fatalf("worker replacement not observed; children=%v", childPIDs(master.cmd.Process.Pid))
	}
	tc2 := stack.openLiveTunnel("post-recovery")
	stack.awaitTick(tc2, 3)
	tc2.close()
	t.Logf("RESULT phase=worker-crash killed_pid=%d break_ms=%d break_err=%v recover_ms=%d replacement_pid=%d",
		worker, breakMS, breakErr, recoverMS, replacement[0])
	tc.close()
}

func TestProxyMasterColdRestartLongConnection(t *testing.T) {
	stack := newProxyStackCfg(t, 1, "drain_timeout: 1s\n")
	start := time.Now()
	first := stack.startMaster("cold-restart-first")
	_, initialServe := stack.awaitServing(start, 30*time.Second)
	tc := stack.openLiveTunnel("pre-upgrade")
	stack.awaitTick(tc, 3)

	termAt := time.Now()
	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	breakErr, broke := tc.awaitBreak(5 * time.Second)
	if !broke {
		t.Fatal("long connection survived proxy master SIGTERM past the drain bound")
	}
	breakMS := time.Since(termAt).Milliseconds()
	exitStart := time.Now()
	exitErr, exited := first.exitWithin(10 * time.Second)
	if !exited {
		_ = first.cmd.Process.Kill()
		t.Fatal("proxy master did not exit after SIGTERM")
	}
	exitMS := time.Since(exitStart).Milliseconds()
	gapStart := time.Now()
	gapErr := stack.tryTunnel(300)
	gapMS := time.Since(gapStart).Milliseconds()

	restartAt := time.Now()
	second := stack.startMaster("cold-restart-second")
	bindDelay, serveDelay := stack.awaitServing(restartAt, 30*time.Second)
	tc2 := stack.openLiveTunnel("post-upgrade")
	stack.awaitTick(tc2, 3)
	tc2.close()
	t.Logf("RESULT phase=sigterm-graceful-drain initial_serve_ms=%d break_ms=%d break_err=%v exit_ms=%d exit_err=%v gap_probe_ms=%d gap_probe_err=%v bind_ms=%d serve_ms=%d",
		initialServe.Milliseconds(), breakMS, breakErr, exitMS, exitErr, gapMS, gapErr, bindDelay.Milliseconds(), serveDelay.Milliseconds())

	tc3 := stack.openLiveTunnel("pre-crash")
	stack.awaitTick(tc3, 3)
	crashWorkers := childPIDs(second.cmd.Process.Pid)
	killAt := time.Now()
	if err := second.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	orphans := 0
	for _, pid := range crashWorkers {
		if processExists(pid) {
			orphans++
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	crashBreakErr, crashBroke := tc3.awaitBreak(5 * time.Second)
	if !crashBroke {
		t.Fatal("long connection survived proxy master SIGKILL")
	}
	crashBreakMS := time.Since(killAt).Milliseconds()
	if err, exited := second.exitWithin(5 * time.Second); !exited {
		t.Fatalf("killed proxy master still running: %v", err)
	}
	crashRestartAt := time.Now()
	third := stack.startMaster("cold-restart-third")
	_, crashServeDelay := stack.awaitServing(crashRestartAt, 30*time.Second)
	t.Logf("RESULT phase=sigkill-crash break_ms=%d break_err=%v orphan_workers_killed=%d serve_ms=%d",
		crashBreakMS, crashBreakErr, orphans, crashServeDelay.Milliseconds())
	_ = third
	tc.close()
	tc3.close()
}

func readTail(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	text := strings.TrimSpace(string(b))
	lines := strings.Split(text, "\n")
	if len(lines) > 6 {
		lines = lines[len(lines)-6:]
	}
	return strings.Join(lines, " | ")
}
