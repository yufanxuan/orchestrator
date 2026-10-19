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

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// spawnHotUpgradeSuccessor starts stack.bin as a hot-upgrade successor with
// the pre-bound data and MMDS listeners passed as inherited descriptors, the
// same environment an old master forks for the on-disk binary. The returned
// release function closes this process's duplicated descriptors; callers
// should drop their own listener copies too so address reusability reflects
// only the successor's lifetime.
func spawnHotUpgradeSuccessor(t *testing.T, stack *proxyStack, dataListener, mmdsListener net.Listener, passMMDS bool, logPath string) (*masterProc, func()) {
	t.Helper()
	dataFile, err := dataListener.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	extraFiles := []*os.File{dataFile}
	env := append(os.Environ(),
		"KUASAR_PROXY_HOT_UPGRADE=1",
		"KUASAR_PROXY_HOT_UPGRADE_DATA_FD=3",
	)
	var mmdsFile *os.File
	if passMMDS {
		file, err := mmdsListener.(*net.TCPListener).File()
		if err != nil {
			t.Fatal(err)
		}
		mmdsFile = file
		extraFiles = append(extraFiles, mmdsFile)
		env = append(env, "KUASAR_PROXY_HOT_UPGRADE_MMDS_FD=4")
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(stack.bin, "proxy", "serve", "--config", stack.cfgPath)
	cmd.Env = env
	cmd.ExtraFiles = extraFiles
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	release := func() {
		_ = dataFile.Close()
		if mmdsFile != nil {
			_ = mmdsFile.Close()
		}
	}
	t.Cleanup(release)
	m := &masterProc{cmd: cmd, done: make(chan error, 1)}
	go func() { m.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if _, exited := m.exitWithin(0); !exited {
			_ = cmd.Process.Kill()
			m.exitWithin(2 * time.Second)
		}
		_ = logFile.Close()
	})
	return m, release
}

// TestProxyHotUpgradeInheritedMMDSListener proves the successor restores both
// the data and the MMDS listener from inherited descriptors when the conductor
// policy enables MMDS: no re-bind happens, the restored MMDS endpoint answers
// alongside the data plane, and the successor tears down cleanly on SIGTERM.
func TestProxyHotUpgradeInheritedMMDSListener(t *testing.T) {
	mmdsAddr := reserveLoopbackAddress(t)
	stack := newProxyStackPolicy(t, 1, "drain_timeout: 1s\n", routesync.Policy{
		Domain: "hotup.test", AuthMode: "off", ParkTimeoutMS: 2000,
		MMDS: &routesync.MMDSProxyPolicy{Enabled: true, Listen: mmdsAddr},
	})
	dataListener, err := net.Listen("tcp", stack.proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer dataListener.Close()
	mmdsListener, err := net.Listen("tcp", mmdsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer mmdsListener.Close()
	logPath := filepath.Join(stack.dir, "hot-mmds.log")
	master, release := spawnHotUpgradeSuccessor(t, stack, dataListener, mmdsListener, true, logPath)
	_ = dataListener.Close()
	_ = mmdsListener.Close()
	release()

	start := time.Now()
	stack.awaitServing(start, 30*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for !proxyHTTPReady(mmdsAddr) {
		if _, exited := master.exitWithin(0); exited {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restored MMDS endpoint never answered: %s", readTail(logPath))
		}
		time.Sleep(10 * time.Millisecond)
	}
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "mmds listener restored from inherited descriptor") {
		t.Fatalf("successor log lacks inherited MMDS line:\n%s", contents)
	}

	if err := master.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err, exited := master.exitWithin(10 * time.Second); !exited {
		_ = master.cmd.Process.Kill()
		t.Fatalf("successor did not exit after SIGTERM: %v", err)
	}
	if err := assertAddressReusable(mmdsAddr); err != nil {
		t.Fatalf("inherited MMDS listener was not released: %v", err)
	}
	t.Logf("RESULT hot-upgrade mmds=inherited-restored mmds_addr=%s", mmdsAddr)
}

// TestProxyHotUpgradeInheritedMMDSDisabledClosesListener proves the successor
// closes the inherited MMDS descriptor when the conductor policy leaves MMDS
// disabled: the data plane still serves from its inherited listener and the
// successor exits cleanly without binding MMDS.
func TestProxyHotUpgradeInheritedMMDSDisabledClosesListener(t *testing.T) {
	mmdsAddr := reserveLoopbackAddress(t)
	stack := newProxyStackCfg(t, 1, "drain_timeout: 1s\nmetrics_listen: 127.0.0.1:0\n")
	dataListener, err := net.Listen("tcp", stack.proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer dataListener.Close()
	mmdsListener, err := net.Listen("tcp", mmdsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer mmdsListener.Close()
	logPath := filepath.Join(stack.dir, "hot-mmds-disabled.log")
	master, release := spawnHotUpgradeSuccessor(t, stack, dataListener, mmdsListener, true, logPath)
	_ = dataListener.Close()
	_ = mmdsListener.Close()
	release()

	start := time.Now()
	stack.awaitServing(start, 30*time.Second)
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "mmds listener restored") {
		t.Fatalf("successor restored MMDS although the policy disables it:\n%s", contents)
	}

	if err := master.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err, exited := master.exitWithin(10 * time.Second); !exited {
		_ = master.cmd.Process.Kill()
		t.Fatalf("successor did not exit after SIGTERM: %v", err)
	}
	if err := assertAddressReusable(stack.proxyAddr); err != nil {
		t.Fatalf("inherited data listener was not released: %v", err)
	}
	t.Logf("RESULT hot-upgrade mmds=disabled-closed mmds_addr=%s", mmdsAddr)
}

// TestProxyMasterMMDSListenFailureExits proves the master fails fast when the
// conductor policy enables MMDS but the MMDS address cannot be bound: the
// process exits with the listen error instead of serving without MMDS.
func TestProxyMasterMMDSListenFailureExits(t *testing.T) {
	mmdsAddr := reserveLoopbackAddress(t)
	held, err := net.Listen("tcp", mmdsAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	stack := newProxyStackPolicy(t, 1, "drain_timeout: 1s\n", routesync.Policy{
		Domain: "hotup.test", AuthMode: "off", ParkTimeoutMS: 2000,
		MMDS: &routesync.MMDSProxyPolicy{Enabled: true, Listen: mmdsAddr},
	})
	logPath := filepath.Join(stack.dir, "mmds-listen-fail.log")
	master := startProxyMaster(stack.t, stack.bin, stack.cfgPath, logPath)
	err, exited := master.exitWithin(10 * time.Second)
	if !exited {
		_ = master.cmd.Process.Kill()
		t.Fatal("master kept running with an unbindable MMDS address")
	}
	if err == nil {
		t.Fatalf("master exited cleanly although the MMDS listen failed:\n%s", readTail(logPath))
	}
	contents, _ := os.ReadFile(logPath)
	if !strings.Contains(string(contents), "listen conductor MMDS address") {
		t.Fatalf("master log lacks the MMDS listen failure:\n%s", contents)
	}
}
