package proxyapp

import (
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
)

func setHotUpgradeEnv(t *testing.T, dataFD, mmdsFD int) {
	t.Helper()
	t.Setenv(hotUpgradeEnv, "1")
	if dataFD >= 0 {
		t.Setenv(hotUpgradeDataFD, strconv.Itoa(dataFD))
	} else {
		_ = os.Unsetenv(hotUpgradeDataFD)
	}
	if mmdsFD >= 0 {
		t.Setenv(hotUpgradeMMDSFD, strconv.Itoa(mmdsFD))
	} else {
		_ = os.Unsetenv(hotUpgradeMMDSFD)
	}
}

func TestHotUpgradeRestoreListenersRoundTrip(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listenerFile, err := listener.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer listenerFile.Close()
	setHotUpgradeEnv(t, int(listenerFile.Fd()), -1)

	data, mmds, err := RestoreHotUpgradeListeners()
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if mmds != nil {
		t.Fatalf("unexpected mmds listener %v", mmds)
	}
	if got := data.Addr().String(); got != listener.Addr().String() {
		t.Fatalf("restored addr=%s, want %s", got, listener.Addr().String())
	}
	for _, name := range []string{hotUpgradeEnv, hotUpgradeDataFD, hotUpgradeMMDSFD} {
		if _, present := os.LookupEnv(name); present {
			t.Fatalf("%s still set after restore", name)
		}
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := data.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case inbound := <-accepted:
		_ = inbound.Close()
	case <-timeoutAfterSeconds(5):
		t.Fatal("restored listener did not accept")
	}
}

func TestHotUpgradeRestoreNegative(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	_ = os.Unsetenv(hotUpgradeEnv)
	if _, _, err := RestoreHotUpgradeListeners(); err == nil {
		t.Fatal("restore without environment succeeded")
	}
	setHotUpgradeEnv(t, 0, -1)
	if _, _, err := RestoreHotUpgradeListeners(); err == nil {
		t.Fatal("restore with fd 0 succeeded")
	}
	setHotUpgradeEnv(t, 99, -1)
	if _, _, err := RestoreHotUpgradeListeners(); err == nil {
		t.Fatal("restore with bogus fd succeeded")
	}
}

func TestMasterControlCaptureListenerFDs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	empty := &MasterControl{}
	if _, _, err := empty.CaptureListenerFDs(); err == nil {
		t.Fatal("capture before serving succeeded")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	control := &MasterControl{}
	control.setListeners(listener, nil)
	dataFile, mmdsFile, err := control.CaptureListenerFDs()
	if err != nil {
		t.Fatal(err)
	}
	defer dataFile.Close()
	if mmdsFile != nil {
		_ = mmdsFile.Close()
		t.Fatalf("unexpected mmds file %v", mmdsFile)
	}
	captured, err := net.FileListener(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Close()
	if got := captured.Addr().String(); got != listener.Addr().String() {
		t.Fatalf("captured addr=%s, want %s", got, listener.Addr().String())
	}
}
