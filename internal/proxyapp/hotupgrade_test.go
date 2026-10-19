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

func TestMasterControlCaptureListenerFDsWithMMDS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	mmds, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer mmds.Close()
	control := &MasterControl{}
	control.setListeners(data, mmds)
	dataFile, mmdsFile, err := control.CaptureListenerFDs()
	if err != nil {
		t.Fatal(err)
	}
	defer dataFile.Close()
	defer mmdsFile.Close()
	capturedData, err := net.FileListener(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	defer capturedData.Close()
	capturedMMDS, err := net.FileListener(mmdsFile)
	if err != nil {
		t.Fatal(err)
	}
	defer capturedMMDS.Close()
	if got := capturedData.Addr().String(); got != data.Addr().String() {
		t.Fatalf("captured data addr=%s, want %s", got, data.Addr().String())
	}
	if got := capturedMMDS.Addr().String(); got != mmds.Addr().String() {
		t.Fatalf("captured mmds addr=%s, want %s", got, mmds.Addr().String())
	}
}

type hotUpgradeStubListener struct{ net.Listener }

func TestMasterControlCaptureListenerFailures(t *testing.T) {
	control := &MasterControl{}
	control.setListeners(hotUpgradeStubListener{}, nil)
	if _, _, err := control.CaptureListenerFDs(); err == nil {
		t.Fatal("capture succeeded with an unsupported data listener")
	}
	supported, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer supported.Close()
	control.setListeners(supported, hotUpgradeStubListener{})
	if _, _, err := control.CaptureListenerFDs(); err == nil {
		t.Fatal("capture succeeded with an unsupported mmds listener")
	}
	// The failed mmds capture must not leave the data listener unusable.
	control.setListeners(supported, nil)
	dataFile, _, err := control.CaptureListenerFDs()
	if err != nil {
		t.Fatal(err)
	}
	defer dataFile.Close()
}

func TestHotUpgradeRestoreListenersWithMMDS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	data, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	dataFile, err := data.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer dataFile.Close()
	mmds, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer mmds.Close()
	mmdsFile, err := mmds.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer mmdsFile.Close()
	setHotUpgradeEnv(t, int(dataFile.Fd()), int(mmdsFile.Fd()))

	restoredData, restoredMMDS, err := RestoreHotUpgradeListeners()
	if err != nil {
		t.Fatal(err)
	}
	defer restoredData.Close()
	defer restoredMMDS.Close()
	if got := restoredData.Addr().String(); got != data.Addr().String() {
		t.Fatalf("restored data addr=%s, want %s", got, data.Addr().String())
	}
	if got := restoredMMDS.Addr().String(); got != mmds.Addr().String() {
		t.Fatalf("restored mmds addr=%s, want %s", got, mmds.Addr().String())
	}
	for _, name := range []string{hotUpgradeEnv, hotUpgradeDataFD, hotUpgradeMMDSFD} {
		if _, present := os.LookupEnv(name); present {
			t.Fatalf("%s still set after restore", name)
		}
	}
}

func TestHotUpgradeRestoreMMDSRejections(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	for name, mmdsFD := range map[string]string{
		"not a number":      "bogus",
		"below minimum":     "2",
		"closed descriptor": "99",
	} {
		t.Run(name, func(t *testing.T) {
			// Each case needs a fresh data descriptor: a successful data
			// restore consumes (dups and closes) the passed descriptor, and a
			// failed mmds restore closes the dup, so descriptors do not
			// survive across cases.
			data, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer data.Close()
			dataFile, err := data.(*net.TCPListener).File()
			if err != nil {
				t.Fatal(err)
			}
			defer dataFile.Close()
			t.Setenv(hotUpgradeEnv, "1")
			t.Setenv(hotUpgradeDataFD, strconv.Itoa(int(dataFile.Fd())))
			t.Setenv(hotUpgradeMMDSFD, mmdsFD)
			if _, _, err := RestoreHotUpgradeListeners(); err == nil {
				t.Fatal("restore succeeded with an invalid mmds descriptor")
			}
			// The failed restore must not break the caller's listener.
			go func() {
				if conn, err := net.Dial("tcp", data.Addr().String()); err == nil {
					_ = conn.Close()
				}
			}()
			conn, err := data.Accept()
			if err != nil {
				t.Fatalf("data listener broken after a failed restore: %v", err)
			}
			_ = conn.Close()
		})
	}
}
