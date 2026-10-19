package proxyapp

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestNotifySystemdReadySendsReadyState(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "notify.sock")
	receiver, err := net.ListenPacket("unixgram", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	t.Setenv("NOTIFY_SOCKET", socketPath)
	received := make(chan string, 1)
	go func() {
		buffer := make([]byte, 64)
		n, _, err := receiver.ReadFrom(buffer)
		if err != nil {
			received <- err.Error()
			return
		}
		received <- string(buffer[:n])
	}()
	notifySystemdReady(slog.New(slog.NewTextHandler(io.Discard, nil)))
	select {
	case message := <-received:
		if message != "READY=1" {
			t.Fatalf("notification=%q, want READY=1", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readiness notification not delivered")
	}
}

func TestNotifySystemdReadyWithoutSocketEnvIsNoop(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	logBuffer := &bytes.Buffer{}
	notifySystemdReady(slog.New(slog.NewTextHandler(logBuffer, nil)))
	if logBuffer.Len() != 0 {
		t.Fatalf("no-op notify logged: %q", logBuffer.String())
	}
}

func TestNotifySystemdReadyFailedSendWarns(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", filepath.Join(t.TempDir(), "missing.sock"))
	logBuffer := &bytes.Buffer{}
	notifySystemdReady(slog.New(slog.NewTextHandler(logBuffer, nil)))
	if !bytes.Contains(logBuffer.Bytes(), []byte("readiness notify")) {
		t.Fatalf("failed notify not logged: %q", logBuffer.String())
	}
}
