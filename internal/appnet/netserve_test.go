package appnet

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

func TestServePlainHTTPAndGracefulCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}), nil)
	}()
	response, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", response.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop after cancellation")
	}
}

func TestServeTLSUsesCoreALPNPolicy(t *testing.T) {
	certificate := appnetCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), &tls.Config{
			Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		})
	}()
	connection, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{
		MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, NextProtos: []string{"h2"}, // test-only trust bypass
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := connection.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("ALPN=%q, want h2", got)
	}
	connection.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TLS Serve did not stop after cancellation")
	}
}

func TestServeMetricsListenerServesUntilListenerCloses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mx := metrics.New()
	mx.Inc("proxy_test_metric")
	done := make(chan struct{})
	go func() {
		ServeMetricsListener(context.Background(), listener, mx,
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()
	response, err := http.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 4096)
	read, _ := response.Body.Read(body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Contains(body[:read], []byte("proxy_test_metric")) {
		t.Fatalf("status=%d body=%q", response.StatusCode, body[:read])
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("metrics serving did not stop after listener close")
	}
}

func TestServeMetricsStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		ServeMetrics(ctx, "127.0.0.1:0", metrics.New(),
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeMetrics did not stop on cancelled context")
	}
}

func TestServeMetricsListenFailureReturns(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	logBuffer := &bytes.Buffer{}
	done := make(chan struct{})
	go func() {
		ServeMetrics(context.Background(), listener.Addr().String(), metrics.New(),
			slog.New(slog.NewTextHandler(logBuffer, nil)))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeMetrics did not return after listen failure")
	}
	if !bytes.Contains(logBuffer.Bytes(), []byte("metrics listen")) {
		t.Fatalf("listen failure not logged: %q", logBuffer.String())
	}
}

func appnetCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
