// Package appnet contains process-assembly networking shared by the built-in
// and statically customized conductor/proxy Apps.
package appnet

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
)

// Serve serves handler until ctx is cancelled. A non-nil TLS config is already
// populated with core-owned policy and key material; nil serves h2c and HTTP/1.1.
func Serve(ctx context.Context, ln net.Listener, handler http.Handler, tlsConfig *tls.Config) error {
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second}
	shutdownDone := make(chan struct{})
	defer close(shutdownDone)
	go func() {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		case <-shutdownDone:
		}
	}()
	var err error
	if tlsConfig != nil {
		srv.Handler = handler
		srv.TLSConfig = tlsConfig.Clone()
		err = srv.ServeTLS(ln, "", "")
	} else {
		srv.Handler = h2c.NewHandler(handler, &http2.Server{})
		err = srv.Serve(ln)
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// ServeMetrics runs a small /metrics endpoint until ctx is cancelled.
func ServeMetrics(ctx context.Context, addr string, mx *metrics.M, log *slog.Logger) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Warn("metrics listen", "addr", addr, "err", err)
		return
	}
	ServeMetricsListener(ctx, ln, mx, log)
}

// ServeMetricsListener serves /metrics on an already-bound listener until ctx
// is cancelled or the listener is closed. Closing the listener ends serving,
// which lets callers stop and later re-bind the metrics endpoint.
func ServeMetricsListener(ctx context.Context, ln net.Listener, mx *metrics.M, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", mx.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		case <-done:
		}
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Warn("metrics server", "addr", ln.Addr(), "err", err)
	}
}
