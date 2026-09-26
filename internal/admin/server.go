// Package admin serves operations endpoints on a separate, usually private,
// listener: metrics, pprof, health and version.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/version"
)

// Options wires the admin server.
type Options struct {
	Config  *config.Config
	Logger  *slog.Logger
	Metrics *observability.Metrics
	Ready   *health.Ready
}

// Server is the operations HTTP server.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	logger     *slog.Logger
}

// New binds the admin listener and builds the mux.
func New(opts Options) (*Server, error) {
	listener, err := net.Listen("tcp", opts.Config.Admin.Addr())
	if err != nil {
		return nil, fmt.Errorf("listen admin: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(opts.Metrics.Registry(), promhttp.HandlerOpts{}))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !opts.Ready.IsReady() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": version.Full()})
	})

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &Server{
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
		logger:   opts.Logger,
	}, nil
}

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until ctx is cancelled or the server fails.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("admin graceful shutdown returned error", "error", err)
		}
	}()

	err := s.httpServer.Serve(s.listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
		return fmt.Errorf("admin serve: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
