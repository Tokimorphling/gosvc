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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/health"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/observability"
	"example.com/gosvc/internal/store"
	"example.com/gosvc/internal/version"
)

// Options wires the admin server.
type Options struct {
	Config     *config.Config
	Logger     *slog.Logger
	Level      *slog.LevelVar
	Metrics    *observability.Metrics
	Ready      *health.Ready
	TimeSeries store.TimeSeries
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

	// Runtime log level control: GET to read, PUT/POST {"level":"debug"} to set.
	mux.HandleFunc("/debug/loglevel", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]string{"level": logging.LevelName(opts.Level)})
		case http.MethodPut, http.MethodPost:
			var body struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&body); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
				return
			}
			if err := logging.SetLevel(opts.Level, body.Level); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"level": logging.LevelName(opts.Level)})
		default:
			w.Header().Set("Allow", "GET, PUT, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	})

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Time-series query: GET /debug/ts?metric=http.requests:/api/v1/hello&minutes=60
	mux.HandleFunc("/debug/ts", func(w http.ResponseWriter, r *http.Request) {
		if opts.TimeSeries == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "time-series store is disabled"})
			return
		}
		metric := r.URL.Query().Get("metric")
		if metric == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "metric query parameter is required"})
			return
		}
		minutes := 60
		if raw := r.URL.Query().Get("minutes"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "minutes must be a positive integer"})
				return
			}
			if parsed > 2880 {
				parsed = 2880
			}
			minutes = parsed
		}

		to := time.Now()
		from := to.Add(-time.Duration(minutes) * time.Minute)
		buckets, err := opts.TimeSeries.Range(r.Context(), metric, from, to)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"metric":  metric,
			"from":    from.UTC(),
			"to":      to.UTC(),
			"buckets": buckets,
		})
	})

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
