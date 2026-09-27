// Package admin serves operations endpoints on a separate, usually private,
// listener: metrics, pprof, health, log controls, config inspection and reload.
package admin

import (
	"context"
	"crypto/subtle"
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

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/logging"
	"github.com/Tokimorphling/gosvc/observability"
	"github.com/Tokimorphling/gosvc/store"
)

// Options wires the admin server.
type Options struct {
	Config        *config.Config
	Logger        *slog.Logger
	Log           *logging.Handle
	Metrics       *observability.Metrics
	Ready         *health.Ready
	TimeSeries    store.TimeSeries
	Reload        func() error
	CurrentConfig func() *config.Config
	// Version is reported by GET /version.
	Version string
}

// Server is the operations HTTP server.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	mux        *http.ServeMux
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
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ready, details := opts.Ready.Check(r.Context())
		status := http.StatusOK
		body := map[string]any{"status": "ready"}
		if !ready {
			status = http.StatusServiceUnavailable
			body["status"] = "not_ready"
		}
		if details != nil {
			body["checks"] = details
		}
		writeJSON(w, status, body)
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": opts.Version})
	})

	registerLogLevel(mux, opts)
	registerLogStats(mux, opts)
	registerConfigEndpoints(mux, opts)
	registerTimeSeries(mux, opts)

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// The admin endpoints (pprof, config, reload, log level) are powerful;
	// when the operator configures a token every request must carry it as
	// "Authorization: Bearer <token>". An empty token keeps the endpoints
	// open, which is only appropriate on a loopback or otherwise private
	// listener.
	handler := http.Handler(mux)
	if token := opts.Config.Admin.Token; token != "" {
		handler = tokenHandler{next: mux, token: token}
	}

	return &Server{
		httpServer: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
		listener: listener,
		mux:      mux,
		logger:   opts.Logger,
	}, nil
}

// tokenHandler enforces the admin bearer token in constant time.
type tokenHandler struct {
	next  http.Handler
	token string
}

func (h tokenHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	provided := "Bearer " + h.token
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(provided)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="gosvc-admin"`)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid admin token"})
		return
	}
	h.next.ServeHTTP(w, r)
}

// registerLogLevel exposes runtime log level control:
// GET to read, PUT/POST {"level":"debug"} to set.
func registerLogLevel(mux *http.ServeMux, opts Options) {
	mux.HandleFunc("/debug/loglevel", func(w http.ResponseWriter, r *http.Request) {
		if opts.Log == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "logging handle is not available"})
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]string{"level": logging.LevelName(opts.Log.Level())})
		case http.MethodPut, http.MethodPost:
			var body struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&body); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
				return
			}
			if err := logging.SetLevel(opts.Log.Level(), body.Level); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"level": logging.LevelName(opts.Log.Level())})
		default:
			w.Header().Set("Allow", "GET, PUT, POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	})
}

// registerLogStats exposes log sampling counters.
func registerLogStats(mux *http.ServeMux, opts Options) {
	mux.HandleFunc("/debug/logstats", func(w http.ResponseWriter, r *http.Request) {
		if opts.Log == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"enabled": false})
			return
		}
		stats := opts.Log.SamplingStats()
		if stats == nil {
			writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled":         true,
			"emitted":         stats.Emitted,
			"dropped":         stats.Dropped,
			"droppedByLevel":  stats.ByLevel,
			"windowStartedAt": stats.WindowFrom,
		})
	})
}

// registerConfigEndpoints exposes the redacted effective config and a manual
// reload trigger (useful when the file is mounted from a ConfigMap).
func registerConfigEndpoints(mux *http.ServeMux, opts Options) {
	mux.HandleFunc("/debug/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		cfg := opts.Config
		if opts.CurrentConfig != nil {
			if current := opts.CurrentConfig(); current != nil {
				cfg = current
			}
		}
		if cfg == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "config is not available"})
			return
		}
		writeJSON(w, http.StatusOK, cfg.Redacted())
	})

	mux.HandleFunc("/debug/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if opts.Reload == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "hot reload is disabled"})
			return
		}
		if err := opts.Reload(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
	})
}

// registerTimeSeries exposes minute-bucket queries:
// GET /debug/ts?metric=http.requests:/api/v1/hello&minutes=60
func registerTimeSeries(mux *http.ServeMux, opts Options) {
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
}

// Mux exposes the admin mux so the runtime can add application routes before
// Serve starts.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// Addr returns the effective listen address.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Serve blocks until ctx is cancelled or the server fails. When ctx is
// cancelled it waits for the graceful drain to finish before returning, so
// callers (gosvc.App.Run) do not exit the process while scrape or reload
// requests are still in flight.
func (s *Server) Serve(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("admin graceful shutdown returned error", "error", err)
		}
	}()

	// Serve returns as soon as Shutdown closes the listener; the drain
	// continues afterwards, so join it.
	err := s.httpServer.Serve(s.listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
		return fmt.Errorf("admin serve: %w", err)
	}
	<-done
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
