package gosvc_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/config"
	pb "github.com/Tokimorphling/gosvc/examples/app/api/greeter/v1"
	"github.com/Tokimorphling/gosvc/jsonrpc"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func regressionConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.HTTP.Host, cfg.HTTP.Port = "127.0.0.1", 0
	cfg.GRPC.Host, cfg.GRPC.Port = "127.0.0.1", 0
	cfg.Admin.Host, cfg.Admin.Port = "127.0.0.1", 0
	cfg.HTTP.ShutdownTimeout, cfg.GRPC.ShutdownTimeout = config.Duration(time.Second), config.Duration(time.Second)
	cfg.Log.Level, cfg.Log.Output = "error", "file"
	cfg.Log.File.Path = filepath.Join(t.TempDir(), "application.log")
	return cfg
}

type runningRuntime struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startRuntime(t *testing.T, app *gosvc.App) *runningRuntime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningRuntime{cancel: cancel, done: make(chan struct{})}
	go func() { defer close(r.done); r.err = app.Run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	client := &http.Client{Timeout: 100 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + app.AdminAddr() + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return r
			}
		}
		select {
		case <-r.done:
			t.Fatalf("Run returned during startup: %v", r.err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("runtime did not start")
	return nil
}

func (r *runningRuntime) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop")
	}
}

func testCertificate(t *testing.T) (config.TLSConfig, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	dir := t.TempDir()
	pair := config.TLSConfig{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem")}
	if err := os.WriteFile(pair.CertFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pair.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("invalid test certificate")
	}
	return pair, &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

func TestNativeTLSRoundTrips(t *testing.T) {
	pair, clientTLS := testCertificate(t)
	cfg := regressionConfig(t)
	cfg.HTTP.TLS, cfg.GRPC.TLS = pair, pair
	app, err := gosvc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := startRuntime(t, app)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true}}
	resp, err := client.Get("https://" + app.HTTPAddr() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HTTPS readiness = %d", resp.StatusCode)
	}
	cc, err := ggrpc.NewClient(app.GRPCAddr(), ggrpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	res, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("gRPC TLS readiness = %v, %v", res, err)
	}
	r.stop(t)
	if r.err != nil {
		t.Fatal(r.err)
	}
}

func TestInvalidCertificatesReturnErrorsAndReleaseListeners(t *testing.T) {
	for _, transport := range []string{"http", "grpc"} {
		for _, invalid := range []string{"missing", "malformed"} {
			t.Run(transport+"/"+invalid, func(t *testing.T) {
				cfg := regressionConfig(t)
				for _, port := range []*int{&cfg.HTTP.Port, &cfg.GRPC.Port} {
					ln, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					*port = ln.Addr().(*net.TCPAddr).Port
					_ = ln.Close()
				}
				dir := t.TempDir()
				pair := config.TLSConfig{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem")}
				if invalid == "malformed" {
					for _, path := range []string{pair.CertFile, pair.KeyFile} {
						if err := os.WriteFile(path, []byte("invalid PEM"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				if transport == "http" {
					cfg.HTTP.TLS = pair
				} else {
					cfg.GRPC.TLS = pair
				}
				if app, err := gosvc.New(cfg); err == nil {
					_ = app.Close()
					t.Fatal("invalid certificate accepted")
				}
				for _, addr := range []string{cfg.HTTP.Addr(), cfg.GRPC.Addr()} {
					ln, err := net.Listen("tcp", addr)
					if err != nil {
						t.Fatalf("listener leaked at %s: %v", addr, err)
					}
					_ = ln.Close()
				}
			})
		}
	}
}

func getStatus(t *testing.T, url string, want int) {
	t.Helper()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("%s: status = %d, want %d", url, resp.StatusCode, want)
	}
}

func TestReadinessAgreesAcrossProtocolsAndWatchRecovers(t *testing.T) {
	app, err := gosvc.New(regressionConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	failed.Store(true)
	app.Health().AddCheck("dependency", func(context.Context) error {
		if failed.Load() {
			return errors.New("offline")
		}
		return nil
	})
	r := startRuntime(t, app)
	cc, err := ggrpc.NewClient(app.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	h := healthpb.NewHealthClient(cc)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	watch, err := h.Watch(ctx, &healthpb.HealthCheckRequest{Service: "grpc.health.v1.Health"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := watch.Recv()
	if err != nil || initial.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial watch = %v, %v", initial, err)
	}
	rpc := jsonrpc.NewHTTPClient("http://" + app.HTTPAddr())
	defer rpc.Close()
	check := func(wantReady bool) {
		t.Helper()
		wantHTTP, wantGRPC := 503, healthpb.HealthCheckResponse_NOT_SERVING
		if wantReady {
			wantHTTP, wantGRPC = 200, healthpb.HealthCheckResponse_SERVING
		}
		getStatus(t, "http://"+app.HTTPAddr()+"/readyz", wantHTTP)
		getStatus(t, "http://"+app.AdminAddr()+"/readyz", wantHTTP)
		getStatus(t, "http://"+app.HTTPAddr()+"/healthz", 200)
		for _, name := range []string{"", "grpc.health.v1.Health"} {
			res, err := h.Check(ctx, &healthpb.HealthCheckRequest{Service: name})
			if err != nil || res.GetStatus() != wantGRPC {
				t.Fatalf("Check(%q) = %v, %v", name, res, err)
			}
		}
		res, err := rpc.Call[struct{}, struct {
			Ready bool `json:"ready"`
		}](ctx, "system.health", struct{}{})
		if err != nil || res.Ready != wantReady {
			t.Fatalf("system.health = %+v, %v", res, err)
		}
	}
	check(false)
	failed.Store(false)
	// Do not issue Check here: the shared monitor must update Watch by itself.
	updated, err := watch.Recv()
	if err != nil || updated.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("recovered watch = %v, %v", updated, err)
	}
	check(true)
	if _, err := h.Check(ctx, &healthpb.HealthCheckRequest{Service: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown service = %v", err)
	}
	cancel()
	r.stop(t)
	if r.err != nil {
		t.Fatal(r.err)
	}
}

func TestShutdownHookDeadlineUnreadiesAndStopsTransports(t *testing.T) {
	cfg := regressionConfig(t)
	cfg.HTTP.ShutdownTimeout, cfg.GRPC.ShutdownTimeout = config.Duration(5*time.Second), config.Duration(5*time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	app, err := gosvc.New(cfg, gosvc.WithShutdownTimeout(300*time.Millisecond), gosvc.WithOnShutdown(func() {
		close(entered)
		<-release
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer close(release)
	r := startRuntime(t, app)
	cc, err := ggrpc.NewClient(app.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	watch, err := healthpb.NewHealthClient(cc).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	r.cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("hook did not start")
	}
	getStatus(t, "http://"+app.HTTPAddr()+"/readyz", 503)
	getStatus(t, "http://"+app.AdminAddr()+"/readyz", 503)
	res, err := watch.Recv()
	if err != nil || res.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("draining Watch = %v, %v", res, err)
	}
	// Keep Watch open: gRPC's own 5s timeout must not restart the 300ms budget.
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked hook or renewed transport budget prevented shutdown")
	}
	if !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want deadline error", r.err)
	}
}

func TestShutdownContextHooksReportErrorsAndRecoverPanics(t *testing.T) {
	sentinel := errors.New("flush failed")
	var calls atomic.Int32
	app, err := gosvc.New(regressionConfig(t), gosvc.WithOnShutdownContext(
		func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("hook missing deadline")
			}
			calls.Add(1)
			return sentinel
		},
		func(context.Context) error { calls.Add(1); panic("hook panic") },
		func(context.Context) error { calls.Add(1); return nil },
	))
	if err != nil {
		t.Fatal(err)
	}
	r := startRuntime(t, app)
	r.stop(t)
	if calls.Load() != 3 || !errors.Is(r.err, sentinel) || !strings.Contains(r.err.Error(), "hook panic") {
		t.Fatalf("calls=%d, Run=%v", calls.Load(), r.err)
	}
}

func TestHealthChecksBypassExhaustedBusinessLimiter(t *testing.T) {
	cfg := regressionConfig(t)
	cfg.Limiter.RPS, cfg.Limiter.Burst = 0.001, 1
	app, err := gosvc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := startRuntime(t, app)
	// HTTP and gRPC share the same per-peer bucket. Exhaust it with a business request.
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	for _, want := range []int{200, 429} {
		resp, err := client.Post("http://"+app.HTTPAddr()+"/rpc", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"system.methods"}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("business status=%d, want %d", resp.StatusCode, want)
		}
	}
	cc, err := ggrpc.NewClient(app.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for range 2 {
		getStatus(t, "http://"+app.HTTPAddr()+"/healthz", 200)
		getStatus(t, "http://"+app.HTTPAddr()+"/readyz", 200)
		res, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil || res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("health was limited: %v, %v", res, err)
		}
	}
	r.stop(t)
	if r.err != nil {
		t.Fatal(r.err)
	}
}

type regressionGreeter struct{ pb.UnimplementedGreeterServer }

func (regressionGreeter) SayHello(_ context.Context, req *pb.SayHelloRequest) (*pb.SayHelloResponse, error) {
	if req.Name == "panic" {
		panic("test panic")
	}
	return &pb.SayHelloResponse{Message: "ok"}, nil
}

func (regressionGreeter) WatchGreetings(req *pb.WatchGreetingsRequest, stream pb.Greeter_WatchGreetingsServer) error {
	if req.Id == -1 {
		panic("test stream panic")
	}
	return stream.Send(&pb.GreetingUpdate{Text: "ok"})
}

func TestGRPCFailuresAreCountedAndAccessLogged(t *testing.T) {
	cfg := regressionConfig(t)
	cfg.Auth.Enabled, cfg.Auth.APIKeys = true, []string{"test-key"}
	cfg.Log.Access.Enabled, cfg.Log.Access.Output = true, "file"
	cfg.Log.Access.File.Path = filepath.Join(t.TempDir(), "access.jsonl")
	app, err := gosvc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.RegisterGRPC(func(s *ggrpc.Server) { pb.RegisterGreeterServer(s, regressionGreeter{}) }); err != nil {
		t.Fatal(err)
	}
	r := startRuntime(t, app)
	cc, err := ggrpc.NewClient(app.GRPCAddr(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	client := pb.NewGreeterClient(cc)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, stream := range []bool{false, true} {
		for _, want := range []codes.Code{codes.Unauthenticated, codes.Internal, codes.OK} {
			callCtx := ctx
			if want != codes.Unauthenticated {
				callCtx = metadata.AppendToOutgoingContext(ctx, "x-api-key", "test-key")
			}
			name, id := "ok", int64(1)
			if want == codes.Internal {
				name, id = "panic", -1
			}
			var callErr error
			if stream {
				var s pb.Greeter_WatchGreetingsClient
				s, callErr = client.WatchGreetings(callCtx, &pb.WatchGreetingsRequest{Id: id})
				if callErr == nil {
					_, callErr = s.Recv()
					if callErr == nil {
						if _, eof := s.Recv(); !errors.Is(eof, io.EOF) {
							t.Fatalf("stream end: %v", eof)
						}
					}
				}
			} else {
				_, callErr = client.SayHello(callCtx, &pb.SayHelloRequest{Name: name})
			}
			if status.Code(callErr) != want {
				t.Fatalf("stream=%v: got %v, want %v", stream, callErr, want)
			}
		}
	}
	r.stop(t)
	if r.err != nil {
		t.Fatal(r.err)
	}
	families, err := app.Metrics().Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "gosvc_grpc_requests_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			counts[labels["method"]+":"+labels["code"]] = metric.Counter.GetValue()
		}
	}
	for _, method := range []string{"SayHello", "WatchGreetings"} {
		for _, code := range []string{"Unauthenticated", "Internal", "OK"} {
			if got := counts["/greeter.v1.Greeter/"+method+":"+code]; got != 1 {
				t.Errorf("%s/%s count=%v, want 1", method, code, got)
			}
		}
	}
	file, err := os.Open(cfg.Log.Access.File.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner, entries := bufio.NewScanner(file), 0
	for scanner.Scan() {
		var entry map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		entries++
		if entry["request_id"] == "" || entry["request_id"] == nil {
			t.Error("access log missing request id")
		}
		if entry["code"] != "Unauthenticated" && entry["subject"] != "api-key" {
			t.Errorf("lost authenticated identity: %v", entry)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if entries != 6 {
		t.Fatalf("access log entries = %d, want 6", entries)
	}
}
