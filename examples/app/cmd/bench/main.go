// Command bench is a small multi-protocol load generator for the template.
//
// Examples:
//
//	bench -mode rest    -http-addr 127.0.0.1:8080 -c 50 -d 10s
//	bench -mode jsonrpc -http-addr 127.0.0.1:8080 -c 50 -d 10s
//	bench -mode grpc    -grpc-addr 127.0.0.1:9090 -c 50 -d 10s
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	greeterv1 "example.com/gosvc/examples/app/api/greeter/v1"
)

type workerResult struct {
	latencies []time.Duration
	requests  int64
	errors    int64
}

func main() {
	mode := flag.String("mode", "rest", "protocol to benchmark: rest|jsonrpc|grpc")
	httpAddr := flag.String("http-addr", "127.0.0.1:8080", "HTTP address for rest and jsonrpc modes")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:9090", "gRPC address")
	concurrency := flag.Int("c", 50, "number of concurrent workers")
	duration := flag.Duration("d", 10*time.Second, "benchmark duration")
	name := flag.String("name", "bench", "name to send in requests")
	flag.Parse()

	switch *mode {
	case "rest", "jsonrpc", "grpc":
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", *mode)
		os.Exit(2)
	}
	if *concurrency <= 0 {
		fmt.Fprintln(os.Stderr, "-c must be > 0")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	results := make(chan workerResult, *concurrency)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch *mode {
			case "rest":
				results <- runREST(ctx, *httpAddr, *name)
			case "jsonrpc":
				results <- runJSONRPC(ctx, *httpAddr, *name)
			case "grpc":
				results <- runGRPC(ctx, *grpcAddr, *name)
			}
		}()
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	merged := workerResult{}
	for result := range results {
		merged.latencies = append(merged.latencies, result.latencies...)
		merged.requests += result.requests
		merged.errors += result.errors
	}
	report(*mode, merged, elapsed)
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1024,
			MaxIdleConnsPerHost: 1024,
			DisableCompression:  true,
		},
		Timeout: 10 * time.Second,
	}
}

func runREST(ctx context.Context, addr, name string) workerResult {
	client := newHTTPClient()
	endpoint := fmt.Sprintf("http://%s/api/v1/hello?name=%s", addr, url.QueryEscape(name))

	var result workerResult
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			result.errors++
			continue
		}

		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			result.errors++
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			result.errors++
			continue
		}
		result.requests++
		result.latencies = append(result.latencies, latency)
	}
	return result
}

func runJSONRPC(ctx context.Context, addr, name string) workerResult {
	client := newHTTPClient()
	endpoint := fmt.Sprintf("http://%s/rpc", addr)

	var result workerResult
	for id := 0; ctx.Err() == nil; id++ {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"greeter.sayHello","params":{"name":%q}}`, id, name)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
		if err != nil {
			result.errors++
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			result.errors++
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			result.errors++
			continue
		}
		result.requests++
		result.latencies = append(result.latencies, latency)
	}
	return result
}

func runGRPC(ctx context.Context, addr, name string) workerResult {
	var result workerResult

	conn, err := ggrpc.NewClient(addr, ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		result.errors++
		return result
	}
	defer conn.Close()
	client := greeterv1.NewGreeterClient(conn)

	// Warm up so dialing is not part of the measured latency.
	_, _ = client.SayHello(ctx, &greeterv1.SayHelloRequest{Name: name})

	for ctx.Err() == nil {
		start := time.Now()
		_, err := client.SayHello(ctx, &greeterv1.SayHelloRequest{Name: name})
		latency := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			result.errors++
			continue
		}
		result.requests++
		result.latencies = append(result.latencies, latency)
	}
	return result
}

func report(mode string, result workerResult, elapsed time.Duration) {
	sort.Slice(result.latencies, func(i, j int) bool {
		return result.latencies[i] < result.latencies[j]
	})

	fmt.Printf("mode=%s duration=%s requests=%d errors=%d qps=%.1f\n",
		mode, elapsed.Round(time.Millisecond), result.requests, result.errors,
		float64(result.requests)/elapsed.Seconds())

	if len(result.latencies) == 0 {
		return
	}
	fmt.Printf("latency p50=%s p90=%s p99=%s max=%s\n",
		percentile(result.latencies, 50),
		percentile(result.latencies, 90),
		percentile(result.latencies, 99),
		result.latencies[len(result.latencies)-1],
	)
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(p)/100.0*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
