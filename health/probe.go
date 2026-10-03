package health

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// Probe performs an HTTP GET against url and returns nil when it answers
// 200 OK. It is the building block for container health checks that must run
// from inside the process (`myapp -healthcheck http://127.0.0.1:6060/readyz`):
// the check cannot dial the listener in-process, so a tiny self-GET through
// the loopback interface is the reliable way to observe what the orchestrator
// observes. Respect a deadline through ctx.
func Probe(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}
