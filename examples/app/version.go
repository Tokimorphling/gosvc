package app

import "fmt"

// Build metadata, injected with -ldflags at build time (see the Makefile).
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

// FullVersion returns a human readable version string for telemetry and the
// admin /version endpoint.
func FullVersion() string {
	return fmt.Sprintf("%s (commit=%s built=%s)", Version, Commit, BuildTime)
}
