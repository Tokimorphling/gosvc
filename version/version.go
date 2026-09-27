// Package version exposes build metadata injected at link time.
package version

import "fmt"

var (
	// Version is the semantic version or git describe output.
	Version = "dev"
	// Commit is the short git commit hash.
	Commit = "none"
	// BuildTime is the UTC build timestamp.
	BuildTime = "unknown"
)

// Full returns a human readable version string.
func Full() string {
	return fmt.Sprintf("%s (commit=%s built=%s)", Version, Commit, BuildTime)
}
