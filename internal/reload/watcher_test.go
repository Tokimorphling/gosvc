package reload

import (
	"path/filepath"
	"testing"
)

func TestSameFileIncludesConfigMapProjectionSwap(t *testing.T) {
	configPath := filepath.Join("/tmp", "projected", "config.toml")
	if !sameFile(configPath, configPath) {
		t.Fatal("direct config event missed")
	}
	if !sameFile(filepath.Join("/tmp", "projected", "..data"), configPath) {
		t.Fatal("Kubernetes ..data symlink swap missed")
	}
	if sameFile(filepath.Join("/tmp", "other", "..data"), configPath) || sameFile(filepath.Join("/tmp", "other", "config.toml"), configPath) {
		t.Fatal("unrelated directory event matched")
	}
}
