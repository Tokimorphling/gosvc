// Package reload watches the configuration file and applies changes at runtime.
package reload

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"

	"example.com/gosvc/internal/config"
)

const defaultDebounce = 300 * time.Millisecond

// Watcher observes a configuration file and calls OnReload with a freshly
// validated config whenever the file changes.
//
// The parent directory is watched rather than the file itself, because editors
// and Kubernetes ConfigMap updates replace files via rename.
type Watcher struct {
	path     string
	logger   *slog.Logger
	debounce time.Duration
	onReload func(*config.Config)
}

// New builds a watcher for path. onReload runs on the watcher goroutine and must
// not block for long.
func New(path string, logger *slog.Logger, onReload func(*config.Config)) *Watcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Watcher{
		path:     path,
		logger:   logger,
		debounce: defaultDebounce,
		onReload: onReload,
	}
}

// Run watches until ctx is done and returns nil on shutdown.
func (w *Watcher) Run(ctx context.Context) error {
	if w.path == "" || w.onReload == nil {
		return nil
	}

	absPath, err := filepath.Abs(w.path)
	if err != nil {
		return fmt.Errorf("resolve config path: %w", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	defer watcher.Close()

	dir := filepath.Dir(absPath)
	if err := watcher.Add(dir); err != nil {
		return fmt.Errorf("watch %s: %w", dir, err)
	}
	w.logger.Info("watching configuration file", "path", absPath)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if !sameFile(event.Name, absPath) {
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}
			w.logger.Debug("configuration file event", "op", event.Op.String(), "path", event.Name)
			resetTimer(timer, w.debounce)

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			w.logger.Warn("configuration watcher error", "error", err)

		case <-timer.C:
			cfg, err := config.Load(w.path)
			if err != nil {
				w.logger.Error("configuration reload failed, keeping the current configuration", "error", err)
				continue
			}
			w.onReload(cfg)
		}
	}
}

func sameFile(eventName, absPath string) bool {
	if filepath.Clean(eventName) == filepath.Clean(absPath) {
		return true
	}
	return filepath.Base(eventName) == filepath.Base(absPath)
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}
