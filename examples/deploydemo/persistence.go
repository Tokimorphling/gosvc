package deploydemo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxStateBytes = 64 << 20

type diskState struct {
	Version int    `json:"version"`
	NextID  int64  `json:"nextId"`
	Tasks   []Task `json:"tasks"`
}

func loadState(cfg TaskConfig) (diskState, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DataFile), 0700); err != nil {
		return diskState{}, fmt.Errorf("create data directory: %w", err)
	}
	f, err := os.Open(cfg.DataFile)
	if errors.Is(err, os.ErrNotExist) {
		return diskState{Version: 1, Tasks: []Task{}}, nil
	}
	if err != nil {
		return diskState{}, fmt.Errorf("open task data: %w", err)
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxStateBytes+1))
	if err != nil {
		return diskState{}, fmt.Errorf("read task data: %w", err)
	}
	if len(raw) > maxStateBytes {
		return diskState{}, errors.New("task data exceeds 64 MiB")
	}
	var state diskState
	if err := json.Unmarshal(raw, &state); err != nil {
		return diskState{}, fmt.Errorf("decode task data: %w", err)
	}
	if state.Version != 1 || state.NextID < 0 || len(state.Tasks) > cfg.MaxTasks {
		return diskState{}, errors.New("invalid task data version, sequence or capacity")
	}
	var previousID int64
	for _, task := range state.Tasks {
		if task.ID <= previousID || task.ID > state.NextID || !validTitle(strings.TrimSpace(task.Title), cfg.MaxTitleLen) || task.CreatedAt.IsZero() {
			return diskState{}, errors.New("invalid task data record")
		}
		previousID = task.ID
	}
	return state, nil
}

// saveState creates a private temporary file in the destination directory so
// rename is atomic. Data and directory entries are synced before acknowledging
// success. The bool reports whether rename committed the new state.
func saveState(ctx context.Context, path string, state diskState) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return false, err
	}
	if len(raw) > maxStateBytes {
		return false, errors.New("task data exceeds 64 MiB")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	defer dir.Close()
	f, err := os.CreateTemp(filepath.Dir(path), ".tasks-*.tmp")
	if err != nil {
		return false, err
	}
	name := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(name)
	}()
	if _, err := f.Write(raw); err != nil {
		return false, err
	}
	if err := f.Sync(); err != nil {
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := os.Rename(name, path); err != nil {
		return false, err
	}
	return true, dir.Sync()
}
