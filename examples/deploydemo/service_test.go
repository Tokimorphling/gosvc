package deploydemo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Tokimorphling/gosvc/apierror"
)

func testTaskConfig(t *testing.T) TaskConfig {
	t.Helper()
	return TaskConfig{DataFile: filepath.Join(t.TempDir(), "private", "tasks.json"), MaxTitleLen: 160, MaxTasks: 1000}
}

func TestPersistenceAcrossRestartAndDeletion(t *testing.T) {
	cfg := testTaskConfig(t)
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	items, err := s.List(t.Context(), struct{}{})
	if err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty list=%v, %v", items, err)
	}
	created, err := s.Create(t.Context(), CreateRequest{Title: "  发布服务  "})
	if err != nil || created.Title != "发布服务" || created.ID != 1 || created.CreatedAt.IsZero() {
		t.Fatalf("created=%+v, %v", created, err)
	}
	created.Title = "caller mutation"
	for range 2 {
		completed, err := s.Complete(t.Context(), IDRequest{ID: 1})
		if err != nil || !completed.Done || completed.Title != "发布服务" {
			t.Fatalf("completed=%+v, %v", completed, err)
		}
	}
	s, err = NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.Get(t.Context(), IDRequest{ID: 1})
	if err != nil || !stored.Done || stored.Title != "发布服务" {
		t.Fatalf("reloaded=%+v, %v", stored, err)
	}
	if _, err := s.Delete(t.Context(), IDRequest{ID: 1}); err != nil {
		t.Fatal(err)
	}
	s, err = NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(t.Context(), IDRequest{ID: 1}); apierror.KindOf(err) != apierror.KindNotFound {
		t.Fatalf("deleted record after reload: %v", err)
	}
	next, err := s.Create(t.Context(), CreateRequest{Title: "next"})
	if err != nil || next.ID != 2 {
		t.Fatalf("IDs must survive deletion and restart: %+v, %v", next, err)
	}
	for path, mode := range map[string]os.FileMode{cfg.DataFile: 0600, filepath.Dir(cfg.DataFile): 0700} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("permissions %s: %v, %v", path, info, err)
		}
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(cfg.DataFile), ".tasks-*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files leaked: %v, %v", leftovers, err)
	}
}

func TestBusinessLimitsAndCanceledMutation(t *testing.T) {
	cfg := testTaskConfig(t)
	cfg.MaxTitleLen, cfg.MaxTasks = 4, 1
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, title := range []string{" ", "12345", "\xff"} {
		if _, err := s.Create(t.Context(), CreateRequest{Title: title}); apierror.KindOf(err) != apierror.KindInvalidArgument {
			t.Fatalf("title %q: %v", title, err)
		}
	}
	if _, err := s.Create(t.Context(), CreateRequest{Title: "first"}); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("overlong title: %v", err)
	}
	if _, err := s.Create(t.Context(), CreateRequest{Title: "okay"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), CreateRequest{Title: "full"}); apierror.KindOf(err) != apierror.KindConflict {
		t.Fatalf("capacity: %v", err)
	}
	if _, err := s.Get(t.Context(), IDRequest{}); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("invalid ID: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Complete(canceled, IDRequest{ID: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled mutation: %v", err)
	}
	reloaded, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	task, err := reloaded.Get(t.Context(), IDRequest{ID: 1})
	if err != nil || task.Done {
		t.Fatalf("canceled mutation changed disk: %+v, %v", task, err)
	}
}

func TestFailedPersistenceKeepsState(t *testing.T) {
	cfg := testTaskConfig(t)
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(t.Context(), CreateRequest{Title: "original"}); err != nil {
		t.Fatal(err)
	}
	// Renaming a temporary regular file onto a directory must fail even when
	// the test process runs as root; do not rely on chmod to simulate failure.
	s.config.DataFile = filepath.Dir(cfg.DataFile)
	if _, err := s.Create(t.Context(), CreateRequest{Title: "must fail"}); apierror.KindOf(err) != apierror.KindInternal {
		t.Fatalf("persistence failure: %v", err)
	}
	s.config.DataFile = cfg.DataFile
	items, err := s.List(t.Context(), struct{}{})
	if err != nil || len(items) != 1 || items[0].Title != "original" {
		t.Fatalf("failed write changed memory: %+v, %v", items, err)
	}
	next, err := s.Create(t.Context(), CreateRequest{Title: "next"})
	if err != nil || next.ID != 2 {
		t.Fatalf("failed write consumed sequence: %+v, %v", next, err)
	}
}

func TestCorruptStateFailsWithoutOverwrite(t *testing.T) {
	for _, raw := range []string{
		`{`,
		`{"version":2,"nextId":0,"tasks":[]}`,
		`{"version":1,"nextId":0,"tasks":[{"id":1,"title":"bad","createdAt":"2026-10-04T00:00:00Z"}]}`,
		`{"version":1,"nextId":1,"tasks":[{"id":1,"title":"bad"}]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			cfg := testTaskConfig(t)
			if err := os.MkdirAll(filepath.Dir(cfg.DataFile), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(cfg.DataFile, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewService(cfg); err == nil {
				t.Fatal("accepted corrupt state")
			}
			got, err := os.ReadFile(cfg.DataFile)
			if err != nil || string(got) != raw {
				t.Fatalf("corrupt data was overwritten: %q, %v", got, err)
			}
		})
	}
}

func TestConcurrentCreatesAreUniqueAndDurable(t *testing.T) {
	const n = 32
	cfg := testTaskConfig(t)
	s, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if _, err := s.Create(t.Context(), CreateRequest{Title: "task"}); err != nil {
				t.Error(err)
			}
			// Concurrent disk reads see complete old or new JSON snapshots.
			raw, err := os.ReadFile(cfg.DataFile)
			if err != nil || !json.Valid(raw) {
				t.Errorf("non-atomic state: %q, %v", raw, err)
			}
		})
	}
	wg.Wait()
	reloaded, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	items, err := reloaded.List(t.Context(), struct{}{})
	if err != nil || len(items) != n {
		t.Fatalf("reloaded=%d, %v", len(items), err)
	}
	for i, task := range items {
		if task.ID != int64(i+1) {
			t.Fatalf("non-unique sequence: %+v", items)
		}
	}
}
