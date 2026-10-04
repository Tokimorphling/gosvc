package tasks

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Tokimorphling/gosvc/apierror"
)

func TestTaskBusinessRules(t *testing.T) {
	s := NewService(TaskConfig{MaxTitleLen: 4, MaxTasks: 1})
	ctx := t.Context()
	for _, title := range []string{" ", "12345"} {
		if _, err := s.Create(ctx, CreateRequest{Title: title}); apierror.KindOf(err) != apierror.KindInvalidArgument {
			t.Fatalf("title %q: %v", title, err)
		}
	}
	created, err := s.Create(ctx, CreateRequest{Title: "  写个服务  "})
	if err != nil {
		t.Fatal(err)
	}
	if created.Title != "写个服务" || created.Done {
		t.Fatalf("created=%+v", created)
	}
	id := IDRequest{ID: created.ID}
	created.Title = "caller mutation"
	stored, err := s.Get(ctx, id)
	if err != nil || stored.Title != "写个服务" {
		t.Fatalf("Get=%+v, %v", stored, err)
	}
	if _, err := s.Create(ctx, CreateRequest{Title: "full"}); apierror.KindOf(err) != apierror.KindConflict {
		t.Fatalf("capacity error=%v", err)
	}
	for range 2 {
		completed, err := s.Complete(ctx, id)
		if err != nil || !completed.Done {
			t.Fatalf("Complete=%+v, %v", completed, err)
		}
	}
	if _, err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, id); apierror.KindOf(err) != apierror.KindNotFound {
		t.Fatalf("deleted task=%v", err)
	}
	if _, err := s.Get(ctx, IDRequest{}); apierror.KindOf(err) != apierror.KindInvalidArgument {
		t.Fatalf("invalid id=%v", err)
	}
	next, err := s.Create(ctx, CreateRequest{Title: "next"})
	if err != nil || next.ID <= id.ID {
		t.Fatalf("capacity not released or id reused: %+v, %v", next, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.List(cancelled, struct{}{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled call=%v", err)
	}
}

func TestConcurrentCreatesRemainUniqueAndSorted(t *testing.T) {
	const n = 32
	s := NewService(TaskConfig{MaxTitleLen: 10, MaxTasks: n})
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if _, err := s.Create(t.Context(), CreateRequest{Title: "task"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	items, err := s.List(t.Context(), struct{}{})
	if err != nil || len(items) != n {
		t.Fatalf("items=%d, err=%v", len(items), err)
	}
	for i, task := range items {
		if task.ID != int64(i+1) {
			t.Fatalf("IDs not unique/sorted: %+v", items)
		}
	}
}
