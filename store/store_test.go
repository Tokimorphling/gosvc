package store

import (
	"context"
	"testing"
)

type fakeRecorder struct {
	calls int
}

func (f *fakeRecorder) Incr(context.Context, string, float64) error {
	f.calls++
	return nil
}

func TestNilableConvertsTypedNil(t *testing.T) {
	var typedNil *fakeRecorder

	if got := Nilable[*fakeRecorder](typedNil); got != nil {
		t.Fatalf("Nilable(typed nil) = %#v, want nil interface", got)
	}
	if got := Nilable[Recorder](nil); got != nil {
		t.Fatalf("Nilable(nil) = %#v, want nil", got)
	}

	recorder := &fakeRecorder{}
	if got := Nilable[Recorder](recorder); got != recorder {
		t.Fatalf("Nilable(recorder) = %#v, want the recorder", got)
	}
}

func TestHolderSwapsRecorder(t *testing.T) {
	first := &fakeRecorder{}
	second := &fakeRecorder{}

	holder := NewHolder(first)
	if err := holder.Incr(context.Background(), "m", 1); err != nil {
		t.Fatalf("Incr: %v", err)
	}

	holder.Set(second)
	if err := holder.Incr(context.Background(), "m", 1); err != nil {
		t.Fatalf("Incr: %v", err)
	}

	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("calls = %d/%d, want 1/1", first.calls, second.calls)
	}

	// Disabling recording must not panic and must stop counting.
	holder.Set(nil)
	if err := holder.Incr(context.Background(), "m", 1); err != nil {
		t.Fatalf("Incr after disable: %v", err)
	}
	if second.calls != 1 {
		t.Fatalf("second recorder kept counting after disable: %d", second.calls)
	}
}
