// Package store defines optional persistence used by the transports.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrDropped is returned when a recorder drops a sample because its queue is
// saturated.
var ErrDropped = errors.New("recorder queue is full, sample dropped")

// Recorder records time-series samples. Implementations must be safe for
// concurrent use. A nil Recorder disables recording.
type Recorder interface {
	Incr(ctx context.Context, metric string, delta float64) error
}

// TimeSeries reads aggregated buckets.
type TimeSeries interface {
	Range(ctx context.Context, metric string, from, to time.Time) ([]Bucket, error)
}

// Bucket is one aggregated sample.
type Bucket struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}
