// Package redisx implements minute-bucket time-series storage on Redis.
//
// Design notes (lessons from the mining pool this template borrows from):
//   - counters are aggregated in memory and flushed in batches by a single
//     writer, so the request path never talks to Redis;
//   - reads use per-minute keys with MGET-style pipelines instead of scanning
//     an unbounded sorted set.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/store"
)

const (
	bucketFormat = "200601021504" // minute precision
	maxBuckets   = 2880           // two days of minutes per query
)

// Store is a Redis backed time-series store.
type Store struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// New connects to Redis and verifies connectivity.
func New(ctx context.Context, cfg config.RedisConfig) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping %s: %w", cfg.Addr, err)
	}

	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "gosvc"
	}
	return &Store{client: client, prefix: prefix, ttl: cfg.BucketTTL.D()}, nil
}

func (s *Store) key(metric string, t time.Time) string {
	return s.prefix + ":ts:" + metric + ":" + t.UTC().Format(bucketFormat)
}

// Incr adds delta to the current minute bucket.
func (s *Store) Incr(ctx context.Context, metric string, delta float64) error {
	if s == nil {
		return nil
	}
	key := s.key(metric, time.Now())
	pipe := s.client.Pipeline()
	pipe.IncrByFloat(ctx, key, delta)
	pipe.Expire(ctx, key, s.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Range returns the buckets in [from, to]. Missing minutes are skipped and
// ranges longer than two days are clamped.
func (s *Store) Range(ctx context.Context, metric string, from, to time.Time) ([]store.Bucket, error) {
	if s == nil {
		return nil, nil
	}
	if to.Before(from) {
		from, to = to, from
	}
	from = from.UTC().Truncate(time.Minute)
	to = to.UTC().Truncate(time.Minute)
	if minutes := int(to.Sub(from).Minutes()); minutes > maxBuckets {
		from = to.Add(-time.Duration(maxBuckets) * time.Minute)
	}

	times := make([]time.Time, 0, 64)
	keys := make([]string, 0, 64)
	for t := from; !t.After(to); t = t.Add(time.Minute) {
		times = append(times, t)
		keys = append(keys, s.key(metric, t))
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	buckets := make([]store.Bucket, 0, len(keys))
	for i, cmd := range cmds {
		value, err := cmd.Float64()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, store.Bucket{Time: times[i], Value: value})
	}
	return buckets, nil
}

// Close releases the Redis connection.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return s.client.Close()
}
