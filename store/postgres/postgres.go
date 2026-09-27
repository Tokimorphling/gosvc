// Package postgres provides a PostgreSQL connection pool built on database/sql
// and pgx, with runtime configuration, health checks and pool metrics.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/health"
)

// DB wraps a database/sql pool with the runtime settings.
type DB struct {
	*sql.DB
	pingTimeout time.Duration
}

// New opens the pool, applies the pool settings and verifies connectivity.
func New(ctx context.Context, cfg config.PostgresConfig) (*DB, error) {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	applyPoolSettings(db, cfg)

	pingTimeout := cfg.PingTimeout.D()
	if pingTimeout <= 0 {
		pingTimeout = 3 * time.Second
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return &DB{DB: db, pingTimeout: pingTimeout}, nil
}

// Ping probes the database, respecting the configured timeout.
func (db *DB) Ping(ctx context.Context) error {
	if db == nil || db.DB == nil {
		return fmt.Errorf("postgres: not configured")
	}
	pingCtx, cancel := context.WithTimeout(ctx, db.pingTimeout)
	defer cancel()
	return db.PingContext(pingCtx)
}

// HealthCheck adapts Ping to the health package.
func (db *DB) HealthCheck() health.Check { return db.Ping }

// Close releases the pool.
func (db *DB) Close() error {
	if db == nil || db.DB == nil {
		return nil
	}
	return db.DB.Close()
}

func applyPoolSettings(db *sql.DB, cfg config.PostgresConfig) {
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if lifetime := cfg.ConnMaxLifetime.D(); lifetime > 0 {
		db.SetConnMaxLifetime(lifetime)
	}
	if idle := cfg.ConnMaxIdleTime.D(); idle > 0 {
		db.SetConnMaxIdleTime(idle)
	}
}
