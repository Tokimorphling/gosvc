package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/Tokimorphling/gosvc/config"
)

func TestNewRejectsInvalidDSN(t *testing.T) {
	cfg := config.Default().Storage.Postgres
	cfg.Enabled = true
	cfg.DSN = "://not-a-dsn"

	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("expected an error for an invalid DSN")
	}
}

func TestApplyPoolSettings(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://user:pass@127.0.0.1:1/app")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	applyPoolSettings(db, config.PostgresConfig{
		MaxOpenConns:    7,
		MaxIdleConns:    3,
		ConnMaxLifetime: config.Duration(time.Minute),
		ConnMaxIdleTime: config.Duration(30 * time.Second),
	})

	if got := db.Stats().MaxOpenConnections; got != 7 {
		t.Fatalf("MaxOpenConnections = %d, want 7", got)
	}
}

func TestPingReportsUnreachableDatabase(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://user:pass@127.0.0.1:1/app")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	wrapped := &DB{DB: db, pingTimeout: 300 * time.Millisecond}

	if err := wrapped.Ping(context.Background()); err == nil {
		t.Fatal("expected ping to fail")
	}
	if err := wrapped.HealthCheck()(context.Background()); err == nil {
		t.Fatal("expected the health check to fail")
	}
}
