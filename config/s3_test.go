package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestS3ValidationEnvAndRedaction(t *testing.T) {
	cfg := Default()
	t.Setenv("CHECK_S3_BUCKET", "files")
	t.Setenv("CHECK_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("CHECK_S3_USE_PATH_STYLE", "true")
	t.Setenv("CHECK_S3_ACCESS_KEY_ID", "sensitive-access-id")
	t.Setenv("CHECK_S3_SECRET_ACCESS_KEY", "sensitive-secret-key")
	t.Setenv("CHECK_S3_SESSION_TOKEN", "sensitive-session-token")
	if err := cfg.ApplyEnv("CHECK"); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if !cfg.Storage.S3.Enabled || !cfg.Storage.S3.UsePathStyle || cfg.Storage.S3.Bucket != "files" {
		t.Fatalf("env not applied")
	}
	raw, err := json.Marshal(cfg.Redacted())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sensitive-") {
		t.Fatal("S3 credentials exposed")
	}
	for _, edit := range []func(*S3Config){
		func(c *S3Config) { c.Bucket = "" }, func(c *S3Config) { c.Endpoint = "https://user:secret@example.com" },
		func(c *S3Config) { c.Endpoint = "https://example.com/path" }, func(c *S3Config) { c.SecretAccessKey = "" },
		func(c *S3Config) { c.PartSizeBytes = 1024 }, func(c *S3Config) { c.UploadConcurrency = 0 },
		func(c *S3Config) { c.MaxConcurrentUploads = 0 }, func(c *S3Config) { c.RequestTimeout = 0 },
		func(c *S3Config) { c.PresignExpiry = 0 }, func(c *S3Config) { c.MaxAttempts = 0 },
	} {
		c := cfg.Storage.S3
		edit(&c)
		if err := c.Validate(); err == nil {
			t.Fatal("invalid S3 config accepted")
		}
	}
}
