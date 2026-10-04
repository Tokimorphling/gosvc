package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// S3Config configures one bucket. Empty credentials use the AWS default
// credential chain; region may come from that chain or default to us-east-1.
type S3Config struct {
	Enabled              bool     `json:"enabled" toml:"enabled"`
	Bucket               string   `json:"bucket" toml:"bucket"`
	Region               string   `json:"region" toml:"region"`
	Endpoint             string   `json:"endpoint" toml:"endpoint"`
	UsePathStyle         bool     `json:"usePathStyle" toml:"usePathStyle"`
	AccessKeyID          string   `json:"accessKeyId" toml:"accessKeyId"`
	SecretAccessKey      string   `json:"secretAccessKey" toml:"secretAccessKey"`
	SessionToken         string   `json:"sessionToken" toml:"sessionToken"`
	RequestTimeout       Duration `json:"requestTimeout" toml:"requestTimeout"`
	HealthTimeout        Duration `json:"healthTimeout" toml:"healthTimeout"`
	CheckBucket          bool     `json:"checkBucket" toml:"checkBucket"`
	PresignExpiry        Duration `json:"presignExpiry" toml:"presignExpiry"`
	PartSizeBytes        int64    `json:"partSizeBytes" toml:"partSizeBytes"`
	UploadConcurrency    int      `json:"uploadConcurrency" toml:"uploadConcurrency"`
	MaxConcurrentUploads int      `json:"maxConcurrentUploads" toml:"maxConcurrentUploads"`
	MaxAttempts          int      `json:"maxAttempts" toml:"maxAttempts"`
}

func defaultS3() S3Config {
	return S3Config{
		RequestTimeout: Duration(2 * time.Minute), HealthTimeout: Duration(3 * time.Second),
		CheckBucket: true, PresignExpiry: Duration(15 * time.Minute), PartSizeBytes: 8 << 20,
		UploadConcurrency: 2, MaxConcurrentUploads: 4, MaxAttempts: 3,
	}
}

func (c S3Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Bucket == "" || strings.TrimSpace(c.Bucket) != c.Bucket || strings.ContainsAny(c.Bucket, "/\\\r\n") {
		return fmt.Errorf("storage.s3.bucket must be a non-empty bucket name")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return fmt.Errorf("storage.s3.endpoint must be an http(s) origin without credentials, query or path")
		}
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") || (c.SessionToken != "" && c.AccessKeyID == "") {
		return fmt.Errorf("storage.s3 explicit credentials require both accessKeyId and secretAccessKey")
	}
	if c.RequestTimeout <= 0 || c.HealthTimeout <= 0 {
		return fmt.Errorf("storage.s3 requestTimeout and healthTimeout must be positive")
	}
	if c.PresignExpiry < Duration(time.Second) || c.PresignExpiry > Duration(7*24*time.Hour) {
		return fmt.Errorf("storage.s3.presignExpiry must be between 1s and 168h")
	}
	if c.PartSizeBytes < 5<<20 || c.PartSizeBytes > 5<<30 {
		return fmt.Errorf("storage.s3.partSizeBytes must be between 5 MiB and 5 GiB")
	}
	if c.UploadConcurrency <= 0 || c.MaxConcurrentUploads <= 0 || c.MaxAttempts <= 0 {
		return fmt.Errorf("storage.s3 uploadConcurrency, maxConcurrentUploads and maxAttempts must be positive")
	}
	return nil
}
