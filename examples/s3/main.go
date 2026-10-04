// Command s3 exercises the object storage connector against an existing bucket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tokimorphling/gosvc/config"
	"github.com/Tokimorphling/gosvc/store/object"
	stores3 "github.com/Tokimorphling/gosvc/store/s3"
)

func main() {
	path := flag.String("c", "examples/s3/config.toml", "TOML configuration path")
	op := flag.String("op", "list", "put|get|head|list|delete|presign-get|presign-put")
	key := flag.String("key", "", "object key")
	file := flag.String("file", "-", "local input/output file; - means stdin/stdout")
	prefix := flag.String("prefix", "", "list prefix")
	token := flag.String("token", "", "list continuation token")
	contentType := flag.String("content-type", "application/octet-stream", "upload content type")
	expires := flag.Duration("expires", 15*time.Minute, "presigned URL lifetime")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := run(ctx, *path, *op, *key, *file, *prefix, *token, *contentType, *expires)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, path, op, key, file, prefix, token, contentType string, expires time.Duration) error {
	cfg, err := (config.Source{Path: path, EnvPrefix: "GOSVC", Strict: true}).Load[config.Config]()
	if err != nil {
		return err
	}
	if !cfg.Storage.S3.Enabled {
		return errors.New("storage.s3 must be enabled")
	}
	client, err := stores3.New(ctx, cfg.Storage.S3)
	if err != nil {
		return err
	}
	defer client.Close()
	var store object.Store = client
	var result any
	switch op {
	case "put":
		input := os.Stdin
		opts := object.PutOptions{ContentType: contentType}
		if file != "-" {
			input, err = os.Open(file)
			if err != nil {
				return err
			}
			defer input.Close()
			info, err := input.Stat()
			if err != nil {
				return err
			}
			size := info.Size()
			opts.Size = &size
		}
		result, err = store.Put(ctx, key, input, opts)
	case "get":
		var download *object.Object
		download, err = store.Get(ctx, key, object.GetOptions{})
		if err != nil {
			return err
		}
		defer download.Body.Close()
		output := os.Stdout
		if file != "-" {
			output, err = os.Create(file)
			if err != nil {
				return err
			}
			defer output.Close()
		}
		_, err = io.Copy(output, download.Body)
		return err
	case "head":
		result, err = store.Head(ctx, key)
	case "list":
		result, err = store.List(ctx, object.ListOptions{Prefix: prefix, ContinuationToken: token, Limit: 100})
	case "delete":
		err = store.Delete(ctx, key)
		result = map[string]bool{"deleted": err == nil}
	case "presign-get":
		result, err = store.PresignGet(ctx, key, expires)
	case "presign-put":
		result, err = store.PresignPut(ctx, key, object.PutOptions{ContentType: contentType}, expires)
	default:
		return fmt.Errorf("unknown operation %q", op)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
