// Command gosvc runs the multi-protocol service template.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/gosvc/internal/app"
	"example.com/gosvc/internal/config"
	"example.com/gosvc/internal/logging"
	"example.com/gosvc/internal/version"
)

func main() {
	configPath := flag.String("c", "", "path to the JSON config file; empty uses defaults plus environment overrides")
	healthcheck := flag.String("healthcheck", "", "GET the given URL and exit 0 when it returns 200 (for container health checks)")
	flag.Parse()

	if *healthcheck != "" {
		os.Exit(runHealthcheck(*healthcheck))
	}

	if err := run(*configPath); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	logger, level, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, version.Version)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	application, err := app.New(cfg, logger, level)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return application.Run(ctx)
}

func runHealthcheck(url string) int {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck status:", resp.StatusCode)
		return 1
	}
	return 0
}
