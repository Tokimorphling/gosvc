// Command gosvc runs the example application.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tokimorphling/gosvc/examples/app"
	"github.com/Tokimorphling/gosvc/health"
	"github.com/Tokimorphling/gosvc/logging"
)

const (
	defaultConfigFile = "configs/config.example.toml"
	envPrefix         = "GOSVC"
)

func main() {
	configPath := flag.String("c", defaultConfigFile, "path to the JSON config file")
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
	cfg, err := app.Load(configPath, envPrefix)
	if err != nil {
		return err
	}

	logHandle, err := logging.New(cfg.Log, cfg.Service.Name, cfg.Service.Env, app.Version)
	if err != nil {
		return err
	}
	slog.SetDefault(logHandle.Logger())

	application, err := app.Build(app.Options{
		Config:     cfg,
		Log:        logHandle,
		ConfigPath: configPath,
		EnvPrefix:  envPrefix,
		Version:    app.Version,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return application.Run(ctx)
}

func runHealthcheck(url string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := health.Probe(ctx, url); err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
		return 1
	}
	return 0
}
