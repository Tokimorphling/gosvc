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

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/examples/tasks"
	"github.com/Tokimorphling/gosvc/health"
)

func main() {
	configPath := flag.String("c", "examples/tasks/config.toml", "TOML configuration path")
	probe := flag.String("healthcheck", "", "probe the URL and exit")
	flag.Parse()
	if *probe != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := health.Probe(ctx, *probe)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(*configPath); err != nil {
		slog.Error("tasks stopped", "error", err)
		os.Exit(1)
	}
}

func run(path string) error {
	cfg, err := (gosvc.Source{Path: path, EnvPrefix: "TASKS", Strict: true}).Load[tasks.Config]()
	if err != nil {
		return err
	}
	application, err := tasks.Build(cfg, gosvc.WithHotReload(path, "TASKS"))
	if err != nil {
		return err
	}
	defer application.Close()
	slog.SetDefault(application.Logger())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return application.Run(ctx)
}
