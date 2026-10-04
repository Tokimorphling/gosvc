// Package tasks is a small application built on gosvc. Its domain service is
// shared by REST and JSON-RPC; infrastructure is owned by the runtime.
package tasks

import (
	"errors"
	"time"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/config"
)

type Config struct {
	gosvc.Config
	Tasks TaskConfig `json:"tasks" toml:"tasks"`
}

type TaskConfig struct {
	MaxTitleLen int `json:"maxTitleLen" toml:"maxTitleLen"`
	MaxTasks    int `json:"maxTasks" toml:"maxTasks"`
}

func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	c.Service.Name = "tasks"
	c.HTTP.Host, c.GRPC.Host = "127.0.0.1", "127.0.0.1"
	c.HTTP.HandlerTimeout = config.Duration(3 * time.Second)
	c.Log.Format = "json"
	c.Tasks = TaskConfig{MaxTitleLen: 128, MaxTasks: 1000}
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.Tasks.MaxTitleLen <= 0 || c.Tasks.MaxTasks <= 0 {
		return errors.New("tasks.maxTitleLen and tasks.maxTasks must be positive")
	}
	return nil
}
