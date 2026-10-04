// Package deploydemo is a small persistent task board built on gosvc.
package deploydemo

import (
	"errors"
	"strings"
	"time"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/config"
)

type Config struct {
	gosvc.Config
	Tasks TaskConfig `json:"tasks" toml:"tasks"`
}

type TaskConfig struct {
	DataFile    string `json:"dataFile" toml:"dataFile"`
	MaxTitleLen int    `json:"maxTitleLen" toml:"maxTitleLen"`
	MaxTasks    int    `json:"maxTasks" toml:"maxTasks"`
}

func (c *Config) SetDefaults() {
	c.Config.SetDefaults()
	c.Service.Name = "gosvc-demo"
	c.HTTP.Host, c.HTTP.Port = "127.0.0.1", 18080
	c.GRPC.Host, c.GRPC.Port = "127.0.0.1", 19090
	c.Admin.Host, c.Admin.Port = "127.0.0.1", 16060
	c.HTTP.HandlerTimeout = config.Duration(5 * time.Second)
	c.HTTP.MaxBodyBytes = 16 << 10
	c.HTTP.CORS.Enabled = false
	c.Log.Format = "json"
	c.Tasks = TaskConfig{DataFile: "data/tasks.json", MaxTitleLen: 160, MaxTasks: 1000}
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	return c.Tasks.validate()
}

func (c TaskConfig) validate() error {
	if strings.TrimSpace(c.DataFile) == "" {
		return errors.New("tasks.dataFile is required")
	}
	if c.MaxTitleLen < 1 || c.MaxTitleLen > 1024 {
		return errors.New("tasks.maxTitleLen must be between 1 and 1024")
	}
	if c.MaxTasks < 1 || c.MaxTasks > 10000 {
		return errors.New("tasks.maxTasks must be between 1 and 10000")
	}
	return nil
}
