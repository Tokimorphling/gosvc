// Package app is the example application built on the gosvc runtime. It shows
// the intended split: infrastructure comes from the library, business code
// lives here.
package app

import (
	"errors"
	"fmt"

	"github.com/Tokimorphling/gosvc"
	"github.com/Tokimorphling/gosvc/config"
)

// Config is the application configuration. It embeds the runtime config and
// adds application sections.
type Config struct {
	gosvc.Config

	Greeting GreetingConfig `json:"greeting" toml:"greeting"`
}

// GreetingConfig holds application-specific settings.
type GreetingConfig struct {
	Prefix     string `json:"prefix" toml:"prefix"`
	MaxNameLen int    `json:"maxNameLen" toml:"maxNameLen"`
}

// SetDefaults fills the runtime defaults and then the application ones. The
// embedded Config promotes the runtime implementation; overriding it here is
// how an application adds its own defaults.
func (c *Config) SetDefaults() {
	c.Config.SetDefaults()

	if c.Greeting.Prefix == "" {
		c.Greeting.Prefix = "hello"
	}
	if c.Greeting.MaxNameLen == 0 {
		c.Greeting.MaxNameLen = 64
	}
}

// Validate runs the runtime checks plus the application ones.
func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if c.Greeting.MaxNameLen <= 0 {
		return errors.New("greeting.maxNameLen must be > 0")
	}
	if c.Greeting.Prefix == "" {
		return errors.New("greeting.prefix must not be empty")
	}
	return nil
}

// Load reads the application configuration: defaults < file < environment.
//
// config.Source.Load is generic; Config is checked at compile time to satisfy
// config.Configurable through its pointer type. Strict mode rejects unknown
// keys, which catches typos in the TOML file.
func Load(path, envPrefix string) (*Config, error) {
	cfg, err := (config.Source{Path: path, EnvPrefix: envPrefix, Strict: true}).Load[Config]()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}
