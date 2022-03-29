package config

import (
	"fmt"
	"github.com/caarlos0/env"
)

type Config struct {
	RabbitURL string `env:"RABBIT_URL"`
}

func NewConfig() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("config: can't parse env values - %s", err)
	}
	return &cfg, nil
}
