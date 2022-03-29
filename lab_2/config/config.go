package config

import (
	"fmt"
	"github.com/caarlos0/env"
)

type Config struct {
	CassandraURL string `env:"CASSANDRA_URL"`
	RedisURL     string `env:"REDIS_URL"`
	StreamName   string `env:"STREAM_NAME"`
}

func NewConfig() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("config: can't parse env values - %s", err)
	}
	return &cfg, nil
}
