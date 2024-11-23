package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres struct {
		Host        string `yaml:"host"`
		Port        int    `yaml:"port"`
		User        string `yaml:"user"`
		Password    string `yaml:"password"`
		Database    string `yaml:"database"`
		Slot        string `yaml:"slot"`
		Publication string `yaml:"publication"`
	} `yaml:"postgres"`

	Tables []struct {
		Schema string `yaml:"schema"`
		Name   string `yaml:"name"`
	} `yaml:"tables"`

	Iceberg struct {
		Path string `yaml:"path"`
	} `yaml:"iceberg"`

	Proxy struct {
		Port int `yaml:"port"`
	} `yaml:"proxy"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
