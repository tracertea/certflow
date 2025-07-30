package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tracertea/certflow/internal/ctprocessor"
)

// CLIConfig represents the CLI configuration
type CLIConfig struct {
	Input        string   `json:"input,omitempty"`
	InputFiles   []string `json:"input_files,omitempty"`
	Output       string   `json:"output"`
	Format       string   `json:"format"`
	Workers      int      `json:"workers"`
	BatchSize    int      `json:"batch_size"`
	LogLevel     string   `json:"log_level"`
	MaxErrors    int64    `json:"max_errors"`
	Concurrent   bool     `json:"concurrent"`
	ShowProgress bool     `json:"show_progress"`
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configFile string) (*CLIConfig, error) {
	config := &CLIConfig{
		Format:       "json",
		Workers:      4,
		BatchSize:    100000,
		LogLevel:     "info",
		MaxErrors:    1000,
		Concurrent:   false,
		ShowProgress: true,
	}

	// Load from config file if specified
	if configFile != "" {
		if err := loadConfigFile(config, configFile); err != nil {
			return nil, fmt.Errorf("failed to load config file: %v", err)
		}
	}

	// Override with environment variables
	loadConfigFromEnv(config)

	return config, nil
}

// loadConfigFile loads configuration from a JSON file
func loadConfigFile(config *CLIConfig, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, config)
}

// loadConfigFromEnv loads configuration from environment variables
func loadConfigFromEnv(config *CLIConfig) {
	if val := os.Getenv("CTPROCESSOR_INPUT"); val != "" {
		config.Input = val
	}
	
	if val := os.Getenv("CTPROCESSOR_INPUT_FILES"); val != "" {
		config.InputFiles = strings.Split(val, ",")
	}
	
	if val := os.Getenv("CTPROCESSOR_OUTPUT"); val != "" {
		config.Output = val
	}
	
	if val := os.Getenv("CTPROCESSOR_FORMAT"); val != "" {
		config.Format = val
	}
	
	if val := os.Getenv("CTPROCESSOR_WORKERS"); val != "" {
		if workers, err := strconv.Atoi(val); err == nil {
			config.Workers = workers
		}
	}
	
	if val := os.Getenv("CTPROCESSOR_BATCH_SIZE"); val != "" {
		if batchSize, err := strconv.Atoi(val); err == nil {
			config.BatchSize = batchSize
		}
	}
	
	if val := os.Getenv("CTPROCESSOR_LOG_LEVEL"); val != "" {
		config.LogLevel = val
	}
	
	if val := os.Getenv("CTPROCESSOR_MAX_ERRORS"); val != "" {
		if maxErrors, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.MaxErrors = maxErrors
		}
	}
	
	if val := os.Getenv("CTPROCESSOR_CONCURRENT"); val != "" {
		config.Concurrent = strings.ToLower(val) == "true"
	}
	
	if val := os.Getenv("CTPROCESSOR_SHOW_PROGRESS"); val != "" {
		config.ShowProgress = strings.ToLower(val) == "true"
	}
}

// ToProcessorConfig converts CLIConfig to processor.Config
func (c *CLIConfig) ToProcessorConfig() ctprocessor.Config {
	config := ctprocessor.Config{
		InputPath:    c.Input,
		OutputPath:   c.Output,
		OutputFormat: c.Format,
		WorkerCount:  c.Workers,
		BatchSize:    c.BatchSize,
		LogLevel:     c.LogLevel,
	}
	
	config.SetDefaults()
	return config
}

// SaveConfig saves the current configuration to a file
func (c *CLIConfig) SaveConfig(filePath string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(filePath, data, 0644)
}

// Validate validates the CLI configuration
func (c *CLIConfig) Validate() error {
	// Note: Directory input is handled separately in the process command
	// This validation is for the basic config structure
	if c.Input == "" && len(c.InputFiles) == 0 {
		// Directory input will be handled by the process command logic
		// so we don't need to validate it here
	}
	
	if c.Output == "" {
		return fmt.Errorf("output path is required")
	}
	
	if c.Format != "json" && c.Format != "jsonl" && c.Format != "csv" && c.Format != "txt" {
		return fmt.Errorf("format must be one of: json, jsonl, csv, txt")
	}
	
	if c.Workers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}
	
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be greater than 0")
	}
	
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("log_level must be one of: debug, info, warn, error")
	}
	
	if c.MaxErrors <= 0 {
		return fmt.Errorf("max_errors must be greater than 0")
	}
	
	return nil
}