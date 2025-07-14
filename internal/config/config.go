package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// A custom type to handle comma-separated lists from flags.
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(value string) error {
	*s = strings.Split(value, ",")
	return nil
}

// Config holds all configuration for the certflow application.
type Config struct {
	LogConfigPath string // Path to the JSON file
	ActiveLog     *Log   // The specific log we are processing

	OutputDir            string
	Continuous           bool
	StateSaveTicker      time.Duration
	Proxies              []string
	LogFile              string
	ProxyFailures        int
	ProxyCooldown        time.Duration
	BatchSize            uint64
	AggregatorBufferSize int // <-- NEW: Max size of the re-ordering buffer
}

// Load parses config and flags, it now requires the path to a log JSON config.
func Load() (*Config, error) {
	cfg := &Config{}
	var proxies stringSlice

	// New required flag for the log list JSON
	flag.StringVar(&cfg.LogConfigPath, "log-config", "", "Path to the log list JSON configuration file (required).")

	// Global flags
	flag.StringVar(&cfg.OutputDir, "output-dir", "./certflow_output", "Directory to save batch files, state, and lock file.")
	flag.BoolVar(&cfg.Continuous, "continuous", true, "Run continuously, polling for new certificates.")
	flag.DurationVar(&cfg.StateSaveTicker, "state-save-interval", 10*time.Second, "How often to save progress.")
	flag.StringVar(&cfg.LogFile, "log-file", "debug.log", "Name of the log file to write to within the output directory. Set to empty to disable.")
	flag.Var(&proxies, "proxies", "Comma-separated list of HTTP/S proxies to use (e.g., http://p1:8080,http://p2:8080).")
	flag.IntVar(&cfg.ProxyFailures, "proxy-max-failures", 5, "Number of consecutive failures before a proxy is put on cooldown.")
	flag.DurationVar(&cfg.ProxyCooldown, "proxy-cooldown", 1*time.Minute, "Initial duration a failing proxy is removed from the pool.")
	flag.Uint64Var(&cfg.BatchSize, "batch-size", 100000, "Number of certificates to store in each output file.")
	flag.IntVar(&cfg.AggregatorBufferSize, "aggregator-buffer-size", 500000, "Maximum number of entries to hold in memory for re-ordering.") // <-- NEW FLAG

	flag.Parse()
	cfg.Proxies = proxies

	if cfg.LogConfigPath == "" {
		return nil, fmt.Errorf("-log-config flag is required")
	}

	// Load and parse the JSON file
	logList, err := LoadLogListFromFile(cfg.LogConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load log config from %s: %w", cfg.LogConfigPath, err)
	}

	// Select the first log from the first operator.
	if len(logList.Operators) == 0 || len(logList.Operators[0].Logs) == 0 {
		return nil, fmt.Errorf("no logs found in the provided log config file")
	}
	cfg.ActiveLog = &logList.Operators[0].Logs[0]

	return cfg, nil
}
