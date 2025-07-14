package config

import (
	"encoding/json"
	"os"
)

// Log represents a single Certificate Transparency log's configuration.
type Log struct {
	Description  string `json:"description"`
	LogID        string `json:"log_id"`
	URL          string `json:"url"`
	DownloadJobs int    `json:"certspotter_download_jobs"`
	DownloadSize uint64 `json:"certspotter_download_size"`
	// We map the fields we need, others are ignored by default.
}

// Operator holds a list of logs managed by a single entity.
type Operator struct {
	Logs []Log `json:"logs"`
}

// LogList is the root structure of the log list JSON file.
type LogList struct {
	Operators []Operator `json:"operators"`
}

// LoadLogListFromFile parses the provided JSON file.
func LoadLogListFromFile(path string) (*LogList, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var logList LogList
	if err := json.Unmarshal(data, &logList); err != nil {
		return nil, err
	}

	return &logList, nil
}
