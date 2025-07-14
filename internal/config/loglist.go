package config

import (
	"encoding/json"
	"os"
	"strings"
)

// Log represents a single Certificate Transparency log's configuration.
type Log struct {
	Description  string `json:"description"`
	LogID        string `json:"log_id"`
	URL          string `json:"url"`
	DownloadJobs int    `json:"certspotter_download_jobs"`
	DownloadSize uint64 `json:"certspotter_download_size"`
	// We map the fields we need, others are ignored by default.
	FileName string
}

// --- NEW: The GetCleanName method you provided ---
func (log *Log) GetCleanName() string {
	if log.FileName != "" {
		return log.FileName
	}

	logEntryPath := log.URL
	logEntryPath = strings.TrimPrefix(logEntryPath, "https://")
	logEntryPath = strings.TrimPrefix(logEntryPath, "http://")
	logEntryPath = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_' {
			return r
		}
		if r == ':' || r == '/' {
			return '_'
		}
		return -1 // remove this character
	}, logEntryPath)
	logEntryPath = strings.TrimSuffix(logEntryPath, "_")
	log.FileName = logEntryPath

	return log.FileName
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
