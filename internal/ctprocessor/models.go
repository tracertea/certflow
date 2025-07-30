package ctprocessor

import (
	"crypto/x509"
	"fmt"
	"strings"
	"time"
)

// CertificateEntry represents a parsed certificate entry from the input file
type CertificateEntry struct {
	SequenceNumber  int64
	CertificateData string
	ParsedCert      *x509.Certificate
}

// DomainMapping represents a mapping from a name (CN or SAN) to an organization
type DomainMapping struct {
	Domain             string `json:"domain"`
	Name               string `json:"name"`                          // Common Name or Subject Alternative Name
	Organization       string `json:"organization"`                  // Organization name
	OrganizationalUnit string `json:"organizational_unit,omitempty"` // Optional organizational unit
	Source             string `json:"source"`                        // "CN" or "SAN"
	SequenceNum        int64  `json:"sequence_number"`               // Sequence number from the certificate entry
}

// ProcessingStats tracks processing statistics
type ProcessingStats struct {
	TotalProcessed   int64
	SuccessfulParsed int64
	Errors           int64
	StartTime        time.Time
	Duration         time.Duration
}

// Config holds configuration parameters
type Config struct {
	InputPath    string
	OutputPath   string
	OutputFormat string // "json", "csv", "txt"
	WorkerCount  int
	BatchSize    int
	LogLevel     string
}

// Validate validates the configuration parameters
func (c *Config) Validate() error {
	if c.InputPath == "" {
		return fmt.Errorf("input path is required")
	}

	if c.OutputPath == "" {
		return fmt.Errorf("output path is required")
	}

	if c.OutputFormat != "json" && c.OutputFormat != "csv" && c.OutputFormat != "txt" && c.OutputFormat != "jsonl" {
		return fmt.Errorf("output format must be one of: json, jsonl, csv, txt")
	}

	if c.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be greater than 0")
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	if c.LogLevel != "" && c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error" {
		return fmt.Errorf("log level must be one of: debug, info, warn, error")
	}

	return nil
}

// SetDefaults sets default values for configuration parameters
func (c *Config) SetDefaults() {
	if c.OutputFormat == "" {
		c.OutputFormat = "json"
	}

	if c.WorkerCount == 0 {
		c.WorkerCount = 4
	}

	if c.BatchSize == 0 {
		c.BatchSize = 100000
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
}

// Validate validates a CertificateEntry
func (ce *CertificateEntry) Validate() error {
	if ce.SequenceNumber < 0 {
		return fmt.Errorf("sequence number must be non-negative")
	}

	if strings.TrimSpace(ce.CertificateData) == "" {
		return fmt.Errorf("certificate data cannot be empty")
	}

	return nil
}

// Validate validates a DomainMapping
func (dm *DomainMapping) Validate() error {
	if strings.TrimSpace(dm.Name) == "" {
		return fmt.Errorf("name cannot be empty")
	}

	if strings.TrimSpace(dm.Organization) == "" && dm.OrganizationalUnit == "" {
		return fmt.Errorf("organization cannot be empty")
	}

	if dm.Source != "CN" && dm.Source != "SAN" {
		return fmt.Errorf("source must be either 'CN' or 'SAN'")
	}

	if dm.SequenceNum < 0 {
		return fmt.Errorf("sequence number must be non-negative")
	}

	return nil
}

// String returns a string representation of the DomainMapping
func (dm *DomainMapping) String() string {
	return fmt.Sprintf("%s -> %s (%s)", dm.Name, dm.Organization, dm.Source)
}

// AddProcessed increments the total processed count
func (ps *ProcessingStats) AddProcessed() {
	ps.TotalProcessed++
}

// AddSuccess increments the successful parsed count
func (ps *ProcessingStats) AddSuccess() {
	ps.SuccessfulParsed++
}

// AddError increments the error count
func (ps *ProcessingStats) AddError() {
	ps.Errors++
}

// UpdateDuration updates the processing duration
func (ps *ProcessingStats) UpdateDuration() {
	ps.Duration = time.Since(ps.StartTime)
}

// SuccessRate returns the success rate as a percentage
func (ps *ProcessingStats) SuccessRate() float64 {
	if ps.TotalProcessed == 0 {
		return 0.0
	}
	return float64(ps.SuccessfulParsed) / float64(ps.TotalProcessed) * 100.0
}

// ErrorRate returns the error rate as a percentage
func (ps *ProcessingStats) ErrorRate() float64 {
	if ps.TotalProcessed == 0 {
		return 0.0
	}
	return float64(ps.Errors) / float64(ps.TotalProcessed) * 100.0
}

// String returns a string representation of the processing statistics
func (ps *ProcessingStats) String() string {
	return fmt.Sprintf("Processed: %d, Success: %d (%.2f%%), Errors: %d (%.2f%%), Duration: %v",
		ps.TotalProcessed, ps.SuccessfulParsed, ps.SuccessRate(), ps.Errors, ps.ErrorRate(), ps.Duration)
}

// CompactFormat returns a compact string representation for inline logging
func (ps *ProcessingStats) CompactFormat() string {
	durationSeconds := ps.Duration.Seconds()
	if ps.Errors > 0 {
		return fmt.Sprintf("%d Success, Errors: %d, Duration: %.3fs", ps.SuccessfulParsed, ps.Errors, durationSeconds)
	} else {
		return fmt.Sprintf("%d Success, Duration: %.3fs", ps.SuccessfulParsed, durationSeconds)
	}
}

// ProcessingState represents the persistent state of S3 file processing
type ProcessingState struct {
	ProcessedFiles map[string]FileState `json:"processed_files"`
	FailedFiles    map[string]FileState `json:"failed_files"`
	LastUpdated    time.Time            `json:"last_updated"`
	Version        string               `json:"version"`
}

// FileState represents the state of a single processed file
type FileState struct {
	Key         string    `json:"key"`
	ETag        string    `json:"etag"`
	Size        int64     `json:"size"`
	ProcessedAt time.Time `json:"processed_at"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
}

// NewProcessingState creates a new ProcessingState with initialized maps
func NewProcessingState() *ProcessingState {
	return &ProcessingState{
		ProcessedFiles: make(map[string]FileState),
		FailedFiles:    make(map[string]FileState),
		LastUpdated:    time.Now(),
		Version:        "1.0",
	}
}

// Validate validates the ProcessingState
func (ps *ProcessingState) Validate() error {
	if ps.ProcessedFiles == nil {
		return fmt.Errorf("processed_files map cannot be nil")
	}

	if ps.FailedFiles == nil {
		return fmt.Errorf("failed_files map cannot be nil")
	}

	if ps.Version == "" {
		return fmt.Errorf("version cannot be empty")
	}

	return nil
}

// GetProcessedCount returns the number of successfully processed files
func (ps *ProcessingState) GetProcessedCount() int {
	return len(ps.ProcessedFiles)
}

// GetFailedCount returns the number of failed files
func (ps *ProcessingState) GetFailedCount() int {
	return len(ps.FailedFiles)
}

// Validate validates the FileState
func (fs *FileState) Validate() error {
	if strings.TrimSpace(fs.Key) == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if strings.TrimSpace(fs.ETag) == "" {
		return fmt.Errorf("etag cannot be empty")
	}

	if fs.Size < 0 {
		return fmt.Errorf("size must be non-negative")
	}

	if fs.ProcessedAt.IsZero() {
		return fmt.Errorf("processed_at cannot be zero")
	}

	return nil
}
