package ctprocessor

import (
	"fmt"
	"strings"
	"time"
)

// S3Object represents an object in an S3 bucket
type S3Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
}

// S3Config holds S3-specific configuration parameters
type S3Config struct {
	// S3 Configuration
	Bucket  string `json:"bucket"`
	Prefix  string `json:"prefix"`
	Region  string `json:"region"`
	Profile string `json:"profile"`

	// Download Configuration
	WorkerCount            int `json:"worker_count"`
	ProcessingWorkerCount  int `json:"processing_worker_count"`
	RetryLimit             int `json:"retry_limit"`
	TimeoutSeconds         int `json:"timeout_seconds"`
	MaxConcurrentDownloads int `json:"max_concurrent_downloads"`

	// Storage Configuration
	CacheDir               string  `json:"cache_dir"`
	CleanupAfterProcessing bool    `json:"cleanup_after_processing"`
	DiskSpaceThresholdGB   float64 `json:"disk_space_threshold_gb"`

	// File Management Configuration
	FileManagerConfig FileManagerConfig `json:"file_manager_config"`

	// Processing Configuration
	ProcessorConfig Config `json:"processor_config"`

	// State Configuration
	StateFilePath string `json:"state_file_path"`
}

// FilterCriteria defines criteria for filtering S3 objects
type FilterCriteria struct {
	Extensions []string // e.g., [".gz"]
	MinSize    int64
	MaxSize    int64
	SortByKey  bool
}

// S3ProcessingStats tracks S3-specific processing statistics
type S3ProcessingStats struct {
	TotalFiles         int64         `json:"total_files"`
	ProcessedFiles     int64         `json:"processed_files"`
	FailedFiles        int64         `json:"failed_files"`
	SkippedFiles       int64         `json:"skipped_files"` // Already processed
	TotalDownloadTime  time.Duration `json:"total_download_time"`
	TotalProcessTime   time.Duration `json:"total_process_time"`
	AverageFileSize    int64         `json:"average_file_size"`
	DownloadThroughput float64       `json:"download_throughput"` // MB/s
	TotalBytes         int64         `json:"total_bytes"`        // Total bytes downloaded
	TotalCertificates  int64         `json:"total_certificates"`  // Total certificates processed
}

// DownloadConfig holds configuration for download operations
type DownloadConfig struct {
	WorkerCount    int    `json:"worker_count"`
	RetryLimit     int    `json:"retry_limit"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	CacheDir       string `json:"cache_dir"`
	ShowProgress   bool   `json:"show_progress"`
}

// DownloadResult represents the result of a file download operation
type DownloadResult struct {
	Object     S3Object      `json:"object"`
	LocalPath  string        `json:"local_path"`
	Success    bool          `json:"success"`
	Error      error         `json:"error,omitempty"`
	Duration   time.Duration `json:"duration"`
	RetryCount int           `json:"retry_count"`
}

// DownloadStats tracks download operation statistics
type DownloadStats struct {
	TotalDownloads      int64         `json:"total_downloads"`
	SuccessfulDownloads int64         `json:"successful_downloads"`
	FailedDownloads     int64         `json:"failed_downloads"`
	TotalBytes          int64         `json:"total_bytes"`
	TotalDuration       time.Duration `json:"total_duration"`
	AverageSpeed        float64       `json:"average_speed"` // MB/s
	RetryCount          int64         `json:"retry_count"`
}

// Validate validates the S3Object
func (s3o *S3Object) Validate() error {
	if strings.TrimSpace(s3o.Key) == "" {
		return fmt.Errorf("S3 object key cannot be empty")
	}

	if s3o.Size < 0 {
		return fmt.Errorf("S3 object size must be non-negative")
	}

	if strings.TrimSpace(s3o.ETag) == "" {
		return fmt.Errorf("S3 object ETag cannot be empty")
	}

	if s3o.LastModified.IsZero() {
		return fmt.Errorf("S3 object LastModified cannot be zero")
	}

	return nil
}

// Validate validates the S3Config
func (s3c *S3Config) Validate() error {
	// Validate S3 Configuration
	if strings.TrimSpace(s3c.Bucket) == "" {
		return fmt.Errorf("S3 bucket name is required")
	}

	if strings.TrimSpace(s3c.Region) == "" {
		return fmt.Errorf("S3 region is required")
	}

	// Validate Download Configuration
	if s3c.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be greater than 0")
	}

	if s3c.RetryLimit < 0 {
		return fmt.Errorf("retry limit must be non-negative")
	}

	if s3c.TimeoutSeconds <= 0 {
		return fmt.Errorf("timeout seconds must be greater than 0")
	}

	// Validate Storage Configuration
	if strings.TrimSpace(s3c.CacheDir) == "" {
		return fmt.Errorf("cache directory is required")
	}

	// Validate State Configuration
	if strings.TrimSpace(s3c.StateFilePath) == "" {
		return fmt.Errorf("state file path is required")
	}

	// Set defaults for FileManagerConfig if not configured
	if s3c.FileManagerConfig.CacheDir == "" {
		s3c.FileManagerConfig.SetDefaults()
	}
	
	// Validate File Manager Configuration
	if err := s3c.FileManagerConfig.Validate(); err != nil {
		return fmt.Errorf("file manager config validation failed: %w", err)
	}

	// Validate embedded ProcessorConfig (skip InputPath validation for S3 processing)
	tempConfig := s3c.ProcessorConfig
	if tempConfig.InputPath == "" {
		tempConfig.InputPath = "temp" // Set temporary value for validation
	}
	if err := tempConfig.Validate(); err != nil {
		return fmt.Errorf("processor config validation failed: %w", err)
	}

	return nil
}

// SetDefaults sets default values for S3Config parameters
func (s3c *S3Config) SetDefaults() {
	// Set default S3 region if not specified
	if s3c.Region == "" {
		s3c.Region = "us-east-1"
	}

	// Set default download configuration
	if s3c.WorkerCount == 0 {
		s3c.WorkerCount = 3
	}
	
	if s3c.ProcessingWorkerCount == 0 {
		s3c.ProcessingWorkerCount = 8  // Default to 8 processing workers for better throughput
	}

	if s3c.RetryLimit == 0 {
		s3c.RetryLimit = 10
	}

	if s3c.TimeoutSeconds == 0 {
		s3c.TimeoutSeconds = 300 // 5 minutes
	}

	// Set default storage configuration
	if s3c.CacheDir == "" {
		s3c.CacheDir = "./cache"
	}

	// Set default file manager configuration
	s3c.FileManagerConfig.SetDefaults()
	if s3c.FileManagerConfig.CacheDir == "" {
		s3c.FileManagerConfig.CacheDir = s3c.CacheDir
	}

	// Set default state file path
	if s3c.StateFilePath == "" {
		s3c.StateFilePath = "./s3_processing_state.json"
	}

	// Set defaults for embedded ProcessorConfig
	s3c.ProcessorConfig.SetDefaults()
}

// String returns a string representation of the S3Object
func (s3o *S3Object) String() string {
	return fmt.Sprintf("S3Object{Key: %s, Size: %d, ETag: %s, LastModified: %s}",
		s3o.Key, s3o.Size, s3o.ETag, s3o.LastModified.Format(time.RFC3339))
}

// String returns a string representation of the S3Config
func (s3c *S3Config) String() string {
	return fmt.Sprintf("S3Config{Bucket: %s, Prefix: %s, Region: %s, Workers: %d, RetryLimit: %d, CacheDir: %s}",
		s3c.Bucket, s3c.Prefix, s3c.Region, s3c.WorkerCount, s3c.RetryLimit, s3c.CacheDir)
}

// AddTotalFile increments the total files count
func (s3ps *S3ProcessingStats) AddTotalFile() {
	s3ps.TotalFiles++
}

// AddProcessedFile increments the processed files count
func (s3ps *S3ProcessingStats) AddProcessedFile() {
	s3ps.ProcessedFiles++
}

// AddFailedFile increments the failed files count
func (s3ps *S3ProcessingStats) AddFailedFile() {
	s3ps.FailedFiles++
}

// AddSkippedFile increments the skipped files count
func (s3ps *S3ProcessingStats) AddSkippedFile() {
	s3ps.SkippedFiles++
}

// UpdateDownloadTime adds to the total download time
func (s3ps *S3ProcessingStats) UpdateDownloadTime(duration time.Duration) {
	s3ps.TotalDownloadTime += duration
}

// UpdateProcessTime adds to the total process time
func (s3ps *S3ProcessingStats) UpdateProcessTime(duration time.Duration) {
	s3ps.TotalProcessTime += duration
}

// CalculateAverageFileSize calculates and updates the average file size
func (s3ps *S3ProcessingStats) CalculateAverageFileSize(totalBytes int64) {
	if s3ps.ProcessedFiles > 0 {
		s3ps.AverageFileSize = totalBytes / s3ps.ProcessedFiles
	}
}

// CalculateDownloadThroughput calculates and updates the download throughput in MB/s
func (s3ps *S3ProcessingStats) CalculateDownloadThroughput(totalBytes int64) {
	if s3ps.TotalDownloadTime.Seconds() > 0 {
		s3ps.DownloadThroughput = float64(totalBytes) / (1024 * 1024) / s3ps.TotalDownloadTime.Seconds()
	}
}

// SuccessRate returns the success rate as a percentage
func (s3ps *S3ProcessingStats) SuccessRate() float64 {
	if s3ps.TotalFiles == 0 {
		return 0.0
	}
	return float64(s3ps.ProcessedFiles) / float64(s3ps.TotalFiles) * 100.0
}

// FailureRate returns the failure rate as a percentage
func (s3ps *S3ProcessingStats) FailureRate() float64 {
	if s3ps.TotalFiles == 0 {
		return 0.0
	}
	return float64(s3ps.FailedFiles) / float64(s3ps.TotalFiles) * 100.0
}

// String returns a string representation of the S3ProcessingStats
func (s3ps *S3ProcessingStats) String() string {
	return fmt.Sprintf("S3Stats{Total: %d, Processed: %d (%.2f%%), Failed: %d (%.2f%%), Skipped: %d, AvgSize: %d bytes, Throughput: %.2f MB/s}",
		s3ps.TotalFiles, s3ps.ProcessedFiles, s3ps.SuccessRate(), s3ps.FailedFiles, s3ps.FailureRate(),
		s3ps.SkippedFiles, s3ps.AverageFileSize, s3ps.DownloadThroughput)
}

// Validate validates the DownloadConfig
func (dc *DownloadConfig) Validate() error {
	if dc.WorkerCount <= 0 {
		return fmt.Errorf("worker count must be greater than 0")
	}

	if dc.RetryLimit < 0 {
		return fmt.Errorf("retry limit must be non-negative")
	}

	if dc.TimeoutSeconds <= 0 {
		return fmt.Errorf("timeout seconds must be greater than 0")
	}

	if strings.TrimSpace(dc.CacheDir) == "" {
		return fmt.Errorf("cache directory is required")
	}

	return nil
}

// SetDefaults sets default values for DownloadConfig
func (dc *DownloadConfig) SetDefaults() {
	if dc.WorkerCount == 0 {
		dc.WorkerCount = 3
	}

	if dc.RetryLimit == 0 {
		dc.RetryLimit = 10
	}

	if dc.TimeoutSeconds == 0 {
		dc.TimeoutSeconds = 300 // 5 minutes
	}

	if dc.CacheDir == "" {
		dc.CacheDir = "./cache"
	}
}

// String returns a string representation of the DownloadConfig
func (dc *DownloadConfig) String() string {
	return fmt.Sprintf("DownloadConfig{Workers: %d, RetryLimit: %d, Timeout: %ds, CacheDir: %s, ShowProgress: %t}",
		dc.WorkerCount, dc.RetryLimit, dc.TimeoutSeconds, dc.CacheDir, dc.ShowProgress)
}

// String returns a string representation of the DownloadResult
func (dr *DownloadResult) String() string {
	status := "SUCCESS"
	if !dr.Success {
		status = "FAILED"
	}
	return fmt.Sprintf("DownloadResult{Key: %s, Status: %s, Duration: %v, Retries: %d}",
		dr.Object.Key, status, dr.Duration, dr.RetryCount)
}

// AddDownload increments download counters
func (ds *DownloadStats) AddDownload(success bool, bytes int64, duration time.Duration, retries int) {
	ds.TotalDownloads++
	ds.TotalBytes += bytes
	ds.TotalDuration += duration
	ds.RetryCount += int64(retries)

	if success {
		ds.SuccessfulDownloads++
	} else {
		ds.FailedDownloads++
	}

	// Calculate average speed in MB/s
	if ds.TotalDuration.Seconds() > 0 {
		ds.AverageSpeed = float64(ds.TotalBytes) / (1024 * 1024) / ds.TotalDuration.Seconds()
	}
}

// SuccessRate returns the download success rate as a percentage
func (ds *DownloadStats) SuccessRate() float64 {
	if ds.TotalDownloads == 0 {
		return 0.0
	}
	return float64(ds.SuccessfulDownloads) / float64(ds.TotalDownloads) * 100.0
}

// FailureRate returns the download failure rate as a percentage
func (ds *DownloadStats) FailureRate() float64 {
	if ds.TotalDownloads == 0 {
		return 0.0
	}
	return float64(ds.FailedDownloads) / float64(ds.TotalDownloads) * 100.0
}

// GetThroughputMBps returns the average throughput in MB/s
func (ds *DownloadStats) GetThroughputMBps() float64 {
	return ds.AverageSpeed
}

// GetTotalSizeMB returns the total downloaded size in MB
func (ds *DownloadStats) GetTotalSizeMB() float64 {
	return float64(ds.TotalBytes) / (1024 * 1024)
}

// GetAverageRetries returns the average number of retries per download
func (ds *DownloadStats) GetAverageRetries() float64 {
	if ds.TotalDownloads == 0 {
		return 0.0
	}
	return float64(ds.RetryCount) / float64(ds.TotalDownloads)
}

// GetAverageDuration returns the average download duration
func (ds *DownloadStats) GetAverageDuration() time.Duration {
	if ds.TotalDownloads == 0 {
		return 0
	}
	return ds.TotalDuration / time.Duration(ds.TotalDownloads)
}

// String returns a string representation of the DownloadStats
func (ds *DownloadStats) String() string {
	return fmt.Sprintf("DownloadStats{Total: %d, Success: %d (%.2f%%), Failed: %d (%.2f%%), TotalBytes: %d, AvgSpeed: %.2f MB/s, Retries: %d}",
		ds.TotalDownloads, ds.SuccessfulDownloads, ds.SuccessRate(), ds.FailedDownloads, ds.FailureRate(),
		ds.TotalBytes, ds.AverageSpeed, ds.RetryCount)
}