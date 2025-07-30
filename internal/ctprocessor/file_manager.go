package ctprocessor

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// FileManager handles file management operations including cleanup and disk space monitoring
type FileManager interface {
	// Cache directory management
	CreateCacheDirectory(path string) error
	CleanupFile(filePath string) error
	CleanupCacheDirectory(cacheDir string, maxAge time.Duration) error

	// Disk space monitoring
	GetDiskSpace(path string) (DiskSpaceInfo, error)
	CheckDiskSpaceThreshold(path string, thresholdBytes int64) (bool, error)
	MonitorDiskSpace(path string, thresholdBytes int64, callback func(DiskSpaceInfo)) error

}

// DiskSpaceInfo contains disk space information
type DiskSpaceInfo struct {
	TotalBytes     int64   `json:"total_bytes"`
	AvailableBytes int64   `json:"available_bytes"`
	UsedBytes      int64   `json:"used_bytes"`
	UsedPercent    float64 `json:"used_percent"`
	Path           string  `json:"path"`
}

// FileManagerConfig holds configuration for file management operations
type FileManagerConfig struct {
	// Cache configuration
	CacheDir               string        `json:"cache_dir"`
	CleanupAfterProcessing bool          `json:"cleanup_after_processing"`
	MaxCacheAge            time.Duration `json:"max_cache_age"`

	// Disk space monitoring
	DiskSpaceThresholdBytes int64         `json:"disk_space_threshold_bytes"`
	DiskSpaceCheckInterval  time.Duration `json:"disk_space_check_interval"`

}

// DefaultFileManager implements the FileManager interface
type DefaultFileManager struct {
	config FileManagerConfig
}

// NewFileManager creates a new DefaultFileManager
func NewFileManager(config FileManagerConfig) *DefaultFileManager {
	// Set defaults
	if config.CacheDir == "" {
		config.CacheDir = "./cache"
	}
	if config.MaxCacheAge == 0 {
		config.MaxCacheAge = 24 * time.Hour // 24 hours default
	}
	if config.DiskSpaceThresholdBytes == 0 {
		config.DiskSpaceThresholdBytes = 1024 * 1024 * 1024 // 1GB default
	}
	if config.DiskSpaceCheckInterval == 0 {
		config.DiskSpaceCheckInterval = 5 * time.Minute // 5 minutes default
	}

	return &DefaultFileManager{
		config: config,
	}
}

// CreateCacheDirectory creates the cache directory if it doesn't exist
func (fm *DefaultFileManager) CreateCacheDirectory(path string) error {
	if path == "" {
		return fmt.Errorf("cache directory path cannot be empty")
	}

	// Create directory with proper permissions
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory %s: %w", path, err)
	}

	// Verify directory is writable
	testFile := filepath.Join(path, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("cache directory %s is not writable: %w", path, err)
	}

	// Clean up test file
	os.Remove(testFile)

	return nil
}

// CleanupFile removes a file from the filesystem
func (fm *DefaultFileManager) CleanupFile(filePath string) error {
	if filePath == "" {
		return fmt.Errorf("file path cannot be empty")
	}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to clean up
	}

	// Remove the file
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", filePath, err)
	}

	return nil
}

// CleanupCacheDirectory removes old files from the cache directory
func (fm *DefaultFileManager) CleanupCacheDirectory(cacheDir string, maxAge time.Duration) error {
	if cacheDir == "" {
		return fmt.Errorf("cache directory cannot be empty")
	}

	// Check if directory exists
	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean up
	}

	cutoffTime := time.Now().Add(-maxAge)
	var cleanedFiles int
	var totalSize int64

	err := filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if file is older than maxAge
		if info.ModTime().Before(cutoffTime) {
			totalSize += info.Size()
			if removeErr := os.Remove(path); removeErr != nil {
				return fmt.Errorf("failed to remove old file %s: %w", path, removeErr)
			}
			cleanedFiles++
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to cleanup cache directory %s: %w", cacheDir, err)
	}

	if cleanedFiles > 0 {
		fmt.Printf("Cleaned up %d old files (%.2f MB) from cache directory\n",
			cleanedFiles, float64(totalSize)/(1024*1024))
	}

	return nil
}

// GetDiskSpace returns disk space information for the given path
func (fm *DefaultFileManager) GetDiskSpace(path string) (DiskSpaceInfo, error) {
	if path == "" {
		return DiskSpaceInfo{}, fmt.Errorf("path cannot be empty")
	}

	// Get absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return DiskSpaceInfo{}, fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Get filesystem stats
	var stat syscall.Statfs_t
	if err := syscall.Statfs(absPath, &stat); err != nil {
		return DiskSpaceInfo{}, fmt.Errorf("failed to get filesystem stats for %s: %w", absPath, err)
	}

	// Calculate disk space information
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	usedBytes := totalBytes - availableBytes
	usedPercent := float64(usedBytes) / float64(totalBytes) * 100.0

	return DiskSpaceInfo{
		TotalBytes:     totalBytes,
		AvailableBytes: availableBytes,
		UsedBytes:      usedBytes,
		UsedPercent:    usedPercent,
		Path:           absPath,
	}, nil
}

// CheckDiskSpaceThreshold checks if available disk space is below the threshold
func (fm *DefaultFileManager) CheckDiskSpaceThreshold(path string, thresholdBytes int64) (bool, error) {
	diskInfo, err := fm.GetDiskSpace(path)
	if err != nil {
		return false, err
	}

	return diskInfo.AvailableBytes < thresholdBytes, nil
}

// MonitorDiskSpace monitors disk space and calls callback when threshold is exceeded
func (fm *DefaultFileManager) MonitorDiskSpace(path string, thresholdBytes int64, callback func(DiskSpaceInfo)) error {
	if callback == nil {
		return fmt.Errorf("callback function cannot be nil")
	}

	// Initial check
	diskInfo, err := fm.GetDiskSpace(path)
	if err != nil {
		return err
	}

	if diskInfo.AvailableBytes < thresholdBytes {
		callback(diskInfo)
	}

	// Start monitoring goroutine
	go func() {
		ticker := time.NewTicker(fm.config.DiskSpaceCheckInterval)
		defer ticker.Stop()

		for range ticker.C {
			diskInfo, err := fm.GetDiskSpace(path)
			if err != nil {
				continue // Skip this check on error
			}

			if diskInfo.AvailableBytes < thresholdBytes {
				callback(diskInfo)
			}
		}
	}()

	return nil
}


// generateOutputFileName generates an output filename based on the S3 key
func (fm *DefaultFileManager) generateOutputFileName(s3Key, format string) string {
	// Extract base filename from S3 key and replace extension
	baseName := filepath.Base(s3Key)

	// Remove .gz extension if present
	if filepath.Ext(baseName) == ".gz" {
		baseName = baseName[:len(baseName)-3]
	}

	// Add appropriate extension based on format
	switch format {
	case "json":
		return baseName + ".json"
	case "jsonl":
		return baseName + ".jsonl"
	case "csv":
		return baseName + ".csv"
	case "txt":
		return baseName + ".txt"
	default:
		return baseName + ".json"
	}
}

// Validate validates the FileManagerConfig
func (fmc *FileManagerConfig) Validate() error {
	if fmc.CacheDir == "" {
		return fmt.Errorf("cache directory is required")
	}

	if fmc.MaxCacheAge < 0 {
		return fmt.Errorf("max cache age must be non-negative")
	}

	if fmc.DiskSpaceThresholdBytes < 0 {
		return fmt.Errorf("disk space threshold must be non-negative")
	}

	if fmc.DiskSpaceCheckInterval < 0 {
		return fmt.Errorf("disk space check interval must be non-negative")
	}

	return nil
}

// SetDefaults sets default values for FileManagerConfig
func (fmc *FileManagerConfig) SetDefaults() {
	if fmc.CacheDir == "" {
		fmc.CacheDir = "./cache"
	}

	if fmc.MaxCacheAge == 0 {
		fmc.MaxCacheAge = 24 * time.Hour
	}

	if fmc.DiskSpaceThresholdBytes == 0 {
		fmc.DiskSpaceThresholdBytes = 1024 * 1024 * 1024 // 1GB
	}

	if fmc.DiskSpaceCheckInterval == 0 {
		fmc.DiskSpaceCheckInterval = 5 * time.Minute
	}
}

// String returns a string representation of DiskSpaceInfo
func (dsi *DiskSpaceInfo) String() string {
	return fmt.Sprintf("DiskSpace{Path: %s, Total: %.2f GB, Available: %.2f GB, Used: %.1f%%}",
		dsi.Path,
		float64(dsi.TotalBytes)/(1024*1024*1024),
		float64(dsi.AvailableBytes)/(1024*1024*1024),
		dsi.UsedPercent)
}

// IsLowSpace returns true if available space is less than the threshold
func (dsi *DiskSpaceInfo) IsLowSpace(thresholdBytes int64) bool {
	return dsi.AvailableBytes < thresholdBytes
}

// GetAvailableSpaceMB returns available space in megabytes
func (dsi *DiskSpaceInfo) GetAvailableSpaceMB() float64 {
	return float64(dsi.AvailableBytes) / (1024 * 1024)
}

// GetAvailableSpaceGB returns available space in gigabytes
func (dsi *DiskSpaceInfo) GetAvailableSpaceGB() float64 {
	return float64(dsi.AvailableBytes) / (1024 * 1024 * 1024)
}
