package ctprocessor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewFileManager(t *testing.T) {
	tests := []struct {
		name           string
		config         FileManagerConfig
		expectedConfig FileManagerConfig
	}{
		{
			name:   "default configuration",
			config: FileManagerConfig{},
			expectedConfig: FileManagerConfig{
				CacheDir:                "./cache",
				MaxCacheAge:             24 * time.Hour,
				DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
				DiskSpaceCheckInterval:  5 * time.Minute,
			},
		},
		{
			name: "custom configuration",
			config: FileManagerConfig{
				CacheDir:                "/tmp/custom",
				CleanupAfterProcessing:  true,
				MaxCacheAge:             12 * time.Hour,
				DiskSpaceThresholdBytes: 500 * 1024 * 1024,
				DiskSpaceCheckInterval:  2 * time.Minute,
			},
			expectedConfig: FileManagerConfig{
				CacheDir:                "/tmp/custom",
				CleanupAfterProcessing:  true,
				MaxCacheAge:             12 * time.Hour,
				DiskSpaceThresholdBytes: 500 * 1024 * 1024,
				DiskSpaceCheckInterval:  2 * time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := NewFileManager(tt.config)
			
			if fm.config.CacheDir != tt.expectedConfig.CacheDir {
				t.Errorf("Expected CacheDir %s, got %s", tt.expectedConfig.CacheDir, fm.config.CacheDir)
			}
			
			if fm.config.MaxCacheAge != tt.expectedConfig.MaxCacheAge {
				t.Errorf("Expected MaxCacheAge %v, got %v", tt.expectedConfig.MaxCacheAge, fm.config.MaxCacheAge)
			}
			
			if fm.config.DiskSpaceThresholdBytes != tt.expectedConfig.DiskSpaceThresholdBytes {
				t.Errorf("Expected DiskSpaceThresholdBytes %d, got %d", 
					tt.expectedConfig.DiskSpaceThresholdBytes, fm.config.DiskSpaceThresholdBytes)
			}
			
		})
	}
}

func TestCreateCacheDirectory(t *testing.T) {
	fm := NewFileManager(FileManagerConfig{})
	
	tests := []struct {
		name        string
		path        string
		expectError bool
	}{
		{
			name:        "valid path",
			path:        filepath.Join(os.TempDir(), "test_cache"),
			expectError: false,
		},
		{
			name:        "empty path",
			path:        "",
			expectError: true,
		},
		{
			name:        "nested path",
			path:        filepath.Join(os.TempDir(), "test_cache", "nested", "deep"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before test
			if tt.path != "" {
				os.RemoveAll(tt.path)
			}
			
			err := fm.CreateCacheDirectory(tt.path)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			// Verify directory was created
			if _, err := os.Stat(tt.path); os.IsNotExist(err) {
				t.Errorf("Directory %s was not created", tt.path)
			}
			
			// Clean up after test
			os.RemoveAll(tt.path)
		})
	}
}

func TestCleanupFile(t *testing.T) {
	fm := NewFileManager(FileManagerConfig{})
	
	tests := []struct {
		name        string
		setupFile   bool
		filePath    string
		expectError bool
	}{
		{
			name:        "existing file",
			setupFile:   true,
			filePath:    filepath.Join(os.TempDir(), "test_cleanup.txt"),
			expectError: false,
		},
		{
			name:        "non-existing file",
			setupFile:   false,
			filePath:    filepath.Join(os.TempDir(), "non_existing.txt"),
			expectError: false, // Should not error for non-existing files
		},
		{
			name:        "empty path",
			setupFile:   false,
			filePath:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test file if needed
			if tt.setupFile && tt.filePath != "" {
				if err := os.WriteFile(tt.filePath, []byte("test content"), 0644); err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
			}
			
			err := fm.CleanupFile(tt.filePath)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			// Verify file was removed (if it existed)
			if tt.setupFile {
				if _, err := os.Stat(tt.filePath); !os.IsNotExist(err) {
					t.Errorf("File %s was not removed", tt.filePath)
				}
			}
		})
	}
}

func TestCleanupCacheDirectory(t *testing.T) {
	fm := NewFileManager(FileManagerConfig{})
	
	// Create temporary directory for testing
	tempDir := filepath.Join(os.TempDir(), "test_cache_cleanup")
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)
	
	// Create test files with different ages
	oldFile := filepath.Join(tempDir, "old_file.txt")
	newFile := filepath.Join(tempDir, "new_file.txt")
	
	// Create old file (2 hours ago)
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.WriteFile(oldFile, []byte("old content"), 0644); err != nil {
		t.Fatalf("Failed to create old test file: %v", err)
	}
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("Failed to set old file time: %v", err)
	}
	
	// Create new file (current time)
	if err := os.WriteFile(newFile, []byte("new content"), 0644); err != nil {
		t.Fatalf("Failed to create new test file: %v", err)
	}
	
	tests := []struct {
		name        string
		cacheDir    string
		maxAge      time.Duration
		expectError bool
		expectOld   bool // Should old file still exist?
		expectNew   bool // Should new file still exist?
	}{
		{
			name:        "cleanup old files",
			cacheDir:    tempDir,
			maxAge:      1 * time.Hour,
			expectError: false,
			expectOld:   false, // Should be removed
			expectNew:   true,  // Should remain
		},
		{
			name:        "no cleanup needed",
			cacheDir:    tempDir,
			maxAge:      3 * time.Hour,
			expectError: false,
			expectOld:   true, // Should remain
			expectNew:   true, // Should remain
		},
		{
			name:        "empty directory path",
			cacheDir:    "",
			maxAge:      1 * time.Hour,
			expectError: true,
			expectOld:   true,
			expectNew:   true,
		},
		{
			name:        "non-existing directory",
			cacheDir:    "/non/existing/path",
			maxAge:      1 * time.Hour,
			expectError: false, // Should not error for non-existing directory
			expectOld:   true,
			expectNew:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Recreate files for each test
			if tt.name != "empty directory path" && tt.name != "non-existing directory" {
				os.WriteFile(oldFile, []byte("old content"), 0644)
				os.Chtimes(oldFile, oldTime, oldTime)
				os.WriteFile(newFile, []byte("new content"), 0644)
			}
			
			err := fm.CleanupCacheDirectory(tt.cacheDir, tt.maxAge)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			// Check if files exist as expected
			if tt.name != "empty directory path" && tt.name != "non-existing directory" {
				_, oldExists := os.Stat(oldFile)
				_, newExists := os.Stat(newFile)
				
				if tt.expectOld && os.IsNotExist(oldExists) {
					t.Error("Expected old file to exist but it was removed")
				}
				if !tt.expectOld && !os.IsNotExist(oldExists) {
					t.Error("Expected old file to be removed but it still exists")
				}
				
				if tt.expectNew && os.IsNotExist(newExists) {
					t.Error("Expected new file to exist but it was removed")
				}
				if !tt.expectNew && !os.IsNotExist(newExists) {
					t.Error("Expected new file to be removed but it still exists")
				}
			}
		})
	}
}

func TestGetDiskSpace(t *testing.T) {
	fm := NewFileManager(FileManagerConfig{})
	
	tests := []struct {
		name        string
		path        string
		expectError bool
	}{
		{
			name:        "valid path",
			path:        os.TempDir(),
			expectError: false,
		},
		{
			name:        "empty path",
			path:        "",
			expectError: true,
		},
		{
			name:        "current directory",
			path:        ".",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diskInfo, err := fm.GetDiskSpace(tt.path)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			// Validate disk space information
			if diskInfo.TotalBytes <= 0 {
				t.Error("Total bytes should be positive")
			}
			
			if diskInfo.AvailableBytes < 0 {
				t.Error("Available bytes should be non-negative")
			}
			
			if diskInfo.UsedBytes < 0 {
				t.Error("Used bytes should be non-negative")
			}
			
			if diskInfo.UsedPercent < 0 || diskInfo.UsedPercent > 100 {
				t.Errorf("Used percent should be between 0 and 100, got %.2f", diskInfo.UsedPercent)
			}
			
			if diskInfo.Path == "" {
				t.Error("Path should not be empty")
			}
			
			// Test helper methods
			if diskInfo.GetAvailableSpaceMB() < 0 {
				t.Error("Available space in MB should be non-negative")
			}
			
			if diskInfo.GetAvailableSpaceGB() < 0 {
				t.Error("Available space in GB should be non-negative")
			}
		})
	}
}

func TestCheckDiskSpaceThreshold(t *testing.T) {
	fm := NewFileManager(FileManagerConfig{})
	
	// Get current disk space to set realistic thresholds
	diskInfo, err := fm.GetDiskSpace(os.TempDir())
	if err != nil {
		t.Fatalf("Failed to get disk space: %v", err)
	}
	
	tests := []struct {
		name          string
		path          string
		threshold     int64
		expectBelow   bool
		expectError   bool
	}{
		{
			name:        "threshold above available space",
			path:        os.TempDir(),
			threshold:   diskInfo.AvailableBytes + 1000,
			expectBelow: true,
			expectError: false,
		},
		{
			name:        "threshold below available space",
			path:        os.TempDir(),
			threshold:   diskInfo.AvailableBytes / 2,
			expectBelow: false,
			expectError: false,
		},
		{
			name:        "empty path",
			path:        "",
			threshold:   1000,
			expectBelow: false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			belowThreshold, err := fm.CheckDiskSpaceThreshold(tt.path, tt.threshold)
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if belowThreshold != tt.expectBelow {
				t.Errorf("Expected below threshold: %t, got: %t", tt.expectBelow, belowThreshold)
			}
		})
	}
}


func TestFileManagerConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      FileManagerConfig
		expectError bool
	}{
		{
			name: "valid configuration",
			config: FileManagerConfig{
				CacheDir:                "/tmp/cache",
				MaxCacheAge:             24 * time.Hour,
				DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
				DiskSpaceCheckInterval:  5 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "empty cache directory",
			config: FileManagerConfig{
				CacheDir: "",
			},
			expectError: true,
		},
		{
			name: "negative max cache age",
			config: FileManagerConfig{
				CacheDir:    "/tmp/cache",
				MaxCacheAge: -1 * time.Hour,
			},
			expectError: true,
		},
		{
			name: "negative disk space threshold",
			config: FileManagerConfig{
				CacheDir:                "/tmp/cache",
				DiskSpaceThresholdBytes: -1000,
			},
			expectError: true,
		},
		{
			name: "negative check interval",
			config: FileManagerConfig{
				CacheDir:               "/tmp/cache",
				DiskSpaceCheckInterval: -1 * time.Minute,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestDiskSpaceInfoMethods(t *testing.T) {
	diskInfo := DiskSpaceInfo{
		TotalBytes:     10 * 1024 * 1024 * 1024, // 10 GB
		AvailableBytes: 2 * 1024 * 1024 * 1024,  // 2 GB
		UsedBytes:      8 * 1024 * 1024 * 1024,  // 8 GB
		UsedPercent:    80.0,
		Path:           "/tmp",
	}
	
	// Test IsLowSpace
	if !diskInfo.IsLowSpace(3 * 1024 * 1024 * 1024) { // 3 GB threshold
		t.Error("Expected IsLowSpace to return true for 3GB threshold")
	}
	
	if diskInfo.IsLowSpace(1 * 1024 * 1024 * 1024) { // 1 GB threshold
		t.Error("Expected IsLowSpace to return false for 1GB threshold")
	}
	
	// Test GetAvailableSpaceMB
	expectedMB := float64(2 * 1024) // 2048 MB
	if diskInfo.GetAvailableSpaceMB() != expectedMB {
		t.Errorf("Expected %.2f MB, got %.2f MB", expectedMB, diskInfo.GetAvailableSpaceMB())
	}
	
	// Test GetAvailableSpaceGB
	expectedGB := float64(2) // 2 GB
	if diskInfo.GetAvailableSpaceGB() != expectedGB {
		t.Errorf("Expected %.2f GB, got %.2f GB", expectedGB, diskInfo.GetAvailableSpaceGB())
	}
	
	// Test String method
	str := diskInfo.String()
	if str == "" {
		t.Error("String method should not return empty string")
	}
}

// MockFileProcessor for testing
type MockFileProcessor struct {
	ProcessFileCalled  bool
	ProcessBatchCalled bool
	ShouldError        bool
}

func (m *MockFileProcessor) ProcessFile(config Config) error {
	m.ProcessFileCalled = true
	if m.ShouldError {
		return fmt.Errorf("mock processing error")
	}
	return nil
}

func (m *MockFileProcessor) ProcessBatch(inputPaths []string, config Config) error {
	m.ProcessBatchCalled = true
	if m.ShouldError {
		return fmt.Errorf("mock batch processing error")
	}
	return nil
}