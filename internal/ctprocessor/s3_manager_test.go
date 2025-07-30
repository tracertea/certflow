package ctprocessor

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Mock implementations for testing

type mockFileDiscovery struct {
	objects []S3Object
	err     error
}

func (m *mockFileDiscovery) ListObjects(bucket, prefix string) ([]S3Object, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.objects, nil
}

func (m *mockFileDiscovery) FilterObjects(objects []S3Object, criteria FilterCriteria) []S3Object {
	var filtered []S3Object
	for _, obj := range objects {
		// Simple filtering by extension
		if len(criteria.Extensions) > 0 {
			for _, ext := range criteria.Extensions {
				if filepath.Ext(obj.Key) == ext {
					filtered = append(filtered, obj)
					break
				}
			}
		} else {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

type mockDownloadManager struct {
	results []DownloadResult
	stats   DownloadStats
}

func (m *mockDownloadManager) DownloadFiles(objects []S3Object, config DownloadConfig) <-chan DownloadResult {
	resultsChan := make(chan DownloadResult, len(m.results))
	
	go func() {
		defer close(resultsChan)
		for _, result := range m.results {
			resultsChan <- result
		}
	}()
	
	return resultsChan
}

func (m *mockDownloadManager) GetDownloadStats() DownloadStats {
	return m.stats
}

type mockStateTracker struct {
	processedFiles map[string]bool
	failedFiles    map[string]bool
	loadError      error
	saveError      error
	processedCount int
	failedCount    int
}

func (m *mockStateTracker) LoadState() error {
	return m.loadError
}

func (m *mockStateTracker) IsProcessed(key string, etag string) bool {
	return m.processedFiles[key+":"+etag]
}

func (m *mockStateTracker) MarkProcessed(key string, etag string, size int64) error {
	if m.processedFiles == nil {
		m.processedFiles = make(map[string]bool)
	}
	m.processedFiles[key+":"+etag] = true
	m.processedCount++
	return nil
}

func (m *mockStateTracker) MarkFailed(key string, etag string, errorMsg string) error {
	if m.failedFiles == nil {
		m.failedFiles = make(map[string]bool)
	}
	m.failedFiles[key+":"+etag] = true
	m.failedCount++
	return nil
}

func (m *mockStateTracker) SaveState() error {
	return m.saveError
}

func (m *mockStateTracker) GetProcessedCount() int {
	return m.processedCount
}

func (m *mockStateTracker) GetFailedCount() int {
	return m.failedCount
}

type mockFileProcessor struct {
	processError error
	processedFiles []string
}

func (m *mockFileProcessor) ProcessFile(config Config) error {
	if m.processError != nil {
		return m.processError
	}
	m.processedFiles = append(m.processedFiles, config.InputPath)
	return nil
}

func (m *mockFileProcessor) ProcessBatch(inputPaths []string, config Config) error {
	if m.processError != nil {
		return m.processError
	}
	m.processedFiles = append(m.processedFiles, inputPaths...)
	return nil
}

// mockFileManager implements FileManager for testing
type mockFileManager struct {
	createCacheDirCalled       bool
	cleanupFileCalled          bool
	cleanupCacheDirCalled      bool
	getDiskSpaceCalled         bool
	checkDiskSpaceThresholdCalled bool
	monitorDiskSpaceCalled     bool
	shouldError                bool
}

func (m *mockFileManager) CreateCacheDirectory(path string) error {
	m.createCacheDirCalled = true
	if m.shouldError {
		return fmt.Errorf("mock create cache directory error")
	}
	return nil
}

func (m *mockFileManager) CleanupFile(filePath string) error {
	m.cleanupFileCalled = true
	if m.shouldError {
		return fmt.Errorf("mock cleanup file error")
	}
	return nil
}

func (m *mockFileManager) CleanupCacheDirectory(cacheDir string, maxAge time.Duration) error {
	m.cleanupCacheDirCalled = true
	if m.shouldError {
		return fmt.Errorf("mock cleanup cache directory error")
	}
	return nil
}

func (m *mockFileManager) GetDiskSpace(path string) (DiskSpaceInfo, error) {
	m.getDiskSpaceCalled = true
	if m.shouldError {
		return DiskSpaceInfo{}, fmt.Errorf("mock get disk space error")
	}
	return DiskSpaceInfo{
		TotalBytes:     10 * 1024 * 1024 * 1024, // 10 GB
		AvailableBytes: 5 * 1024 * 1024 * 1024,  // 5 GB
		UsedBytes:      5 * 1024 * 1024 * 1024,  // 5 GB
		UsedPercent:    50.0,
		Path:           path,
	}, nil
}

func (m *mockFileManager) CheckDiskSpaceThreshold(path string, thresholdBytes int64) (bool, error) {
	m.checkDiskSpaceThresholdCalled = true
	if m.shouldError {
		return false, fmt.Errorf("mock check disk space threshold error")
	}
	return false, nil // Assume sufficient space
}

func (m *mockFileManager) MonitorDiskSpace(path string, thresholdBytes int64, callback func(DiskSpaceInfo)) error {
	m.monitorDiskSpaceCalled = true
	if m.shouldError {
		return fmt.Errorf("mock monitor disk space error")
	}
	return nil
}


func TestNewS3Manager(t *testing.T) {
	fileDiscovery := &mockFileDiscovery{}
	downloadManager := &mockDownloadManager{}
	stateTracker := &mockStateTracker{}
	fileProcessor := &mockFileProcessor{}
	fileManager := &mockFileManager{}
	logger := log.New(os.Stdout, "test: ", log.LstdFlags)

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, fileManager, logger)

	if manager == nil {
		t.Fatal("Expected non-nil S3Manager")
	}

	if manager.fileDiscovery != fileDiscovery {
		t.Error("FileDiscovery not set correctly")
	}

	if manager.downloadManager != downloadManager {
		t.Error("DownloadManager not set correctly")
	}

	if manager.stateTracker != stateTracker {
		t.Error("StateTracker not set correctly")
	}

	if manager.fileProcessor != fileProcessor {
		t.Error("FileProcessor not set correctly")
	}

	if manager.logger != logger {
		t.Error("Logger not set correctly")
	}

	// Verify initial stats are empty
	stats := manager.GetProcessingStats()
	if stats.TotalFiles != 0 {
		t.Error("Expected initial TotalFiles to be 0")
	}
	if stats.ProcessedFiles != 0 {
		t.Error("Expected initial ProcessedFiles to be 0")
	}
	if stats.FailedFiles != 0 {
		t.Error("Expected initial FailedFiles to be 0")
	}
}

func TestNewS3Manager_NilLogger(t *testing.T) {
	fileDiscovery := &mockFileDiscovery{}
	downloadManager := &mockDownloadManager{}
	stateTracker := &mockStateTracker{}
	fileProcessor := &mockFileProcessor{}

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

	if manager.logger == nil {
		t.Error("Expected default logger when nil is provided")
	}
}

func TestS3Manager_ListFiles(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		prefix         string
		mockObjects    []S3Object
		mockError      error
		expectedCount  int
		expectedError  bool
	}{
		{
			name:   "successful listing",
			bucket: "test-bucket",
			prefix: "data/",
			mockObjects: []S3Object{
				{Key: "data/file1.gz", Size: 1000, ETag: "etag1", LastModified: time.Now()},
				{Key: "data/file2.gz", Size: 2000, ETag: "etag2", LastModified: time.Now()},
				{Key: "data/file3.txt", Size: 500, ETag: "etag3", LastModified: time.Now()},
			},
			expectedCount: 2, // Only .gz files should be returned
			expectedError: false,
		},
		{
			name:          "empty bucket name",
			bucket:        "",
			prefix:        "data/",
			expectedError: true,
		},
		{
			name:          "discovery error",
			bucket:        "test-bucket",
			prefix:        "data/",
			mockError:     errors.New("S3 access denied"),
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileDiscovery := &mockFileDiscovery{
				objects: tt.mockObjects,
				err:     tt.mockError,
			}
			downloadManager := &mockDownloadManager{}
			stateTracker := &mockStateTracker{}
			fileProcessor := &mockFileProcessor{}

			manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

			objects, err := manager.ListFiles(tt.bucket, tt.prefix)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(objects) != tt.expectedCount {
				t.Errorf("Expected %d objects, got %d", tt.expectedCount, len(objects))
			}

			// Verify all returned objects have .gz extension
			for _, obj := range objects {
				if filepath.Ext(obj.Key) != ".gz" {
					t.Errorf("Expected .gz file, got %s", obj.Key)
				}
			}
		})
	}
}

func TestS3Manager_ProcessS3Bucket_InvalidConfig(t *testing.T) {
	manager := NewS3Manager(&mockFileDiscovery{}, &mockDownloadManager{}, &mockStateTracker{}, &mockFileProcessor{}, &mockFileManager{}, nil)

	config := S3Config{
		// Missing required fields
	}

	err := manager.ProcessS3Bucket(config)
	if err == nil {
		t.Error("Expected validation error for invalid config")
	}
}

func TestS3Manager_ProcessS3Bucket_SuccessfulWorkflow(t *testing.T) {
	// Create temporary directory for cache
	tempDir, err := os.MkdirTemp("", "s3manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup mock objects
	mockObjects := []S3Object{
		{Key: "data/file1.gz", Size: 1000, ETag: "etag1", LastModified: time.Now()},
		{Key: "data/file2.gz", Size: 2000, ETag: "etag2", LastModified: time.Now()},
	}

	fileDiscovery := &mockFileDiscovery{objects: mockObjects}
	
	downloadResults := []DownloadResult{
		{
			Object:    mockObjects[0],
			LocalPath: filepath.Join(tempDir, "file1.gz"),
			Success:   true,
			Duration:  time.Second,
		},
		{
			Object:    mockObjects[1],
			LocalPath: filepath.Join(tempDir, "file2.gz"),
			Success:   true,
			Duration:  time.Second,
		},
	}
	
	// Create mock downloaded files
	for _, result := range downloadResults {
		if err := os.WriteFile(result.LocalPath, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create mock file: %v", err)
		}
	}

	downloadManager := &mockDownloadManager{results: downloadResults}
	stateTracker := &mockStateTracker{}
	fileProcessor := &mockFileProcessor{}

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

	config := S3Config{
		Bucket:                 "test-bucket",
		Prefix:                 "data/",
		Region:                 "us-east-1",
		WorkerCount:            2,
		RetryLimit:             3,
		TimeoutSeconds:         60,
		CacheDir:               tempDir,
		CleanupAfterProcessing: true,
		FileManagerConfig: FileManagerConfig{
			CacheDir:                tempDir,
			CleanupAfterProcessing:  true,
			MaxCacheAge:             24 * time.Hour,
			DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
			DiskSpaceCheckInterval:  5 * time.Minute,
		},
		ProcessorConfig: Config{
			InputPath:    "", // Will be set per file
			OutputPath:   filepath.Join(tempDir, "output.json"),
			OutputFormat: "json",
			WorkerCount:  1,
			BatchSize:    100,
		},
		StateFilePath: filepath.Join(tempDir, "state.json"),
	}

	err = manager.ProcessS3Bucket(config)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify statistics
	stats := manager.GetProcessingStats()
	if stats.TotalFiles != 2 {
		t.Errorf("Expected 2 total files, got %d", stats.TotalFiles)
	}

	if stats.ProcessedFiles != 2 {
		t.Errorf("Expected 2 processed files, got %d", stats.ProcessedFiles)
	}

	// Verify files were processed
	if len(fileProcessor.processedFiles) != 2 {
		t.Errorf("Expected 2 files to be processed, got %d", len(fileProcessor.processedFiles))
	}

	// Verify state tracking
	if stateTracker.GetProcessedCount() != 2 {
		t.Errorf("Expected 2 files marked as processed, got %d", stateTracker.GetProcessedCount())
	}
}

func TestS3Manager_ProcessS3Bucket_WithSkippedFiles(t *testing.T) {
	// Create temporary directory for cache
	tempDir, err := os.MkdirTemp("", "s3manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup mock objects
	mockObjects := []S3Object{
		{Key: "data/file1.gz", Size: 1000, ETag: "etag1", LastModified: time.Now()},
		{Key: "data/file2.gz", Size: 2000, ETag: "etag2", LastModified: time.Now()},
	}

	fileDiscovery := &mockFileDiscovery{objects: mockObjects}
	
	// Only one file will be downloaded (the other is already processed)
	downloadResults := []DownloadResult{
		{
			Object:    mockObjects[1],
			LocalPath: filepath.Join(tempDir, "file2.gz"),
			Success:   true,
			Duration:  time.Second,
		},
	}
	
	// Create mock downloaded file
	if err := os.WriteFile(downloadResults[0].LocalPath, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create mock file: %v", err)
	}

	downloadManager := &mockDownloadManager{results: downloadResults}
	
	// State tracker with one file already processed
	stateTracker := &mockStateTracker{
		processedFiles: map[string]bool{
			"data/file1.gz:etag1": true,
		},
	}
	
	fileProcessor := &mockFileProcessor{}

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

	config := S3Config{
		Bucket:                 "test-bucket",
		Prefix:                 "data/",
		Region:                 "us-east-1",
		WorkerCount:            2,
		RetryLimit:             3,
		TimeoutSeconds:         60,
		CacheDir:               tempDir,
		CleanupAfterProcessing: false,
FileManagerConfig: FileManagerConfig{
CacheDir:                tempDir,
CleanupAfterProcessing:  false,
MaxCacheAge:             24 * time.Hour,
DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
DiskSpaceCheckInterval:  5 * time.Minute,
},
		ProcessorConfig: Config{
			InputPath:    "", // Will be set per file
			OutputPath:   filepath.Join(tempDir, "output.json"),
			OutputFormat: "json",
			WorkerCount:  1,
			BatchSize:    100,
		},
		StateFilePath: filepath.Join(tempDir, "state.json"),
	}

	err = manager.ProcessS3Bucket(config)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify statistics
	stats := manager.GetProcessingStats()
	if stats.TotalFiles != 2 {
		t.Errorf("Expected 2 total files, got %d", stats.TotalFiles)
	}

	if stats.SkippedFiles != 1 {
		t.Errorf("Expected 1 skipped file, got %d", stats.SkippedFiles)
	}

	if stats.ProcessedFiles != 1 {
		t.Errorf("Expected 1 processed file, got %d", stats.ProcessedFiles)
	}
}

func TestS3Manager_ProcessS3Bucket_DownloadFailures(t *testing.T) {
	// Create temporary directory for cache
	tempDir, err := os.MkdirTemp("", "s3manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup mock objects
	mockObjects := []S3Object{
		{Key: "data/file1.gz", Size: 1000, ETag: "etag1", LastModified: time.Now()},
		{Key: "data/file2.gz", Size: 2000, ETag: "etag2", LastModified: time.Now()},
	}

	fileDiscovery := &mockFileDiscovery{objects: mockObjects}
	
	// One successful download, one failed
	downloadResults := []DownloadResult{
		{
			Object:    mockObjects[0],
			LocalPath: filepath.Join(tempDir, "file1.gz"),
			Success:   true,
			Duration:  time.Second,
		},
		{
			Object:    mockObjects[1],
			LocalPath: "",
			Success:   false,
			Error:     errors.New("network timeout"),
			Duration:  time.Second * 5,
		},
	}
	
	// Create mock downloaded file for successful download
	if err := os.WriteFile(downloadResults[0].LocalPath, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create mock file: %v", err)
	}

	downloadManager := &mockDownloadManager{results: downloadResults}
	stateTracker := &mockStateTracker{}
	fileProcessor := &mockFileProcessor{}

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

	config := S3Config{
		Bucket:                 "test-bucket",
		Prefix:                 "data/",
		Region:                 "us-east-1",
		WorkerCount:            2,
		RetryLimit:             3,
		TimeoutSeconds:         60,
		CacheDir:               tempDir,
		CleanupAfterProcessing: false,
FileManagerConfig: FileManagerConfig{
CacheDir:                tempDir,
CleanupAfterProcessing:  false,
MaxCacheAge:             24 * time.Hour,
DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
DiskSpaceCheckInterval:  5 * time.Minute,
},
		ProcessorConfig: Config{
			InputPath:    "", // Will be set per file
			OutputPath:   filepath.Join(tempDir, "output.json"),
			OutputFormat: "json",
			WorkerCount:  1,
			BatchSize:    100,
		},
		StateFilePath: filepath.Join(tempDir, "state.json"),
	}

	err = manager.ProcessS3Bucket(config)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify statistics
	stats := manager.GetProcessingStats()
	if stats.TotalFiles != 2 {
		t.Errorf("Expected 2 total files, got %d", stats.TotalFiles)
	}

	if stats.ProcessedFiles != 1 {
		t.Errorf("Expected 1 processed file, got %d", stats.ProcessedFiles)
	}

	if stats.FailedFiles != 1 {
		t.Errorf("Expected 1 failed file, got %d", stats.FailedFiles)
	}

	// Verify state tracking
	if stateTracker.GetProcessedCount() != 1 {
		t.Errorf("Expected 1 file marked as processed, got %d", stateTracker.GetProcessedCount())
	}

	if stateTracker.GetFailedCount() != 1 {
		t.Errorf("Expected 1 file marked as failed, got %d", stateTracker.GetFailedCount())
	}
}

func TestS3Manager_ProcessS3Bucket_ProcessingFailures(t *testing.T) {
	// Create temporary directory for cache
	tempDir, err := os.MkdirTemp("", "s3manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup mock objects
	mockObjects := []S3Object{
		{Key: "data/file1.gz", Size: 1000, ETag: "etag1", LastModified: time.Now()},
	}

	fileDiscovery := &mockFileDiscovery{objects: mockObjects}
	
	downloadResults := []DownloadResult{
		{
			Object:    mockObjects[0],
			LocalPath: filepath.Join(tempDir, "file1.gz"),
			Success:   true,
			Duration:  time.Second,
		},
	}
	
	// Create mock downloaded file
	if err := os.WriteFile(downloadResults[0].LocalPath, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create mock file: %v", err)
	}

	downloadManager := &mockDownloadManager{results: downloadResults}
	stateTracker := &mockStateTracker{}
	
	// File processor that fails
	fileProcessor := &mockFileProcessor{
		processError: errors.New("processing failed"),
	}

	manager := NewS3Manager(fileDiscovery, downloadManager, stateTracker, fileProcessor, &mockFileManager{}, nil)

	config := S3Config{
		Bucket:                 "test-bucket",
		Prefix:                 "data/",
		Region:                 "us-east-1",
		WorkerCount:            2,
		RetryLimit:             3,
		TimeoutSeconds:         60,
		CacheDir:               tempDir,
		CleanupAfterProcessing: false,
FileManagerConfig: FileManagerConfig{
CacheDir:                tempDir,
CleanupAfterProcessing:  false,
MaxCacheAge:             24 * time.Hour,
DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
DiskSpaceCheckInterval:  5 * time.Minute,
},
		ProcessorConfig: Config{
			InputPath:    "", // Will be set per file
			OutputPath:   filepath.Join(tempDir, "output.json"),
			OutputFormat: "json",
			WorkerCount:  1,
			BatchSize:    100,
		},
		StateFilePath: filepath.Join(tempDir, "state.json"),
	}

	err = manager.ProcessS3Bucket(config)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify statistics
	stats := manager.GetProcessingStats()
	if stats.ProcessedFiles != 0 {
		t.Errorf("Expected 0 processed files, got %d", stats.ProcessedFiles)
	}

	if stats.FailedFiles != 1 {
		t.Errorf("Expected 1 failed file, got %d", stats.FailedFiles)
	}

	// Verify state tracking
	if stateTracker.GetFailedCount() != 1 {
		t.Errorf("Expected 1 file marked as failed, got %d", stateTracker.GetFailedCount())
	}
}

func TestS3Manager_GetProcessingStats(t *testing.T) {
	manager := NewS3Manager(&mockFileDiscovery{}, &mockDownloadManager{}, &mockStateTracker{}, &mockFileProcessor{}, &mockFileManager{}, nil)

	// Initially should be empty stats
	stats := manager.GetProcessingStats()
	if stats.TotalFiles != 0 {
		t.Errorf("Expected 0 total files, got %d", stats.TotalFiles)
	}

	// Modify internal stats
	manager.stats.TotalFiles = 10
	manager.stats.ProcessedFiles = 8
	manager.stats.FailedFiles = 2

	stats = manager.GetProcessingStats()
	if stats.TotalFiles != 10 {
		t.Errorf("Expected 10 total files, got %d", stats.TotalFiles)
	}

	if stats.ProcessedFiles != 8 {
		t.Errorf("Expected 8 processed files, got %d", stats.ProcessedFiles)
	}

	if stats.FailedFiles != 2 {
		t.Errorf("Expected 2 failed files, got %d", stats.FailedFiles)
	}
}

func TestS3Manager_generateOutputFileName(t *testing.T) {
	manager := NewS3Manager(&mockFileDiscovery{}, &mockDownloadManager{}, &mockStateTracker{}, &mockFileProcessor{}, &mockFileManager{}, nil)

	tests := []struct {
		s3Key    string
		format   string
		expected string
	}{
		{"data/file1.batch.gz", "json", "file1.batch.json"},
		{"data/file2.batch.gz", "csv", "file2.batch.csv"},
		{"data/file3.batch.gz", "txt", "file3.batch.txt"},
		{"data/file4.batch.gz", "unknown", "file4.batch.json"}, // defaults to json
		{"file5.batch.gz", "json", "file5.batch.json"},
		{"path/to/file6.batch", "json", "file6.batch.json"}, // no .gz extension
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%s", tt.s3Key, tt.format), func(t *testing.T) {
			result := manager.generateOutputFileName(tt.s3Key, tt.format)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}