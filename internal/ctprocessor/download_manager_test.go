package ctprocessor

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// DownloadMockS3Client implements S3Client for download testing
type DownloadMockS3Client struct {
	objects       map[string][]byte
	downloadDelay time.Duration
	failureRate   float64
	callCount     int
	keyCalls      map[string]int
	mutex         sync.Mutex
}

func NewDownloadMockS3Client() *DownloadMockS3Client {
	return &DownloadMockS3Client{
		objects:  make(map[string][]byte),
		keyCalls: make(map[string]int),
	}
}

func (m *DownloadMockS3Client) AddObject(key string, data []byte) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.objects[key] = data
}

func (m *DownloadMockS3Client) SetDownloadDelay(delay time.Duration) {
	m.downloadDelay = delay
}

func (m *DownloadMockS3Client) SetFailureRate(rate float64) {
	m.failureRate = rate
}

func (m *DownloadMockS3Client) GetCallCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.callCount
}

func (m *DownloadMockS3Client) ListObjectsV2(ctx context.Context, bucket, prefix string, continuationToken string) (*ListObjectsV2Output, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *DownloadMockS3Client) DownloadFile(ctx context.Context, bucket, key, localPath string) error {
	return fmt.Errorf("not implemented in mock")
}

func (m *DownloadMockS3Client) GetObjectAttributes(ctx context.Context, bucket, key string) (*ObjectAttributes, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *DownloadMockS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.callCount++
	m.keyCalls[key]++
	
	// Simulate download delay
	if m.downloadDelay > 0 {
		time.Sleep(m.downloadDelay)
	}
	
	// Simulate failure rate - fail for the first few attempts per key
	keyCallCount := m.keyCalls[key]
	if m.failureRate > 0 && keyCallCount <= int(m.failureRate*10) {
		return nil, fmt.Errorf("connection timeout")
	}
	
	data, exists := m.objects[key]
	if !exists {
		return nil, fmt.Errorf("object not found: %s", key)
	}
	
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func TestNewDownloadManager(t *testing.T) {
	mockClient := NewDownloadMockS3Client()
	dm := NewDownloadManager(mockClient)
	
	if dm == nil {
		t.Fatal("NewDownloadManager returned nil")
	}
	
	if dm.s3Client != mockClient {
		t.Error("DownloadManager should store the provided S3Client")
	}
	
	stats := dm.GetDownloadStats()
	if stats.TotalDownloads != 0 {
		t.Error("Initial stats should be zero")
	}
}

func TestDownloadFiles_SingleFile(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	testData := []byte("test file content")
	testKey := "test/file.gz"
	mockClient.AddObject(testKey, testData)
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create test object
	testObj := S3Object{
		Key:          testKey,
		Size:         int64(len(testData)),
		ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
		LastModified: time.Now(),
	}
	
	config := DownloadConfig{
		WorkerCount:    1,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download
	resultChan := dm.DownloadFiles([]S3Object{testObj}, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	// Verify results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	result := results[0]
	if !result.Success {
		t.Errorf("Download should have succeeded: %v", result.Error)
	}
	
	if result.Object.Key != testKey {
		t.Errorf("Expected key %s, got %s", testKey, result.Object.Key)
	}
	
	// Verify file was created and has correct content
	content, err := os.ReadFile(result.LocalPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	
	if string(content) != string(testData) {
		t.Errorf("File content mismatch. Expected %s, got %s", string(testData), string(content))
	}
	
	// Verify stats
	stats := dm.GetDownloadStats()
	if stats.TotalDownloads != 1 {
		t.Errorf("Expected 1 total download, got %d", stats.TotalDownloads)
	}
	
	if stats.SuccessfulDownloads != 1 {
		t.Errorf("Expected 1 successful download, got %d", stats.SuccessfulDownloads)
	}
}

func TestDownloadFiles_MultipleFiles(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create test objects
	testObjects := []S3Object{}
	for i := 0; i < 5; i++ {
		testData := []byte(fmt.Sprintf("test file content %d", i))
		testKey := fmt.Sprintf("test/file%d.gz", i)
		mockClient.AddObject(testKey, testData)
		
		testObjects = append(testObjects, S3Object{
			Key:          testKey,
			Size:         int64(len(testData)),
			ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
			LastModified: time.Now(),
		})
	}
	
	config := DownloadConfig{
		WorkerCount:    3,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download
	resultChan := dm.DownloadFiles(testObjects, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	// Verify results
	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}
	
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}
	
	if successCount != 5 {
		t.Errorf("Expected 5 successful downloads, got %d", successCount)
	}
	
	// Verify stats
	stats := dm.GetDownloadStats()
	if stats.TotalDownloads != 5 {
		t.Errorf("Expected 5 total downloads, got %d", stats.TotalDownloads)
	}
	
	if stats.SuccessfulDownloads != 5 {
		t.Errorf("Expected 5 successful downloads, got %d", stats.SuccessfulDownloads)
	}
}

func TestDownloadFiles_WithRetries(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	mockClient.SetFailureRate(0.3) // 30% failure rate to trigger retries
	
	testData := []byte("test file content")
	testKey := "test/file.gz"
	mockClient.AddObject(testKey, testData)
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	testObj := S3Object{
		Key:          testKey,
		Size:         int64(len(testData)),
		ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
		LastModified: time.Now(),
	}
	
	config := DownloadConfig{
		WorkerCount:    1,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download
	resultChan := dm.DownloadFiles([]S3Object{testObj}, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	// Verify results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	result := results[0]
	
	// With high failure rate and retries, we should see retry attempts
	if result.RetryCount == 0 {
		t.Error("Expected retry attempts due to high failure rate")
	}
	
	// Verify call count is higher than 1 due to retries
	callCount := mockClient.GetCallCount()
	if callCount <= 1 {
		t.Errorf("Expected multiple calls due to retries, got %d", callCount)
	}
}

func TestDownloadFiles_IntegrityValidation(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	testData := []byte("test file content")
	testKey := "test/file.gz"
	mockClient.AddObject(testKey, testData)
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Test with correct ETag
	correctETag := fmt.Sprintf(`"%x"`, md5.Sum(testData))
	testObj := S3Object{
		Key:          testKey,
		Size:         int64(len(testData)),
		ETag:         correctETag,
		LastModified: time.Now(),
	}
	
	config := DownloadConfig{
		WorkerCount:    1,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download with correct ETag
	resultChan := dm.DownloadFiles([]S3Object{testObj}, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	if !results[0].Success {
		t.Errorf("Download with correct ETag should succeed: %v", results[0].Error)
	}
	
	// Test with incorrect ETag
	dm.ResetStats()
	incorrectETag := `"incorrect_etag"`
	testObj.ETag = incorrectETag
	
	resultChan = dm.DownloadFiles([]S3Object{testObj}, config)
	
	results = nil
	for result := range resultChan {
		results = append(results, result)
	}
	
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	if results[0].Success {
		t.Error("Download with incorrect ETag should fail")
	}
	
	if !strings.Contains(results[0].Error.Error(), "MD5 mismatch") {
		t.Errorf("Expected MD5 mismatch error, got: %v", results[0].Error)
	}
}

func TestDownloadFiles_SizeValidation(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	testData := []byte("test file content")
	testKey := "test/file.gz"
	mockClient.AddObject(testKey, testData)
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Test with incorrect size
	testObj := S3Object{
		Key:          testKey,
		Size:         999, // Incorrect size
		ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
		LastModified: time.Now(),
	}
	
	config := DownloadConfig{
		WorkerCount:    1,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download
	resultChan := dm.DownloadFiles([]S3Object{testObj}, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	if results[0].Success {
		t.Error("Download with incorrect size should fail")
	}
	
	if !strings.Contains(results[0].Error.Error(), "size mismatch") {
		t.Errorf("Expected size mismatch error, got: %v", results[0].Error)
	}
}

func TestDownloadConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      DownloadConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			config: DownloadConfig{
				WorkerCount:    3,
				RetryLimit:     5,
				TimeoutSeconds: 30,
				CacheDir:       "/tmp/cache",
			},
			expectError: false,
		},
		{
			name: "zero worker count",
			config: DownloadConfig{
				WorkerCount:    0,
				RetryLimit:     5,
				TimeoutSeconds: 30,
				CacheDir:       "/tmp/cache",
			},
			expectError: true,
			errorMsg:    "worker count must be greater than 0",
		},
		{
			name: "negative retry limit",
			config: DownloadConfig{
				WorkerCount:    3,
				RetryLimit:     -1,
				TimeoutSeconds: 30,
				CacheDir:       "/tmp/cache",
			},
			expectError: true,
			errorMsg:    "retry limit must be non-negative",
		},
		{
			name: "zero timeout",
			config: DownloadConfig{
				WorkerCount:    3,
				RetryLimit:     5,
				TimeoutSeconds: 0,
				CacheDir:       "/tmp/cache",
			},
			expectError: true,
			errorMsg:    "timeout seconds must be greater than 0",
		},
		{
			name: "empty cache dir",
			config: DownloadConfig{
				WorkerCount:    3,
				RetryLimit:     5,
				TimeoutSeconds: 30,
				CacheDir:       "",
			},
			expectError: true,
			errorMsg:    "cache directory is required",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			
			if tt.expectError {
				if err == nil {
					t.Error("Expected validation error, got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error, got: %v", err)
				}
			}
		})
	}
}

func TestDownloadConfig_SetDefaults(t *testing.T) {
	config := DownloadConfig{}
	config.SetDefaults()
	
	if config.WorkerCount != 3 {
		t.Errorf("Expected default WorkerCount 3, got %d", config.WorkerCount)
	}
	
	if config.RetryLimit != 10 {
		t.Errorf("Expected default RetryLimit 10, got %d", config.RetryLimit)
	}
	
	if config.TimeoutSeconds != 300 {
		t.Errorf("Expected default TimeoutSeconds 300, got %d", config.TimeoutSeconds)
	}
	
	if config.CacheDir != "./cache" {
		t.Errorf("Expected default CacheDir './cache', got %s", config.CacheDir)
	}
}

func TestDownloadStats_Methods(t *testing.T) {
	stats := DownloadStats{}
	
	// Test initial state
	if stats.SuccessRate() != 0.0 {
		t.Error("Initial success rate should be 0.0")
	}
	
	if stats.FailureRate() != 0.0 {
		t.Error("Initial failure rate should be 0.0")
	}
	
	// Add some downloads
	stats.AddDownload(true, 1024, time.Second, 0)
	stats.AddDownload(false, 512, time.Second*2, 2)
	stats.AddDownload(true, 2048, time.Second, 1)
	
	// Test stats
	if stats.TotalDownloads != 3 {
		t.Errorf("Expected 3 total downloads, got %d", stats.TotalDownloads)
	}
	
	if stats.SuccessfulDownloads != 2 {
		t.Errorf("Expected 2 successful downloads, got %d", stats.SuccessfulDownloads)
	}
	
	if stats.FailedDownloads != 1 {
		t.Errorf("Expected 1 failed download, got %d", stats.FailedDownloads)
	}
	
	if stats.TotalBytes != 3584 {
		t.Errorf("Expected 3584 total bytes, got %d", stats.TotalBytes)
	}
	
	if stats.RetryCount != 3 {
		t.Errorf("Expected 3 total retries, got %d", stats.RetryCount)
	}
	
	expectedSuccessRate := 2.0 / 3.0 * 100.0
	actualSuccessRate := stats.SuccessRate()
	if actualSuccessRate < expectedSuccessRate-0.01 || actualSuccessRate > expectedSuccessRate+0.01 {
		t.Errorf("Expected success rate %.2f, got %.2f", expectedSuccessRate, actualSuccessRate)
	}
	
	expectedFailureRate := 1.0 / 3.0 * 100.0
	actualFailureRate := stats.FailureRate()
	if actualFailureRate < expectedFailureRate-0.01 || actualFailureRate > expectedFailureRate+0.01 {
		t.Errorf("Expected failure rate %.2f, got %.2f", expectedFailureRate, actualFailureRate)
	}
	
	// Test average speed calculation
	if stats.AverageSpeed <= 0 {
		t.Error("Average speed should be greater than 0")
	}
}

func TestDownloadManager_WorkerPool(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	mockClient.SetDownloadDelay(100 * time.Millisecond) // Add delay to test concurrency
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "download_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create multiple test objects
	numFiles := 10
	testObjects := make([]S3Object, numFiles)
	for i := 0; i < numFiles; i++ {
		testData := []byte(fmt.Sprintf("test file content %d", i))
		testKey := fmt.Sprintf("test/file%d.gz", i)
		mockClient.AddObject(testKey, testData)
		
		testObjects[i] = S3Object{
			Key:          testKey,
			Size:         int64(len(testData)),
			ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
			LastModified: time.Now(),
		}
	}
	
	config := DownloadConfig{
		WorkerCount:    3,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   false,
	}
	
	// Test download with timing
	start := time.Now()
	resultChan := dm.DownloadFiles(testObjects, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	duration := time.Since(start)
	
	// Verify all files were downloaded
	if len(results) != numFiles {
		t.Fatalf("Expected %d results, got %d", numFiles, len(results))
	}
	
	// With 3 workers and 100ms delay per file, 10 files should take roughly:
	// ceil(10/3) * 100ms = 4 * 100ms = 400ms
	// Allow generous buffer for overhead and test environment variability
	maxExpectedDuration := 2 * time.Second
	if duration > maxExpectedDuration {
		t.Errorf("Download took too long: %v (expected < %v)", duration, maxExpectedDuration)
	}
	
	// Verify all downloads succeeded
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}
	
	if successCount != numFiles {
		t.Errorf("Expected %d successful downloads, got %d", numFiles, successCount)
	}
}

func TestDownloadManager_ResetStats(t *testing.T) {
	mockClient := NewDownloadMockS3Client()
	dm := NewDownloadManager(mockClient)
	
	// Manually add some stats
	dm.statsMux.Lock()
	dm.stats.AddDownload(true, 1024, time.Second, 0)
	dm.stats.AddDownload(false, 512, time.Second, 1)
	dm.statsMux.Unlock()
	
	// Verify stats are not zero
	stats := dm.GetDownloadStats()
	if stats.TotalDownloads == 0 {
		t.Error("Stats should not be zero before reset")
	}
	
	// Reset stats
	dm.ResetStats()
	
	// Verify stats are reset
	stats = dm.GetDownloadStats()
	if stats.TotalDownloads != 0 {
		t.Error("Stats should be zero after reset")
	}
	
	if stats.SuccessfulDownloads != 0 {
		t.Error("Successful downloads should be zero after reset")
	}
	
	if stats.FailedDownloads != 0 {
		t.Error("Failed downloads should be zero after reset")
	}
}

func TestDownloadProgressReporter_BasicFunctionality(t *testing.T) {
	pr := NewDownloadProgressReporter()
	
	// Test initial state
	if pr.GetActiveDownloads() != 0 {
		t.Error("Initial active downloads should be 0")
	}
	
	// Add a tracker
	testObj := S3Object{
		Key:  "test/file.gz",
		Size: 1024,
	}
	pr.AddTracker(testObj)
	
	if pr.GetActiveDownloads() != 1 {
		t.Error("Active downloads should be 1 after adding tracker")
	}
	
	// Update progress
	pr.UpdateProgress(testObj.Key, 512)
	
	// Should still be active before completion
	if pr.GetActiveDownloads() != 1 {
		t.Error("Active downloads should be 1 before completion")
	}
	
	// Complete tracker
	pr.CompleteTracker(testObj.Key, nil)
	
	// Wait for cleanup to happen (cleanup happens after 2 seconds)
	time.Sleep(2500 * time.Millisecond)
	
	// Now active downloads should be 0
	if pr.GetActiveDownloads() != 0 {
		t.Error("Active downloads should be 0 after cleanup delay")
	}
}

func TestDownloadProgressReporter_WithReporting(t *testing.T) {
	pr := NewDownloadProgressReporter()
	
	var receivedUpdates []ProgressUpdate
	var updateMutex sync.Mutex
	
	displayFunc := func(update ProgressUpdate) {
		updateMutex.Lock()
		receivedUpdates = append(receivedUpdates, update)
		updateMutex.Unlock()
	}
	
	// Start reporting
	pr.StartReporting(displayFunc)
	defer pr.StopReporting()
	
	// Add tracker and update progress
	testObj := S3Object{
		Key:  "test/file.gz",
		Size: 1024,
	}
	pr.AddTracker(testObj)
	pr.UpdateProgress(testObj.Key, 512)
	pr.CompleteTracker(testObj.Key, nil)
	
	// Wait for updates to be processed
	time.Sleep(200 * time.Millisecond)
	
	updateMutex.Lock()
	updateCount := len(receivedUpdates)
	updateMutex.Unlock()
	
	if updateCount == 0 {
		t.Error("Should have received progress updates")
	}
	
	// Check that we received a completion update
	updateMutex.Lock()
	hasCompletion := false
	for _, update := range receivedUpdates {
		if update.Completed {
			hasCompletion = true
			break
		}
	}
	updateMutex.Unlock()
	
	if !hasCompletion {
		t.Error("Should have received a completion update")
	}
}

func TestProgressReader_TrackingProgress(t *testing.T) {
	testData := "This is test data for progress tracking"
	reader := strings.NewReader(testData)
	
	var progressUpdates []int64
	var progressMutex sync.Mutex
	
	progressFunc := func(key string, bytesRead int64) {
		progressMutex.Lock()
		progressUpdates = append(progressUpdates, bytesRead)
		progressMutex.Unlock()
	}
	
	progressReader := NewProgressReader(reader, "test-key", int64(len(testData)), progressFunc)
	
	// Read data in chunks
	buffer := make([]byte, 10)
	var totalRead int64
	
	for {
		n, err := progressReader.Read(buffer)
		if n > 0 {
			totalRead += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	
	// Verify total bytes read
	if totalRead != int64(len(testData)) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), totalRead)
	}
	
	// Verify progress updates were called
	progressMutex.Lock()
	updateCount := len(progressUpdates)
	progressMutex.Unlock()
	
	if updateCount == 0 {
		t.Error("Progress function should have been called")
	}
	
	// Verify final progress update matches total
	progressMutex.Lock()
	finalProgress := progressUpdates[len(progressUpdates)-1]
	progressMutex.Unlock()
	
	if finalProgress != int64(len(testData)) {
		t.Errorf("Final progress should be %d, got %d", len(testData), finalProgress)
	}
}

func TestDownloadFiles_WithProgressTracking(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	testData := []byte("test file content for progress tracking")
	testKey := "test/progress-file.gz"
	mockClient.AddObject(testKey, testData)
	
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "progress_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	testObj := S3Object{
		Key:          testKey,
		Size:         int64(len(testData)),
		ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
		LastModified: time.Now(),
	}
	
	config := DownloadConfig{
		WorkerCount:    1,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   true, // Enable progress tracking
	}
	
	// Test download with progress
	resultChan := dm.DownloadFiles([]S3Object{testObj}, config)
	
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	// Verify results
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	
	if !results[0].Success {
		t.Errorf("Download should have succeeded: %v", results[0].Error)
	}
	
	// Verify progress stats
	completed, total, _ := dm.GetProgressStats()
	if completed != 1 {
		t.Errorf("Expected 1 completed file, got %d", completed)
	}
	if total != 1 {
		t.Errorf("Expected 1 total file, got %d", total)
	}
}

func TestDownloadStats_ExtendedMethods(t *testing.T) {
	stats := DownloadStats{}
	
	// Test initial values
	if stats.GetThroughputMBps() != 0.0 {
		t.Error("Initial throughput should be 0.0")
	}
	
	if stats.GetTotalSizeMB() != 0.0 {
		t.Error("Initial total size should be 0.0")
	}
	
	if stats.GetAverageRetries() != 0.0 {
		t.Error("Initial average retries should be 0.0")
	}
	
	if stats.GetAverageDuration() != 0 {
		t.Error("Initial average duration should be 0")
	}
	
	// Add some downloads
	stats.AddDownload(true, 1024*1024, time.Second, 1)      // 1MB in 1 second, 1 retry
	stats.AddDownload(true, 2*1024*1024, 2*time.Second, 0) // 2MB in 2 seconds, 0 retries
	stats.AddDownload(false, 512*1024, time.Second, 3)     // 0.5MB in 1 second, 3 retries
	
	// Test throughput (should be calculated automatically)
	throughput := stats.GetThroughputMBps()
	if throughput <= 0 {
		t.Error("Throughput should be greater than 0")
	}
	
	// Test total size in MB
	expectedSizeMB := float64(3.5) // 1 + 2 + 0.5 MB
	actualSizeMB := stats.GetTotalSizeMB()
	if actualSizeMB < expectedSizeMB-0.01 || actualSizeMB > expectedSizeMB+0.01 {
		t.Errorf("Expected total size %.2f MB, got %.2f MB", expectedSizeMB, actualSizeMB)
	}
	
	// Test average retries
	expectedAvgRetries := float64(4) / float64(3) // (1+0+3)/3
	actualAvgRetries := stats.GetAverageRetries()
	if actualAvgRetries < expectedAvgRetries-0.01 || actualAvgRetries > expectedAvgRetries+0.01 {
		t.Errorf("Expected average retries %.2f, got %.2f", expectedAvgRetries, actualAvgRetries)
	}
	
	// Test average duration
	expectedAvgDuration := time.Duration(4*time.Second) / 3 // (1+2+1)/3 seconds
	actualAvgDuration := stats.GetAverageDuration()
	if actualAvgDuration < expectedAvgDuration-time.Millisecond || actualAvgDuration > expectedAvgDuration+time.Millisecond {
		t.Errorf("Expected average duration %v, got %v", expectedAvgDuration, actualAvgDuration)
	}
}

func TestDownloadManager_ProgressStatsIntegration(t *testing.T) {
	// Setup
	mockClient := NewDownloadMockS3Client()
	dm := NewDownloadManager(mockClient)
	
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "progress_stats_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create multiple test objects
	numFiles := 5
	testObjects := make([]S3Object, numFiles)
	for i := 0; i < numFiles; i++ {
		testData := []byte(fmt.Sprintf("test file content %d", i))
		testKey := fmt.Sprintf("test/file%d.gz", i)
		mockClient.AddObject(testKey, testData)
		
		testObjects[i] = S3Object{
			Key:          testKey,
			Size:         int64(len(testData)),
			ETag:         fmt.Sprintf(`"%x"`, md5.Sum(testData)),
			LastModified: time.Now(),
		}
	}
	
	config := DownloadConfig{
		WorkerCount:    2,
		RetryLimit:     3,
		TimeoutSeconds: 30,
		CacheDir:       tempDir,
		ShowProgress:   true,
	}
	
	// Test initial progress stats
	completed, total, active := dm.GetProgressStats()
	if completed != 0 || total != 0 || active != 0 {
		t.Error("Initial progress stats should be zero")
	}
	
	// Start download
	resultChan := dm.DownloadFiles(testObjects, config)
	
	// Check progress during download
	time.Sleep(50 * time.Millisecond) // Allow some downloads to start
	completed, total, active = dm.GetProgressStats()
	
	if total != int64(numFiles) {
		t.Errorf("Expected total files %d, got %d", numFiles, total)
	}
	
	// Consume all results
	var results []DownloadResult
	for result := range resultChan {
		results = append(results, result)
	}
	
	// Check final progress stats
	completed, total, active = dm.GetProgressStats()
	if completed != int64(numFiles) {
		t.Errorf("Expected completed files %d, got %d", numFiles, completed)
	}
	if total != int64(numFiles) {
		t.Errorf("Expected total files %d, got %d", numFiles, total)
	}
	
	// Verify all downloads succeeded
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}
	
	if successCount != numFiles {
		t.Errorf("Expected %d successful downloads, got %d", numFiles, successCount)
	}
}