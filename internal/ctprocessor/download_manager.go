package ctprocessor

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ProgressTracker tracks download progress for individual files
type ProgressTracker struct {
	Object          S3Object
	BytesDownloaded int64
	TotalBytes      int64
	StartTime       time.Time
	LastUpdate      time.Time
	TransferRate    float64 // MB/s
	Completed       bool
	Error           error
}

// DownloadProgressReporter provides real-time progress updates for downloads
type DownloadProgressReporter struct {
	trackers    map[string]*ProgressTracker
	trackersMux sync.RWMutex
	updateChan  chan ProgressUpdate
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// ProgressUpdate represents a progress update event
type ProgressUpdate struct {
	Object          S3Object
	BytesDownloaded int64
	TotalBytes      int64
	TransferRate    float64
	Completed       bool
	Error           error
}

// DefaultDownloadManager implements the DownloadManager interface
type DefaultDownloadManager struct {
	s3Client              S3Client
	stats                 DownloadStats
	statsMux              sync.RWMutex
	progressReporter      *DownloadProgressReporter
	totalFiles            int64
	completedFiles        int64
	customProgressDisplay func(ProgressUpdate)
}

// NewDownloadProgressReporter creates a new DownloadProgressReporter
func NewDownloadProgressReporter() *DownloadProgressReporter {
	return &DownloadProgressReporter{
		trackers:   make(map[string]*ProgressTracker),
		updateChan: make(chan ProgressUpdate, 100),
		stopChan:   make(chan struct{}),
	}
}

// StartReporting starts the progress reporting goroutine
func (pr *DownloadProgressReporter) StartReporting(displayFunc func(ProgressUpdate)) {
	pr.wg.Add(1)
	go func() {
		defer pr.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case update := <-pr.updateChan:
				if displayFunc != nil {
					displayFunc(update)
				}
			case <-ticker.C:
				// Send periodic updates for active downloads
				pr.sendPeriodicUpdates()
			case <-pr.stopChan:
				return
			}
		}
	}()
}

// StopReporting stops the progress reporting
func (pr *DownloadProgressReporter) StopReporting() {
	close(pr.stopChan)
	pr.wg.Wait()
}

// AddTracker adds a new progress tracker for a file
func (pr *DownloadProgressReporter) AddTracker(obj S3Object) {
	pr.trackersMux.Lock()
	defer pr.trackersMux.Unlock()

	pr.trackers[obj.Key] = &ProgressTracker{
		Object:     obj,
		TotalBytes: obj.Size,
		StartTime:  time.Now(),
		LastUpdate: time.Now(),
	}
}

// UpdateProgress updates the progress for a file
func (pr *DownloadProgressReporter) UpdateProgress(key string, bytesDownloaded int64) {
	pr.trackersMux.Lock()
	defer pr.trackersMux.Unlock()

	tracker, exists := pr.trackers[key]
	if !exists {
		return
	}

	now := time.Now()
	prevBytesDownloaded := tracker.BytesDownloaded // Store previous value before updating
	tracker.BytesDownloaded = bytesDownloaded

	// Calculate transfer rate
	if !tracker.LastUpdate.IsZero() {
		timeDiff := now.Sub(tracker.LastUpdate).Seconds()
		if timeDiff > 0 {
			bytesDiff := bytesDownloaded - prevBytesDownloaded // Use stored previous value
			tracker.TransferRate = float64(bytesDiff) / (1024 * 1024) / timeDiff
		}
	}

	tracker.LastUpdate = now

	// Send update
	select {
	case pr.updateChan <- ProgressUpdate{
		Object:          tracker.Object,
		BytesDownloaded: bytesDownloaded,
		TotalBytes:      tracker.TotalBytes,
		TransferRate:    tracker.TransferRate,
		Completed:       false,
	}:
	default:
		// Channel full, skip this update
	}
}

// CompleteTracker marks a tracker as completed
func (pr *DownloadProgressReporter) CompleteTracker(key string, err error) {
	pr.trackersMux.Lock()
	defer pr.trackersMux.Unlock()

	tracker, exists := pr.trackers[key]
	if !exists {
		return
	}

	tracker.Completed = true
	tracker.Error = err

	// Send completion update
	select {
	case pr.updateChan <- ProgressUpdate{
		Object:          tracker.Object,
		BytesDownloaded: tracker.BytesDownloaded,
		TotalBytes:      tracker.TotalBytes,
		TransferRate:    tracker.TransferRate,
		Completed:       true,
		Error:           err,
	}:
	default:
		// Channel full, skip this update
	}

	// Remove completed tracker after a delay to allow final update
	go func() {
		time.Sleep(2 * time.Second)
		pr.trackersMux.Lock()
		delete(pr.trackers, key)
		pr.trackersMux.Unlock()
	}()
}

// sendPeriodicUpdates sends periodic updates for active downloads
func (pr *DownloadProgressReporter) sendPeriodicUpdates() {
	pr.trackersMux.RLock()
	defer pr.trackersMux.RUnlock()

	for _, tracker := range pr.trackers {
		if !tracker.Completed {
			select {
			case pr.updateChan <- ProgressUpdate{
				Object:          tracker.Object,
				BytesDownloaded: tracker.BytesDownloaded,
				TotalBytes:      tracker.TotalBytes,
				TransferRate:    tracker.TransferRate,
				Completed:       false,
			}:
			default:
				// Channel full, skip this update
			}
		}
	}
}

// GetActiveDownloads returns the number of active downloads
func (pr *DownloadProgressReporter) GetActiveDownloads() int {
	pr.trackersMux.RLock()
	defer pr.trackersMux.RUnlock()

	count := 0
	for _, tracker := range pr.trackers {
		if !tracker.Completed {
			count++
		}
	}
	return count
}

// NewDownloadManager creates a new DefaultDownloadManager
func NewDownloadManager(s3Client S3Client) *DefaultDownloadManager {
	return &DefaultDownloadManager{
		s3Client:         s3Client,
		stats:            DownloadStats{},
		progressReporter: NewDownloadProgressReporter(),
	}
}

// ProgressReader wraps an io.Reader to track progress
type ProgressReader struct {
	reader         io.Reader
	key            string
	totalBytes     int64
	bytesRead      int64
	progressFunc   func(string, int64)
	lastUpdate     time.Time
	lastReported   int64
	updateInterval time.Duration
}

// NewProgressReader creates a new ProgressReader
func NewProgressReader(reader io.Reader, key string, totalBytes int64, progressFunc func(string, int64)) *ProgressReader {
	return &ProgressReader{
		reader:         reader,
		key:            key,
		totalBytes:     totalBytes,
		progressFunc:   progressFunc,
		lastUpdate:     time.Now(),
		updateInterval: 500 * time.Millisecond, // Update every 500ms max
	}
}

// Read implements io.Reader interface with progress tracking
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 {
		bytesRead := atomic.AddInt64(&pr.bytesRead, int64(n))

		// Throttle progress updates to avoid spamming
		now := time.Now()
		timeSinceUpdate := now.Sub(pr.lastUpdate)

		// Calculate progress percentage
		progressPercent := float64(bytesRead) / float64(pr.totalBytes) * 100
		lastProgressPercent := float64(pr.lastReported) / float64(pr.totalBytes) * 100
		progressDelta := progressPercent - lastProgressPercent

		// Update if enough time has passed OR significant progress change OR completion
		shouldUpdate := timeSinceUpdate >= pr.updateInterval ||
			progressDelta >= 1.0 || // Update every 1% progress
			bytesRead >= pr.totalBytes || // Always update on completion
			pr.lastReported == 0 // Always update on first read

		if shouldUpdate && pr.progressFunc != nil {
			pr.progressFunc(pr.key, bytesRead)
			pr.lastUpdate = now
			pr.lastReported = bytesRead
		}
	}
	return n, err
}

// DownloadFiles downloads files in parallel using a worker pool pattern
func (dm *DefaultDownloadManager) DownloadFiles(objects []S3Object, config DownloadConfig) <-chan DownloadResult {
	resultChan := make(chan DownloadResult, len(objects))

	// Initialize progress tracking
	atomic.StoreInt64(&dm.totalFiles, int64(len(objects)))
	atomic.StoreInt64(&dm.completedFiles, 0)

	// Start progress reporting if enabled
	if config.ShowProgress {
		displayFunc := dm.defaultProgressDisplay
		if dm.customProgressDisplay != nil {
			displayFunc = dm.customProgressDisplay
		}
		dm.progressReporter.StartReporting(displayFunc)
	}

	// Create work channel
	workChan := make(chan S3Object, len(objects))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < config.WorkerCount; i++ {
		wg.Add(1)
		go dm.downloadWorker(workChan, resultChan, config, &wg)
	}

	// Send work to workers
	go func() {
		defer close(workChan)
		for _, obj := range objects {
			// Add progress tracker for each file
			if config.ShowProgress {
				dm.progressReporter.AddTracker(obj)
			}
			workChan <- obj
		}
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		if config.ShowProgress {
			dm.progressReporter.StopReporting()
		}
		close(resultChan)
	}()

	return resultChan
}

// downloadWorker processes download jobs from the work channel
func (dm *DefaultDownloadManager) downloadWorker(workChan <-chan S3Object, resultChan chan<- DownloadResult, config DownloadConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	for obj := range workChan {
		result := dm.downloadWithRetry(obj, config)

		// Update stats
		dm.statsMux.Lock()
		dm.stats.AddDownload(result.Success, obj.Size, result.Duration, result.RetryCount)
		dm.statsMux.Unlock()

		// Complete progress tracking
		if config.ShowProgress {
			dm.progressReporter.CompleteTracker(obj.Key, result.Error)
		}

		// Update completed files counter
		atomic.AddInt64(&dm.completedFiles, 1)

		resultChan <- result
	}
}

// downloadWithRetry downloads a file with exponential backoff retry logic
func (dm *DefaultDownloadManager) downloadWithRetry(obj S3Object, config DownloadConfig) DownloadResult {
	startTime := time.Now()

	// Create local file path
	localPath := filepath.Join(config.CacheDir, filepath.Base(obj.Key))

	// Ensure cache directory exists
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return DownloadResult{
			Object:     obj,
			LocalPath:  localPath,
			Success:    false,
			Error:      fmt.Errorf("failed to create cache directory: %w", err),
			Duration:   time.Since(startTime),
			RetryCount: 0,
		}
	}

	var lastErr error
	for attempt := 0; attempt <= config.RetryLimit; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			backoffDuration := dm.calculateBackoff(attempt)
			time.Sleep(backoffDuration)
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.TimeoutSeconds)*time.Second)

		// Attempt download
		err := dm.downloadFile(ctx, obj, localPath)
		cancel()

		if err == nil {
			// Verify file integrity
			if err := dm.verifyFileIntegrity(obj, localPath); err != nil {
				lastErr = fmt.Errorf("integrity verification failed: %w", err)
				// Remove corrupted file
				os.Remove(localPath)
				continue
			}

			// Success
			return DownloadResult{
				Object:     obj,
				LocalPath:  localPath,
				Success:    true,
				Error:      nil,
				Duration:   time.Since(startTime),
				RetryCount: attempt,
			}
		}

		lastErr = err

		// Check if error is retryable
		if !dm.isRetryableError(err) {
			break
		}
	}

	// All retries exhausted or non-retryable error
	return DownloadResult{
		Object:     obj,
		LocalPath:  localPath,
		Success:    false,
		Error:      fmt.Errorf("download failed after %d attempts: %w", config.RetryLimit+1, lastErr),
		Duration:   time.Since(startTime),
		RetryCount: config.RetryLimit,
	}
}

// downloadFile performs the actual file download
func (dm *DefaultDownloadManager) downloadFile(ctx context.Context, obj S3Object, localPath string) error {
	// Create temporary file
	tempPath := localPath + ".tmp"

	// Clean up temp file on error
	defer func() {
		if _, err := os.Stat(tempPath); err == nil {
			os.Remove(tempPath)
		}
	}()

	// Create temp file
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Download file content
	reader, err := dm.s3Client.GetObject(ctx, obj.Key)
	if err != nil {
		return fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer reader.Close()

	// Create progress reader if progress tracking is enabled
	var copyReader io.Reader = reader
	if dm.progressReporter != nil {
		copyReader = NewProgressReader(reader, obj.Key, obj.Size, dm.progressReporter.UpdateProgress)
	}

	// Copy with progress tracking
	_, err = io.Copy(tempFile, copyReader)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Sync to ensure data is written
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Close temp file before rename
	tempFile.Close()

	// Atomic rename
	if err := os.Rename(tempPath, localPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// verifyFileIntegrity verifies the downloaded file matches expected size and ETag
func (dm *DefaultDownloadManager) verifyFileIntegrity(obj S3Object, localPath string) error {
	// Check file size
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if fileInfo.Size() != obj.Size {
		return fmt.Errorf("size mismatch: expected %d, got %d", obj.Size, fileInfo.Size())
	}

	// For simple ETags (not multipart), verify MD5 hash
	if dm.isSimpleETag(obj.ETag) {
		expectedMD5 := strings.Trim(obj.ETag, `"`)
		actualMD5, err := dm.calculateFileMD5(localPath)
		if err != nil {
			return fmt.Errorf("failed to calculate MD5: %w", err)
		}

		if actualMD5 != expectedMD5 {
			return fmt.Errorf("MD5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
		}
	}

	return nil
}

// isSimpleETag checks if the ETag is a simple MD5 hash (not multipart upload)
func (dm *DefaultDownloadManager) isSimpleETag(etag string) bool {
	// Simple ETags don't contain hyphens (multipart uploads do)
	return !strings.Contains(etag, "-")
}

// calculateFileMD5 calculates the MD5 hash of a file
func (dm *DefaultDownloadManager) calculateFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// calculateBackoff calculates exponential backoff duration with jitter
func (dm *DefaultDownloadManager) calculateBackoff(attempt int) time.Duration {
	// Base delay: 1 second
	baseDelay := time.Second

	// Exponential backoff: 2^attempt seconds, max 60 seconds
	backoffSeconds := math.Min(math.Pow(2, float64(attempt)), 60)
	backoffDuration := time.Duration(backoffSeconds) * baseDelay

	// Add jitter (±25%)
	jitter := time.Duration(rand.Float64() * 0.5 * float64(backoffDuration))
	if rand.Float64() < 0.5 {
		jitter = -jitter
	}

	return backoffDuration + jitter
}

// isRetryableError determines if an error is retryable
func (dm *DefaultDownloadManager) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Network-related errors are typically retryable
	retryableErrors := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"temporary failure",
		"service unavailable",
		"internal server error",
		"bad gateway",
		"gateway timeout",
		"too many requests",
		"throttling",
		"rate limit",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, retryable) {
			return true
		}
	}

	return false
}

// GetDownloadStats returns the current download statistics
func (dm *DefaultDownloadManager) GetDownloadStats() DownloadStats {
	dm.statsMux.RLock()
	defer dm.statsMux.RUnlock()

	// Return a copy to avoid race conditions
	return dm.stats
}

// ResetStats resets the download statistics
func (dm *DefaultDownloadManager) ResetStats() {
	dm.statsMux.Lock()
	defer dm.statsMux.Unlock()

	dm.stats = DownloadStats{}
	atomic.StoreInt64(&dm.totalFiles, 0)
	atomic.StoreInt64(&dm.completedFiles, 0)
}

// GetProgressStats returns current progress statistics
func (dm *DefaultDownloadManager) GetProgressStats() (completed, total int64, activeDownloads int) {
	completed = atomic.LoadInt64(&dm.completedFiles)
	total = atomic.LoadInt64(&dm.totalFiles)
	activeDownloads = dm.progressReporter.GetActiveDownloads()
	return
}

// defaultProgressDisplay provides a default progress display function
func (dm *DefaultDownloadManager) defaultProgressDisplay(update ProgressUpdate) {
	// Only log completion events to reduce verbosity
	if update.Completed {
		if update.Error != nil {
			fmt.Printf("✗ %s failed: %v\n", update.Object.Key, update.Error)
		} else {
			fmt.Printf("✓ %s completed (%.2f MB/s)\n", update.Object.Key, update.TransferRate)
		}
	}
}

// SetProgressDisplayFunc allows setting a custom progress display function
func (dm *DefaultDownloadManager) SetProgressDisplayFunc(displayFunc func(ProgressUpdate)) {
	dm.customProgressDisplay = displayFunc
}
