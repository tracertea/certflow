package ctprocessor

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// S3ManagerImpl implements the S3Manager interface
type S3ManagerImpl struct {
	fileDiscovery   FileDiscovery
	downloadManager DownloadManager
	stateTracker    StateTracker
	fileProcessor   FileProcessor
	fileManager     FileManager
	logger          *log.Logger
	stats           S3ProcessingStats
}

// NewS3Manager creates a new S3Manager instance
func NewS3Manager(
	fileDiscovery FileDiscovery,
	downloadManager DownloadManager,
	stateTracker StateTracker,
	fileProcessor FileProcessor,
	fileManager FileManager,
	logger *log.Logger,
) *S3ManagerImpl {
	if logger == nil {
		logger = log.Default()
	}

	return &S3ManagerImpl{
		fileDiscovery:   fileDiscovery,
		downloadManager: downloadManager,
		stateTracker:    stateTracker,
		fileProcessor:   fileProcessor,
		fileManager:     fileManager,
		logger:          logger,
		stats:           S3ProcessingStats{},
	}
}

// ProcessS3Bucket orchestrates the complete S3 processing workflow
func (sm *S3ManagerImpl) ProcessS3Bucket(config S3Config) error {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid S3 configuration: %w", err)
	}

	sm.logger.Printf("Starting S3 bucket processing: bucket=%s, prefix=%s", config.Bucket, config.Prefix)

	// Initialize state tracker
	if err := sm.stateTracker.LoadState(); err != nil {
		sm.logger.Printf("Warning: Failed to load state, starting fresh: %v", err)
	}

	// Create cache directory using FileManager
	if err := sm.fileManager.CreateCacheDirectory(config.CacheDir); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Start disk space monitoring if configured
	if config.FileManagerConfig.DiskSpaceThresholdBytes > 0 {
		err := sm.fileManager.MonitorDiskSpace(config.CacheDir, config.FileManagerConfig.DiskSpaceThresholdBytes,
			func(diskInfo DiskSpaceInfo) {
				sm.logger.Printf("WARNING: Low disk space detected: %s", diskInfo.String())
			})
		if err != nil {
			sm.logger.Printf("Warning: Failed to start disk space monitoring: %v", err)
		}
	}

	// Clean up old cache files if configured
	if config.FileManagerConfig.MaxCacheAge > 0 {
		if err := sm.fileManager.CleanupCacheDirectory(config.CacheDir, config.FileManagerConfig.MaxCacheAge); err != nil {
			sm.logger.Printf("Warning: Failed to cleanup old cache files: %v", err)
		}
	}

	// Phase 1: Discovery - List and filter files from S3
	sm.logger.Printf("Phase 1: Discovering files in S3 bucket")
	objects, err := sm.discoverFiles(config.Bucket, config.Prefix)
	if err != nil {
		return fmt.Errorf("file discovery failed: %w", err)
	}

	sm.logger.Printf("Discovered %d files in S3", len(objects))
	sm.stats.TotalFiles = int64(len(objects))

	// Phase 2: State Check - Filter out already processed files
	sm.logger.Printf("Phase 2: Checking processing state")
	pendingObjects := sm.filterPendingFiles(objects)
	sm.stats.SkippedFiles = sm.stats.TotalFiles - int64(len(pendingObjects))

	sm.logger.Printf("Found %d files to process (%d already processed)",
		len(pendingObjects), sm.stats.SkippedFiles)

	if len(pendingObjects) == 0 {
		sm.logger.Printf("No files to process")
		return nil
	}

	// Phase 3: Download - Download files in parallel
	sm.logger.Printf("Phase 3: Downloading files")
	downloadConfig := DownloadConfig{
		WorkerCount:    config.WorkerCount,
		RetryLimit:     config.RetryLimit,
		TimeoutSeconds: config.TimeoutSeconds,
		CacheDir:       config.CacheDir,
		ShowProgress:   true,
	}

	_, err = sm.downloadFiles(pendingObjects, downloadConfig, config)
	if err != nil {
		return fmt.Errorf("download phase failed: %w", err)
	}

	// Phase 4: Processing - Files are now processed immediately after download
	sm.logger.Printf("Phase 4: Processing complete - files processed during download")

	// Phase 5: State Update - Save final state
	sm.logger.Printf("Phase 5: Updating processing state")
	if err := sm.stateTracker.SaveState(); err != nil {
		sm.logger.Printf("Warning: Failed to save state: %v", err)
	}

	sm.logger.Printf("S3 bucket processing completed: %s", sm.stats.String())
	return nil
}

// ListFiles lists files from the S3 bucket with the given prefix
func (sm *S3ManagerImpl) ListFiles(bucket, prefix string) ([]S3Object, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}

	sm.logger.Printf("Listing files from S3: bucket=%s, prefix=%s", bucket, prefix)

	objects, err := sm.fileDiscovery.ListObjects(bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects: %w", err)
	}

	// Apply default filtering for .gz files
	criteria := FilterCriteria{
		Extensions: []string{".gz"},
		SortByKey:  true,
	}

	filteredObjects := sm.fileDiscovery.FilterObjects(objects, criteria)
	sm.logger.Printf("Found %d .gz files after filtering", len(filteredObjects))

	return filteredObjects, nil
}

// GetProcessingStats returns the current processing statistics
func (sm *S3ManagerImpl) GetProcessingStats() S3ProcessingStats {
	return sm.stats
}

// SetProgressDisplayFunc sets a custom progress display function for downloads
func (sm *S3ManagerImpl) SetProgressDisplayFunc(displayFunc func(ProgressUpdate)) {
	if dm, ok := sm.downloadManager.(*DefaultDownloadManager); ok {
		dm.SetProgressDisplayFunc(displayFunc)
	}
}

// discoverFiles handles the file discovery phase
func (sm *S3ManagerImpl) discoverFiles(bucket, prefix string) ([]S3Object, error) {
	startTime := time.Now()

	objects, err := sm.ListFiles(bucket, prefix)
	if err != nil {
		return nil, err
	}

	discoveryDuration := time.Since(startTime)
	sm.logger.Printf("File discovery completed in %v", discoveryDuration)

	return objects, nil
}

// filterPendingFiles filters out files that have already been processed
func (sm *S3ManagerImpl) filterPendingFiles(objects []S3Object) []S3Object {
	var pendingObjects []S3Object

	for _, obj := range objects {
		if !sm.stateTracker.IsProcessed(obj.Key, obj.ETag) {
			pendingObjects = append(pendingObjects, obj)
		}
	}

	return pendingObjects
}

// downloadFiles handles the parallel download phase
func (sm *S3ManagerImpl) downloadFiles(objects []S3Object, config DownloadConfig, s3Config S3Config) ([]DownloadResult, error) {
	if len(objects) == 0 {
		return []DownloadResult{}, nil
	}

	startTime := time.Now()
	sm.logger.Printf("Starting download of %d files with %d workers", len(objects), config.WorkerCount)

	// Start download process
	resultsChan := sm.downloadManager.DownloadFiles(objects, config)

	// Collect results
	var results []DownloadResult
	var totalBytes int64

	// Create a concurrent processing pipeline
	// Queue capacity should accommodate both download and processing workers
	queueCapacity := max(config.WorkerCount, s3Config.ProcessingWorkerCount) * 2
	processingChan := make(chan DownloadResult, queueCapacity)

	// Track files sent to processing to ensure all are processed
	var processingWaitGroup sync.WaitGroup

	// Start processing workers
	var processingWg sync.WaitGroup
	processingWorkerCount := s3Config.ProcessingWorkerCount

	for i := 0; i < processingWorkerCount; i++ {
		processingWg.Add(1)
		go sm.processingWorker(processingChan, s3Config, &processingWg, &processingWaitGroup)
	}

	stopMonitoring := make(chan bool)
	go sm.monitorPipeline(processingChan, config.WorkerCount, processingWorkerCount, stopMonitoring)

	// Process download results and send successful ones to processing pipeline
	for result := range resultsChan {
		results = append(results, result)

		if result.Success {
			totalBytes += result.Object.Size
			sm.logger.Printf("Downloaded: %s (%d bytes) in %v",
				result.Object.Key, result.Object.Size, result.Duration)

			// Increment wait group for each file sent to processing
			processingWaitGroup.Add(1)

			// Send to processing pipeline (non-blocking)
			select {
			case processingChan <- result:
				// File sent to processing pipeline
			default:
				sm.logger.Printf("WARNING: Processing pipeline full, processing %s synchronously", result.Object.Key)
				// Fallback to immediate processing if pipeline is full
				sm.processFileImmediatelyWithErrorHandling(result, s3Config)
				processingWaitGroup.Done() // Immediately mark as done since processed synchronously
			}
		} else {
			sm.logger.Printf("Download failed: %s - %v", result.Object.Key, result.Error)
			// Mark failed downloads in state tracker
			errorMsg := "download failed"
			if result.Error != nil {
				errorMsg = result.Error.Error()
			}
			if err := sm.stateTracker.MarkFailed(result.Object.Key, result.Object.ETag, errorMsg); err != nil {
				sm.logger.Printf("Warning: Failed to mark file as failed in state: %v", err)
			}
			sm.stats.AddFailedFile()
		}

		// Progress reporting
		if len(results)%10 == 0 {
			sm.logger.Printf("Download progress: %d/%d files completed", len(results), len(objects))
		}
	}

	close(processingChan)

	sm.logger.Printf("Waiting for all files to be processed...")
	processingWaitGroup.Wait()
	sm.logger.Printf("All files have been processed")

	processingWg.Wait()
	sm.logger.Printf("All processing workers completed cleanup")

	stopMonitoring <- true

	downloadDuration := time.Since(startTime)
	sm.stats.UpdateDownloadTime(downloadDuration)
	sm.stats.CalculateDownloadThroughput(totalBytes)

	sm.logger.Printf("Download phase completed: %d successful, %d failed in %v",
		sm.countSuccessfulDownloads(results), sm.countFailedDownloads(results), downloadDuration)

	return results, nil
}


// processFile processes a single downloaded file using concurrent processing
func (sm *S3ManagerImpl) processFile(result DownloadResult, config S3Config) error {
	// Create output path based on the S3 key
	outputFileName := sm.generateOutputFileName(result.Object.Key, config.ProcessorConfig.OutputFormat)

	// If OutputPath is a directory (doesn't have an extension), use it as the directory
	// Otherwise, use its directory part
	var outputDir string
	if filepath.Ext(config.ProcessorConfig.OutputPath) == "" {
		// OutputPath is a directory
		outputDir = config.ProcessorConfig.OutputPath
	} else {
		// OutputPath is a file, use its directory
		outputDir = filepath.Dir(config.ProcessorConfig.OutputPath)
	}

	// If a prefix is specified, include it in the output path to organize files
	if config.Prefix != "" {
		// Clean the prefix to make it safe for filesystem paths
		// Convert S3-style prefix (e.g., "logs/2024/") to filesystem path
		prefixPath := strings.TrimSuffix(config.Prefix, "/")
		prefixPath = strings.ReplaceAll(prefixPath, "//", "/")
		outputDir = filepath.Join(outputDir, prefixPath)
	}

	outputPath := filepath.Join(outputDir, outputFileName)

	sm.logger.Printf("Processing file: %s -> %s", result.LocalPath, outputPath)

	// Create a concurrent processor for this file
	concurrentProcessor := NewConcurrentProcessor(sm.logger)

	// Create processing config for this file
	processingConfig := config.ProcessorConfig
	processingConfig.InputPath = result.LocalPath
	processingConfig.OutputPath = outputPath

	// Use 4 workers per file for optimal file I/O parallelization
	processingConfig.WorkerCount = 4

	// Process the file with concurrent workers
	if err := concurrentProcessor.ProcessFileWithWorkers(processingConfig); err != nil {
		return fmt.Errorf("concurrent file processing failed: %w", err)
	}

	return nil
}

// generateOutputFileName generates an output filename based on the S3 key
func (sm *S3ManagerImpl) generateOutputFileName(s3Key, format string) string {
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

// monitorPipeline monitors channel depths and sends pipeline metrics
func (sm *S3ManagerImpl) monitorPipeline(processingChan chan DownloadResult, downloadWorkers, processingWorkers int, stopChan <-chan bool) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
		}
	}
}

// processingWorker processes files from the processing channel
func (sm *S3ManagerImpl) processingWorker(processingChan <-chan DownloadResult, s3Config S3Config, wg *sync.WaitGroup, fileWg *sync.WaitGroup) {
	defer wg.Done()

	for result := range processingChan {
		sm.processFileImmediatelyWithErrorHandling(result, s3Config)
		fileWg.Done()
	}
}

// processFileImmediatelyWithErrorHandling processes a file with full error handling and state tracking
func (sm *S3ManagerImpl) processFileImmediatelyWithErrorHandling(result DownloadResult, s3Config S3Config) {
	if err := sm.processFileImmediatelyAfterDownload(result, s3Config); err != nil {
		sm.logger.Printf("Processing failed for %s: %v", result.Object.Key, err)
		if stateErr := sm.stateTracker.MarkFailed(result.Object.Key, result.Object.ETag, err.Error()); stateErr != nil {
			sm.logger.Printf("Warning: Failed to mark file as failed in state: %v", stateErr)
		}
		sm.stats.AddFailedFile()
	} else {
		if err := sm.stateTracker.MarkProcessed(result.Object.Key, result.Object.ETag, result.Object.Size); err != nil {
			sm.logger.Printf("Warning: Failed to mark file as processed in state: %v", err)
		}

		// Processing completion message is now logged by the pipeline processor
		// with combined filename, size, and stats in compact format

		sm.stats.AddProcessedFile()
	}
}

// processFileImmediatelyAfterDownload processes a single file right after download
func (sm *S3ManagerImpl) processFileImmediatelyAfterDownload(result DownloadResult, s3Config S3Config) error {
	// Track processing time for this file
	processStartTime := time.Now()
	
	// Process the file
	if err := sm.processFile(result, s3Config); err != nil {
		return err
	}
	
	// Update total processing time
	processingDuration := time.Since(processStartTime)
	sm.stats.UpdateProcessTime(processingDuration)

	// Clean up downloaded file if configured
	if s3Config.CleanupAfterProcessing {
		if err := sm.fileManager.CleanupFile(result.LocalPath); err != nil {
			sm.logger.Printf("Warning: Failed to cleanup file %s: %v", result.LocalPath, err)
		}
	}

	return nil
}

// countSuccessfulDownloads counts successful downloads in results
func (sm *S3ManagerImpl) countSuccessfulDownloads(results []DownloadResult) int {
	count := 0
	for _, result := range results {
		if result.Success {
			count++
		}
	}
	return count
}

// countFailedDownloads counts failed downloads in results
func (sm *S3ManagerImpl) countFailedDownloads(results []DownloadResult) int {
	count := 0
	for _, result := range results {
		if !result.Success {
			count++
		}
	}
	return count
}

// CreateS3ManagerFromConfig creates a new S3Manager with all dependencies from config
func CreateS3ManagerFromConfig(config S3Config) (*S3ManagerImpl, error) {
	// Create logger
	logger := log.New(os.Stdout, "[S3Manager] ", log.LstdFlags)
	return CreateS3ManagerFromConfigWithLogger(config, logger)
}

// CreateS3ManagerFromConfigWithLogger creates a new S3Manager with all dependencies from config and custom logger
func CreateS3ManagerFromConfigWithLogger(config S3Config, logger *log.Logger) (*S3ManagerImpl, error) {

	// Create AWS configuration
	ctx := context.Background()
	awsCfg, err := loadAWSConfig(ctx, config.Region, config.Profile)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := NewAWSS3ClientWithConfig(awsCfg, config.Bucket)

	// Create components
	fileDiscovery := NewS3FileDiscovery(s3Client)
	downloadManager := NewDownloadManager(s3Client)
	stateTracker := NewFileStateTracker(config.StateFilePath, true)
	fileProcessor := NewStreamingProcessor(logger) // Using the streaming processor
	fileManager := NewFileManager(FileManagerConfig{
		CacheDir:                config.CacheDir,
		CleanupAfterProcessing:  config.CleanupAfterProcessing,
		DiskSpaceThresholdBytes: int64(config.DiskSpaceThresholdGB * 1024 * 1024 * 1024),
	})

	// Create S3Manager
	return NewS3Manager(
		fileDiscovery,
		downloadManager,
		stateTracker,
		fileProcessor,
		fileManager,
		logger,
	), nil
}

// loadAWSConfig loads AWS configuration with optional profile
func loadAWSConfig(ctx context.Context, region, profile string) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}

	return config.LoadDefaultConfig(ctx, opts...)
}
