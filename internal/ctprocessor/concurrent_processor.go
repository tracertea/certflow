package ctprocessor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// minInt returns the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ConcurrentProcessor handles concurrent certificate processing with worker pools
type ConcurrentProcessor struct {
	fileHandler *FileHandler
	certParser  *CertificateParser
	factory     *OutputWriterFactory
	logger      *log.Logger
}

// NewConcurrentProcessor creates a new concurrent processor
func NewConcurrentProcessor(logger *log.Logger) *ConcurrentProcessor {
	if logger == nil {
		logger = log.Default()
	}

	return &ConcurrentProcessor{
		fileHandler: NewFileHandler(),
		certParser:  NewCertificateParser(),
		factory:     NewOutputWriterFactory(),
		logger:      logger,
	}
}

// ProcessFileWithWorkers processes a file using concurrent workers
func (cp *ConcurrentProcessor) ProcessFileWithWorkers(config Config) error {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	cp.logger.Printf("Starting concurrent processing: %s -> %s (workers: %d, batch: %d)",
		config.InputPath, config.OutputPath, config.WorkerCount, config.BatchSize)

	// Initialize statistics
	stats := &ProcessingStats{
		StartTime: time.Now(),
	}

	// Create output writer
	writer, err := cp.factory.CreateWriter(config.OutputPath, config.OutputFormat)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %v", err)
	}
	defer writer.Close()

	// Set stats for JSON writer if applicable
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	// Create worker pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create channels for work distribution
	entryChannel := make(chan *CertificateEntry, config.BatchSize)
	resultChannel := make(chan WorkerResult, config.BatchSize)

	// Start workers
	var workerWg sync.WaitGroup
	for i := 0; i < config.WorkerCount; i++ {
		workerWg.Add(1)
		go cp.worker(ctx, i, entryChannel, resultChannel, &workerWg)
	}

	// Start result collector
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go cp.resultCollector(ctx, resultChannel, writer, stats, &collectorWg)

	// Process file and send entries to workers
	err = cp.fileHandler.ProcessFileLines(config.InputPath, func(entry *CertificateEntry) error {
		select {
		case entryChannel <- entry:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Close entry channel to signal workers to finish
	close(entryChannel)

	// Wait for workers to complete
	workerWg.Wait()

	// Close result channel to signal collector to finish
	close(resultChannel)

	// Wait for collector to complete
	collectorWg.Wait()

	if err != nil {
		return fmt.Errorf("failed to process file: %v", err)
	}

	// Update final statistics
	stats.UpdateDuration()

	// Set final stats for JSON writer
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	cp.logger.Printf("Concurrent processing completed: %s", stats.String())
	return nil
}

// ProcessBatchWithWorkers processes multiple files using concurrent workers
func (cp *ConcurrentProcessor) ProcessBatchWithWorkers(inputPaths []string, config Config) error {
	if len(inputPaths) == 0 {
		return fmt.Errorf("no input files provided")
	}

	// Validate configuration (skip InputPath validation for batch processing)
	tempConfig := config
	tempConfig.InputPath = "temp"
	if err := tempConfig.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	// Calculate optimal file workers (limit to number of files or reasonable I/O limit)
	fileWorkers := minInt(len(inputPaths), 4) // Don't overwhelm I/O subsystem
	if fileWorkers < 1 {
		fileWorkers = 1
	}

	cp.logger.Printf("Starting parallel file processing: %d files -> %s (file workers: %d, cert workers: %d)",
		len(inputPaths), config.OutputPath, fileWorkers, config.WorkerCount)

	// Initialize statistics
	stats := &ProcessingStats{
		StartTime: time.Now(),
	}

	// Create output writer
	writer, err := cp.factory.CreateWriter(config.OutputPath, config.OutputFormat)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %v", err)
	}
	defer writer.Close()

	// Set stats for JSON writer if applicable
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	// Create worker pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create channels for work distribution
	// Larger buffer to handle multiple files feeding certificates
	bufferSize := config.BatchSize * minInt(fileWorkers, 2) // Balance memory vs throughput
	entryChannel := make(chan *CertificateEntry, bufferSize)
	resultChannel := make(chan WorkerResult, bufferSize)

	// Channel for file processing tasks
	fileTaskChannel := make(chan string, len(inputPaths))

	// Start certificate processing workers
	var certWorkerWg sync.WaitGroup
	for i := 0; i < config.WorkerCount; i++ {
		certWorkerWg.Add(1)
		go cp.worker(ctx, i, entryChannel, resultChannel, &certWorkerWg)
	}

	// Start result collector
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go cp.resultCollector(ctx, resultChannel, writer, stats, &collectorWg)

	// Start file processing workers
	var fileWorkerWg sync.WaitGroup
	for i := 0; i < fileWorkers; i++ {
		fileWorkerWg.Add(1)
		go cp.fileWorker(ctx, i, fileTaskChannel, entryChannel, &fileWorkerWg)
	}

	// Send all file paths to file workers
	for i, inputPath := range inputPaths {
		cp.logger.Printf("Queuing file %d/%d: %s", i+1, len(inputPaths), inputPath)
		select {
		case fileTaskChannel <- inputPath:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Close file task channel to signal file workers to finish
	close(fileTaskChannel)

	// Wait for file workers to complete
	fileWorkerWg.Wait()

	// Close entry channel to signal certificate workers to finish
	close(entryChannel)

	// Wait for certificate workers to complete
	certWorkerWg.Wait()

	// Close result channel to signal collector to finish
	close(resultChannel)

	// Wait for collector to complete
	collectorWg.Wait()

	// Update final statistics
	stats.UpdateDuration()

	// Set final stats for JSON writer
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	cp.logger.Printf("Concurrent batch processing completed: %s", stats.String())
	return nil
}

// WorkerResult represents the result of processing a certificate entry
type WorkerResult struct {
	Mappings []DomainMapping
	Error    error
	WorkerID int
}

// fileWorker processes files from the file task channel and sends entries to the entry channel
func (cp *ConcurrentProcessor) fileWorker(ctx context.Context, workerID int, fileTaskChannel <-chan string, entryChannel chan<- *CertificateEntry, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case filePath, ok := <-fileTaskChannel:
			if !ok {
				// Channel closed, exit worker
				return
			}

			cp.logger.Printf("File worker %d processing: %s", workerID, filePath)

			// Process file lines and send entries to the shared entry channel
			err := cp.fileHandler.ProcessFileLines(filePath, func(entry *CertificateEntry) error {
				select {
				case entryChannel <- entry:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})

			if err != nil {
				cp.logger.Printf("File worker %d error processing %s: %v", workerID, filePath, err)
				// Continue with other files
			} else {
				cp.logger.Printf("File worker %d completed: %s", workerID, filePath)
			}

		case <-ctx.Done():
			// Context cancelled, exit worker
			return
		}
	}
}

// worker processes certificate entries from the entry channel
func (cp *ConcurrentProcessor) worker(ctx context.Context, workerID int, entryChannel <-chan *CertificateEntry, resultChannel chan<- WorkerResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case entry, ok := <-entryChannel:
			if !ok {
				// Channel closed, worker should exit
				return
			}

			// Process the certificate entry
			mappings, err := cp.certParser.ProcessCertificateWithRecovery(entry)

			result := WorkerResult{
				Mappings: mappings,
				Error:    err,
				WorkerID: workerID,
			}

			select {
			case resultChannel <- result:
				// Result sent successfully
			case <-ctx.Done():
				// Context cancelled, exit
				return
			}

		case <-ctx.Done():
			// Context cancelled, exit
			return
		}
	}
}

// resultCollector collects results from workers and writes them to the output
func (cp *ConcurrentProcessor) resultCollector(ctx context.Context, resultChannel <-chan WorkerResult, writer OutputWriter, stats *ProcessingStats, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case result, ok := <-resultChannel:
			if !ok {
				// Channel closed, collector should exit
				return
			}

			stats.AddProcessed()

			if result.Error != nil {
				stats.AddError()
				cp.logger.Printf("Warning: Worker %d failed to process certificate: %v",
					result.WorkerID, result.Error)
				continue
			}

			if len(result.Mappings) > 0 {
				if err := writer.WriteBatch(result.Mappings); err != nil {
					cp.logger.Printf("Error: Failed to write mappings from worker %d: %v",
						result.WorkerID, err)
					stats.AddError()
					continue
				}
				stats.AddSuccess()
			} else {
				// No mappings found (e.g., no organization)
				stats.AddSuccess()
			}

		case <-ctx.Done():
			// Context cancelled, exit
			return
		}
	}
}

// WorkerPool manages a pool of workers for processing certificates
type WorkerPool struct {
	workerCount   int
	batchSize     int
	entryChannel  chan *CertificateEntry
	resultChannel chan WorkerResult
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	processor     *ConcurrentProcessor
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(processor *ConcurrentProcessor, workerCount, batchSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount:   workerCount,
		batchSize:     batchSize,
		entryChannel:  make(chan *CertificateEntry, batchSize),
		resultChannel: make(chan WorkerResult, batchSize),
		ctx:           ctx,
		cancel:        cancel,
		processor:     processor,
	}
}

// Start starts the worker pool
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.processor.worker(wp.ctx, i, wp.entryChannel, wp.resultChannel, &wp.wg)
	}
}

// Submit submits a certificate entry for processing
func (wp *WorkerPool) Submit(entry *CertificateEntry) error {
	select {
	case wp.entryChannel <- entry:
		return nil
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	default:
		// Channel is full, implement backpressure
		return fmt.Errorf("worker pool is at capacity, try again later")
	}
}

// Results returns the result channel for collecting processed results
func (wp *WorkerPool) Results() <-chan WorkerResult {
	return wp.resultChannel
}

// Stop stops the worker pool and waits for all workers to finish
func (wp *WorkerPool) Stop() {
	close(wp.entryChannel)
	wp.wg.Wait()
	close(wp.resultChannel)
	wp.cancel()
}

// BackpressureController manages backpressure to prevent resource exhaustion
type BackpressureController struct {
	maxQueueSize     int
	currentQueueSize int
	mutex            sync.RWMutex
	logger           *log.Logger
}

// NewBackpressureController creates a new backpressure controller
func NewBackpressureController(maxQueueSize int, logger *log.Logger) *BackpressureController {
	if logger == nil {
		logger = log.Default()
	}

	return &BackpressureController{
		maxQueueSize: maxQueueSize,
		logger:       logger,
	}
}

// CanAccept checks if the system can accept more work
func (bc *BackpressureController) CanAccept() bool {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return bc.currentQueueSize < bc.maxQueueSize
}

// AddWork increments the current queue size
func (bc *BackpressureController) AddWork() error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.currentQueueSize >= bc.maxQueueSize {
		return fmt.Errorf("queue is at maximum capacity (%d)", bc.maxQueueSize)
	}

	bc.currentQueueSize++
	return nil
}

// CompleteWork decrements the current queue size
func (bc *BackpressureController) CompleteWork() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if bc.currentQueueSize > 0 {
		bc.currentQueueSize--
	}
}

// GetQueueSize returns the current queue size
func (bc *BackpressureController) GetQueueSize() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return bc.currentQueueSize
}

// GetUtilization returns the queue utilization as a percentage
func (bc *BackpressureController) GetUtilization() float64 {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return float64(bc.currentQueueSize) / float64(bc.maxQueueSize) * 100.0
}
