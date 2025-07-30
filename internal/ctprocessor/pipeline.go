package ctprocessor

import (
	"fmt"
	"log"
	"os"
	"time"
)

// StreamingProcessor implements the main processing pipeline
type StreamingProcessor struct {
	fileHandler *FileHandler
	certParser  *CertificateParser
	factory     *OutputWriterFactory
	logger      *log.Logger
}

// NewStreamingProcessor creates a new streaming processor
func NewStreamingProcessor(logger *log.Logger) *StreamingProcessor {
	if logger == nil {
		logger = log.Default()
	}

	return &StreamingProcessor{
		fileHandler: NewFileHandler(),
		certParser:  NewCertificateParser(),
		factory:     NewOutputWriterFactory(),
		logger:      logger,
	}
}

// ProcessFile processes a single gzip file and outputs the results
func (sp *StreamingProcessor) ProcessFile(config Config) error {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	sp.logger.Printf("Starting processing: %s -> %s (format: %s)", 
		config.InputPath, config.OutputPath, config.OutputFormat)

	// Initialize statistics
	stats := &ProcessingStats{
		StartTime: time.Now(),
	}

	// Create output writer
	writer, err := sp.factory.CreateWriter(config.OutputPath, config.OutputFormat)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %v", err)
	}
	defer writer.Close()

	// Set stats for JSON writer if applicable
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	// Process the file line by line
	err = sp.fileHandler.ProcessFileLines(config.InputPath, func(entry *CertificateEntry) error {
		stats.AddProcessed()

		// Parse the certificate
		if err := sp.certParser.ParseCertificateEntry(entry); err != nil {
			stats.AddError()
			sp.logger.Printf("Warning: Failed to parse certificate at sequence %d: %v", 
				entry.SequenceNumber, err)
			return nil // Continue processing other certificates
		}

		// Extract domain mappings
		mappings, err := sp.certParser.ExtractDomainMappings(entry.ParsedCert)
		if err != nil {
			stats.AddError()
			sp.logger.Printf("Warning: Failed to extract mappings for sequence %d: %v", 
				entry.SequenceNumber, err)
			return nil // Continue processing
		}

		// Set sequence numbers and write mappings
		for i := range mappings {
			mappings[i].SequenceNum = entry.SequenceNumber
		}

		if len(mappings) > 0 {
			if err := writer.WriteBatch(mappings); err != nil {
				return fmt.Errorf("failed to write mappings for sequence %d: %v", 
					entry.SequenceNumber, err)
			}
			stats.AddSuccess()
		} else {
			// No mappings found (e.g., no organization)
			stats.AddSuccess()
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to process file: %v", err)
	}

	// Update final statistics
	stats.UpdateDuration()

	// Set final stats for JSON writer
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	// Create compact log message with filename and file size
	fileSizeMB := float64(sp.getFileSize(config.InputPath)) / (1024 * 1024)
	sp.logger.Printf("Processed: %s (%.1f MB) - %s", 
		config.InputPath, fileSizeMB, stats.CompactFormat())
	return nil
}

// ProcessBatch processes multiple files and outputs to a single result file
func (sp *StreamingProcessor) ProcessBatch(inputPaths []string, config Config) error {
	if len(inputPaths) == 0 {
		return fmt.Errorf("no input files provided")
	}

	// Validate configuration (skip InputPath validation for batch processing)
	tempConfig := config
	tempConfig.InputPath = "temp" // Set temporary value for validation
	if err := tempConfig.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	sp.logger.Printf("Starting batch processing: %d files -> %s (format: %s)", 
		len(inputPaths), config.OutputPath, config.OutputFormat)

	// Initialize statistics
	stats := &ProcessingStats{
		StartTime: time.Now(),
	}

	// Create output writer
	writer, err := sp.factory.CreateWriter(config.OutputPath, config.OutputFormat)
	if err != nil {
		return fmt.Errorf("failed to create output writer: %v", err)
	}
	defer writer.Close()

	// Set stats for JSON writer if applicable
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	// Process each file
	for i, inputPath := range inputPaths {
		sp.logger.Printf("Processing file %d/%d: %s", i+1, len(inputPaths), inputPath)

		fileConfig := config
		fileConfig.InputPath = inputPath

		err := sp.processFileToWriter(fileConfig, writer, stats)
		if err != nil {
			sp.logger.Printf("Error processing file %s: %v", inputPath, err)
			// Continue with other files
		}
	}

	// Update final statistics
	stats.UpdateDuration()

	// Set final stats for JSON writer
	if jsonWriter, ok := writer.(*JSONWriter); ok {
		jsonWriter.SetStats(stats)
	}

	sp.logger.Printf("Batch processing completed: %s", stats.String())
	return nil
}

// processFileToWriter processes a single file and writes to the provided writer
func (sp *StreamingProcessor) processFileToWriter(config Config, writer OutputWriter, stats *ProcessingStats) error {
	return sp.fileHandler.ProcessFileLines(config.InputPath, func(entry *CertificateEntry) error {
		stats.AddProcessed()

		// Parse the certificate
		if err := sp.certParser.ParseCertificateEntry(entry); err != nil {
			stats.AddError()
			sp.logger.Printf("Warning: Failed to parse certificate at sequence %d in %s: %v", 
				entry.SequenceNumber, config.InputPath, err)
			return nil // Continue processing
		}

		// Extract domain mappings
		mappings, err := sp.certParser.ExtractDomainMappings(entry.ParsedCert)
		if err != nil {
			stats.AddError()
			sp.logger.Printf("Warning: Failed to extract mappings for sequence %d in %s: %v", 
				entry.SequenceNumber, config.InputPath, err)
			return nil // Continue processing
		}

		// Set sequence numbers and write mappings
		for i := range mappings {
			mappings[i].SequenceNum = entry.SequenceNumber
		}

		if len(mappings) > 0 {
			if err := writer.WriteBatch(mappings); err != nil {
				return fmt.Errorf("failed to write mappings for sequence %d: %v", 
					entry.SequenceNumber, err)
			}
			stats.AddSuccess()
		} else {
			// No mappings found (e.g., no organization)
			stats.AddSuccess()
		}

		return nil
	})
}

// GetFileInfo returns information about a gzip file without processing it
func (sp *StreamingProcessor) GetFileInfo(filePath string) (*FileInfo, error) {
	if err := sp.fileHandler.ValidateInputFile(filePath); err != nil {
		return nil, err
	}

	lineCount, err := sp.fileHandler.CountLines(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to count lines: %v", err)
	}

	return &FileInfo{
		Path:      filePath,
		LineCount: lineCount,
	}, nil
}

// FileInfo contains information about a processed file
type FileInfo struct {
	Path      string
	LineCount int64
}

// String returns a string representation of the file info
func (fi *FileInfo) String() string {
	return fmt.Sprintf("%s: %d certificates", fi.Path, fi.LineCount)
}

// ProgressReporter provides progress reporting functionality
type ProgressReporter struct {
	logger   *log.Logger
	interval int64
}

// NewProgressReporter creates a new progress reporter
func NewProgressReporter(logger *log.Logger, interval int64) *ProgressReporter {
	if logger == nil {
		logger = log.Default()
	}
	if interval <= 0 {
		interval = 100000
	}

	return &ProgressReporter{
		logger:   logger,
		interval: interval,
	}
}

// Report reports progress if the count is at an interval
func (pr *ProgressReporter) Report(stats *ProcessingStats) {
	if stats.TotalProcessed%pr.interval == 0 {
		pr.logger.Printf("Progress: %s", stats.String())
	}
}

// ReportFinal reports final statistics
func (pr *ProgressReporter) ReportFinal(stats *ProcessingStats) {
	pr.logger.Printf("Final: %s", stats.String())
}

// getFileSize returns the size of a file in bytes, or 0 if error
func (sp *StreamingProcessor) getFileSize(filePath string) int64 {
	if info, err := os.Stat(filePath); err == nil {
		return info.Size()
	}
	return 0
}