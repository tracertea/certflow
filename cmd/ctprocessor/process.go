package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/tracertea/certflow/internal/ctprocessor"
)

var (
	inputFile     string
	inputFiles    []string
	inputDir      string
	recursive     bool
	outputFile    string
	outputFormat  string
	workerCount   int
	batchSize     int
	logLevel      string
	maxErrors     int64
	concurrent    bool
	showProgress  bool
	configFile    string
)

var processCmd = &cobra.Command{
	Use:   "process",
	Short: "Process certificate transparency data",
	Long: `Process gzip files containing certificate transparency data and extract domain-to-organization mappings.

Examples:
  # Process single file
  ctprocessor process -i data.gz -o output.json

  # Process with multiple workers
  ctprocessor process -i data.gz -o output.json --workers 8 --concurrent

  # Process multiple files
  ctprocessor process --input-files file1.gz,file2.gz -o combined.json

  # Process directory of files
  ctprocessor process -d /path/to/ct-data/ -o combined.json

  # Process directory recursively
  ctprocessor process -d /path/to/ct-data/ -r -o combined.json --concurrent

  # Different output formats
  ctprocessor process -i data.gz -o output.csv --format csv
  ctprocessor process -i data.gz -o output.txt --format txt`,
	RunE: runProcess,
}

func runProcess(cmd *cobra.Command, args []string) error {
	cliConfig, err := LoadConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	// Override with command line flags (flags take precedence)
	if cmd.Flags().Changed("input") {
		cliConfig.Input = inputFile
	}
	if cmd.Flags().Changed("input-files") {
		cliConfig.InputFiles = inputFiles
	}
	if cmd.Flags().Changed("output") {
		cliConfig.Output = outputFile
	}
	if cmd.Flags().Changed("format") {
		cliConfig.Format = outputFormat
	}
	if cmd.Flags().Changed("workers") {
		cliConfig.Workers = workerCount
	}
	if cmd.Flags().Changed("batch") {
		cliConfig.BatchSize = batchSize
	}
	if cmd.Flags().Changed("log-level") {
		cliConfig.LogLevel = logLevel
	}
	if cmd.Flags().Changed("max-errors") {
		cliConfig.MaxErrors = maxErrors
	}
	if cmd.Flags().Changed("concurrent") {
		cliConfig.Concurrent = concurrent
	}

	// Validate CLI configuration (skip if using directory input)
	if inputDir == "" {
		if err := cliConfig.Validate(); err != nil {
			return fmt.Errorf("configuration error: %v", err)
		}
	}

	logger := createLogger(cliConfig.LogLevel)

	config := cliConfig.ToProcessorConfig()

	var inputPaths []string

	if inputDir != "" {
		// Directory mode - scan for .gz files
		inputPaths, err = scanDirectoryForGzipFiles(inputDir, recursive, logger)
		if err != nil {
			return fmt.Errorf("failed to scan directory: %v", err)
		}
		if len(inputPaths) == 0 {
			return fmt.Errorf("no .gz files found in directory: %s", inputDir)
		}
	} else if len(cliConfig.InputFiles) > 0 {
		inputPaths = cliConfig.InputFiles
	} else if cliConfig.Input != "" {
		inputPaths = []string{cliConfig.Input}
		config.InputPath = cliConfig.Input
	} else {
		return fmt.Errorf("one of --input, --input-files, or --input-dir must be specified")
	}

	// Validate processor configuration for single file mode
	if len(inputPaths) == 1 {
		if err := config.Validate(); err != nil {
			return fmt.Errorf("configuration error: %v", err)
		}
	}

	logger.Printf("Starting certificate transparency processing")
	logger.Printf("Input files: %d", len(inputPaths))
	logger.Printf("Output: %s (format: %s)", cliConfig.Output, cliConfig.Format)
	logger.Printf("Workers: %d, Batch size: %d", cliConfig.Workers, cliConfig.BatchSize)
	logger.Printf("Concurrent: %v, Max errors: %d", cliConfig.Concurrent, cliConfig.MaxErrors)

	if cliConfig.Concurrent && cliConfig.Workers > 1 {
		return runConcurrentProcessing(inputPaths, config, logger)
	} else {
		return runSequentialProcessing(inputPaths, config, logger)
	}
}

func runSequentialProcessing(inputPaths []string, config ctprocessor.Config, logger *log.Logger) error {
	sp := ctprocessor.NewStreamingProcessor(logger)

	if len(inputPaths) == 1 {
		return sp.ProcessFile(config)
	} else {
		return sp.ProcessBatch(inputPaths, config)
	}
}

func runConcurrentProcessing(inputPaths []string, config ctprocessor.Config, logger *log.Logger) error {
	cp := ctprocessor.NewConcurrentProcessor(logger)

	if len(inputPaths) == 1 {
		return cp.ProcessFileWithWorkers(config)
	} else {
		return cp.ProcessBatchWithWorkers(inputPaths, config)
	}
}

func createLogger(level string) *log.Logger {
	prefix := "CTPROCESSOR: "
	flags := log.LstdFlags

	switch strings.ToLower(level) {
	case "debug":
		flags |= log.Lshortfile
		prefix = "DEBUG: "
	case "error":
		prefix = "ERROR: "
	case "warn":
		prefix = "WARN: "
	default:
		prefix = "INFO: "
	}

	return log.New(os.Stdout, prefix, flags)
}

func init() {
	processCmd.Flags().StringVarP(&inputFile, "input", "i", "", "Input gzip file path")
	processCmd.Flags().StringSliceVar(&inputFiles, "input-files", []string{}, "Multiple input gzip files (comma-separated)")
	processCmd.Flags().StringVarP(&inputDir, "input-dir", "d", "", "Input directory containing gzip files")
	processCmd.Flags().BoolVarP(&recursive, "recursive", "r", false, "Process directories recursively")
	
	processCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Output file path (required)")
	processCmd.Flags().StringVarP(&outputFormat, "format", "f", "json", "Output format (json, jsonl, csv, txt)")
	
	processCmd.Flags().IntVarP(&workerCount, "workers", "w", 4, "Number of worker goroutines")
	processCmd.Flags().IntVarP(&batchSize, "batch", "b", 100000, "Batch size for processing")
	processCmd.Flags().BoolVar(&concurrent, "concurrent", false, "Use concurrent processing")
	
	processCmd.Flags().Int64Var(&maxErrors, "max-errors", 1000, "Maximum number of errors before stopping")
	
	processCmd.Flags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	processCmd.Flags().BoolVar(&showProgress, "progress", true, "Show progress information")
	
	processCmd.Flags().StringVar(&configFile, "config", "", "Configuration file path")
	
	processCmd.MarkFlagRequired("output")
}