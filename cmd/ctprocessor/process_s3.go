package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tracertea/certflow/internal/ctprocessor"
	"github.com/spf13/cobra"
)

var (
	// S3-specific flags
	s3Bucket                 string
	s3Prefix                 string
	s3Region                 string
	s3Profile                string
	s3Workers                int
	s3ProcessingWorkers      int
	s3RetryLimit             int
	s3TimeoutSeconds         int
	s3CacheDir               string
	s3CleanupAfterProcess    bool
	s3DiskThresholdGB        float64
	s3StateFile              string
	s3MaxConcurrentDownloads int

	// Processing configuration flags
	s3OutputPath   string
	s3OutputFormat string
	s3BufferSize   int
	s3Verbose      bool
)

// processS3Cmd represents the process-s3 command
var processS3Cmd = &cobra.Command{
	Use:   "process-s3",
	Short: "Process certificate transparency data directly from AWS S3",
	Long: `Process certificate transparency data files stored in AWS S3 buckets.

This command discovers, downloads, and processes CT data files from S3,
maintaining state to enable resumable processing and avoid duplicates.

Examples:
  # Process all .gz files from an S3 bucket
  ctprocessor process-s3 --bucket my-ct-bucket --output output.json

  # Process files with a specific prefix
  ctprocessor process-s3 --bucket my-ct-bucket --prefix 2024/01/ --output january.csv

  # Use a specific AWS profile and region
  ctprocessor process-s3 --bucket my-ct-bucket --profile production --region us-west-2

  # Process and delete files to minimize disk usage
  ctprocessor process-s3 --bucket my-ct-bucket --cleanup

  # Resume processing from previous state
  ctprocessor process-s3 --bucket my-ct-bucket --state-file ./processing-state.json`,
	RunE: runProcessS3,
}

func init() {
	rootCmd.AddCommand(processS3Cmd)

	processS3Cmd.Flags().StringVar(&s3Bucket, "bucket", "", "S3 bucket name (required)")
	processS3Cmd.Flags().StringVar(&s3Prefix, "prefix", "", "S3 prefix to filter objects")
	processS3Cmd.Flags().StringVar(&s3Region, "region", "us-east-1", "AWS region")
	processS3Cmd.Flags().StringVar(&s3Profile, "profile", "", "AWS profile to use")

	processS3Cmd.Flags().IntVar(&s3Workers, "workers", 6, "Number of parallel download workers")
	processS3Cmd.Flags().IntVar(&s3ProcessingWorkers, "processing-workers", 8, "Number of parallel processing workers")
	processS3Cmd.Flags().IntVar(&s3RetryLimit, "retry-limit", 10, "Maximum download retry attempts")
	processS3Cmd.Flags().IntVar(&s3TimeoutSeconds, "timeout", 300, "Download timeout in seconds")
	processS3Cmd.Flags().IntVar(&s3MaxConcurrentDownloads, "max-downloads", 6, "Maximum concurrent downloads")

	processS3Cmd.Flags().StringVar(&s3CacheDir, "cache-dir", "./s3-cache", "Local cache directory for downloaded files")
	processS3Cmd.Flags().BoolVar(&s3CleanupAfterProcess, "cleanup", false, "Delete files after successful processing")
	processS3Cmd.Flags().Float64Var(&s3DiskThresholdGB, "disk-threshold", 1.0, "Minimum free disk space in GB")

	processS3Cmd.Flags().StringVar(&s3StateFile, "state-file", "", "State file path for resumable processing")

	processS3Cmd.Flags().StringVarP(&s3OutputPath, "output", "o", "", "Output file path (required)")
	processS3Cmd.Flags().StringVarP(&s3OutputFormat, "format", "f", "json", "Output format: json, jsonl, csv, or txt")
	processS3Cmd.Flags().IntVar(&s3BufferSize, "buffer-size", 10000, "Size of the processing buffer")
	processS3Cmd.Flags().BoolVarP(&s3Verbose, "verbose", "v", false, "Enable verbose logging")

	processS3Cmd.MarkFlagRequired("bucket")
	processS3Cmd.MarkFlagRequired("output")
}

func runProcessS3(cmd *cobra.Command, args []string) error {
	if s3Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if s3OutputPath == "" {
		return fmt.Errorf("output path is required")
	}

	s3Config := ctprocessor.S3Config{
		Bucket:  s3Bucket,
		Prefix:  s3Prefix,
		Region:  s3Region,
		Profile: s3Profile,

		WorkerCount:            s3Workers,
		ProcessingWorkerCount:  s3ProcessingWorkers,
		RetryLimit:             s3RetryLimit,
		TimeoutSeconds:         s3TimeoutSeconds,
		MaxConcurrentDownloads: s3MaxConcurrentDownloads,

		CacheDir:               s3CacheDir,
		CleanupAfterProcessing: s3CleanupAfterProcess,
		DiskSpaceThresholdGB:   s3DiskThresholdGB,

		StateFilePath: s3StateFile,

		ProcessorConfig: ctprocessor.Config{
			InputPath:    "", // Will be set per file
			OutputPath:   s3OutputPath,
			OutputFormat: s3OutputFormat,
			WorkerCount:  s3Workers,
			BatchSize:    s3BufferSize,
			LogLevel:     getLogLevel(s3Verbose),
		},
	}

	// Set default state file if not provided
	if s3Config.StateFilePath == "" {
		s3Config.StateFilePath = filepath.Join(s3CacheDir, fmt.Sprintf("%s-state.json", s3Bucket))
	}

	if err := s3Config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if err := os.MkdirAll(s3CacheDir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	s3Manager, err := ctprocessor.CreateS3ManagerFromConfig(s3Config)
	if err != nil {
		return fmt.Errorf("failed to initialize S3 manager: %w", err)
	}

	if s3Verbose {
		fmt.Printf("Starting S3 processing from bucket: %s\n", s3Bucket)
		if s3Prefix != "" {
			fmt.Printf("Using prefix: %s\n", s3Prefix)
		}
		fmt.Printf("Output will be written to: %s\n", s3OutputPath)
		fmt.Printf("State file: %s\n", s3Config.StateFilePath)
	}

	if err := s3Manager.ProcessS3Bucket(s3Config); err != nil {
		return fmt.Errorf("S3 processing failed: %w", err)
	}

	stats := s3Manager.GetProcessingStats()
	displayS3Stats(stats)

	return nil
}

func getLogLevel(verbose bool) string {
	if verbose {
		return "debug"
	}
	return "info"
}

func displayS3Stats(stats ctprocessor.S3ProcessingStats) {
	fmt.Println("\n=== S3 Processing Summary ===")
	fmt.Printf("Total files discovered: %d\n", stats.TotalFiles)
	fmt.Printf("Files processed: %d\n", stats.ProcessedFiles)
	fmt.Printf("Files failed: %d\n", stats.FailedFiles)
	fmt.Printf("Files skipped (already processed): %d\n", stats.SkippedFiles)

	if stats.ProcessedFiles > 0 {
		fmt.Printf("\nDownload statistics:\n")
		fmt.Printf("  Total download time: %v\n", stats.TotalDownloadTime)
		fmt.Printf("  Average download speed: %.2f MB/s\n", stats.DownloadThroughput)

		fmt.Printf("\nProcessing statistics:\n")
		fmt.Printf("  Total processing time: %v\n", stats.TotalProcessTime)
		if stats.TotalCertificates > 0 {
			fmt.Printf("  Total certificates processed: %d\n", stats.TotalCertificates)
			fmt.Printf("  Average processing rate: %.2f certs/sec\n",
				float64(stats.TotalCertificates)/stats.TotalProcessTime.Seconds())
		}
	}

	if stats.TotalBytes > 0 {
		fmt.Printf("\nData volume:\n")
		fmt.Printf("  Total data downloaded: %.2f GB\n", float64(stats.TotalBytes)/(1024*1024*1024))
	}
}
