package ctprocessor

import (
	"strings"
	"testing"
	"time"
)

func TestS3Object_Validate(t *testing.T) {
	tests := []struct {
		name    string
		s3obj   S3Object
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid S3Object",
			s3obj: S3Object{
				Key:          "test/file.gz",
				Size:         1024,
				ETag:         "abc123",
				LastModified: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "empty key",
			s3obj: S3Object{
				Key:          "",
				Size:         1024,
				ETag:         "abc123",
				LastModified: time.Now(),
			},
			wantErr: true,
			errMsg:  "S3 object key cannot be empty",
		},
		{
			name: "whitespace only key",
			s3obj: S3Object{
				Key:          "   ",
				Size:         1024,
				ETag:         "abc123",
				LastModified: time.Now(),
			},
			wantErr: true,
			errMsg:  "S3 object key cannot be empty",
		},
		{
			name: "negative size",
			s3obj: S3Object{
				Key:          "test/file.gz",
				Size:         -1,
				ETag:         "abc123",
				LastModified: time.Now(),
			},
			wantErr: true,
			errMsg:  "S3 object size must be non-negative",
		},
		{
			name: "empty ETag",
			s3obj: S3Object{
				Key:          "test/file.gz",
				Size:         1024,
				ETag:         "",
				LastModified: time.Now(),
			},
			wantErr: true,
			errMsg:  "S3 object ETag cannot be empty",
		},
		{
			name: "zero LastModified",
			s3obj: S3Object{
				Key:          "test/file.gz",
				Size:         1024,
				ETag:         "abc123",
				LastModified: time.Time{},
			},
			wantErr: true,
			errMsg:  "S3 object LastModified cannot be zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.s3obj.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("S3Object.Validate() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("S3Object.Validate() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("S3Object.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestS3Config_Validate(t *testing.T) {
	validProcessorConfig := Config{
		InputPath:    "/tmp/input",
		OutputPath:   "/tmp/output",
		OutputFormat: "json",
		WorkerCount:  4,
		BatchSize:    1000,
		LogLevel:     "info",
	}

	tests := []struct {
		name    string
		config  S3Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid S3Config",
			config: S3Config{
				Bucket:                 "test-bucket",
				Prefix:                 "test/",
				Region:                 "us-east-1",
				WorkerCount:            3,
				RetryLimit:             10,
				TimeoutSeconds:         300,
				CacheDir:               "/tmp/cache",
				CleanupAfterProcessing: true,
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					CleanupAfterProcessing:  true,
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig:        validProcessorConfig,
				StateFilePath:          "/tmp/state.json",
			},
			wantErr: false,
		},
		{
			name: "empty bucket",
			config: S3Config{
				Bucket:          "",
				Region:          "us-east-1",
				WorkerCount:     3,
				RetryLimit:      10,
				TimeoutSeconds:  300,
				CacheDir:        "/tmp/cache",
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "S3 bucket name is required",
		},
		{
			name: "empty region",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "",
				WorkerCount:     3,
				RetryLimit:      10,
				TimeoutSeconds:  300,
				CacheDir:        "/tmp/cache",
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "S3 region is required",
		},
		{
			name: "zero worker count",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				WorkerCount:     0,
				RetryLimit:      10,
				TimeoutSeconds:  300,
				CacheDir:        "/tmp/cache",
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "worker count must be greater than 0",
		},
		{
			name: "negative retry limit",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				WorkerCount:     3,
				RetryLimit:      -1,
				TimeoutSeconds:  300,
				CacheDir:        "/tmp/cache",
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "retry limit must be non-negative",
		},
		{
			name: "zero timeout",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				WorkerCount:     3,
				RetryLimit:      10,
				TimeoutSeconds:  0,
				CacheDir:        "/tmp/cache",
				FileManagerConfig: FileManagerConfig{
					CacheDir:                "/tmp/cache",
					MaxCacheAge:             24 * time.Hour,
					DiskSpaceThresholdBytes: 1024 * 1024 * 1024,
					DiskSpaceCheckInterval:  5 * time.Minute,
				},
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "timeout seconds must be greater than 0",
		},
		{
			name: "empty cache dir",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				WorkerCount:     3,
				RetryLimit:      10,
				TimeoutSeconds:  300,
				CacheDir:        "",
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "cache directory is required",
		},
		{
			name: "empty state file path",
			config: S3Config{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				WorkerCount:     3,
				RetryLimit:      10,
				TimeoutSeconds:  300,
				CacheDir:        "/tmp/cache",
				ProcessorConfig: validProcessorConfig,
				StateFilePath:   "",
			},
			wantErr: true,
			errMsg:  "state file path is required",
		},
		{
			name: "invalid processor config",
			config: S3Config{
				Bucket:         "test-bucket",
				Region:         "us-east-1",
				WorkerCount:    3,
				RetryLimit:     10,
				TimeoutSeconds: 300,
				CacheDir:       "/tmp/cache",
				ProcessorConfig: Config{
					InputPath:    "", // Invalid - empty input path
					OutputPath:   "/tmp/output",
					OutputFormat: "json",
				},
				StateFilePath: "/tmp/state.json",
			},
			wantErr: true,
			errMsg:  "processor config validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("S3Config.Validate() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("S3Config.Validate() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("S3Config.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestS3Config_SetDefaults(t *testing.T) {
	config := &S3Config{}
	config.SetDefaults()

	// Check S3-specific defaults
	if config.Region != "us-east-1" {
		t.Errorf("Expected default region 'us-east-1', got %s", config.Region)
	}

	if config.WorkerCount != 3 {
		t.Errorf("Expected default worker count 3, got %d", config.WorkerCount)
	}

	if config.RetryLimit != 10 {
		t.Errorf("Expected default retry limit 10, got %d", config.RetryLimit)
	}

	if config.TimeoutSeconds != 300 {
		t.Errorf("Expected default timeout 300 seconds, got %d", config.TimeoutSeconds)
	}

	if config.CacheDir != "./cache" {
		t.Errorf("Expected default cache dir './cache', got %s", config.CacheDir)
	}

	if config.StateFilePath != "./s3_processing_state.json" {
		t.Errorf("Expected default state file path './s3_processing_state.json', got %s", config.StateFilePath)
	}

	// Check that processor config defaults are also set
	if config.ProcessorConfig.OutputFormat != "json" {
		t.Errorf("Expected default processor output format 'json', got %s", config.ProcessorConfig.OutputFormat)
	}

	if config.ProcessorConfig.WorkerCount != 4 {
		t.Errorf("Expected default processor worker count 4, got %d", config.ProcessorConfig.WorkerCount)
	}
}

func TestS3ProcessingStats_Methods(t *testing.T) {
	stats := &S3ProcessingStats{}

	// Test increment methods
	stats.AddTotalFile()
	stats.AddTotalFile()
	stats.AddProcessedFile()
	stats.AddFailedFile()
	stats.AddSkippedFile()

	if stats.TotalFiles != 2 {
		t.Errorf("Expected TotalFiles 2, got %d", stats.TotalFiles)
	}

	if stats.ProcessedFiles != 1 {
		t.Errorf("Expected ProcessedFiles 1, got %d", stats.ProcessedFiles)
	}

	if stats.FailedFiles != 1 {
		t.Errorf("Expected FailedFiles 1, got %d", stats.FailedFiles)
	}

	if stats.SkippedFiles != 1 {
		t.Errorf("Expected SkippedFiles 1, got %d", stats.SkippedFiles)
	}

	// Test time updates
	stats.UpdateDownloadTime(time.Second * 10)
	stats.UpdateProcessTime(time.Second * 5)

	if stats.TotalDownloadTime != time.Second*10 {
		t.Errorf("Expected TotalDownloadTime 10s, got %v", stats.TotalDownloadTime)
	}

	if stats.TotalProcessTime != time.Second*5 {
		t.Errorf("Expected TotalProcessTime 5s, got %v", stats.TotalProcessTime)
	}

	// Test calculations
	stats.CalculateAverageFileSize(2048) // 2048 bytes / 1 processed file = 2048
	if stats.AverageFileSize != 2048 {
		t.Errorf("Expected AverageFileSize 2048, got %d", stats.AverageFileSize)
	}

	stats.CalculateDownloadThroughput(10485760) // 10MB in 10 seconds = 1 MB/s
	expectedThroughput := 1.0                   // 10MB / 10s = 1 MB/s
	if stats.DownloadThroughput != expectedThroughput {
		t.Errorf("Expected DownloadThroughput %.2f MB/s, got %.2f MB/s", expectedThroughput, stats.DownloadThroughput)
	}

	// Test rates
	successRate := stats.SuccessRate()
	expectedSuccessRate := 50.0 // 1 processed / 2 total = 50%
	if successRate != expectedSuccessRate {
		t.Errorf("Expected SuccessRate %.2f%%, got %.2f%%", expectedSuccessRate, successRate)
	}

	failureRate := stats.FailureRate()
	expectedFailureRate := 50.0 // 1 failed / 2 total = 50%
	if failureRate != expectedFailureRate {
		t.Errorf("Expected FailureRate %.2f%%, got %.2f%%", expectedFailureRate, failureRate)
	}
}

func TestS3ProcessingStats_ZeroValues(t *testing.T) {
	stats := &S3ProcessingStats{}

	// Test rates with zero total files
	successRate := stats.SuccessRate()
	if successRate != 0.0 {
		t.Errorf("Expected SuccessRate 0.0%% with zero total files, got %.2f%%", successRate)
	}

	failureRate := stats.FailureRate()
	if failureRate != 0.0 {
		t.Errorf("Expected FailureRate 0.0%% with zero total files, got %.2f%%", failureRate)
	}

	// Test average file size with zero processed files
	stats.CalculateAverageFileSize(1024)
	if stats.AverageFileSize != 0 {
		t.Errorf("Expected AverageFileSize 0 with zero processed files, got %d", stats.AverageFileSize)
	}

	// Test throughput with zero download time
	stats.CalculateDownloadThroughput(1024)
	if stats.DownloadThroughput != 0.0 {
		t.Errorf("Expected DownloadThroughput 0.0 MB/s with zero download time, got %.2f MB/s", stats.DownloadThroughput)
	}
}

func TestS3Object_String(t *testing.T) {
	testTime := time.Date(2023, 12, 25, 10, 30, 0, 0, time.UTC)
	s3obj := S3Object{
		Key:          "test/file.gz",
		Size:         1024,
		ETag:         "abc123",
		LastModified: testTime,
	}

	result := s3obj.String()
	expected := "S3Object{Key: test/file.gz, Size: 1024, ETag: abc123, LastModified: 2023-12-25T10:30:00Z}"

	if result != expected {
		t.Errorf("S3Object.String() = %v, want %v", result, expected)
	}
}

func TestS3Config_String(t *testing.T) {
	config := S3Config{
		Bucket:      "test-bucket",
		Prefix:      "test/",
		Region:      "us-east-1",
		WorkerCount: 3,
		RetryLimit:  10,
		CacheDir:    "/tmp/cache",
	}

	result := config.String()
	expected := "S3Config{Bucket: test-bucket, Prefix: test/, Region: us-east-1, Workers: 3, RetryLimit: 10, CacheDir: /tmp/cache}"

	if result != expected {
		t.Errorf("S3Config.String() = %v, want %v", result, expected)
	}
}