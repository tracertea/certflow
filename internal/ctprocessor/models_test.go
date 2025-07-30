package ctprocessor

import (
	"testing"
	"time"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputPath:   "/path/to/output.json",
				OutputFormat: "json",
				WorkerCount:  4,
				BatchSize:    1000,
				LogLevel:     "info",
			},
			wantErr: false,
		},
		{
			name: "missing input path",
			config: Config{
				OutputPath:   "/path/to/output.json",
				OutputFormat: "json",
				WorkerCount:  4,
				BatchSize:    1000,
			},
			wantErr: true,
			errMsg:  "input path is required",
		},
		{
			name: "missing output path",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputFormat: "json",
				WorkerCount:  4,
				BatchSize:    1000,
			},
			wantErr: true,
			errMsg:  "output path is required",
		},
		{
			name: "invalid output format",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputPath:   "/path/to/output.json",
				OutputFormat: "xml",
				WorkerCount:  4,
				BatchSize:    1000,
			},
			wantErr: true,
			errMsg:  "output format must be one of: json, jsonl, csv, txt",
		},
		{
			name: "invalid worker count",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputPath:   "/path/to/output.json",
				OutputFormat: "json",
				WorkerCount:  0,
				BatchSize:    1000,
			},
			wantErr: true,
			errMsg:  "worker count must be greater than 0",
		},
		{
			name: "invalid batch size",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputPath:   "/path/to/output.json",
				OutputFormat: "json",
				WorkerCount:  4,
				BatchSize:    -1,
			},
			wantErr: true,
			errMsg:  "batch size must be greater than 0",
		},
		{
			name: "invalid log level",
			config: Config{
				InputPath:    "/path/to/input.gz",
				OutputPath:   "/path/to/output.json",
				OutputFormat: "json",
				WorkerCount:  4,
				BatchSize:    1000,
				LogLevel:     "invalid",
			},
			wantErr: true,
			errMsg:  "log level must be one of: debug, info, warn, error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Config.Validate() expected error but got none")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("Config.Validate() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Config.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	config := &Config{}
	config.SetDefaults()

	if config.OutputFormat != "json" {
		t.Errorf("Expected default OutputFormat to be 'json', got %s", config.OutputFormat)
	}
	if config.WorkerCount != 4 {
		t.Errorf("Expected default WorkerCount to be 4, got %d", config.WorkerCount)
	}
	if config.BatchSize != 1000 {
		t.Errorf("Expected default BatchSize to be 1000, got %d", config.BatchSize)
	}
	if config.LogLevel != "info" {
		t.Errorf("Expected default LogLevel to be 'info', got %s", config.LogLevel)
	}
}

func TestCertificateEntry_Validate(t *testing.T) {
	tests := []struct {
		name    string
		entry   CertificateEntry
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid entry",
			entry: CertificateEntry{
				SequenceNumber:  123,
				CertificateData: "MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			},
			wantErr: false,
		},
		{
			name: "negative sequence number",
			entry: CertificateEntry{
				SequenceNumber:  -1,
				CertificateData: "MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			},
			wantErr: true,
			errMsg:  "sequence number must be non-negative",
		},
		{
			name: "empty certificate data",
			entry: CertificateEntry{
				SequenceNumber:  123,
				CertificateData: "",
			},
			wantErr: true,
			errMsg:  "certificate data cannot be empty",
		},
		{
			name: "whitespace only certificate data",
			entry: CertificateEntry{
				SequenceNumber:  123,
				CertificateData: "   \t\n  ",
			},
			wantErr: true,
			errMsg:  "certificate data cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("CertificateEntry.Validate() expected error but got none")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("CertificateEntry.Validate() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("CertificateEntry.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestDomainMapping_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mapping DomainMapping
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid CN mapping",
			mapping: DomainMapping{
				Name:         "example.com",
				Organization: "Example Corp",
				Source:       "CN",
				SequenceNum:  123,
			},
			wantErr: false,
		},
		{
			name: "valid SAN mapping",
			mapping: DomainMapping{
				Name:         "api.example.com",
				Organization: "Example Corp",
				Source:       "SAN",
				SequenceNum:  456,
			},
			wantErr: false,
		},
		{
			name: "empty name",
			mapping: DomainMapping{
				Name:         "",
				Organization: "Example Corp",
				Source:       "CN",
				SequenceNum:  123,
			},
			wantErr: true,
			errMsg:  "name cannot be empty",
		},
		{
			name: "whitespace only name",
			mapping: DomainMapping{
				Name:         "   \t  ",
				Organization: "Example Corp",
				Source:       "CN",
				SequenceNum:  123,
			},
			wantErr: true,
			errMsg:  "name cannot be empty",
		},
		{
			name: "empty organization",
			mapping: DomainMapping{
				Name:         "example.com",
				Organization: "",
				Source:       "CN",
				SequenceNum:  123,
			},
			wantErr: true,
			errMsg:  "organization cannot be empty",
		},
		{
			name: "invalid source",
			mapping: DomainMapping{
				Name:         "example.com",
				Organization: "Example Corp",
				Source:       "INVALID",
				SequenceNum:  123,
			},
			wantErr: true,
			errMsg:  "source must be either 'CN' or 'SAN'",
		},
		{
			name: "negative sequence number",
			mapping: DomainMapping{
				Name:         "example.com",
				Organization: "Example Corp",
				Source:       "CN",
				SequenceNum:  -1,
			},
			wantErr: true,
			errMsg:  "sequence number must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mapping.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("DomainMapping.Validate() expected error but got none")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("DomainMapping.Validate() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("DomainMapping.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestDomainMapping_String(t *testing.T) {
	mapping := DomainMapping{
		Name:         "example.com",
		Organization: "Example Corp",
		Source:       "CN",
		SequenceNum:  123,
	}

	expected := "example.com -> Example Corp (CN)"
	result := mapping.String()

	if result != expected {
		t.Errorf("DomainMapping.String() = %v, want %v", result, expected)
	}
}

func TestProcessingStats_Methods(t *testing.T) {
	stats := &ProcessingStats{
		StartTime: time.Now(),
	}

	// Test initial state
	if stats.TotalProcessed != 0 {
		t.Errorf("Expected initial TotalProcessed to be 0, got %d", stats.TotalProcessed)
	}
	if stats.SuccessfulParsed != 0 {
		t.Errorf("Expected initial SuccessfulParsed to be 0, got %d", stats.SuccessfulParsed)
	}
	if stats.Errors != 0 {
		t.Errorf("Expected initial Errors to be 0, got %d", stats.Errors)
	}

	// Test adding processed
	stats.AddProcessed()
	stats.AddProcessed()
	if stats.TotalProcessed != 2 {
		t.Errorf("Expected TotalProcessed to be 2, got %d", stats.TotalProcessed)
	}

	// Test adding success
	stats.AddSuccess()
	if stats.SuccessfulParsed != 1 {
		t.Errorf("Expected SuccessfulParsed to be 1, got %d", stats.SuccessfulParsed)
	}

	// Test adding error
	stats.AddError()
	if stats.Errors != 1 {
		t.Errorf("Expected Errors to be 1, got %d", stats.Errors)
	}

	// Test success rate
	expectedSuccessRate := 50.0 // 1 success out of 2 total
	if rate := stats.SuccessRate(); rate != expectedSuccessRate {
		t.Errorf("Expected SuccessRate to be %.2f, got %.2f", expectedSuccessRate, rate)
	}

	// Test error rate
	expectedErrorRate := 50.0 // 1 error out of 2 total
	if rate := stats.ErrorRate(); rate != expectedErrorRate {
		t.Errorf("Expected ErrorRate to be %.2f, got %.2f", expectedErrorRate, rate)
	}

	// Test update duration
	time.Sleep(10 * time.Millisecond) // Small delay to ensure duration > 0
	stats.UpdateDuration()
	if stats.Duration <= 0 {
		t.Errorf("Expected Duration to be greater than 0, got %v", stats.Duration)
	}

	// Test string representation
	str := stats.String()
	if str == "" {
		t.Errorf("Expected non-empty string representation")
	}
}

func TestProcessingStats_ZeroDivision(t *testing.T) {
	stats := &ProcessingStats{}

	// Test rates with zero total processed
	if rate := stats.SuccessRate(); rate != 0.0 {
		t.Errorf("Expected SuccessRate to be 0.0 with zero total, got %.2f", rate)
	}
	if rate := stats.ErrorRate(); rate != 0.0 {
		t.Errorf("Expected ErrorRate to be 0.0 with zero total, got %.2f", rate)
	}
}

func TestNewProcessingState(t *testing.T) {
	state := NewProcessingState()
	
	if state == nil {
		t.Fatal("Expected non-nil ProcessingState")
	}
	
	if state.ProcessedFiles == nil {
		t.Error("Expected ProcessedFiles map to be initialized")
	}
	
	if state.FailedFiles == nil {
		t.Error("Expected FailedFiles map to be initialized")
	}
	
	if state.Version != "1.0" {
		t.Errorf("Expected Version to be '1.0', got '%s'", state.Version)
	}
	
	if state.LastUpdated.IsZero() {
		t.Error("Expected LastUpdated to be set")
	}
	
	if len(state.ProcessedFiles) != 0 {
		t.Error("Expected ProcessedFiles map to be empty initially")
	}
	
	if len(state.FailedFiles) != 0 {
		t.Error("Expected FailedFiles map to be empty initially")
	}
}

func TestProcessingState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		state   ProcessingState
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid state",
			state: ProcessingState{
				ProcessedFiles: make(map[string]FileState),
				FailedFiles:    make(map[string]FileState),
				LastUpdated:    time.Now(),
				Version:        "1.0",
			},
			wantErr: false,
		},
		{
			name: "nil processed files",
			state: ProcessingState{
				ProcessedFiles: nil,
				FailedFiles:    make(map[string]FileState),
				LastUpdated:    time.Now(),
				Version:        "1.0",
			},
			wantErr: true,
			errMsg:  "processed_files map cannot be nil",
		},
		{
			name: "nil failed files",
			state: ProcessingState{
				ProcessedFiles: make(map[string]FileState),
				FailedFiles:    nil,
				LastUpdated:    time.Now(),
				Version:        "1.0",
			},
			wantErr: true,
			errMsg:  "failed_files map cannot be nil",
		},
		{
			name: "empty version",
			state: ProcessingState{
				ProcessedFiles: make(map[string]FileState),
				FailedFiles:    make(map[string]FileState),
				LastUpdated:    time.Now(),
				Version:        "",
			},
			wantErr: true,
			errMsg:  "version cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.state.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ProcessingState.Validate() expected error but got none")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("ProcessingState.Validate() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ProcessingState.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestProcessingState_GetCounts(t *testing.T) {
	state := NewProcessingState()
	
	// Initially should have zero counts
	if count := state.GetProcessedCount(); count != 0 {
		t.Errorf("Expected initial processed count to be 0, got %d", count)
	}
	
	if count := state.GetFailedCount(); count != 0 {
		t.Errorf("Expected initial failed count to be 0, got %d", count)
	}
	
	// Add some files
	state.ProcessedFiles["key1"] = FileState{
		Key:         "key1",
		ETag:        "etag1",
		Size:        1024,
		ProcessedAt: time.Now(),
	}
	
	state.ProcessedFiles["key2"] = FileState{
		Key:         "key2",
		ETag:        "etag2",
		Size:        2048,
		ProcessedAt: time.Now(),
	}
	
	state.FailedFiles["key3"] = FileState{
		Key:         "key3",
		ETag:        "etag3",
		Size:        0,
		ProcessedAt: time.Now(),
		ErrorMsg:    "test error",
	}
	
	// Check counts
	if count := state.GetProcessedCount(); count != 2 {
		t.Errorf("Expected processed count to be 2, got %d", count)
	}
	
	if count := state.GetFailedCount(); count != 1 {
		t.Errorf("Expected failed count to be 1, got %d", count)
	}
}

func TestFileState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		state   FileState
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid file state",
			state: FileState{
				Key:         "test-key",
				ETag:        "test-etag",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "valid file state with error message",
			state: FileState{
				Key:         "test-key",
				ETag:        "test-etag",
				Size:        0,
				ProcessedAt: time.Now(),
				ErrorMsg:    "test error",
			},
			wantErr: false,
		},
		{
			name: "empty key",
			state: FileState{
				Key:         "",
				ETag:        "test-etag",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
			wantErr: true,
			errMsg:  "key cannot be empty",
		},
		{
			name: "whitespace only key",
			state: FileState{
				Key:         "   \t  ",
				ETag:        "test-etag",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
			wantErr: true,
			errMsg:  "key cannot be empty",
		},
		{
			name: "empty etag",
			state: FileState{
				Key:         "test-key",
				ETag:        "",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
			wantErr: true,
			errMsg:  "etag cannot be empty",
		},
		{
			name: "whitespace only etag",
			state: FileState{
				Key:         "test-key",
				ETag:        "   \t  ",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
			wantErr: true,
			errMsg:  "etag cannot be empty",
		},
		{
			name: "negative size",
			state: FileState{
				Key:         "test-key",
				ETag:        "test-etag",
				Size:        -1,
				ProcessedAt: time.Now(),
			},
			wantErr: true,
			errMsg:  "size must be non-negative",
		},
		{
			name: "zero processed at time",
			state: FileState{
				Key:         "test-key",
				ETag:        "test-etag",
				Size:        1024,
				ProcessedAt: time.Time{},
			},
			wantErr: true,
			errMsg:  "processed_at cannot be zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.state.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("FileState.Validate() expected error but got none")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("FileState.Validate() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("FileState.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}