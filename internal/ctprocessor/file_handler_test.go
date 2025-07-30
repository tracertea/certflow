package ctprocessor

import (
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNewFileHandler(t *testing.T) {
	fh := NewFileHandler()
	if fh == nil {
		t.Fatal("NewFileHandler() returned nil")
	}
	if fh.lineRegex == nil {
		t.Fatal("FileHandler lineRegex is nil")
	}
}

func TestFileHandler_ValidateInputFile(t *testing.T) {
	fh := NewFileHandler()

	tests := []struct {
		name     string
		filePath string
		setup    func() string // Returns actual file path to test
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "empty file path",
			filePath: "",
			wantErr:  true,
			errMsg:   "file path cannot be empty",
		},
		{
			name:     "whitespace only file path",
			filePath: "   \t  ",
			wantErr:  true,
			errMsg:   "file path cannot be empty",
		},
		{
			name:     "non-existent file",
			filePath: "/path/to/nonexistent/file.gz",
			wantErr:  true,
			errMsg:   "file does not exist",
		},
		{
			name: "directory instead of file",
			setup: func() string {
				tmpDir, _ := os.MkdirTemp("", "test_dir")
				return tmpDir
			},
			wantErr: true,
			errMsg:  "path is a directory, not a file",
		},
		{
			name: "empty file",
			setup: func() string {
				tmpFile, _ := os.CreateTemp("", "empty_test.gz")
				tmpFile.Close()
				return tmpFile.Name()
			},
			wantErr: true,
			errMsg:  "file is empty",
		},
		{
			name: "valid file",
			setup: func() string {
				tmpFile, _ := os.CreateTemp("", "valid_test.gz")
				tmpFile.WriteString("test content")
				tmpFile.Close()
				return tmpFile.Name()
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := tt.filePath
			if tt.setup != nil {
				filePath = tt.setup()
				defer os.RemoveAll(filePath) // Clean up
			}

			err := fh.ValidateInputFile(filePath)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateInputFile() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateInputFile() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateInputFile() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestFileHandler_ParseLine(t *testing.T) {
	fh := NewFileHandler()

	tests := []struct {
		name    string
		line    string
		want    *CertificateEntry
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid line",
			line: "123 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			want: &CertificateEntry{
				SequenceNumber:  123,
				CertificateData: "MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			},
			wantErr: false,
		},
		{
			name: "valid line with multiple spaces",
			line: "456   MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			want: &CertificateEntry{
				SequenceNumber:  456,
				CertificateData: "MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			},
			wantErr: false,
		},
		{
			name:    "empty line",
			line:    "",
			wantErr: true,
			errMsg:  "line is empty",
		},
		{
			name:    "whitespace only line",
			line:    "   \t  ",
			wantErr: true,
			errMsg:  "line is empty",
		},
		{
			name:    "missing certificate data",
			line:    "123",
			wantErr: true,
			errMsg:  "line does not match expected format",
		},
		{
			name:    "missing sequence number",
			line:    "MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			wantErr: true,
			errMsg:  "line does not match expected format",
		},
		{
			name:    "invalid sequence number",
			line:    "abc MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			wantErr: true,
			errMsg:  "line does not match expected format",
		},
		{
			name:    "negative sequence number",
			line:    "-1 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
			wantErr: true,
			errMsg:  "line does not match expected format",
		},
		{
			name:    "empty certificate data",
			line:    "123  ",
			wantErr: true,
			errMsg:  "certificate data is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fh.ParseLine(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseLine() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ParseLine() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ParseLine() unexpected error = %v", err)
					return
				}
				if got.SequenceNumber != tt.want.SequenceNumber {
					t.Errorf("ParseLine() SequenceNumber = %v, want %v", got.SequenceNumber, tt.want.SequenceNumber)
				}
				if got.CertificateData != tt.want.CertificateData {
					t.Errorf("ParseLine() CertificateData = %v, want %v", got.CertificateData, tt.want.CertificateData)
				}
			}
		})
	}
}

func TestFileHandler_OpenGzipFile(t *testing.T) {
	fh := NewFileHandler()

	// Create a test gzip file
	tmpFile, err := os.CreateTemp("", "test.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write gzipped content
	gzipWriter := gzip.NewWriter(tmpFile)
	testContent := "0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/\n1 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/"
	gzipWriter.Write([]byte(testContent))
	gzipWriter.Close()
	tmpFile.Close()

	// Test opening the gzip file
	scanner, file, gzipReader, err := fh.OpenGzipFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("OpenGzipFile() error = %v", err)
	}
	defer file.Close()
	defer gzipReader.Close()

	if scanner == nil {
		t.Error("OpenGzipFile() scanner is nil")
	}
	if file == nil {
		t.Error("OpenGzipFile() file is nil")
	}
	if gzipReader == nil {
		t.Error("OpenGzipFile() gzipReader is nil")
	}

	// Test reading lines
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		line := scanner.Text()
		if line == "" {
			t.Error("OpenGzipFile() read empty line")
		}
	}

	if lineCount != 2 {
		t.Errorf("OpenGzipFile() read %d lines, want 2", lineCount)
	}
}

func TestFileHandler_ProcessFileLines(t *testing.T) {
	fh := NewFileHandler()

	// Create a test gzip file
	tmpFile, err := os.CreateTemp("", "test_process.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write gzipped content
	gzipWriter := gzip.NewWriter(tmpFile)
	testContent := "0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/\n1 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/\n2 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/"
	gzipWriter.Write([]byte(testContent))
	gzipWriter.Close()
	tmpFile.Close()

	// Test processing
	var processedEntries []*CertificateEntry
	handler := func(entry *CertificateEntry) error {
		processedEntries = append(processedEntries, entry)
		return nil
	}

	err = fh.ProcessFileLines(tmpFile.Name(), handler)
	if err != nil {
		t.Fatalf("ProcessFileLines() error = %v", err)
	}

	if len(processedEntries) != 3 {
		t.Errorf("ProcessFileLines() processed %d entries, want 3", len(processedEntries))
	}

	// Check sequence numbers
	expectedSequences := []int64{0, 1, 2}
	for i, entry := range processedEntries {
		if entry.SequenceNumber != expectedSequences[i] {
			t.Errorf("ProcessFileLines() entry %d sequence = %d, want %d", i, entry.SequenceNumber, expectedSequences[i])
		}
	}
}

func TestFileHandler_CountLines(t *testing.T) {
	fh := NewFileHandler()

	// Create a test gzip file
	tmpFile, err := os.CreateTemp("", "test_count.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write gzipped content with known number of lines
	gzipWriter := gzip.NewWriter(tmpFile)
	lines := []string{
		"0 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
		"1 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
		"2 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
		"3 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
		"4 MIIFBDCCAuygAwIBAgIHBVZSjFKAPTANBgkqhkiG9w0BAQsFADB/",
	}
	testContent := strings.Join(lines, "\n")
	gzipWriter.Write([]byte(testContent))
	gzipWriter.Close()
	tmpFile.Close()

	// Test counting
	count, err := fh.CountLines(tmpFile.Name())
	if err != nil {
		t.Fatalf("CountLines() error = %v", err)
	}

	expectedCount := int64(5)
	if count != expectedCount {
		t.Errorf("CountLines() = %d, want %d", count, expectedCount)
	}
}

func TestFileHandler_WithRealData(t *testing.T) {
	fh := NewFileHandler()

	// Test with actual data file if it exists
	testFile := "../../data/0.batch.gz"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("Skipping real data test - test file not found")
		return
	}

	// Test validation
	err := fh.ValidateInputFile(testFile)
	if err != nil {
		t.Errorf("ValidateInputFile() with real data failed: %v", err)
	}

	// Test counting lines
	count, err := fh.CountLines(testFile)
	if err != nil {
		t.Errorf("CountLines() with real data failed: %v", err)
	}
	if count == 0 {
		t.Error("CountLines() returned 0 for real data file")
	}
	t.Logf("Real data file contains %d lines", count)

	// Test processing first few lines
	processedCount := 0
	maxProcess := 10 // Only process first 10 lines for testing
	handler := func(entry *CertificateEntry) error {
		processedCount++
		if processedCount > maxProcess {
			return nil // Stop processing after maxProcess entries
		}
		
		// Validate the parsed entry
		if entry.SequenceNumber < 0 {
			return fmt.Errorf("invalid sequence number: %d", entry.SequenceNumber)
		}
		if len(entry.CertificateData) == 0 {
			return fmt.Errorf("empty certificate data")
		}
		
		t.Logf("Processed entry %d with %d bytes of certificate data", entry.SequenceNumber, len(entry.CertificateData))
		return nil
	}

	// We expect this to process successfully for at least the first few entries
	err = fh.ProcessFileLines(testFile, handler)
	if err != nil && processedCount == 0 {
		t.Errorf("ProcessFileLines() with real data failed: %v", err)
	}
}