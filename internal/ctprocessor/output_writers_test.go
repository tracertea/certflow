package ctprocessor

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewOutputWriterFactory(t *testing.T) {
	factory := NewOutputWriterFactory()
	if factory == nil {
		t.Fatal("NewOutputWriterFactory() returned nil")
	}
}

func TestOutputWriterFactory_CreateWriter(t *testing.T) {
	factory := NewOutputWriterFactory()
	tempDir := t.TempDir()

	tests := []struct {
		name       string
		outputPath string
		format     string
		wantType   string
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "JSON writer",
			outputPath: filepath.Join(tempDir, "test.json"),
			format:     "json",
			wantType:   "*processor.JSONWriter",
			wantErr:    false,
		},
		{
			name:       "CSV writer",
			outputPath: filepath.Join(tempDir, "test.csv"),
			format:     "csv",
			wantType:   "*processor.CSVWriter",
			wantErr:    false,
		},
		{
			name:       "Text writer",
			outputPath: filepath.Join(tempDir, "test.txt"),
			format:     "txt",
			wantType:   "*processor.TextWriter",
			wantErr:    false,
		},
		{
			name:       "Text writer with 'text' format",
			outputPath: filepath.Join(tempDir, "test2.txt"),
			format:     "text",
			wantType:   "*processor.TextWriter",
			wantErr:    false,
		},
		{
			name:       "Unsupported format",
			outputPath: filepath.Join(tempDir, "test.xml"),
			format:     "xml",
			wantErr:    true,
			errMsg:     "unsupported output format",
		},
		{
			name:       "Empty output path",
			outputPath: "",
			format:     "json",
			wantErr:    true,
			errMsg:     "output path cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := factory.CreateWriter(tt.outputPath, tt.format)
			if tt.wantErr {
				if err == nil {
					t.Errorf("CreateWriter() expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("CreateWriter() error = %v, want to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("CreateWriter() unexpected error = %v", err)
					return
				}
				if writer == nil {
					t.Errorf("CreateWriter() returned nil writer")
					return
				}
				// Clean up
				writer.Close()
			}
		})
	}
}

func TestJSONWriter(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "test.json")

	// Create test mappings
	mappings := []DomainMapping{
		{
			Name:         "example.com",
			Organization: "Example Corp",
			Source:       "CN",
			SequenceNum:  123,
		},
		{
			Name:         "api.example.com",
			Organization: "Example Corp",
			Source:       "SAN",
			SequenceNum:  123,
		},
	}

	// Test writing
	writer, err := NewJSONWriter(outputPath)
	if err != nil {
		t.Fatalf("NewJSONWriter() error = %v", err)
	}

	// Set stats
	stats := &ProcessingStats{
		TotalProcessed:   2,
		SuccessfulParsed: 2,
		Errors:          0,
		StartTime:       time.Now(),
		Duration:        time.Second,
	}
	writer.SetStats(stats)

	// Write mappings
	for _, mapping := range mappings {
		if err := writer.WriteMapping(mapping); err != nil {
			t.Errorf("WriteMapping() error = %v", err)
		}
	}

	// Close and write to file
	if err := writer.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify file contents
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Parse JSON to verify structure
	var output struct {
		Mappings []DomainMapping   `json:"mappings"`
		Stats    *ProcessingStats `json:"stats"`
	}

	if err := json.Unmarshal(content, &output); err != nil {
		t.Fatalf("Failed to parse JSON output: %v", err)
	}

	if len(output.Mappings) != 2 {
		t.Errorf("Expected 2 mappings, got %d", len(output.Mappings))
	}

	if output.Stats == nil {
		t.Error("Expected stats to be present")
	}

	// Test WriteBatch
	writer2, err := NewJSONWriter(filepath.Join(tempDir, "test2.json"))
	if err != nil {
		t.Fatalf("NewJSONWriter() error = %v", err)
	}

	if err := writer2.WriteBatch(mappings); err != nil {
		t.Errorf("WriteBatch() error = %v", err)
	}

	writer2.Close()
}

func TestCSVWriter(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "test.csv")

	// Create test mappings
	mappings := []DomainMapping{
		{
			Name:         "example.com",
			Organization: "Example Corp",
			Source:       "CN",
			SequenceNum:  123,
		},
		{
			Name:         "api.example.com",
			Organization: "Example Corp",
			Source:       "SAN",
			SequenceNum:  456,
		},
	}

	// Test writing
	writer, err := NewCSVWriter(outputPath)
	if err != nil {
		t.Fatalf("NewCSVWriter() error = %v", err)
	}

	// Write mappings
	for _, mapping := range mappings {
		if err := writer.WriteMapping(mapping); err != nil {
			t.Errorf("WriteMapping() error = %v", err)
		}
	}

	// Close and flush
	if err := writer.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify file contents
	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Should have header + 2 data rows
	if len(records) != 3 {
		t.Errorf("Expected 3 records (header + 2 data), got %d", len(records))
	}

	// Check header
	expectedHeader := []string{"Name", "Organization", "Source", "SequenceNumber"}
	if len(records) > 0 {
		for i, col := range expectedHeader {
			if i >= len(records[0]) || records[0][i] != col {
				t.Errorf("Header column %d: expected %s, got %s", i, col, records[0][i])
			}
		}
	}

	// Check first data row
	if len(records) > 1 {
		expected := []string{"example.com", "Example Corp", "CN", "123"}
		for i, col := range expected {
			if i >= len(records[1]) || records[1][i] != col {
				t.Errorf("Data row 1 column %d: expected %s, got %s", i, col, records[1][i])
			}
		}
	}

	// Test WriteBatch
	writer2, err := NewCSVWriter(filepath.Join(tempDir, "test2.csv"))
	if err != nil {
		t.Fatalf("NewCSVWriter() error = %v", err)
	}

	if err := writer2.WriteBatch(mappings); err != nil {
		t.Errorf("WriteBatch() error = %v", err)
	}

	writer2.Close()
}

func TestTextWriter(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "test.txt")

	// Create test mappings
	mappings := []DomainMapping{
		{
			Name:         "example.com",
			Organization: "Example Corp",
			Source:       "CN",
			SequenceNum:  123,
		},
		{
			Name:         "api.example.com",
			Organization: "Example Corp",
			Source:       "SAN",
			SequenceNum:  456,
		},
	}

	// Test writing
	writer, err := NewTextWriter(outputPath)
	if err != nil {
		t.Fatalf("NewTextWriter() error = %v", err)
	}

	// Write mappings
	for _, mapping := range mappings {
		if err := writer.WriteMapping(mapping); err != nil {
			t.Errorf("WriteMapping() error = %v", err)
		}
	}

	// Close
	if err := writer.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify file contents
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	contentStr := string(content)
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")

	if len(lines) != 2 {
		t.Errorf("Expected 2 lines, got %d", len(lines))
	}

	// Check first line format
	expectedLine1 := "example.com -> Example Corp (CN) [seq:123]"
	if len(lines) > 0 && lines[0] != expectedLine1 {
		t.Errorf("Line 1: expected %s, got %s", expectedLine1, lines[0])
	}

	// Check second line format
	expectedLine2 := "api.example.com -> Example Corp (SAN) [seq:456]"
	if len(lines) > 1 && lines[1] != expectedLine2 {
		t.Errorf("Line 2: expected %s, got %s", expectedLine2, lines[1])
	}

	// Test WriteBatch
	writer2, err := NewTextWriter(filepath.Join(tempDir, "test2.txt"))
	if err != nil {
		t.Fatalf("NewTextWriter() error = %v", err)
	}

	if err := writer2.WriteBatch(mappings); err != nil {
		t.Errorf("WriteBatch() error = %v", err)
	}

	writer2.Close()
}

func TestOutputWriters_InvalidMappings(t *testing.T) {
	tempDir := t.TempDir()

	// Invalid mapping (empty name)
	invalidMapping := DomainMapping{
		Name:         "",
		Organization: "Example Corp",
		Source:       "CN",
		SequenceNum:  123,
	}

	// Test JSON writer with invalid mapping
	jsonWriter, err := NewJSONWriter(filepath.Join(tempDir, "invalid.json"))
	if err != nil {
		t.Fatalf("NewJSONWriter() error = %v", err)
	}
	defer jsonWriter.Close()

	if err := jsonWriter.WriteMapping(invalidMapping); err == nil {
		t.Error("JSONWriter.WriteMapping() expected error for invalid mapping")
	}

	// Test CSV writer with invalid mapping
	csvWriter, err := NewCSVWriter(filepath.Join(tempDir, "invalid.csv"))
	if err != nil {
		t.Fatalf("NewCSVWriter() error = %v", err)
	}
	defer csvWriter.Close()

	if err := csvWriter.WriteMapping(invalidMapping); err == nil {
		t.Error("CSVWriter.WriteMapping() expected error for invalid mapping")
	}

	// Test Text writer with invalid mapping
	textWriter, err := NewTextWriter(filepath.Join(tempDir, "invalid.txt"))
	if err != nil {
		t.Fatalf("NewTextWriter() error = %v", err)
	}
	defer textWriter.Close()

	if err := textWriter.WriteMapping(invalidMapping); err == nil {
		t.Error("TextWriter.WriteMapping() expected error for invalid mapping")
	}
}

func TestOutputWriters_DoubleClose(t *testing.T) {
	tempDir := t.TempDir()

	// Test JSON writer double close
	jsonWriter, err := NewJSONWriter(filepath.Join(tempDir, "double.json"))
	if err != nil {
		t.Fatalf("NewJSONWriter() error = %v", err)
	}

	if err := jsonWriter.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}

	if err := jsonWriter.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Test CSV writer double close
	csvWriter, err := NewCSVWriter(filepath.Join(tempDir, "double.csv"))
	if err != nil {
		t.Fatalf("NewCSVWriter() error = %v", err)
	}

	if err := csvWriter.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}

	if err := csvWriter.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Test Text writer double close
	textWriter, err := NewTextWriter(filepath.Join(tempDir, "double.txt"))
	if err != nil {
		t.Fatalf("NewTextWriter() error = %v", err)
	}

	if err := textWriter.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}

	if err := textWriter.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}