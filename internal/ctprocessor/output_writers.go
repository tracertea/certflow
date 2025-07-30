package ctprocessor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// OutputWriterFactory creates output writers based on format
type OutputWriterFactory struct{}

// NewOutputWriterFactory creates a new OutputWriterFactory
func NewOutputWriterFactory() *OutputWriterFactory {
	return &OutputWriterFactory{}
}

// CreateWriter creates an appropriate output writer based on the format and output path
func (owf *OutputWriterFactory) CreateWriter(outputPath, format string) (OutputWriter, error) {
	if err := owf.validateOutputPath(outputPath); err != nil {
		return nil, err
	}

	switch strings.ToLower(format) {
	case "json":
		return NewJSONWriter(outputPath)
	case "jsonl":
		return NewJSONLineWriter(outputPath)
	case "csv":
		return NewCSVWriter(outputPath)
	case "txt", "text":
		return NewTextWriter(outputPath)
	default:
		return nil, fmt.Errorf("unsupported output format: %s", format)
	}
}

// validateOutputPath validates the output path
func (owf *OutputWriterFactory) validateOutputPath(outputPath string) error {
	if strings.TrimSpace(outputPath) == "" {
		return fmt.Errorf("output path cannot be empty")
	}

	// Check if directory exists and is writable
	dir := filepath.Dir(outputPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory %s: %v", dir, err)
		}
	}

	return nil
}

// JSONWriter implements OutputWriter for JSON format
type JSONWriter struct {
	outputPath string
	file       *os.File
	encoder    *json.Encoder
	mappings   []DomainMapping
	stats      *ProcessingStats
}

// NewJSONWriter creates a new JSON output writer
func NewJSONWriter(outputPath string) (*JSONWriter, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON output file %s: %v", outputPath, err)
	}

	writer := &JSONWriter{
		outputPath: outputPath,
		file:       file,
		encoder:    json.NewEncoder(file),
		mappings:   make([]DomainMapping, 0),
		stats:      &ProcessingStats{},
	}

	// Set pretty printing for JSON
	writer.encoder.SetIndent("", "  ")

	return writer, nil
}

type JSONLWriter struct {
	outputPath string
	file       *os.File
	encoder    *json.Encoder
	mappings   []DomainMapping
	stats      *ProcessingStats
}

func NewJSONLineWriter(outputPath string) (*JSONLWriter, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON output file %s: %v", outputPath, err)
	}

	writer := &JSONLWriter{
		outputPath: outputPath,
		file:       file,
		encoder:    json.NewEncoder(file),
		mappings:   make([]DomainMapping, 0),
		stats:      &ProcessingStats{},
	}

	// Disable pretty printing for line-by-line output
	writer.encoder.SetIndent("", "")
	// set SetEscapeHTML
	writer.encoder.SetEscapeHTML(false)

	return writer, nil
}

func (jlw *JSONLWriter) WriteMapping(mapping DomainMapping) error {
	if err := mapping.Validate(); err != nil {
		return fmt.Errorf("invalid mapping: %v", err)
	}
	jlw.mappings = append(jlw.mappings, mapping)

	// Write each mapping as a separate line
	if err := jlw.encoder.Encode(mapping); err != nil {
		return fmt.Errorf("failed to write JSONL record: %v", err)
	}

	return nil
}

func (jlw *JSONLWriter) WriteBatch(mappings []DomainMapping) error {
	for _, mapping := range mappings {
		if err := jlw.WriteMapping(mapping); err != nil {
			return err
		}
	}
	return nil
}

// SetStats sets the processing statistics
func (jlw *JSONLWriter) SetStats(stats *ProcessingStats) {
	jlw.stats = stats
}

func (jlw *JSONLWriter) Close() error {
	if jlw.file == nil {
		return nil // Already closed
	}

	// Write the stats as a final line if they exist
	if jlw.stats != nil {
		if err := jlw.encoder.Encode(jlw.stats); err != nil {
			jlw.file.Close()
			return fmt.Errorf("failed to write JSONL stats: %v", err)
		}
	}

	err := jlw.file.Close()
	jlw.file = nil
	return err
}

// WriteMapping adds a mapping to the buffer (will be written on Close)
func (jw *JSONWriter) WriteMapping(mapping DomainMapping) error {
	if err := mapping.Validate(); err != nil {
		return fmt.Errorf("invalid mapping: %v", err)
	}
	jw.mappings = append(jw.mappings, mapping)
	return nil
}

// WriteBatch adds multiple mappings to the buffer
func (jw *JSONWriter) WriteBatch(mappings []DomainMapping) error {
	for _, mapping := range mappings {
		if err := jw.WriteMapping(mapping); err != nil {
			return err
		}
	}
	return nil
}

// SetStats sets the processing statistics
func (jw *JSONWriter) SetStats(stats *ProcessingStats) {
	jw.stats = stats
}

// Close writes all buffered mappings to the JSON file and closes it
func (jw *JSONWriter) Close() error {
	if jw.file == nil {
		return nil // Already closed
	}

	// Create the output structure
	output := struct {
		Mappings []DomainMapping  `json:"mappings"`
		Stats    *ProcessingStats `json:"stats,omitempty"`
	}{
		Mappings: jw.mappings,
		Stats:    jw.stats,
	}

	// Write the JSON
	if err := jw.encoder.Encode(output); err != nil {
		jw.file.Close()
		return fmt.Errorf("failed to write JSON output: %v", err)
	}

	err := jw.file.Close()
	jw.file = nil
	return err
}

// CSVWriter implements OutputWriter for CSV format
type CSVWriter struct {
	outputPath    string
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
}

// NewCSVWriter creates a new CSV output writer
func NewCSVWriter(outputPath string) (*CSVWriter, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV output file %s: %v", outputPath, err)
	}

	writer := &CSVWriter{
		outputPath:    outputPath,
		file:          file,
		writer:        csv.NewWriter(file),
		headerWritten: false,
	}

	return writer, nil
}

// writeHeader writes the CSV header if not already written
func (cw *CSVWriter) writeHeader() error {
	if cw.headerWritten {
		return nil
	}

	header := []string{"Name", "Organization", "Source", "SequenceNumber"}
	if err := cw.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	cw.headerWritten = true
	return nil
}

// WriteMapping writes a single mapping to the CSV file
func (cw *CSVWriter) WriteMapping(mapping DomainMapping) error {
	if err := mapping.Validate(); err != nil {
		return fmt.Errorf("invalid mapping: %v", err)
	}

	if err := cw.writeHeader(); err != nil {
		return err
	}

	record := []string{
		mapping.Name,
		mapping.Organization,
		mapping.Source,
		fmt.Sprintf("%d", mapping.SequenceNum),
	}

	if err := cw.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %v", err)
	}

	return nil
}

// WriteBatch writes multiple mappings to the CSV file
func (cw *CSVWriter) WriteBatch(mappings []DomainMapping) error {
	for _, mapping := range mappings {
		if err := cw.WriteMapping(mapping); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes the CSV file
func (cw *CSVWriter) Close() error {
	if cw.file == nil {
		return nil // Already closed
	}

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		cw.file.Close()
		return fmt.Errorf("failed to flush CSV writer: %v", err)
	}

	err := cw.file.Close()
	cw.file = nil
	return err
}

// TextWriter implements OutputWriter for plain text format
type TextWriter struct {
	outputPath string
	file       *os.File
}

// NewTextWriter creates a new text output writer
func NewTextWriter(outputPath string) (*TextWriter, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create text output file %s: %v", outputPath, err)
	}

	writer := &TextWriter{
		outputPath: outputPath,
		file:       file,
	}

	return writer, nil
}

// WriteMapping writes a single mapping to the text file
func (tw *TextWriter) WriteMapping(mapping DomainMapping) error {
	if err := mapping.Validate(); err != nil {
		return fmt.Errorf("invalid mapping: %v", err)
	}

	line := fmt.Sprintf("%s -> %s (%s) [seq:%d]\n",
		mapping.Name,
		mapping.Organization,
		mapping.Source,
		mapping.SequenceNum)

	if _, err := tw.file.WriteString(line); err != nil {
		return fmt.Errorf("failed to write text record: %v", err)
	}

	return nil
}

// WriteBatch writes multiple mappings to the text file
func (tw *TextWriter) WriteBatch(mappings []DomainMapping) error {
	for _, mapping := range mappings {
		if err := tw.WriteMapping(mapping); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the text file
func (tw *TextWriter) Close() error {
	if tw.file == nil {
		return nil // Already closed
	}

	err := tw.file.Close()
	tw.file = nil
	return err
}
