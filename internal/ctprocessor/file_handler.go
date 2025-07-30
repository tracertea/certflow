package ctprocessor

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// FileHandler handles gzip file operations and line parsing
type FileHandler struct {
	lineRegex *regexp.Regexp
}

// NewFileHandler creates a new FileHandler instance
func NewFileHandler() *FileHandler {
	// Regex to match: sequence_number one_or_more_spaces certificate_data
	lineRegex := regexp.MustCompile(`^(\d+)\s+(.+)$`)
	return &FileHandler{
		lineRegex: lineRegex,
	}
}

// ValidateInputFile checks if the input file exists and is readable
func (fh *FileHandler) ValidateInputFile(filePath string) error {
	if strings.TrimSpace(filePath) == "" {
		return fmt.Errorf("file path cannot be empty")
	}

	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", filePath)
		}
		return fmt.Errorf("cannot access file %s: %v", filePath, err)
	}

	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", filePath)
	}

	if info.Size() == 0 {
		return fmt.Errorf("file is empty: %s", filePath)
	}

	// Try to open the file to check permissions
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %v", filePath, err)
	}
	file.Close()

	return nil
}

// OpenGzipFile opens and decompresses a gzip file, returning a scanner for line-by-line reading
func (fh *FileHandler) OpenGzipFile(filePath string) (*bufio.Scanner, *os.File, *gzip.Reader, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}

	// Create gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, nil, nil, fmt.Errorf("failed to create gzip reader for %s: %v", filePath, err)
	}

	// Create scanner for line-by-line reading
	scanner := bufio.NewScanner(gzipReader)
	
	// Set a larger buffer size for long certificate lines
	const maxCapacity = 1024 * 1024 // 1MB buffer
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	return scanner, file, gzipReader, nil
}

// ParseLine parses a single line from the CT data file
func (fh *FileHandler) ParseLine(line string) (*CertificateEntry, error) {
	if strings.TrimSpace(line) == "" {
		return nil, fmt.Errorf("line is empty")
	}

	matches := fh.lineRegex.FindStringSubmatch(line)
	if len(matches) != 3 {
		return nil, fmt.Errorf("line does not match expected format 'sequence_number certificate_data': %s", line[:min(50, len(line))])
	}

	// Parse sequence number
	sequenceNum, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid sequence number '%s': %v", matches[1], err)
	}

	// Extract certificate data
	certData := strings.TrimSpace(matches[2])
	if certData == "" {
		return nil, fmt.Errorf("certificate data is empty")
	}

	entry := &CertificateEntry{
		SequenceNumber:  sequenceNum,
		CertificateData: certData,
	}

	// Validate the entry
	if err := entry.Validate(); err != nil {
		return nil, fmt.Errorf("invalid certificate entry: %v", err)
	}

	return entry, nil
}

// ProcessFileLines processes a gzip file line by line, calling the provided handler for each parsed entry
func (fh *FileHandler) ProcessFileLines(filePath string, handler func(*CertificateEntry) error) error {
	// Validate input file
	if err := fh.ValidateInputFile(filePath); err != nil {
		return err
	}

	// Open gzip file
	scanner, file, gzipReader, err := fh.OpenGzipFile(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	defer gzipReader.Close()

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()

		// Parse the line
		entry, err := fh.ParseLine(line)
		if err != nil {
			return fmt.Errorf("error parsing line %d: %v", lineNumber, err)
		}

		// Call the handler
		if err := handler(entry); err != nil {
			return fmt.Errorf("error processing line %d: %v", lineNumber, err)
		}
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", filePath, err)
	}

	return nil
}

// CountLines counts the number of lines in a gzip file without processing them
func (fh *FileHandler) CountLines(filePath string) (int64, error) {
	scanner, file, gzipReader, err := fh.OpenGzipFile(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	defer gzipReader.Close()

	var count int64
	for scanner.Scan() {
		count++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error counting lines in %s: %v", filePath, err)
	}

	return count, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}