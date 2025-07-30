package ctprocessor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStateTracker implements the StateTracker interface for managing S3 processing state
type FileStateTracker struct {
	state      *ProcessingState
	stateFile  string
	mutex      sync.RWMutex
	autoSave   bool
	isDirty    bool
}

// NewFileStateTracker creates a new FileStateTracker instance
func NewFileStateTracker(stateFilePath string, autoSave bool) *FileStateTracker {
	return &FileStateTracker{
		state:     NewProcessingState(),
		stateFile: stateFilePath,
		autoSave:  autoSave,
		isDirty:   false,
	}
}

// LoadState loads the processing state from the state file
func (fst *FileStateTracker) LoadState() error {
	fst.mutex.Lock()
	defer fst.mutex.Unlock()

	// If state file doesn't exist, start with empty state
	if _, err := os.Stat(fst.stateFile); os.IsNotExist(err) {
		fst.state = NewProcessingState()
		fst.isDirty = false
		return nil
	}

	data, err := os.ReadFile(fst.stateFile)
	if err != nil {
		return fmt.Errorf("failed to read state file: %w", err)
	}

	// Handle empty file
	if len(data) == 0 {
		fst.state = NewProcessingState()
		fst.isDirty = false
		return nil
	}

	var loadedState ProcessingState
	if err := json.Unmarshal(data, &loadedState); err != nil {
		// State file is corrupted, attempt recovery
		return fst.recoverFromCorruption(data, err)
	}

	// Validate loaded state
	if err := loadedState.Validate(); err != nil {
		return fst.recoverFromCorruption(data, fmt.Errorf("invalid state: %w", err))
	}

	fst.state = &loadedState
	fst.isDirty = false
	return nil
}

// recoverFromCorruption attempts to recover from state file corruption
func (fst *FileStateTracker) recoverFromCorruption(data []byte, originalErr error) error {
	// Create backup of corrupted file
	backupPath := fst.stateFile + ".corrupted." + time.Now().Format("20060102-150405")
	if err := os.WriteFile(backupPath, data, 0644); err != nil {
		// If we can't even backup, just log and continue with fresh state
		fmt.Printf("Warning: Could not backup corrupted state file: %v\n", err)
	} else {
		fmt.Printf("Warning: State file corrupted, backed up to %s\n", backupPath)
	}

	// Try to extract any valid data from the corrupted file
	recoveredState := fst.attemptPartialRecovery(data)
	if recoveredState != nil {
		fst.state = recoveredState
		fst.isDirty = true // Mark as dirty to save the recovered state
		fmt.Printf("Partial recovery successful: recovered %d processed files and %d failed files\n",
			len(recoveredState.ProcessedFiles), len(recoveredState.FailedFiles))
		return nil
	}

	// If partial recovery fails, start with fresh state
	fmt.Printf("Warning: Could not recover state file, starting fresh. Original error: %v\n", originalErr)
	fst.state = NewProcessingState()
	fst.isDirty = true
	return nil
}

// attemptPartialRecovery tries to extract valid data from corrupted JSON
func (fst *FileStateTracker) attemptPartialRecovery(data []byte) *ProcessingState {
	// Try to parse as a partial JSON object
	var partial map[string]interface{}
	if err := json.Unmarshal(data, &partial); err != nil {
		return nil
	}

	recovered := NewProcessingState()

	// Try to recover processed files
	if processedData, ok := partial["processed_files"]; ok {
		if processedMap, ok := processedData.(map[string]interface{}); ok {
			for key, value := range processedMap {
				if fileData, ok := value.(map[string]interface{}); ok {
					if fileState := fst.parseFileState(fileData); fileState != nil {
						recovered.ProcessedFiles[key] = *fileState
					}
				}
			}
		}
	}

	// Try to recover failed files
	if failedData, ok := partial["failed_files"]; ok {
		if failedMap, ok := failedData.(map[string]interface{}); ok {
			for key, value := range failedMap {
				if fileData, ok := value.(map[string]interface{}); ok {
					if fileState := fst.parseFileState(fileData); fileState != nil {
						recovered.FailedFiles[key] = *fileState
					}
				}
			}
		}
	}

	// Only return recovered state if we got some data
	if len(recovered.ProcessedFiles) > 0 || len(recovered.FailedFiles) > 0 {
		return recovered
	}

	return nil
}

// parseFileState attempts to parse a FileState from a map
func (fst *FileStateTracker) parseFileState(data map[string]interface{}) *FileState {
	fs := &FileState{}

	if key, ok := data["key"].(string); ok {
		fs.Key = key
	} else {
		return nil
	}

	if etag, ok := data["etag"].(string); ok {
		fs.ETag = etag
	} else {
		return nil
	}

	if size, ok := data["size"].(float64); ok {
		fs.Size = int64(size)
	}

	if processedAtStr, ok := data["processed_at"].(string); ok {
		if processedAt, err := time.Parse(time.RFC3339, processedAtStr); err == nil {
			fs.ProcessedAt = processedAt
		}
	}

	if errorMsg, ok := data["error_msg"].(string); ok {
		fs.ErrorMsg = errorMsg
	}

	// Validate the parsed state
	if err := fs.Validate(); err != nil {
		return nil
	}

	return fs
}

// IsProcessed checks if a file has been successfully processed
func (fst *FileStateTracker) IsProcessed(key string, etag string) bool {
	fst.mutex.RLock()
	defer fst.mutex.RUnlock()

	fileState, exists := fst.state.ProcessedFiles[key]
	if !exists {
		return false
	}

	// Check if ETag matches (file hasn't changed)
	return fileState.ETag == etag
}

// MarkProcessed marks a file as successfully processed
func (fst *FileStateTracker) MarkProcessed(key string, etag string, size int64) error {
	fst.mutex.Lock()
	defer fst.mutex.Unlock()

	fileState := FileState{
		Key:         key,
		ETag:        etag,
		Size:        size,
		ProcessedAt: time.Now(),
	}

	if err := fileState.Validate(); err != nil {
		return fmt.Errorf("invalid file state: %w", err)
	}

	// Remove from failed files if it exists there
	delete(fst.state.FailedFiles, key)

	// Add to processed files
	fst.state.ProcessedFiles[key] = fileState
	fst.state.LastUpdated = time.Now()
	fst.isDirty = true

	if fst.autoSave {
		return fst.saveStateUnsafe()
	}

	return nil
}

// MarkFailed marks a file as failed to process
func (fst *FileStateTracker) MarkFailed(key string, etag string, errorMsg string) error {
	fst.mutex.Lock()
	defer fst.mutex.Unlock()

	fileState := FileState{
		Key:         key,
		ETag:        etag,
		Size:        0, // Size not relevant for failed files
		ProcessedAt: time.Now(),
		ErrorMsg:    errorMsg,
	}

	// For failed files, we don't require size validation
	if key == "" || etag == "" {
		return fmt.Errorf("key and etag are required for failed file state")
	}

	// Remove from processed files if it exists there
	delete(fst.state.ProcessedFiles, key)

	// Add to failed files
	fst.state.FailedFiles[key] = fileState
	fst.state.LastUpdated = time.Now()
	fst.isDirty = true

	if fst.autoSave {
		return fst.saveStateUnsafe()
	}

	return nil
}

// SaveState saves the current state to the state file
func (fst *FileStateTracker) SaveState() error {
	fst.mutex.Lock()
	defer fst.mutex.Unlock()

	return fst.saveStateUnsafe()
}

// saveStateUnsafe saves state without acquiring mutex (internal use)
func (fst *FileStateTracker) saveStateUnsafe() error {
	if !fst.isDirty {
		return nil // No changes to save
	}

	// Ensure directory exists
	dir := filepath.Dir(fst.stateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	// Update last updated timestamp
	fst.state.LastUpdated = time.Now()

	// Marshal to JSON with indentation for readability
	data, err := json.MarshalIndent(fst.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to temporary file first, then rename for atomic operation
	tempFile := fst.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary state file: %w", err)
	}

	if err := os.Rename(tempFile, fst.stateFile); err != nil {
		// Clean up temp file on failure
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename temporary state file: %w", err)
	}

	fst.isDirty = false
	return nil
}

// GetProcessedCount returns the number of successfully processed files
func (fst *FileStateTracker) GetProcessedCount() int {
	fst.mutex.RLock()
	defer fst.mutex.RUnlock()

	return fst.state.GetProcessedCount()
}

// GetFailedCount returns the number of failed files
func (fst *FileStateTracker) GetFailedCount() int {
	fst.mutex.RLock()
	defer fst.mutex.RUnlock()

	return fst.state.GetFailedCount()
}

// GetState returns a copy of the current processing state (for testing)
func (fst *FileStateTracker) GetState() ProcessingState {
	fst.mutex.RLock()
	defer fst.mutex.RUnlock()

	// Return a deep copy to prevent external modification
	stateCopy := ProcessingState{
		ProcessedFiles: make(map[string]FileState),
		FailedFiles:    make(map[string]FileState),
		LastUpdated:    fst.state.LastUpdated,
		Version:        fst.state.Version,
	}

	for k, v := range fst.state.ProcessedFiles {
		stateCopy.ProcessedFiles[k] = v
	}

	for k, v := range fst.state.FailedFiles {
		stateCopy.FailedFiles[k] = v
	}

	return stateCopy
}

// IsDirty returns whether the state has unsaved changes
func (fst *FileStateTracker) IsDirty() bool {
	fst.mutex.RLock()
	defer fst.mutex.RUnlock()

	return fst.isDirty
}