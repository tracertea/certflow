package ctprocessor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewFileStateTracker(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", true)
	
	if tracker == nil {
		t.Fatal("Expected non-nil tracker")
	}
	
	if tracker.stateFile != "/tmp/test_state.json" {
		t.Errorf("Expected state file '/tmp/test_state.json', got '%s'", tracker.stateFile)
	}
	
	if !tracker.autoSave {
		t.Error("Expected autoSave to be true")
	}
	
	if tracker.state == nil {
		t.Error("Expected state to be initialized")
	}
	
	if tracker.state.ProcessedFiles == nil {
		t.Error("Expected ProcessedFiles map to be initialized")
	}
	
	if tracker.state.FailedFiles == nil {
		t.Error("Expected FailedFiles map to be initialized")
	}
}

func TestLoadState_NewFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	tracker := NewFileStateTracker(stateFile, false)
	
	err := tracker.LoadState()
	if err != nil {
		t.Fatalf("Expected no error loading non-existent state file, got: %v", err)
	}
	
	if len(tracker.state.ProcessedFiles) != 0 {
		t.Error("Expected empty ProcessedFiles map")
	}
	
	if len(tracker.state.FailedFiles) != 0 {
		t.Error("Expected empty FailedFiles map")
	}
}

func TestLoadState_ExistingFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	// Create a test state file
	testState := ProcessingState{
		ProcessedFiles: map[string]FileState{
			"test-key": {
				Key:         "test-key",
				ETag:        "test-etag",
				Size:        1024,
				ProcessedAt: time.Now(),
			},
		},
		FailedFiles: map[string]FileState{
			"failed-key": {
				Key:         "failed-key",
				ETag:        "failed-etag",
				Size:        0,
				ProcessedAt: time.Now(),
				ErrorMsg:    "test error",
			},
		},
		LastUpdated: time.Now(),
		Version:     "1.0",
	}
	
	data, err := json.Marshal(testState)
	if err != nil {
		t.Fatalf("Failed to marshal test state: %v", err)
	}
	
	err = os.WriteFile(stateFile, data, 0644)
	if err != nil {
		t.Fatalf("Failed to write test state file: %v", err)
	}
	
	tracker := NewFileStateTracker(stateFile, false)
	err = tracker.LoadState()
	if err != nil {
		t.Fatalf("Expected no error loading existing state file, got: %v", err)
	}
	
	if len(tracker.state.ProcessedFiles) != 1 {
		t.Errorf("Expected 1 processed file, got %d", len(tracker.state.ProcessedFiles))
	}
	
	if len(tracker.state.FailedFiles) != 1 {
		t.Errorf("Expected 1 failed file, got %d", len(tracker.state.FailedFiles))
	}
	
	processedFile, exists := tracker.state.ProcessedFiles["test-key"]
	if !exists {
		t.Error("Expected 'test-key' to exist in processed files")
	} else {
		if processedFile.ETag != "test-etag" {
			t.Errorf("Expected ETag 'test-etag', got '%s'", processedFile.ETag)
		}
	}
}

func TestLoadState_CorruptedFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	// Write corrupted JSON
	corruptedData := `{"processed_files": {"key1": {"key": "key1", "etag": "etag1", "size": 1024, "processed_at": "2023-01-01T00:00:00Z"}}, "invalid_json"`
	err := os.WriteFile(stateFile, []byte(corruptedData), 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupted state file: %v", err)
	}
	
	tracker := NewFileStateTracker(stateFile, false)
	err = tracker.LoadState()
	if err != nil {
		t.Fatalf("Expected no error with corrupted file (should recover), got: %v", err)
	}
	
	// Should start with fresh state since corruption recovery failed
	if len(tracker.state.ProcessedFiles) != 0 {
		t.Error("Expected empty ProcessedFiles map after corruption recovery")
	}
	
	// Check that backup file was created
	backupFiles, err := filepath.Glob(stateFile + ".corrupted.*")
	if err != nil {
		t.Fatalf("Failed to check for backup files: %v", err)
	}
	
	if len(backupFiles) == 0 {
		t.Error("Expected backup file to be created for corrupted state")
	}
}

func TestLoadState_PartialRecovery(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	// Write partially valid JSON (missing closing brace but with valid processed_files)
	partialData := `{
		"processed_files": {
			"key1": {
				"key": "key1",
				"etag": "etag1",
				"size": 1024,
				"processed_at": "2023-01-01T00:00:00Z"
			}
		},
		"failed_files": {
			"key2": {
				"key": "key2",
				"etag": "etag2",
				"size": 0,
				"processed_at": "2023-01-01T00:00:00Z",
				"error_msg": "test error"
			}
		}
	}`
	
	err := os.WriteFile(stateFile, []byte(partialData), 0644)
	if err != nil {
		t.Fatalf("Failed to write partial state file: %v", err)
	}
	
	tracker := NewFileStateTracker(stateFile, false)
	err = tracker.LoadState()
	if err != nil {
		t.Fatalf("Expected no error with partial recovery, got: %v", err)
	}
	
	// Should have recovered the valid data
	if len(tracker.state.ProcessedFiles) != 1 {
		t.Errorf("Expected 1 recovered processed file, got %d", len(tracker.state.ProcessedFiles))
	}
	
	if len(tracker.state.FailedFiles) != 1 {
		t.Errorf("Expected 1 recovered failed file, got %d", len(tracker.state.FailedFiles))
	}
}

func TestIsProcessed(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// Initially, no files should be processed
	if tracker.IsProcessed("test-key", "test-etag") {
		t.Error("Expected file to not be processed initially")
	}
	
	// Mark a file as processed
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	// Now it should be processed
	if !tracker.IsProcessed("test-key", "test-etag") {
		t.Error("Expected file to be processed")
	}
	
	// Different ETag should not be processed
	if tracker.IsProcessed("test-key", "different-etag") {
		t.Error("Expected file with different ETag to not be processed")
	}
	
	// Different key should not be processed
	if tracker.IsProcessed("different-key", "test-etag") {
		t.Error("Expected different key to not be processed")
	}
}

func TestMarkProcessed(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	if tracker.GetProcessedCount() != 1 {
		t.Errorf("Expected 1 processed file, got %d", tracker.GetProcessedCount())
	}
	
	state := tracker.GetState()
	fileState, exists := state.ProcessedFiles["test-key"]
	if !exists {
		t.Error("Expected 'test-key' to exist in processed files")
	} else {
		if fileState.Key != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", fileState.Key)
		}
		if fileState.ETag != "test-etag" {
			t.Errorf("Expected ETag 'test-etag', got '%s'", fileState.ETag)
		}
		if fileState.Size != 1024 {
			t.Errorf("Expected size 1024, got %d", fileState.Size)
		}
		if fileState.ProcessedAt.IsZero() {
			t.Error("Expected ProcessedAt to be set")
		}
	}
}

func TestMarkProcessed_InvalidData(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// Test with empty key
	err := tracker.MarkProcessed("", "test-etag", 1024)
	if err == nil {
		t.Error("Expected error with empty key")
	}
	
	// Test with empty ETag
	err = tracker.MarkProcessed("test-key", "", 1024)
	if err == nil {
		t.Error("Expected error with empty ETag")
	}
	
	// Test with negative size
	err = tracker.MarkProcessed("test-key", "test-etag", -1)
	if err == nil {
		t.Error("Expected error with negative size")
	}
}

func TestMarkFailed(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	err := tracker.MarkFailed("failed-key", "failed-etag", "test error message")
	if err != nil {
		t.Fatalf("Failed to mark file as failed: %v", err)
	}
	
	if tracker.GetFailedCount() != 1 {
		t.Errorf("Expected 1 failed file, got %d", tracker.GetFailedCount())
	}
	
	state := tracker.GetState()
	fileState, exists := state.FailedFiles["failed-key"]
	if !exists {
		t.Error("Expected 'failed-key' to exist in failed files")
	} else {
		if fileState.Key != "failed-key" {
			t.Errorf("Expected key 'failed-key', got '%s'", fileState.Key)
		}
		if fileState.ETag != "failed-etag" {
			t.Errorf("Expected ETag 'failed-etag', got '%s'", fileState.ETag)
		}
		if fileState.ErrorMsg != "test error message" {
			t.Errorf("Expected error message 'test error message', got '%s'", fileState.ErrorMsg)
		}
		if fileState.ProcessedAt.IsZero() {
			t.Error("Expected ProcessedAt to be set")
		}
	}
}

func TestMarkFailed_InvalidData(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// Test with empty key
	err := tracker.MarkFailed("", "test-etag", "error")
	if err == nil {
		t.Error("Expected error with empty key")
	}
	
	// Test with empty ETag
	err = tracker.MarkFailed("test-key", "", "error")
	if err == nil {
		t.Error("Expected error with empty ETag")
	}
}

func TestMarkProcessed_RemovesFromFailed(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// First mark as failed
	err := tracker.MarkFailed("test-key", "test-etag", "initial error")
	if err != nil {
		t.Fatalf("Failed to mark file as failed: %v", err)
	}
	
	if tracker.GetFailedCount() != 1 {
		t.Error("Expected 1 failed file")
	}
	
	// Then mark as processed
	err = tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	if tracker.GetFailedCount() != 0 {
		t.Error("Expected 0 failed files after marking as processed")
	}
	
	if tracker.GetProcessedCount() != 1 {
		t.Error("Expected 1 processed file")
	}
}

func TestMarkFailed_RemovesFromProcessed(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// First mark as processed
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	if tracker.GetProcessedCount() != 1 {
		t.Error("Expected 1 processed file")
	}
	
	// Then mark as failed
	err = tracker.MarkFailed("test-key", "test-etag", "processing error")
	if err != nil {
		t.Fatalf("Failed to mark file as failed: %v", err)
	}
	
	if tracker.GetProcessedCount() != 0 {
		t.Error("Expected 0 processed files after marking as failed")
	}
	
	if tracker.GetFailedCount() != 1 {
		t.Error("Expected 1 failed file")
	}
}

func TestSaveState(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	tracker := NewFileStateTracker(stateFile, false)
	
	// Mark some files
	err := tracker.MarkProcessed("processed-key", "processed-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	err = tracker.MarkFailed("failed-key", "failed-etag", "test error")
	if err != nil {
		t.Fatalf("Failed to mark file as failed: %v", err)
	}
	
	// Save state
	err = tracker.SaveState()
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}
	
	// Verify file was created
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("Expected state file to be created")
	}
	
	// Load state in new tracker and verify
	newTracker := NewFileStateTracker(stateFile, false)
	err = newTracker.LoadState()
	if err != nil {
		t.Fatalf("Failed to load saved state: %v", err)
	}
	
	if newTracker.GetProcessedCount() != 1 {
		t.Errorf("Expected 1 processed file in loaded state, got %d", newTracker.GetProcessedCount())
	}
	
	if newTracker.GetFailedCount() != 1 {
		t.Errorf("Expected 1 failed file in loaded state, got %d", newTracker.GetFailedCount())
	}
}

func TestAutoSave(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	tracker := NewFileStateTracker(stateFile, true) // Enable auto-save
	
	// Mark a file as processed (should auto-save)
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	// Verify file was created automatically
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("Expected state file to be created automatically")
	}
	
	// Load state in new tracker and verify
	newTracker := NewFileStateTracker(stateFile, false)
	err = newTracker.LoadState()
	if err != nil {
		t.Fatalf("Failed to load auto-saved state: %v", err)
	}
	
	if newTracker.GetProcessedCount() != 1 {
		t.Error("Expected 1 processed file in auto-saved state")
	}
}

func TestConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	tracker := NewFileStateTracker(stateFile, false)
	
	const numGoroutines = 10
	const numOperationsPerGoroutine = 100
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Start multiple goroutines performing concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, j)
				etag := fmt.Sprintf("etag-%d-%d", goroutineID, j)
				
				// Alternate between marking as processed and failed
				if j%2 == 0 {
					err := tracker.MarkProcessed(key, etag, int64(j))
					if err != nil {
						t.Errorf("Failed to mark file as processed: %v", err)
					}
				} else {
					err := tracker.MarkFailed(key, etag, "test error")
					if err != nil {
						t.Errorf("Failed to mark file as failed: %v", err)
					}
				}
				
				// Check if processed
				tracker.IsProcessed(key, etag)
				
				// Get counts
				tracker.GetProcessedCount()
				tracker.GetFailedCount()
			}
		}(i)
	}
	
	wg.Wait()
	
	// Verify final state
	totalExpected := numGoroutines * numOperationsPerGoroutine
	totalActual := tracker.GetProcessedCount() + tracker.GetFailedCount()
	
	if totalActual != totalExpected {
		t.Errorf("Expected %d total files, got %d", totalExpected, totalActual)
	}
	
	// Save state and verify it can be loaded
	err := tracker.SaveState()
	if err != nil {
		t.Fatalf("Failed to save state after concurrent operations: %v", err)
	}
	
	newTracker := NewFileStateTracker(stateFile, false)
	err = newTracker.LoadState()
	if err != nil {
		t.Fatalf("Failed to load state after concurrent operations: %v", err)
	}
	
	newTotal := newTracker.GetProcessedCount() + newTracker.GetFailedCount()
	if newTotal != totalExpected {
		t.Errorf("Expected %d total files after reload, got %d", totalExpected, newTotal)
	}
}

func TestIsDirty(t *testing.T) {
	tracker := NewFileStateTracker("/tmp/test_state.json", false)
	
	// Initially should not be dirty
	if tracker.IsDirty() {
		t.Error("Expected tracker to not be dirty initially")
	}
	
	// Mark a file as processed
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	// Should be dirty now
	if !tracker.IsDirty() {
		t.Error("Expected tracker to be dirty after marking file as processed")
	}
	
	// Save state
	err = tracker.SaveState()
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}
	
	// Should not be dirty after save
	if tracker.IsDirty() {
		t.Error("Expected tracker to not be dirty after saving")
	}
}

func TestAtomicSave(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "test_state.json")
	
	tracker := NewFileStateTracker(stateFile, false)
	
	// Mark a file as processed
	err := tracker.MarkProcessed("test-key", "test-etag", 1024)
	if err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}
	
	// Save state
	err = tracker.SaveState()
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}
	
	// Verify no temporary files are left behind
	tempFiles, err := filepath.Glob(stateFile + ".tmp*")
	if err != nil {
		t.Fatalf("Failed to check for temporary files: %v", err)
	}
	
	if len(tempFiles) > 0 {
		t.Errorf("Expected no temporary files, found: %v", tempFiles)
	}
	
	// Verify state file exists and is valid
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Error("Expected state file to exist")
	}
	
	// Verify content is valid JSON
	data, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("Failed to read state file: %v", err)
	}
	
	var state ProcessingState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("State file contains invalid JSON: %v", err)
	}
}