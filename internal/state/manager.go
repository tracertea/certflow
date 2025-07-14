package state

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gofrs/flock"
)

const (
	stateFileName = "downloader.state"
	lockFileName  = ".lock"
)

// Manager handles the lifecycle of the state and lock files.
type Manager struct {
	lock      *flock.Flock
	statePath string
	logger    *slog.Logger
}

// NewManager creates a new StateManager, creating the output directory and
// acquiring a file lock. It returns an error if the lock is already held.
func NewManager(outputDir string, logger *slog.Logger) (*Manager, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create output directory %s: %w", outputDir, err)
	}

	lockPath := filepath.Join(outputDir, lockFileName)
	fileLock := flock.New(lockPath)

	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("could not acquire file lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("output directory %s is locked by another certflow instance", outputDir)
	}

	logger.Info("Acquired file lock.", "path", lockPath)

	return &Manager{
		lock:      fileLock,
		statePath: filepath.Join(outputDir, stateFileName),
		logger:    logger,
	}, nil
}

// ReadState reads the last saved index from the state file.
// It returns 0 if the file does not exist or is empty.
func (m *Manager) ReadState() (uint64, error) {
	data, err := os.ReadFile(m.statePath)
	if os.IsNotExist(err) {
		m.logger.Info("State file not found, starting from index 0.", "path", m.statePath)
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("could not read state file: %w", err)
	}

	content := strings.TrimSpace(string(data))
	if content == "" {
		m.logger.Info("State file is empty, starting from index 0.")
		return 0, nil
	}

	index, err := strconv.ParseUint(content, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse state file content: %w", err)
	}

	return index, nil
}

// WriteState atomically writes the current index to the state file.
func (m *Manager) WriteState(index uint64) error {
	tempFile, err := os.CreateTemp(filepath.Dir(m.statePath), "state-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp state file: %w", err)
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.WriteString(strconv.FormatUint(index, 10)); err != nil {
		return fmt.Errorf("failed to write to temp state file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp state file: %w", err)
	}

	if err := os.Rename(tempFile.Name(), m.statePath); err != nil {
		return fmt.Errorf("failed to atomically move state file: %w", err)
	}

	return nil
}

// Close releases the file lock.
func (m *Manager) Close() {
	if err := m.lock.Unlock(); err != nil {
		m.logger.Error("Failed to release file lock.", "error", err)
	} else {
		m.logger.Info("Released file lock.")
	}
	// The lock file itself is left behind as a breadcrumb, which is fine.
}
