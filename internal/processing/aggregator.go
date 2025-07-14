package processing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tracertea/certflow/internal/state"
)

// FileAggregator consumes formatted entries and writes them to batch files, managing state.
type FileAggregator struct {
	logger            *slog.Logger
	stateMgr          *state.Manager
	formattedChan     <-chan *FormattedEntry
	outputDir         string
	batchSize         uint64
	stateSaveInterval time.Duration
	lastSavedIndex    atomic.Uint64
}

// NewFileAggregator creates a new file aggregator.
func NewFileAggregator(stateMgr *state.Manager, formattedChan <-chan *FormattedEntry, outputDir string, batchSize uint64, stateSaveInterval time.Duration, logger *slog.Logger) *FileAggregator {
	return &FileAggregator{
		logger:            logger.With("component", "aggregator"),
		stateMgr:          stateMgr,
		formattedChan:     formattedChan,
		outputDir:         outputDir,
		batchSize:         batchSize,
		stateSaveInterval: stateSaveInterval,
	}
}

// Run starts the aggregator loop. This must be run in a single goroutine.
func (a *FileAggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	a.logger.Info("Starting file aggregator.")

	var currentFile *os.File
	var currentBatchStart uint64 = 0

	// This deferred function ensures that no matter how we exit the loop (gracefully),
	// the last file handle is closed and a final state save is attempted.
	defer func() {
		if currentFile != nil {
			if err := currentFile.Close(); err != nil {
				a.logger.Error("Failed to close final batch file on shutdown.", "error", err)
			}
		}
		// Perform one last state save on exit to capture the very last entry.
		a.saveState()
		a.logger.Info("File aggregator shut down.")
	}()

	stateSaveTicker := time.NewTicker(a.stateSaveInterval)
	defer stateSaveTicker.Stop()

	// This loop will only terminate when the 'formattedChan' is closed and drained.
	for {
		select {
		case entry, ok := <-a.formattedChan:
			if !ok {
				// The channel is closed and empty. This is the signal to shut down.
				// We simply return, and the deferred function above handles cleanup.
				return
			}

			// Determine which batch this entry belongs to.
			batchStart := (entry.LogIndex / a.batchSize) * a.batchSize

			// If it belongs to a new batch, close the old file and open a new one.
			if currentFile == nil || batchStart != currentBatchStart {
				if currentFile != nil {
					if err := currentFile.Close(); err != nil {
						a.logger.Error("Failed to close batch file.", "error", err)
						// This is a serious error, but we'll attempt to continue.
					}
				}

				filePath := filepath.Join(a.outputDir, fmt.Sprintf("%d.batch", batchStart))
				var err error
				currentFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					a.logger.Error("Failed to open new batch file, exiting aggregator.", "path", filePath, "error", err)
					// Cannot proceed if we can't write to files.
					return
				}
				currentBatchStart = batchStart
			}

			// Write the entry to the file, followed by a newline.
			entry.Buffer.WriteByte('\n')
			if _, err := currentFile.Write(entry.Buffer.Bytes()); err != nil {
				a.logger.Error("Failed to write to batch file.", "error", err)
			}

			// Update the latest index we have successfully processed.
			a.lastSavedIndex.Store(entry.LogIndex)

			// Return the buffer to the pool for reuse.
			bufferPool.Put(entry.Buffer)

		case <-stateSaveTicker.C:
			a.saveState()
		}
	}
}

func (a *FileAggregator) saveState() {
	indexToSave := a.lastSavedIndex.Load()
	if indexToSave > 0 { // Only save if we've actually processed something
		if err := a.stateMgr.WriteState(indexToSave); err != nil {
			a.logger.Error("Failed to save state.", "error", err)
		} else {
			a.logger.Info("Successfully saved progress.", "index", indexToSave)
		}
	}
}

func (a *FileAggregator) LastSavedIndex() uint64 {
	return a.lastSavedIndex.Load()
}
