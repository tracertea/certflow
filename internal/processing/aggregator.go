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

// FileAggregator consumes formatted entries, re-orders them, and writes them to batch files.
type FileAggregator struct {
	logger              *slog.Logger
	stateMgr            *state.Manager
	formattedChan       <-chan *FormattedEntry
	outputDir           string
	batchSize           uint64
	stateSaveInterval   time.Duration
	bufferHighWaterMark int // Max items to hold in memory
	lastProcessedIndex  atomic.Uint64
	nextIndexToWrite    uint64 // The next log index we MUST write
	pendingEntries      map[uint64]*FormattedEntry
}

// NewFileAggregator creates a new file aggregator.
func NewFileAggregator(stateMgr *state.Manager, formattedChan <-chan *FormattedEntry, outputDir string, batchSize uint64, stateSaveInterval time.Duration, bufferSize int, startIndex uint64, logger *slog.Logger) *FileAggregator {
	fa := &FileAggregator{
		logger:              logger.With("component", "aggregator"),
		stateMgr:            stateMgr,
		formattedChan:       formattedChan,
		outputDir:           outputDir,
		batchSize:           batchSize,
		stateSaveInterval:   stateSaveInterval,
		bufferHighWaterMark: bufferSize,
		nextIndexToWrite:    startIndex,
		pendingEntries:      make(map[uint64]*FormattedEntry, bufferSize),
	}
	fa.lastProcessedIndex.Store(startIndex)
	return fa
}

// Run starts the aggregator loop. This must be run in a single goroutine.
func (a *FileAggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	a.logger.Info("Starting file aggregator.", "start_index", a.nextIndexToWrite, "buffer_size", a.bufferHighWaterMark)

	stateSaveTicker := time.NewTicker(a.stateSaveInterval)
	defer stateSaveTicker.Stop()

	// This is the main processing loop.
	for {
		// We use a select to manage multiple events: receiving new data, saving state, or shutting down.
		select {
		case <-ctx.Done():
			// Shutdown signal received. Perform final cleanup and exit.
			a.shutdown()
			return

		case entry, ok := <-a.getReceiveChannel():
			if !ok {
				// Channel is closed, meaning no more data will ever arrive.
				// Perform final cleanup and exit.
				a.shutdown()
				return
			}
			// Add the new entry to our re-ordering buffer.
			a.pendingEntries[entry.LogIndex] = entry

			// Attempt to write all sequential entries we now have.
			a.flushBuffer()

		case <-stateSaveTicker.C:
			a.saveState()
		}
	}
}

// getReceiveChannel is a helper that implements backpressure.
// It returns the channel to read from ONLY if the buffer is not full.
// If the buffer is full, it returns nil, which effectively disables the case in the select statement.
func (a *FileAggregator) getReceiveChannel() <-chan *FormattedEntry {
	if len(a.pendingEntries) >= a.bufferHighWaterMark {
		a.logger.Warn("Aggregator buffer is full. Pausing consumption to apply backpressure.", "size", len(a.pendingEntries))
		return nil // Returning nil makes the select case block forever.
	}
	return a.formattedChan
}

// flushBuffer checks for and writes all available sequential entries from the buffer to disk.
func (a *FileAggregator) flushBuffer() {
	// We use a map for the file handles to avoid opening/closing files repeatedly.
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	// Loop as long as we have the next required certificate in our buffer.
	for {
		entry, found := a.pendingEntries[a.nextIndexToWrite]
		if !found {
			break // We are missing the next certificate, so we must wait.
		}

		// Determine the correct batch file for this entry.
		batchStart := (entry.LogIndex / a.batchSize) * a.batchSize
		filePath := filepath.Join(a.outputDir, fmt.Sprintf("%d.batch", batchStart))

		// Get the file handle, opening the file if it's the first time we've seen it.
		file, isOpen := openFiles[filePath]
		if !isOpen {
			var err error
			file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				a.logger.Error("FATAL: Failed to open batch file, exiting aggregator.", "path", filePath, "error", err)
				return // Cannot proceed.
			}
			openFiles[filePath] = file
		}

		// Write the entry to the file.
		entry.Buffer.WriteByte('\n')
		if _, err := file.Write(entry.Buffer.Bytes()); err != nil {
			a.logger.Error("Failed to write to batch file.", "error", err)
		}

		// Return the buffer to the pool for reuse.
		bufferPool.Put(entry.Buffer)

		// IMPORTANT: Update state only after a successful write.
		a.lastProcessedIndex.Store(entry.LogIndex)
		delete(a.pendingEntries, a.nextIndexToWrite)
		a.nextIndexToWrite++
	}
}

// shutdown handles the final state saving and logging when the application is closing.
func (a *FileAggregator) shutdown() {
	a.logger.Info("Aggregator shutting down. Flushing remaining buffer...")
	a.flushBuffer() // Perform one last flush.
	a.saveState()   // Perform one last state save.
	if len(a.pendingEntries) > 0 {
		a.logger.Error("Aggregator shut down with pending entries in buffer. This will result in lost data.", "count", len(a.pendingEntries), "next_needed", a.nextIndexToWrite)
	}
	a.logger.Info("File aggregator shut down.")
}

// saveState writes the last successfully processed index to the state file.
func (a *FileAggregator) saveState() {
	// We save `lastProcessedIndex`, which is the index of the last cert successfully written to disk.
	indexToSave := a.lastProcessedIndex.Load()
	if indexToSave > 0 {
		if err := a.stateMgr.WriteState(indexToSave + 1); err != nil { // Save the *next* index to start from
			a.logger.Error("Failed to save state.", "error", err)
		} else {
			a.logger.Info("Successfully saved progress.", "next_index", indexToSave+1)
		}
	}
}

// LastSavedIndex returns the last index successfully written to disk.
func (a *FileAggregator) LastSavedIndex() uint64 {
	return a.lastProcessedIndex.Load()
}
