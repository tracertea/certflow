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
	bufferHighWaterMark int
	lastProcessedIndex  atomic.Uint64
	pendingBufferSize   atomic.Int64
	nextIndexToWrite    uint64
	pendingEntries      map[uint64]*FormattedEntry
	gzipChan            chan<- string
	openFiles           map[string]*os.File
}

// NewFileAggregator creates a new file aggregator.
func NewFileAggregator(stateMgr *state.Manager, formattedChan <-chan *FormattedEntry, outputDir string, batchSize uint64, stateSaveInterval time.Duration, gzipChan chan<- string, bufferSize int, startIndex uint64, logger *slog.Logger) *FileAggregator {
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
		gzipChan:            gzipChan,
		openFiles:           make(map[string]*os.File),
	}
	fa.lastProcessedIndex.Store(startIndex)
	return fa
}

// Run starts the aggregator loop. This must be run in a single goroutine.
func (a *FileAggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(a.gzipChan)
	a.logger.Info("Starting file aggregator.", "start_index", a.nextIndexToWrite, "buffer_size", a.bufferHighWaterMark)

	stateSaveTicker := time.NewTicker(a.stateSaveInterval)
	defer stateSaveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return

		case entry, ok := <-a.getReceiveChannel():
			if !ok {
				a.shutdown()
				return
			}
			a.pendingEntries[entry.LogIndex] = entry
			a.pendingBufferSize.Store(int64(len(a.pendingEntries))) // <-- UPDATE atomic counter

			a.flushBuffer()

		case <-stateSaveTicker.C:
			a.saveState()
		}
	}
}

func (a *FileAggregator) getReceiveChannel() <-chan *FormattedEntry {
	if len(a.pendingEntries) >= a.bufferHighWaterMark {
		a.logger.Warn("Aggregator buffer is full. Pausing consumption to apply backpressure.", "size", len(a.pendingEntries))
		return nil
	}
	return a.formattedChan
}

func (a *FileAggregator) flushBuffer() {
	for {
		entry, found := a.pendingEntries[a.nextIndexToWrite]
		if !found {
			break // The next sequential entry isn't available yet.
		}

		batchStart := (entry.LogIndex / a.batchSize) * a.batchSize
		filePath := filepath.Join(a.outputDir, fmt.Sprintf("%d.batch", batchStart))

		file, isOpen := a.openFiles[filePath]
		if !isOpen {
			var err error
			file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				a.logger.Error("FATAL: Failed to open batch file", "path", filePath, "error", err)
				return
			}
			a.openFiles[filePath] = file
		}

		entry.Buffer.WriteByte('\n')
		if _, err := file.Write(entry.Buffer.Bytes()); err != nil {
			a.logger.Error("Failed to write to batch file", "path", filePath, "error", err)
			return
		}

		// Update state and clean up the processed entry
		a.lastProcessedIndex.Store(entry.LogIndex)
		delete(a.pendingEntries, a.nextIndexToWrite)
		a.nextIndexToWrite++
		a.pendingBufferSize.Store(int64(len(a.pendingEntries)))

		// --- NEW: Check if this entry completes a batch file ---
		lastIndexInBatch := batchStart + a.batchSize - 1
		if entry.LogIndex == lastIndexInBatch {
			a.logger.Info("Completed batch file, queueing for compression", "path", filePath)
			if err := file.Close(); err != nil {
				a.logger.Error("Failed to close completed batch file", "path", filePath, "error", err)
			}
			delete(a.openFiles, filePath)
			a.gzipChan <- filePath
		}
	}
}

// Update shutdown to handle any remaining open, incomplete files.
func (a *FileAggregator) shutdown() {
	a.logger.Info("Aggregator shutting down. Flushing remaining buffer...")
	a.flushBuffer()

	// --- NEW: Close and queue any remaining open files for compression ---
	if len(a.openFiles) > 0 {
		a.logger.Info("Queueing remaining incomplete batch files for compression")
		for path, file := range a.openFiles {
			a.logger.Info("Closing and queueing incomplete batch file", "path", path)
			if err := file.Close(); err != nil {
				a.logger.Error("Failed to close batch file on shutdown", "path", path, "error", err)
			}
			a.gzipChan <- path
		}
	}

	a.saveState()
	if len(a.pendingEntries) > 0 {
		a.logger.Error("Aggregator shut down with pending entries in buffer, resulting in lost data", "count", len(a.pendingEntries), "next_needed", a.nextIndexToWrite)
	}
	a.logger.Info("File aggregator shut down.")
}

func (a *FileAggregator) saveState() {
	indexToSave := a.lastProcessedIndex.Load()
	if indexToSave > 0 {
		if err := a.stateMgr.WriteState(indexToSave + 1); err != nil {
			a.logger.Error("Failed to save state.", "error", err)
		} else {
			//a.logger.Info("Successfully saved progress.", "next_index", indexToSave+1)
		}
	}
}

// LastSavedIndex returns the last index successfully written to disk.
func (a *FileAggregator) LastSavedIndex() uint64 {
	return a.lastProcessedIndex.Load()
}

// PendingBufferSize returns the current number of entries in the re-ordering buffer.
// It is safe to call from other goroutines.
func (a *FileAggregator) PendingBufferSize() int64 {
	return a.pendingBufferSize.Load()
}
