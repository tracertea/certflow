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
	pendingBufferSize   atomic.Int64 // <-- NEW: Safely expose buffer size to UI.
	nextIndexToWrite    uint64
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
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	var wroteData bool
	for {
		entry, found := a.pendingEntries[a.nextIndexToWrite]
		if !found {
			break
		}
		wroteData = true

		batchStart := (entry.LogIndex / a.batchSize) * a.batchSize
		filePath := filepath.Join(a.outputDir, fmt.Sprintf("%d.batch", batchStart))

		file, isOpen := openFiles[filePath]
		if !isOpen {
			var err error
			file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				a.logger.Error("FATAL: Failed to open batch file, exiting aggregator.", "path", filePath, "error", err)
				return
			}
			openFiles[filePath] = file
		}

		entry.Buffer.WriteByte('\n')
		if _, err := file.Write(entry.Buffer.Bytes()); err != nil {
			a.logger.Error("Failed to write to batch file.", "error", err)
		}

		bufferPool.Put(entry.Buffer)

		a.lastProcessedIndex.Store(entry.LogIndex)
		delete(a.pendingEntries, a.nextIndexToWrite)
		a.nextIndexToWrite++
	}

	if wroteData {
		a.pendingBufferSize.Store(int64(len(a.pendingEntries))) // <-- UPDATE atomic counter after flushing
	}
}

func (a *FileAggregator) shutdown() {
	a.logger.Info("Aggregator shutting down. Flushing remaining buffer...")
	a.flushBuffer()
	a.saveState()
	if len(a.pendingEntries) > 0 {
		a.logger.Error("Aggregator shut down with pending entries in buffer. This will result in lost data.", "count", len(a.pendingEntries), "next_needed", a.nextIndexToWrite)
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
