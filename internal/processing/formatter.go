package processing

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"strconv"
	"sync"

	"github.com/tracertea/certflow/internal/network"
)

// entriesResponse is the structure of the JSON received from a get-entries endpoint.
type entriesResponse struct {
	Entries []struct {
		LeafInput string `json:"leaf_input"`
	} `json:"entries"`
}

// FormattedEntry holds a single line ready to be written to a batch file.
type FormattedEntry struct {
	LogIndex uint64
	// Using a buffer from a sync.Pool is more efficient than a string
	// for high-throughput scenarios as it reduces allocations.
	Buffer *bytes.Buffer
}

// bufferPool helps reuse buffers to reduce garbage collection pressure.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var (
	ErrInvalidMerkleLeaf   = errors.New("invalid merkle tree leaf")
	ErrUnknownLogEntryType = errors.New("unknown log entry type")
)

func extractCertificateFromLeaf(leafInput string) ([]byte, error) {
	rawLeaf, err := base64.StdEncoding.DecodeString(leafInput)
	if err != nil {
		return nil, err
	}

	if len(rawLeaf) < 2 { // Need at least version and leaf type
		return nil, ErrInvalidMerkleLeaf
	}
	// version := rawLeaf[0]
	leafType := rawLeaf[1]
	if leafType != 0 { // 0 = timestamped_entry
		return nil, ErrInvalidMerkleLeaf
	}

	// We have a timestamped entry, so we need at least 12 bytes total for the header.
	if len(rawLeaf) < 12 {
		return nil, ErrInvalidMerkleLeaf
	}

	entryType := binary.BigEndian.Uint16(rawLeaf[10:12])

	var certBytes []byte
	var certLenOffset int

	// CORRECTED: Handle both x509_entry and precert_entry
	switch entryType {
	case 0: // x509_entry
		certLenOffset = 12 // Length starts after timestamp(8) + entry_type(2)
		if len(rawLeaf) < certLenOffset+3 {
			return nil, ErrInvalidMerkleLeaf
		}
		certLen := uint32(rawLeaf[certLenOffset])<<16 | uint32(rawLeaf[certLenOffset+1])<<8 | uint32(rawLeaf[certLenOffset+2])
		certStart := certLenOffset + 3
		if len(rawLeaf) < certStart+int(certLen) {
			return nil, ErrInvalidMerkleLeaf
		}
		certBytes = rawLeaf[certStart : certStart+int(certLen)]

	case 1: // precert_entry
		// A precert has a 32-byte issuer key hash before the certificate data.
		// We skip the hash to get to the certificate.
		certLenOffset = 12 + 32 // Length starts after timestamp(8) + entry_type(2) + issuer_key_hash(32)
		if len(rawLeaf) < certLenOffset+3 {
			return nil, ErrInvalidMerkleLeaf
		}
		certLen := uint32(rawLeaf[certLenOffset])<<16 | uint32(rawLeaf[certLenOffset+1])<<8 | uint32(rawLeaf[certLenOffset+2])
		certStart := certLenOffset + 3
		if len(rawLeaf) < certStart+int(certLen) {
			return nil, ErrInvalidMerkleLeaf
		}
		certBytes = rawLeaf[certStart : certStart+int(certLen)]

	default:
		return nil, ErrUnknownLogEntryType
	}

	return certBytes, nil
}

// FormattingWorkerPool manages a pool of goroutines that format raw download results.
type FormattingWorkerPool struct {
	logger        *slog.Logger
	resultsChan   <-chan *network.DownloadResult
	formattedChan chan<- *FormattedEntry
}

// NewFormattingWorkerPool creates a new formatter pool.
func NewFormattingWorkerPool(resultsChan <-chan *network.DownloadResult, formattedChan chan<- *FormattedEntry, logger *slog.Logger) *FormattingWorkerPool {
	return &FormattingWorkerPool{
		logger:        logger.With("component", "formatter_pool"),
		resultsChan:   resultsChan,
		formattedChan: formattedChan,
	}
}

// Run starts the formatting workers. The number of workers is based on the number of available CPU cores.
func (p *FormattingWorkerPool) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(p.formattedChan) // Close downstream channel when this stage is done.

	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2 // Ensure at least 2 workers
	}
	p.logger.Info("Starting formatting worker pool.", "worker_count", numWorkers)

	var workerWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go p.worker(ctx, &workerWg, i)
	}

	workerWg.Wait()
	p.logger.Info("Formatting worker pool shut down.")
}

func (p *FormattingWorkerPool) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	for {
		select {
		case result, ok := <-p.resultsChan:
			if !ok {
				// resultsChan is closed, so we are done.
				return
			}
			p.processResult(ctx, result)
		case <-ctx.Done():
			// Shutdown signal received.
			return
		}
	}
}

// processResult now uses the improved extraction function.
func (p *FormattingWorkerPool) processResult(ctx context.Context, result *network.DownloadResult) {
	p.logger.Debug("Formatter received result.", "start", result.Job.Start, "end", result.Job.End)

	var response entriesResponse
	if err := json.Unmarshal(result.Data, &response); err != nil {
		p.logger.Warn("Failed to unmarshal get-entries JSON.", "error", err, "start", result.Job.Start)
		return
	}

	currentIndex := result.Job.Start
	for _, entry := range response.Entries {
		certBytes, err := extractCertificateFromLeaf(entry.LeafInput)
		if err != nil {
			// This is now normal behavior for unknown log types, so we use Debug level.
			p.logger.Debug("Skipping entry: could not extract certificate.", "index", currentIndex, "reason", err.Error())
			currentIndex++
			continue
		}

		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteString(strconv.FormatUint(currentIndex, 10))
		buf.WriteByte(' ')

		encoder := base64.NewEncoder(base64.StdEncoding, buf)
		encoder.Write(certBytes)
		encoder.Close()

		formatted := FormattedEntry{
			LogIndex: currentIndex,
			Buffer:   buf,
		}

		select {
		case p.formattedChan <- &formatted:
		case <-ctx.Done():
			bufferPool.Put(buf)
			return
		}

		currentIndex++
	}
}
