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
	ErrInvalidMerkleLeaf = errors.New("invalid merkle tree leaf")
	ErrNotX509Entry      = errors.New("log entry is not an x509 certificate")
)

// extractCertificateFromLeaf decodes the leaf_input and parses the MerkleTreeLeaf
// structure to extract the raw ASN.1 DER-encoded certificate.
// See RFC 6962, Section 3.4.
func extractCertificateFromLeaf(leafInput string) ([]byte, error) {
	// 1. Base64 decode the leaf_input.
	rawLeaf, err := base64.StdEncoding.DecodeString(leafInput)
	if err != nil {
		return nil, err
	}

	// 2. Parse the MerkleTreeLeaf structure.
	// struct {
	//     Version                uint8
	//     MerkleLeafType         uint8
	//     TimestampedEntry       // variable
	// } MerkleTreeLeaf;
	if len(rawLeaf) < 11 { // Basic sanity check for length
		return nil, ErrInvalidMerkleLeaf
	}

	leafVersion := rawLeaf[0]
	leafType := rawLeaf[1]
	if leafVersion != 0 || leafType != 0 { // We only care about TimestampedEntry leaves
		return nil, ErrInvalidMerkleLeaf
	}

	// 3. Parse the TimestampedEntry structure.
	// struct {
	//     Timestamp              uint64
	//     LogEntryType           uint16
	//     select (entry_type) {
	//         case x509_entry:      ASN.1Cert;
	//         case precert_entry:   PrecertEntry;
	//     } entry;
	// } TimestampedEntry;
	// We skip the 8-byte timestamp (bytes 2-9)
	entryType := binary.BigEndian.Uint16(rawLeaf[10:12])
	if entryType != 0 { // 0 = x509_entry
		return nil, ErrNotX509Entry
	}

	// The certificate length is a 3-byte uint24.
	certLen := uint32(rawLeaf[12])<<16 | uint32(rawLeaf[13])<<8 | uint32(rawLeaf[14])

	// Check if the indicated length is valid given the slice size.
	if len(rawLeaf) < 15+int(certLen) {
		return nil, ErrInvalidMerkleLeaf
	}

	// The certificate is the payload after this header.
	certBytes := rawLeaf[15 : 15+certLen]
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

func (p *FormattingWorkerPool) processResult(ctx context.Context, result *network.DownloadResult) {
	var response entriesResponse
	if err := json.Unmarshal(result.Data, &response); err != nil {
		p.logger.Warn("Failed to unmarshal get-entries JSON.", "error", err, "start", result.Job.Start)
		return
	}

	currentIndex := result.Job.Start
	for _, entry := range response.Entries {
		// 1. Extract the raw DER certificate bytes.
		certBytes, err := extractCertificateFromLeaf(entry.LeafInput)
		if err != nil {
			// This is expected for precerts, so we just skip them.
			currentIndex++
			continue
		}

		// 2. Get a buffer and write the log index.
		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteString(strconv.FormatUint(currentIndex, 10))
		buf.WriteByte(' ')

		// 3. Base64-encode the raw certificate bytes directly into the buffer.
		// This avoids the PEM headers entirely.
		encoder := base64.NewEncoder(base64.StdEncoding, buf)
		encoder.Write(certBytes)
		encoder.Close()

		// 4. Send the result down the pipeline.
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
