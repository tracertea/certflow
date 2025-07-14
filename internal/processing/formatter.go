package processing

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"runtime"
	"strconv"
	"sync"

	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/tls"
	jsoniter "github.com/json-iterator/go"
	"github.com/tracertea/certflow/internal/network"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// entriesResponse is the structure of the JSON received from a get-entries endpoint.
type entriesResponse struct {
	Entries []struct {
		LeafInput string `json:"leaf_input"`
	} `json:"entries"`
}

// FormattedEntry holds a single line ready to be written to a batch file.
type FormattedEntry struct {
	LogIndex uint64
	Buffer   *bytes.Buffer
}

// bufferPool helps reuse buffers to reduce garbage collection pressure.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var (
	ErrLogEntryUnsupported = errors.New("unsupported log entry type")
)

// extractCertificateFromLeaf uses the official CT library to parse the leaf.
func extractCertificateData(leafInput string) (derBytes []byte, entryType ct.LogEntryType, err error) {
	leafBytes, err := base64.StdEncoding.DecodeString(leafInput)
	if err != nil {
		return nil, 0, err
	}

	var leaf ct.MerkleTreeLeaf
	if _, err := tls.Unmarshal(leafBytes, &leaf); err != nil {
		return nil, 0, err
	}

	entryType = leaf.TimestampedEntry.EntryType
	switch entryType {
	case ct.X509LogEntryType:
		derBytes = leaf.TimestampedEntry.X509Entry.Data
	case ct.PrecertLogEntryType:
		derBytes = leaf.TimestampedEntry.PrecertEntry.TBSCertificate
	default:
		err = ErrLogEntryUnsupported
	}
	return
}

// getFinalCertificateDer correctly reconstructs the final certificate using the
// available library functions from the provided ct.go source.
func getFinalCertificateDer(entry ct.LeafEntry) ([]byte, error) {
	// First, we always need to parse the leaf_input to determine the entry type.
	var leaf ct.MerkleTreeLeaf
	if _, err := tls.Unmarshal(entry.LeafInput, &leaf); err != nil {
		return nil, err
	}

	switch leaf.TimestampedEntry.EntryType {
	case ct.X509LogEntryType:
		// For a standard certificate, the data is in the leaf input.
		return leaf.TimestampedEntry.X509Entry.Data, nil

	case ct.PrecertLogEntryType:
		// For a precertificate, the final "poisoned" cert is in the extra_data field.
		// We must unmarshal extra_data as a PrecertChainEntry.
		var chainEntry ct.PrecertChainEntry
		if _, err := tls.Unmarshal(entry.ExtraData, &chainEntry); err != nil {
			return nil, err
		}
		// The PreCertificate field contains the ASN1Cert we need.
		return chainEntry.PreCertificate.Data, nil

	default:
		return nil, ErrLogEntryUnsupported
	}
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

// Run starts the formatting workers.
func (p *FormattingWorkerPool) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(p.formattedChan)

	numWorkers := runtime.NumCPU()
	if numWorkers < 2 {
		numWorkers = 2
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
				return
			}
			p.processResult(ctx, result)
		case <-ctx.Done():
			return
		}
	}
}

// processResult logic remains the same.
func (p *FormattingWorkerPool) processResult(ctx context.Context, result *network.DownloadResult) {
	//p.logger.Debug("Formatter received result.", "start", result.Job.Start, "end", result.Job.End)

	var response ct.GetEntriesResponse                             // Use the correct struct from the ct package
	if err := json.Unmarshal(result.Data, &response); err != nil { // <-- This now uses the faster library
		p.logger.Warn("Failed to unmarshal get-entries JSON.", "error", err, "start", result.Job.Start)
		return
	}

	currentIndex := result.Job.Start
	for _, entry := range response.Entries {
		// Pass the entire entry struct, which contains both leaf_input and extra_data.
		derBytes, err := getFinalCertificateDer(entry)
		if err != nil {
			p.logger.Debug("Skipping entry: could not process leaf.", "index", currentIndex, "reason", err.Error())
			currentIndex++
			continue
		}

		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteString(strconv.FormatUint(currentIndex, 10))
		buf.WriteByte(' ')

		encoder := base64.NewEncoder(base64.StdEncoding, buf)
		encoder.Write(derBytes)
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
