package network

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"

	"github.com/tracertea/certflow/internal/ctlog"
)

// DownloadResult holds the raw data from a successful get-entries request.
type DownloadResult struct {
	Job  *ctlog.DownloadJob
	Data []byte
}

// DownloadWorker is a goroutine that fetches certificate entries.
type DownloadWorker struct {
	id           int
	logURL       string
	jobsChan     <-chan *ctlog.DownloadJob
	resultsChan  chan<- *DownloadResult
	requeueChan  chan<- *ctlog.DownloadJob
	proxyManager *ProxyManager
	logger       *slog.Logger
}

// NewDownloadWorker creates a new worker.
func NewDownloadWorker(id int, logURL string, jobsChan <-chan *ctlog.DownloadJob, resultsChan chan<- *DownloadResult, requeueChan chan<- *ctlog.DownloadJob, proxyMgr *ProxyManager, logger *slog.Logger) *DownloadWorker {
	return &DownloadWorker{
		id:           id,
		logURL:       logURL,
		jobsChan:     jobsChan,
		resultsChan:  resultsChan,
		requeueChan:  requeueChan,
		proxyManager: proxyMgr,
		logger:       logger.With("worker_id", id),
	}
}

// Run starts the worker loop. It will exit when the jobsChan is closed.
func (w *DownloadWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	w.logger.Info("Download worker starting.")

	for {
		select {
		case job, ok := <-w.jobsChan:
			if !ok {
				w.logger.Info("Jobs channel closed, worker shutting down.")
				return
			}
			w.processJob(ctx, job)
		case <-ctx.Done():
			w.logger.Info("Shutdown signal received, worker shutting down.")
			return
		}
	}
}

func (w *DownloadWorker) processJob(ctx context.Context, job *ctlog.DownloadJob) {
	// 1. Get a proxy client. This call will block until one is available.
	proxy, err := w.proxyManager.GetClient()
	if err != nil {
		w.logger.Error("Failed to get a proxy client.", "error", err)
		job.Retries++
		w.requeueJob(ctx, job)
		return
	}

	// 2. Defer the release of the client, reporting success or failure.
	var success bool
	defer func() {
		w.proxyManager.ReleaseClient(proxy, success)
	}()

	// 3. Build the request URL.
	getEntriesURL, err := url.Parse(w.logURL)
	if err != nil {
		w.logger.Error("Base log URL is invalid.", "url", w.logURL, "error", err)
		return
	}
	getEntriesURL, _ = getEntriesURL.Parse("ct/v1/get-entries") // Appending path
	q := getEntriesURL.Query()
	q.Set("start", fmt.Sprint(job.Start))
	q.Set("end", fmt.Sprint(job.End))
	getEntriesURL.RawQuery = q.Encode()

	// 4. Execute the request.
	req, _ := http.NewRequestWithContext(ctx, "GET", getEntriesURL.String(), nil)
	resp, err := proxy.client.Do(req)
	if err != nil {
		w.logger.Warn("get-entries request failed, will retry.", "start", job.Start, "proxy", proxy.URL, "error", err)
		job.Retries++
		w.requeueJob(ctx, job)
		return // success remains false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.logger.Warn("get-entries non-200 status, will retry.", "status", resp.StatusCode, "proxy", proxy.URL)
		job.Retries++
		w.requeueJob(ctx, job)
		return // success remains false
	}

	// 5. Read the response body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		w.logger.Warn("Failed to read response body, will retry.", "proxy", proxy.URL, "error", err)
		job.Retries++
		w.requeueJob(ctx, job)
		return // success remains false
	}

	// 6. Success! Send the result to the next stage.
	result := &DownloadResult{Job: job, Data: body}
	select {
	case w.resultsChan <- result:
		success = true // Mark as successful only after sending
	case <-ctx.Done():
		w.logger.Info("Shutdown during result send.")
	}
}

// requeueJob is a new helper to safely send to the requeue channel.
func (w *DownloadWorker) requeueJob(ctx context.Context, job *ctlog.DownloadJob) {
	select {
	case w.requeueChan <- job:
		// Job successfully requeued.
	case <-ctx.Done():
		w.logger.Warn("Shutdown occurred, could not requeue failed job.", "start", job.Start, "end", job.End)
	}
}
