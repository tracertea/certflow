package network

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tracertea/certflow/internal/ctlog"
)

const maxJobRetries = 10

type DownloadResult struct {
	Job  *ctlog.DownloadJob
	Data []byte
}

type DownloadWorker struct {
	id           int
	logURL       string
	jobsChan     <-chan *ctlog.DownloadJob
	resultsChan  chan<- *DownloadResult
	proxyManager *ProxyManager
	logger       *slog.Logger
}

func NewDownloadWorker(id int, logURL string, jobsChan <-chan *ctlog.DownloadJob, resultsChan chan<- *DownloadResult, proxyMgr *ProxyManager, logger *slog.Logger) *DownloadWorker {
	return &DownloadWorker{
		id:           id,
		logURL:       logURL,
		jobsChan:     jobsChan,
		resultsChan:  resultsChan,
		proxyManager: proxyMgr,
		logger:       logger.With("worker_id", id),
	}
}

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
	getEntriesURL, err := url.Parse(w.logURL)
	if err != nil {
		w.logger.Error("Base log URL is invalid, dropping job.", "start", job.Start, "error", err)
		return
	}
	getEntriesURL, _ = getEntriesURL.Parse("ct/v1/get-entries")
	q := getEntriesURL.Query()
	q.Set("start", fmt.Sprint(job.Start))
	q.Set("end", fmt.Sprint(job.End))
	getEntriesURL.RawQuery = q.Encode()

	for attempt := 0; attempt < maxJobRetries; attempt++ {
		if ctx.Err() != nil {
			return // Exit if the application is shutting down.
		}

		if attempt > 0 {
			w.logger.Warn("Retrying job.", "start", job.Start, "attempt", attempt+1)
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}

		proxy, err := w.proxyManager.GetClient()
		if err != nil {
			w.logger.Warn("Download attempt failed: could not get proxy.", "start", job.Start, "attempt", attempt+1, "error", err)
			continue
		}

		// --- NEW, ROBUST RELEASE PATTERN ---
		var success bool
		var certCountInJob uint64
		var duration time.Duration
		// Defer the release call. It will execute when the function returns,
		// using the final values of the variables.
		defer func() {
			w.proxyManager.ReleaseClient(proxy, success, certCountInJob, duration)
		}()
		// --- END NEW ---

		req, _ := http.NewRequestWithContext(ctx, "GET", getEntriesURL.String(), nil)

		// Time the network request
		startTime := time.Now()
		resp, err := proxy.client.Do(req)
		duration = time.Since(startTime)

		if err != nil {
			w.logger.Warn("Download attempt failed: http request error.", "start", job.Start, "proxy", proxy.URL, "error", err, "duration", duration)
			continue // defer will run, releasing the client with success=false
		}

		if resp.StatusCode != http.StatusOK {
			w.logger.Warn("Download attempt failed: non-200 status.", "start", job.Start, "proxy", proxy.URL, "status", resp.StatusCode, "duration", duration)
			resp.Body.Close()
			continue // defer will run, releasing the client with success=false
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			w.logger.Warn("Download attempt failed: could not read body.", "start", job.Start, "proxy", proxy.URL, "error", err, "duration", duration)
			continue // defer will run, releasing the client with success=false
		}

		// SUCCESS!
		result := &DownloadResult{Job: job, Data: body}
		select {
		case w.resultsChan <- result:
			success = true // Mark as success for the deferred release call.
			certCountInJob = (job.End - job.Start) + 1
			return // defer will run, releasing the client correctly.
		case <-ctx.Done():
			// Context cancelled while trying to send.
			return // defer will run, releasing client with success=false.
		}
	}

	w.logger.Error("CRITICAL: Job failed permanently and was dropped.", "start", job.Start, "end", job.End)
}
