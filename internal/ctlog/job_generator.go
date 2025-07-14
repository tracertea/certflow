package ctlog

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DownloadJob now includes a retry counter.
type DownloadJob struct {
	Start   uint64
	End     uint64
	Retries int // <-- NEW
}

// JobGenerator now manages requeued jobs.
type JobGenerator struct {
	logger       *slog.Logger
	sthPoller    *STHPoller
	jobsChan     chan<- *DownloadJob
	requeueChan  <-chan *DownloadJob // <-- NEW: Receives failed jobs
	nextIndex    uint64
	downloadSize uint64
	isContinuous bool
	maxRetries   int // <-- NEW
}

// NewJobGenerator has a new signature.
func NewJobGenerator(sthPoller *STHPoller, jobsChan chan<- *DownloadJob, requeueChan <-chan *DownloadJob, startIndex, downloadSize uint64, isContinuous bool, logger *slog.Logger) *JobGenerator {
	return &JobGenerator{
		logger:       logger.With("component", "job_generator"),
		sthPoller:    sthPoller,
		jobsChan:     jobsChan,
		requeueChan:  requeueChan,
		nextIndex:    startIndex,
		downloadSize: downloadSize,
		isContinuous: isContinuous,
		maxRetries:   5, // Allow up to 5 retries for a job.
	}
}

// Run is now more complex, using a select to handle new jobs and retries.
func (g *JobGenerator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(g.jobsChan)

	g.logger.Info("Starting Job Generator.", "start_index", g.nextIndex, "job_size", g.downloadSize)

	for {
		// Priority 1: Always try to process a requeued job first.
		select {
		case job := <-g.requeueChan:
			if job.Retries < g.maxRetries {
				g.logger.Warn("Requeuing job.", "start", job.Start, "end", job.End, "retries", job.Retries)
				g.jobsChan <- job // Send it back to the workers
			} else {
				g.logger.Error("Job failed permanently after max retries.", "start", job.Start, "end", job.End)
			}
			continue // Go back to the top of the loop to check for more requeued jobs.
		default:
			// No requeued jobs waiting, proceed with normal logic.
		}

		// Priority 2: Check for shutdown.
		select {
		case <-ctx.Done():
			g.logger.Info("Stopping Job Generator.")
			return
		default:
		}

		latestTreeSize := g.sthPoller.LatestTreeSize()
		if latestTreeSize == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		if g.nextIndex >= latestTreeSize {
			if !g.isContinuous {
				g.logger.Info("Caught up to tree head. Shutting down.")
				return
			}
			// Wait for new certs or a requeued job.
			select {
			case job := <-g.requeueChan:
				if job.Retries < g.maxRetries {
					g.logger.Warn("Requeuing job.", "start", job.Start, "end", job.End, "retries", job.Retries)
					g.jobsChan <- job
				} else {
					g.logger.Error("Job failed permanently after max retries.", "start", job.Start, "end", job.End)
				}
			case <-time.After(5 * time.Second):
				// Waited long enough, loop again to check STH.
			case <-ctx.Done():
				return // Shutdown
			}
			continue
		}

		// Priority 3: Generate a new job.
		end := g.nextIndex + g.downloadSize - 1
		if end >= latestTreeSize {
			end = latestTreeSize - 1
		}

		job := &DownloadJob{Start: g.nextIndex, End: end, Retries: 0}

		select {
		case g.jobsChan <- job:
			g.nextIndex = end + 1
		case <-ctx.Done():
			return
		}
	}
}
