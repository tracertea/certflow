package ctlog

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DownloadJob no longer needs the Retries field here, as the worker handles it internally.
type DownloadJob struct {
	Start uint64
	End   uint64
}

// JobGenerator is now simplified, without the requeue channel or retry logic.
type JobGenerator struct {
	logger       *slog.Logger
	sthPoller    *STHPoller
	jobsChan     chan<- *DownloadJob
	nextIndex    uint64
	downloadSize uint64
	isContinuous bool
}

// NewJobGenerator has the corrected, simpler signature.
func NewJobGenerator(sthPoller *STHPoller, jobsChan chan<- *DownloadJob, startIndex, downloadSize uint64, isContinuous bool, logger *slog.Logger) *JobGenerator {
	return &JobGenerator{
		logger:       logger.With("component", "job_generator"),
		sthPoller:    sthPoller,
		jobsChan:     jobsChan,
		nextIndex:    startIndex,
		downloadSize: downloadSize,
		isContinuous: isContinuous,
	}
}

// Run is now back to its simpler form. It only generates new jobs.
func (g *JobGenerator) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	// When the generator is done, close the jobs channel to signal to workers.
	defer close(g.jobsChan)

	g.logger.Info("Starting Job Generator.", "start_index", g.nextIndex, "job_size", g.downloadSize)

	// This is a simple ticker to prevent a fast-spinning loop when caught up.
	idleTicker := time.NewTicker(5 * time.Second)
	defer idleTicker.Stop()

	for {
		// Check for shutdown signal first.
		select {
		case <-ctx.Done():
			g.logger.Info("Stopping Job Generator.")
			return
		default:
		}

		latestTreeSize := g.sthPoller.LatestTreeSize()
		if latestTreeSize == 0 {
			time.Sleep(2 * time.Second) // Wait for the first STH poll to succeed.
			continue
		}

		if g.nextIndex >= latestTreeSize {
			if !g.isContinuous {
				g.logger.Info("Caught up to tree head in fixed-range mode. Shutting down.")
				return
			}

			// We are caught up. Wait for the next tick or for shutdown.
			select {
			case <-idleTicker.C:
				continue
			case <-ctx.Done():
				return
			}
		}

		// Calculate job range.
		end := g.nextIndex + g.downloadSize - 1
		if end >= latestTreeSize {
			end = latestTreeSize - 1
		}

		job := &DownloadJob{Start: g.nextIndex, End: end}

		// Send the new job, but respect the shutdown signal.
		select {
		case g.jobsChan <- job:
			g.nextIndex = end + 1
		case <-ctx.Done():
			// Don't log here, the main shutdown message is sufficient.
			return
		}
	}
}
