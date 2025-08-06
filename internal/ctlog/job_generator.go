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

	wasIdle := false
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

		// Generate jobs until caught up or interrupted
		jobsCreated := 0
		for g.nextIndex < latestTreeSize {
			// Check for shutdown during job generation
			select {
			case <-ctx.Done():
				g.logger.Info("Stopping Job Generator during job creation.")
				return
			default:
			}

			// Calculate job range.
			end := g.nextIndex + g.downloadSize - 1
			if end >= latestTreeSize {
				end = latestTreeSize - 1
			}

			job := &DownloadJob{Start: g.nextIndex, End: end}

			// Send the new job, but respect the shutdown signal and channel capacity.
			select {
			case g.jobsChan <- job:
				g.nextIndex = end + 1
				jobsCreated++
				// Log job creation for debugging
				if (end-job.Start+1) < g.downloadSize {
					// This is a partial job at the tree boundary
					g.logger.Debug("Created partial job at tree boundary",
						"start", job.Start, "end", job.End, "size", end-job.Start+1)
				}
			case <-ctx.Done():
				// Don't log here, the main shutdown message is sufficient.
				return
			default:
				// Channel is full, break out of inner loop to check for updates
				if jobsCreated > 0 {
					g.logger.Debug("Job channel full, created jobs so far",
						"jobs_created", jobsCreated, "next_index", g.nextIndex)
				}
				// Give workers time to process jobs
				time.Sleep(100 * time.Millisecond)
				// Continue outer loop to re-check conditions
				break
			}
		}

		// Log batch job creation if we were previously idle
		if wasIdle && jobsCreated > 0 {
			g.logger.Info("Resumed from idle and created jobs",
				"jobs_created", jobsCreated, "next_index", g.nextIndex, "tree_size", latestTreeSize)
			wasIdle = false
		}

		// Check if we're caught up now
		if g.nextIndex >= latestTreeSize {
			if !g.isContinuous {
				g.logger.Info("Caught up to tree head in fixed-range mode. Shutting down.")
				return
			}

			// Log when entering idle state
			if !wasIdle {
				g.logger.Info("Caught up to tree head in continuous mode. Waiting for new entries.",
					"next_index", g.nextIndex, "tree_size", latestTreeSize)
				wasIdle = true
			}

			// We are caught up. Wait for the next tick or for shutdown.
			select {
			case <-idleTicker.C:
				// Re-fetch tree size after waking up
				newTreeSize := g.sthPoller.LatestTreeSize()
				if newTreeSize > g.nextIndex {
					g.logger.Info("New entries detected after idle. Resuming job generation.",
						"next_index", g.nextIndex, "new_tree_size", newTreeSize,
						"new_entries", newTreeSize-g.nextIndex)
					wasIdle = false
				}
				// Continue to outer loop to generate jobs
			case <-ctx.Done():
				return
			}
		}
	}
}
