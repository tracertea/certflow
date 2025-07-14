package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tracertea/certflow/internal/config"
	"github.com/tracertea/certflow/internal/ctlog"
	"github.com/tracertea/certflow/internal/logging"
	"github.com/tracertea/certflow/internal/network"
	"github.com/tracertea/certflow/internal/processing"
	"github.com/tracertea/certflow/internal/state"
	"github.com/tracertea/certflow/internal/ui"
)

func main() {
	// 1. Initialize core components.
	logger := logging.New()
	cfg, err := config.Load()
	if err != nil {
		// Use structured logger for fatal startup errors.
		logger.Error("Configuration error.", "error", err)
		// Print default usage message to stdout for user convenience.
		flag.Usage()
		os.Exit(1)
	}

	// 2. Set up context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// 3. Listen for OS signals (Ctrl+C).
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-shutdownChan
		// Log the shutdown signal for debugging purposes. This will be overwritten by the UI but is useful in logs.
		logger.Warn("Shutdown signal received, initiating shutdown.", "signal", sig.String())
		cancel()
	}()

	// 4. Initialize and lock the State Manager.
	stateMgr, err := state.NewManager(cfg.OutputDir, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager.", "error", err)
		os.Exit(1)
	}
	defer stateMgr.Close()

	// 5. Read the starting index from the last saved state.
	startIndex, err := stateMgr.ReadState()
	if err != nil {
		logger.Error("Failed to read initial state.", "error", err)
		os.Exit(1)
	}

	// Log initial configuration details before the UI takes over the screen.
	activeLog := cfg.ActiveLog
	logger.Info(
		"Certflow starting.",
		"log_description", activeLog.Description,
		"log_url", activeLog.URL,
		"concurrency_per_proxy", activeLog.DownloadJobs,
		"job_size", activeLog.DownloadSize,
		"resume_from_index", startIndex,
	)

	// 6. Create pipeline channels and all processing components.
	concurrency := activeLog.DownloadJobs
	jobsChan := make(chan *ctlog.DownloadJob, concurrency*2)
	requeueChan := make(chan *ctlog.DownloadJob, concurrency*2)
	resultsChan := make(chan *network.DownloadResult, concurrency*2)
	formattedChan := make(chan *processing.FormattedEntry, concurrency*4)

	// Stage 0: STH Poller
	sthPoller := ctlog.NewSTHPoller(activeLog.URL, logger)

	// Stage 1: Job Generator
	jobGenerator := ctlog.NewJobGenerator(sthPoller, jobsChan, requeueChan, startIndex, activeLog.DownloadSize, cfg.Continuous, logger)

	// Networking Components
	proxyManager, err := network.NewProxyManager(concurrency, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize proxy manager.", "error", err)
		os.Exit(1)
	}

	// Stage 3: Formatting Worker Pool
	formatterPool := processing.NewFormattingWorkerPool(resultsChan, formattedChan, logger)

	// Stage 4: File Aggregator
	// We'll define a batch size for our output files. 100,000 is a reasonable default.
	outputBatchSize := uint64(100000)
	fileAggregator := processing.NewFileAggregator(stateMgr, formattedChan, cfg.OutputDir, outputBatchSize, cfg.StateSaveTicker, logger)

	// UI Component
	display := ui.NewDisplay(sthPoller, fileAggregator, proxyManager)

	// =================================================================
	//                       START THE PIPELINE & UI
	// =================================================================

	// Start all pipeline stages as goroutines.
	wg.Add(1)
	go sthPoller.Run(ctx, &wg)

	wg.Add(1)
	go jobGenerator.Run(ctx, &wg)

	// Stage 2: Download Worker Pool
	numWorkers := concurrency * len(cfg.Proxies)
	if numWorkers == 0 {
		numWorkers = concurrency
	}
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		worker := network.NewDownloadWorker(i, activeLog.URL, jobsChan, resultsChan, requeueChan, proxyManager, logger)
		go worker.Run(ctx, &wg)
	}

	wg.Add(1)
	go formatterPool.Run(ctx, &wg)

	wg.Add(1)
	go fileAggregator.Run(ctx, &wg)

	// Start the UI renderer as the final goroutine. It will take over the terminal.
	wg.Add(1)
	go display.Run(ctx, &wg)

	// Wait for the shutdown signal (from the context being cancelled).
	<-ctx.Done()

	// Wait for all goroutines in the WaitGroup to finish their cleanup.
	wg.Wait()

	// The UI will be gone now, so this final log message will be visible.
	logger.Info("Certflow has shut down gracefully.")
}
