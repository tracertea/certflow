package main

import (
	"context"
	"flag"
	"fmt"
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
	// Load configuration first.
	cfg, err := config.Load()
	if err != nil {
		flag.Usage()
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Could not create output directory %s: %v\n", cfg.OutputDir, err)
		os.Exit(1)
	}

	// Initialize the logger.
	logger, logFile := logging.New(cfg.OutputDir, cfg.LogFile)
	if logFile != nil {
		defer logFile.Close()
	}

	// Set up graceful shutdown context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-shutdownChan
		logger.Warn("Shutdown signal received, initiating shutdown.", "signal", sig.String())
		cancel()
	}()

	// Initialize and lock the State Manager.
	stateMgr, err := state.NewManager(cfg.OutputDir, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager.", "error", err)
		os.Exit(1)
	}
	defer stateMgr.Close()

	// Read the starting index from disk, which is now the source of truth.
	startIndex, err := stateMgr.ReadState()
	if err != nil {
		logger.Error("Failed to read initial state.", "error", err)
		os.Exit(1)
	}

	// Log initial configuration details.
	activeLog := cfg.ActiveLog
	logger.Info(
		"Certflow starting.",
		"log_description", activeLog.Description,
		"log_url", activeLog.URL,
		"concurrency_per_proxy", activeLog.DownloadJobs,
		"job_size", activeLog.DownloadSize,
		"batch_size", cfg.BatchSize,
		"aggregator_buffer", cfg.AggregatorBufferSize, // <-- Log new config
		"resume_from_index", startIndex,
	)

	// Create pipeline channels.
	concurrency := activeLog.DownloadJobs
	jobsChan := make(chan *ctlog.DownloadJob, concurrency*2)
	resultsChan := make(chan *network.DownloadResult, concurrency*2)
	// Make the formatted chan larger to accommodate the aggregator buffer
	formattedChan := make(chan *processing.FormattedEntry, cfg.AggregatorBufferSize)

	// --- Instantiate all pipeline components ---

	sthPoller := ctlog.NewSTHPoller(activeLog.URL, logger)
	jobGenerator := ctlog.NewJobGenerator(sthPoller, jobsChan, startIndex, activeLog.DownloadSize, cfg.Continuous, logger)
	proxyManager, err := network.NewProxyManager(activeLog.URL, concurrency, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize proxy manager.", "error", err)
		os.Exit(1)
	}
	formatterPool := processing.NewFormattingWorkerPool(resultsChan, formattedChan, logger)

	// MODIFIED: Pass the buffer size and the accurate start index to the aggregator.
	fileAggregator := processing.NewFileAggregator(stateMgr, formattedChan, cfg.OutputDir, cfg.BatchSize, cfg.StateSaveTicker, cfg.AggregatorBufferSize, startIndex, logger)
	display := ui.NewDisplay(sthPoller, fileAggregator, proxyManager)

	// =================================================================
	//                       START THE PIPELINE & UI
	// =================================================================

	wg.Add(1)
	go sthPoller.Run(ctx, &wg)

	wg.Add(1)
	go jobGenerator.Run(ctx, &wg)

	numWorkers := concurrency * len(cfg.Proxies)
	if numWorkers == 0 {
		numWorkers = concurrency
	}
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		worker := network.NewDownloadWorker(i, activeLog.URL, jobsChan, resultsChan, proxyManager, logger)
		go worker.Run(ctx, &wg)
	}

	wg.Add(1)
	go formatterPool.Run(ctx, &wg)

	wg.Add(1)
	go fileAggregator.Run(ctx, &wg)

	wg.Add(1)
	go display.Run(ctx, &wg)

	<-ctx.Done()
	wg.Wait()

	logger.Info("Certflow has shut down gracefully.")
}
