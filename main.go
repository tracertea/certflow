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
	// Load configuration first to determine logging paths.
	cfg, err := config.Load()
	if err != nil {
		// Use a temporary basic logger for pre-init errors as we don't have the config path yet.
		flag.Usage()
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Could not create output directory %s: %v\n", cfg.OutputDir, err)
		os.Exit(1)
	}

	// Initialize the file-based, multi-writer logger.
	logger, logFile := logging.New(cfg.OutputDir, cfg.LogFile)
	if logFile != nil {
		defer logFile.Close()
	}

	// Set up context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	// Listen for OS signals (Ctrl+C).
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

	// Read the starting index from the last saved state.
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

	// Create pipeline channels. The requeueChan has been removed.
	concurrency := activeLog.DownloadJobs
	jobsChan := make(chan *ctlog.DownloadJob, concurrency*2)
	resultsChan := make(chan *network.DownloadResult, concurrency*2)
	formattedChan := make(chan *processing.FormattedEntry, concurrency*4)

	// --- Instantiate all pipeline components ---

	sthPoller := ctlog.NewSTHPoller(activeLog.URL, logger)
	jobGenerator := ctlog.NewJobGenerator(sthPoller, jobsChan, startIndex, activeLog.DownloadSize, cfg.Continuous, logger)
	proxyManager, err := network.NewProxyManager(concurrency, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize proxy manager.", "error", err)
		os.Exit(1)
	}
	formatterPool := processing.NewFormattingWorkerPool(resultsChan, formattedChan, logger)
	outputBatchSize := uint64(100000)
	fileAggregator := processing.NewFileAggregator(stateMgr, formattedChan, cfg.OutputDir, outputBatchSize, cfg.StateSaveTicker, logger)
	display := ui.NewDisplay(sthPoller, fileAggregator, proxyManager)

	// =================================================================
	//                       START THE PIPELINE & UI
	// =================================================================

	// Start all pipeline stages as goroutines.
	wg.Add(1)
	go sthPoller.Run(ctx, &wg)

	wg.Add(1)
	go jobGenerator.Run(ctx, &wg)

	// Start the Download Worker Pool. Note the simplified constructor call.
	numWorkers := concurrency * len(cfg.Proxies)
	if numWorkers == 0 { // Account for direct mode (no proxies)
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

	// Start the UI renderer as the final goroutine.
	wg.Add(1)
	go display.Run(ctx, &wg)

	// Wait for the shutdown signal (from the context being cancelled).
	<-ctx.Done()

	// Wait for all goroutines in the WaitGroup to finish their cleanup.
	wg.Wait()

	// The UI will be gone now, so this final log message will be visible in the console and log file.
	logger.Info("Certflow has shut down gracefully.")
}
