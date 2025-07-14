package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath" // Make sure filepath is imported
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

	go func() {
		slog.Info("Starting profiling server on http://localhost:6060/debug/pprof")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			slog.Error("Profiling server failed", "error", err)
		}
	}()

	// --- NEW: Define the log-specific base directory ---
	activeLog := cfg.ActiveLog
	logSpecificDir := filepath.Join(cfg.OutputDir, activeLog.GetCleanName())

	// Create the main directory for this specific log run.
	if err := os.MkdirAll(logSpecificDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: Could not create log-specific output directory %s: %v\n", logSpecificDir, err)
		os.Exit(1)
	}

	// Initialize the logger to write to the log-specific directory.
	logger, logFile := logging.New(logSpecificDir, cfg.LogFile)
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

	// Initialize and lock the State Manager in the log-specific directory.
	stateMgr, err := state.NewManager(logSpecificDir, logger)
	if err != nil {
		logger.Error("Failed to initialize state manager.", "error", err)
		os.Exit(1)
	}
	defer stateMgr.Close()

	// Read the starting index from disk.
	startIndex, err := stateMgr.ReadState()
	if err != nil {
		logger.Error("Failed to read initial state.", "error", err)
		os.Exit(1)
	}

	// --- Concurrency and Channel Sizing ---
	concurrencyPerProxy := activeLog.DownloadJobs
	totalPossibleConcurrency := concurrencyPerProxy
	if len(cfg.Proxies) > 0 {
		totalPossibleConcurrency = concurrencyPerProxy * len(cfg.Proxies)
	}

	logger.Info(
		"Certflow starting.",
		"log_dir", logSpecificDir, // Log the directory we're using
		"concurrency_per_proxy", concurrencyPerProxy,
		"total_possible_concurrency", totalPossibleConcurrency,
		"job_size", activeLog.DownloadSize,
		"batch_size", cfg.BatchSize,
		"resume_from_index", startIndex,
	)

	// --- Create Pipeline Channels ---
	jobsChan := make(chan *ctlog.DownloadJob, totalPossibleConcurrency*2)
	resultsChan := make(chan *network.DownloadResult, totalPossibleConcurrency*2)
	formattedChan := make(chan *processing.FormattedEntry, cfg.AggregatorBufferSize)
	gzipChan := make(chan string, 100)

	// --- Instantiate all pipeline components ---

	sthPoller := ctlog.NewSTHPoller(activeLog.URL, logger)
	jobGenerator := ctlog.NewJobGenerator(sthPoller, jobsChan, startIndex, activeLog.DownloadSize, cfg.Continuous, logger)
	proxyManager, err := network.NewProxyManager(activeLog.URL, concurrencyPerProxy, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize proxy manager.", "error", err)
		os.Exit(1)
	}
	formatterPool := processing.NewFormattingWorkerPool(resultsChan, formattedChan, logger)
	gzipper := processing.NewGZipper(gzipChan, logger)
	fileAggregator, err := processing.NewFileAggregator(
		stateMgr,
		formattedChan,
		logSpecificDir, // <-- Pass the log-specific path
		cfg.BatchSize,
		cfg.StateSaveTicker,
		gzipChan,
		cfg.AggregatorBufferSize,
		startIndex,
		logger,
	)
	if err != nil {
		logger.Error("Failed to initialize file aggregator.", "error", err)
		os.Exit(1)
	}
	display := ui.NewDisplay(sthPoller, fileAggregator, proxyManager, cfg.AggregatorBufferSize, jobsChan, resultsChan, formattedChan)

	// =================================================================
	//                       START THE PIPELINE & UI
	// =================================================================

	wg.Add(1)
	go sthPoller.Run(ctx, &wg)

	wg.Add(1)
	go jobGenerator.Run(ctx, &wg)

	numWorkers := 100
	logger.Info("Starting download worker pool", "worker_count", numWorkers)
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
	go gzipper.Run(ctx, &wg)

	wg.Add(1)
	go display.Run(ctx, &wg)

	<-ctx.Done()
	wg.Wait()

	logger.Info("Certflow has shut down gracefully.")
}
