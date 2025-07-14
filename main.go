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

	// Read the starting index from disk.
	startIndex, err := stateMgr.ReadState()
	if err != nil {
		logger.Error("Failed to read initial state.", "error", err)
		os.Exit(1)
	}

	// --- Concurrency and Channel Sizing ---
	activeLog := cfg.ActiveLog
	concurrencyPerProxy := activeLog.DownloadJobs
	totalPossibleConcurrency := concurrencyPerProxy
	if len(cfg.Proxies) > 0 {
		totalPossibleConcurrency = concurrencyPerProxy * len(cfg.Proxies)
	}

	logger.Info(
		"Certflow starting.",
		"log_description", activeLog.Description,
		"log_url", activeLog.URL,
		"concurrency_per_proxy", concurrencyPerProxy,
		"total_possible_concurrency", totalPossibleConcurrency,
		"job_size", activeLog.DownloadSize,
		"batch_size", cfg.BatchSize,
		"aggregator_buffer", cfg.AggregatorBufferSize,
		"resume_from_index", startIndex,
	)

	// --- Create Pipeline Channels ---
	jobsChan := make(chan *ctlog.DownloadJob, totalPossibleConcurrency*2)
	resultsChan := make(chan *network.DownloadResult, totalPossibleConcurrency*2)
	formattedChan := make(chan *processing.FormattedEntry, cfg.AggregatorBufferSize)
	// Create a channel for completed batch files to be sent for compression.
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
	// Instantiate the new GZipper component.
	gzipper := processing.NewGZipper(gzipChan, logger)
	// Pass the new gzipChan to the FileAggregator constructor.
	fileAggregator := processing.NewFileAggregator(
		stateMgr,
		formattedChan,
		cfg.OutputDir,
		cfg.BatchSize,
		cfg.StateSaveTicker,
		gzipChan, // <-- Pass the new channel
		cfg.AggregatorBufferSize,
		startIndex,
		logger,
	)
	display := ui.NewDisplay(sthPoller, fileAggregator, proxyManager, cfg.AggregatorBufferSize, jobsChan, resultsChan, formattedChan)

	// =================================================================
	//                       START THE PIPELINE & UI
	// =================================================================

	wg.Add(1)
	go sthPoller.Run(ctx, &wg)

	wg.Add(1)
	go jobGenerator.Run(ctx, &wg)

	// Worker pool is a fixed, generous size. The ProxyManager enforces per-proxy limits.
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

	// Launch the GZipper goroutine.
	wg.Add(1)
	go gzipper.Run(ctx, &wg)

	wg.Add(1)
	go display.Run(ctx, &wg)

	<-ctx.Done()
	wg.Wait()

	logger.Info("Certflow has shut down gracefully.")
}
