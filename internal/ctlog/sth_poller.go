package ctlog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// SignedTreeHead represents the structure of the JSON response for a get-sth request.
type SignedTreeHead struct {
	TreeSize uint64 `json:"tree_size"`
	// We only need tree_size for now, so other fields are omitted.
}

// STHPoller periodically fetches the Signed Tree Head to get the latest tree size.
type STHPoller struct {
	logURL         string
	httpClient     *http.Client
	logger         *slog.Logger
	latestTreeSize atomic.Uint64 // Thread-safe container for the tree size
	pollInterval   time.Duration
}

// NewSTHPoller creates and initializes a new STHPoller.
func NewSTHPoller(logURL string, logger *slog.Logger) *STHPoller {
	return &STHPoller{
		logURL: logURL,
		logger: logger.With("component", "sth_poller"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		pollInterval: 5 * time.Minute, // Default poll interval
	}
}

// Run starts the polling loop. It blocks until the context is canceled.
func (p *STHPoller) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	p.logger.Info("Starting STH Poller.")

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	// Perform an initial poll immediately on startup.
	if err := p.poll(); err != nil {
		p.logger.Error("Initial STH poll failed.", "error", err)
		// In a more robust system, we might want to retry with backoff here before starting the ticker.
	}

	for {
		select {
		case <-ticker.C:
			if err := p.poll(); err != nil {
				p.logger.Error("STH poll failed.", "error", err)
			}
		case <-ctx.Done():
			p.logger.Info("Stopping STH Poller.")
			return
		}
	}
}

// poll performs a single STH fetch and updates the atomic tree size.
func (p *STHPoller) poll() error {
	sthURL, err := url.JoinPath(p.logURL, "ct/v1/get-sth")
	if err != nil {
		return fmt.Errorf("could not create get-sth URL: %w", err)
	}

	resp, err := p.httpClient.Get(sthURL)
	if err != nil {
		return fmt.Errorf("http request to get-sth failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 status code from get-sth: %d", resp.StatusCode)
	}

	var sth SignedTreeHead
	if err := json.NewDecoder(resp.Body).Decode(&sth); err != nil {
		return fmt.Errorf("could not decode get-sth response: %w", err)
	}

	currentSize := p.LatestTreeSize()
	if sth.TreeSize > currentSize {
		p.latestTreeSize.Store(sth.TreeSize)
		p.logger.Info("New tree size detected.", "new_size", sth.TreeSize, "previous_size", currentSize)
	}

	return nil
}

// LatestTreeSize provides thread-safe access to the latest known tree size.
func (p *STHPoller) LatestTreeSize() uint64 {
	return p.latestTreeSize.Load()
}
