package ui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tracertea/certflow/internal/ctlog"
	"github.com/tracertea/certflow/internal/network"
	"github.com/tracertea/certflow/internal/processing"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
)

type rateDataPoint struct {
	time  time.Time
	value uint64
}

type Display struct {
	sthPoller               *ctlog.STHPoller
	aggregator              *processing.FileAggregator
	proxyManager            *network.ProxyManager
	maxAggregatorBufferSize int

	// --- NEW: Add channel fields ---
	jobsChan      <-chan *ctlog.DownloadJob
	resultsChan   <-chan *network.DownloadResult
	formattedChan <-chan *processing.FormattedEntry
	// --- End NEW ---

	// For rolling average rate calculation
	rateWindow  time.Duration
	rateHistory []rateDataPoint
	currentRate float64

	// For per-proxy CPM calculation
	proxyHistory map[string][]rateDataPoint
}

// NewDisplay now accepts the max buffer size for display purposes.
func NewDisplay(
	poller *ctlog.STHPoller,
	aggregator *processing.FileAggregator,
	proxyMgr *network.ProxyManager,
	maxBufferSize int,
	jobsChan <-chan *ctlog.DownloadJob,
	resultsChan <-chan *network.DownloadResult,
	formattedChan <-chan *processing.FormattedEntry,
) *Display {
	// --- End NEW ---
	return &Display{
		sthPoller:               poller,
		aggregator:              aggregator,
		proxyManager:            proxyMgr,
		maxAggregatorBufferSize: maxBufferSize,
		// --- NEW: Assign the channels ---
		jobsChan:      jobsChan,
		resultsChan:   resultsChan,
		formattedChan: formattedChan,
		// --- End NEW ---
		rateWindow:   60 * time.Second,
		proxyHistory: make(map[string][]rateDataPoint),
	}
}
func (d *Display) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Print("\033[?25l")
	defer fmt.Print("\033[?25h")

	ticker := time.NewTicker(5000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.render()
		case <-ctx.Done():
			d.render()
			return
		}
	}
}

// In /internal/ui/display.go

func (d *Display) render() {
	// --- THIS SECTION IS NOW CORRECT ---
	// Gather data
	currentIndex := d.aggregator.LastSavedIndex()
	totalSize := d.sthPoller.LatestTreeSize()
	proxyStatuses := d.proxyManager.GetStatuses()
	bufferSize := d.aggregator.PendingBufferSize()

	// Get channel lengths and capacities
	jobsLen, jobsCap := len(d.jobsChan), cap(d.jobsChan)
	resultsLen, resultsCap := len(d.resultsChan), cap(d.resultsChan)
	formattedLen, formattedCap := len(d.formattedChan), cap(d.formattedChan)

	// Update and calculate rates
	d.updateOverallRate(currentIndex)
	proxyCpms := d.updateAndGetProxyCpms(proxyStatuses)
	eta := d.calculateETA(currentIndex, totalSize)
	// --- END CORRECTION ---

	var sb strings.Builder
	sb.WriteString("\033[H\033[2J")

	// Progress Bar
	var progress float64
	if totalSize > 0 {
		progress = (float64(currentIndex) / float64(totalSize)) * 100
	}
	sb.WriteString(fmt.Sprintf("Progress: %s%.2f%%%s (%d / %d)\n", colorGreen, progress, colorReset, currentIndex+1, totalSize))
	sb.WriteString(d.buildProgressBar(progress))
	sb.WriteString("\n")

	// --- MERGED AND CORRECTED STATS AND QUEUE LINES ---
	// Main stats line
	sb.WriteString(fmt.Sprintf("Rate (60s avg): %s%.0f certs/s%s | ETA: %s%s%s\n",
		colorBlue, d.currentRate, colorReset,
		colorYellow, eta, colorReset,
	))

	// Queue status line
	sb.WriteString(fmt.Sprintf("Queues: Jobs: %s%d/%d%s | Results: %s%d/%d%s | Formatted: %s%d/%d%s | Aggregator Buffer: %s%d/%d%s\n\n",
		colorPurple, jobsLen, jobsCap, colorReset,
		colorPurple, resultsLen, resultsCap, colorReset,
		colorPurple, formattedLen, formattedCap, colorReset,
		colorPurple, bufferSize, d.maxAggregatorBufferSize, colorReset,
	))
	// --- END CORRECTION ---

	// Proxies
	sb.WriteString("Proxy Status:\n")
	for _, status := range proxyStatuses {
		var stateColor string
		if status.State == "Healthy" {
			stateColor = colorGreen
		} else {
			stateColor = colorRed
		}
		cpm := proxyCpms[status.URL]
		// Round for cleaner display
		mean := status.Latency.Mean.Round(time.Millisecond)
		p99 := status.Latency.P99.Round(time.Millisecond)

		// --- MODIFIED: Display the mean and p99 latency ---
		sb.WriteString(fmt.Sprintf("  [%s%8s%s] %s | CPM: %s%6.0f%s | Latency (avg/p99): %s%s/%s%s\n",
			stateColor, status.State, colorReset,
			status.URL,
			colorPurple, cpm, colorReset,
			colorYellow, mean, p99, colorReset,
		))
	}

	fmt.Print(sb.String())
}

func (d *Display) buildProgressBar(progress float64) string {
	barWidth := 40
	filledWidth := int((progress / 100) * float64(barWidth))
	if filledWidth > barWidth {
		filledWidth = barWidth
	}
	return fmt.Sprintf("[%s%s%s%s]", colorGreen, strings.Repeat("=", filledWidth), ">", strings.Repeat(" ", barWidth-filledWidth))
}

func (d *Display) updateOverallRate(currentIndex uint64) {
	now := time.Now()
	d.rateHistory = append(d.rateHistory, rateDataPoint{time: now, value: currentIndex})

	cutoff := now.Add(-d.rateWindow)
	firstValidIndex := 0
	for i, dp := range d.rateHistory {
		if !dp.time.Before(cutoff) {
			firstValidIndex = i
			break
		}
	}
	d.rateHistory = d.rateHistory[firstValidIndex:]

	if len(d.rateHistory) < 2 {
		d.currentRate = 0
		return
	}
	first := d.rateHistory[0]
	last := d.rateHistory[len(d.rateHistory)-1]
	elapsedSeconds := last.time.Sub(first.time).Seconds()
	if elapsedSeconds < 1 {
		d.currentRate = 0
		return
	}
	d.currentRate = float64(last.value-first.value) / elapsedSeconds
}

func (d *Display) updateAndGetProxyCpms(statuses []network.ProxyStatus) map[string]float64 {
	now := time.Now()
	cpms := make(map[string]float64)
	cutoff := now.Add(-d.rateWindow)

	for _, status := range statuses {
		proxyURL := status.URL
		d.proxyHistory[proxyURL] = append(d.proxyHistory[proxyURL], rateDataPoint{time: now, value: status.CertsDownloaded})

		history := d.proxyHistory[proxyURL]
		firstValidIndex := 0
		for i, dp := range history {
			if !dp.time.Before(cutoff) {
				firstValidIndex = i
				break
			}
		}
		d.proxyHistory[proxyURL] = history[firstValidIndex:]

		currentHistory := d.proxyHistory[proxyURL]
		if len(currentHistory) < 2 {
			cpms[proxyURL] = 0
			continue
		}

		first := currentHistory[0]
		last := currentHistory[len(currentHistory)-1]
		elapsedSeconds := last.time.Sub(first.time).Seconds()
		if elapsedSeconds < 1 {
			cpms[proxyURL] = 0
			continue
		}
		certsPerSecond := float64(last.value-first.value) / elapsedSeconds
		cpms[proxyURL] = certsPerSecond * 60
	}
	return cpms
}

func (d *Display) calculateETA(current, total uint64) string {
	if d.currentRate < 1 || total <= current {
		return "n/a"
	}
	remaining := total - current
	seconds := int(float64(remaining) / d.currentRate)
	duration := time.Duration(seconds) * time.Second
	return duration.String()
}
