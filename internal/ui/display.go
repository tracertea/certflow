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
)

// Display manages rendering the terminal UI.
type Display struct {
	sthPoller    *ctlog.STHPoller
	aggregator   *processing.FileAggregator
	proxyManager *network.ProxyManager

	// For rate calculation
	lastSampleTime   time.Time
	lastSampledIndex uint64
	currentRate      float64
}

// NewDisplay creates a new UI display renderer.
func NewDisplay(poller *ctlog.STHPoller, aggregator *processing.FileAggregator, proxyMgr *network.ProxyManager) *Display {
	return &Display{
		sthPoller:    poller,
		aggregator:   aggregator,
		proxyManager: proxyMgr,
	}
}

// Run starts the UI rendering loop. It should be run in a goroutine.
func (d *Display) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Hide cursor on start, show on exit.
	fmt.Print("\033[?25l")
	defer fmt.Print("\033[?25h")

	d.lastSampleTime = time.Now()
	ticker := time.NewTicker(500 * time.Millisecond) // Redraw twice a second
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.render()
		case <-ctx.Done():
			d.render() // One final render before exiting
			return
		}
	}
}

func (d *Display) render() {
	// Gather data
	currentIndex := d.aggregator.LastSavedIndex()
	totalSize := d.sthPoller.LatestTreeSize()
	proxyStatuses := d.proxyManager.GetStatuses()

	// Calculate progress and rate
	var progress float64
	if totalSize > 0 {
		progress = (float64(currentIndex) / float64(totalSize)) * 100
	}

	d.updateRate(currentIndex)
	eta := d.calculateETA(currentIndex, totalSize)

	// Use a string builder for efficient rendering
	var sb strings.Builder

	// Move cursor to top left and clear screen
	sb.WriteString("\033[H\033[2J")

	// Line 1: Progress Bar
	sb.WriteString(fmt.Sprintf("Progress: %s%.2f%%%s (%d / %d)\n", colorGreen, progress, colorReset, currentIndex, totalSize))
	sb.WriteString(d.buildProgressBar(progress))
	sb.WriteString("\n")

	// Line 2: Stats
	sb.WriteString(fmt.Sprintf("Rate: %s%.0f certs/s%s | ETA: %s%s%s\n\n", colorBlue, d.currentRate, colorReset, colorYellow, eta, colorReset))

	// Section 3: Proxies
	sb.WriteString("Proxy Status:\n")
	for _, status := range proxyStatuses {
		var stateColor string
		if status.State == "Healthy" {
			stateColor = colorGreen
		} else {
			stateColor = colorRed
		}
		sb.WriteString(fmt.Sprintf("  [%s%8s%s] %s (failures: %d)\n", stateColor, status.State, colorReset, status.URL, status.Failures))
	}

	// Print the entire frame at once
	fmt.Print(sb.String())
}

func (d *Display) buildProgressBar(progress float64) string {
	barWidth := 40
	filledWidth := int((progress / 100) * float64(barWidth))
	return fmt.Sprintf("[%s%s%s>%s]", colorGreen, strings.Repeat("=", filledWidth), colorReset, strings.Repeat(" ", barWidth-filledWidth))
}

func (d *Display) updateRate(currentIndex uint64) {
	now := time.Now()
	elapsed := now.Sub(d.lastSampleTime).Seconds()
	if elapsed < 1 { // Only update rate every second
		return
	}

	downloaded := currentIndex - d.lastSampledIndex
	d.currentRate = float64(downloaded) / elapsed

	d.lastSampleTime = now
	d.lastSampledIndex = currentIndex
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
