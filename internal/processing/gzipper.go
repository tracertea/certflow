// in /internal/processing/gzipper.go

package processing

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// GZipper listens for file paths on a channel and compresses them in the background.
type GZipper struct {
	logger   *slog.Logger
	gzipChan <-chan string
}

// NewGZipper creates a new GZipper component.
func NewGZipper(gzipChan <-chan string, logger *slog.Logger) *GZipper {
	return &GZipper{
		logger:   logger.With("component", "gzipper"),
		gzipChan: gzipChan,
	}
}

// Run starts the GZipper's main loop, listening for files to compress.
func (g *GZipper) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	g.logger.Info("GZipper started.")

	for {
		select {
		case path, ok := <-g.gzipChan:
			if !ok {
				g.logger.Info("Channel closed, GZipper shutting down.")
				return
			}
			g.compressFile(path)
		case <-ctx.Done():
			g.logger.Info("Shutdown signal received, GZipper shutting down.")
			return
		}
	}
}

// compressFile handles the compression of a single file.
func (g *GZipper) compressFile(sourcePath string) {
	g.logger.Info("Compressing batch file", "path", sourcePath)
	startTime := time.Now()

	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		g.logger.Error("Failed to open source file for compression", "path", sourcePath, "error", err)
		return
	}
	defer sourceFile.Close()

	destPath := fmt.Sprintf("%s.gz", sourcePath)
	destFile, err := os.Create(destPath)
	if err != nil {
		g.logger.Error("Failed to create destination gzip file", "path", destPath, "error", err)
		return
	}
	defer destFile.Close()

	gzipWriter := gzip.NewWriter(destFile)
	bytesCopied, err := io.Copy(gzipWriter, sourceFile)
	if err != nil {
		g.logger.Error("Failed to compress data", "path", sourcePath, "error", err)
		gzipWriter.Close()
		destFile.Close()
		os.Remove(destPath) // Clean up the failed .gz file.
		return
	}

	// Important: We must close writers to flush all data to the file.
	if err := gzipWriter.Close(); err != nil {
		g.logger.Error("Failed to finalize gzip writer", "path", destPath, "error", err)
	}

	// The source file's deferred Close() will run, then we can remove it.
	sourceFile.Close()
	if err := os.Remove(sourcePath); err != nil {
		g.logger.Error("Failed to remove original batch file after compression", "path", sourcePath, "error", err)
		return
	}

	g.logger.Info("Successfully compressed batch file",
		"path", destPath,
		"original_size_bytes", bytesCopied,
		"duration", time.Since(startTime))
}
