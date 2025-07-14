package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// New initializes a logger that writes to both stdout and a log file.
func New(outputDir, logFileName string) (*slog.Logger, *os.File) {
	var logWriter io.Writer = os.Stdout
	var logFile *os.File

	// CORRECTED: Set the minimum log level to DEBUG.
	handlerOpts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	if logFileName != "" {
		logPath := filepath.Join(outputDir, logFileName)
		var err error
		logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			slog.Error("Failed to open log file, continuing with stdout only", "error", err, "path", logPath)
		} else {
			mw := io.MultiWriter(os.Stdout, logFile)
			logWriter = mw
		}
	}

	logger := slog.New(slog.NewJSONHandler(logWriter, handlerOpts))
	slog.SetDefault(logger)

	return logger, logFile
}
