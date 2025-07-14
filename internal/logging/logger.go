package logging

import (
	"log/slog"
	"os"
)

// New initializes a new structured JSON logger.
func New() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}
