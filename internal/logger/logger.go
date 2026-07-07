package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// New initializes and returns a new *slog.Logger based on log level and format.
func New(levelStr, formatStr string, output io.Writer) (*slog.Logger, error) {
	if output == nil {
		output = os.Stdout
	}

	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level: %q", levelStr)
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var handler slog.Handler
	switch strings.ToLower(formatStr) {
	case "json":
		handler = slog.NewJSONHandler(output, opts)
	case "text":
		handler = slog.NewTextHandler(output, opts)
	default:
		return nil, fmt.Errorf("unsupported log format: %q", formatStr)
	}

	return slog.New(handler), nil
}
