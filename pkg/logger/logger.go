package logger

import (
	"log/slog"
	"os"
	"strings"
)

func New(level string) *slog.Logger {
	var logLevel slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
