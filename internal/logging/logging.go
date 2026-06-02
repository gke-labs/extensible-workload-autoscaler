package logging

import (
	"log/slog"
	"os"
)

// Setup configures the default slog logger to output JSON in a format to standard output.
func Setup(debug bool) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if debug {
		opts.Level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}
