// Package log provides structured logging with a pretty terminal handler.
//
// Usage:
//
//	log.Info("server started", "port", 8080)
//	log.Error("failed to connect", "err", err)
//	log.With("component", "indexer").Info("processing block", "number", 123)
//
// The package uses slog under the hood with a custom terminal handler that
// provides colored output with clear level identification.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	defaultLogger atomic.Pointer[slog.Logger]
)

func init() {
	SetDefault(New(nil))
}

func New(opts *HandlerOptions) *slog.Logger {
	handler := NewTerminalHandler(os.Stderr, opts)
	return slog.New(handler)
}

func SetDefault(l *slog.Logger) {
	defaultLogger.Store(l)
	slog.SetDefault(l)
}

func Default() *slog.Logger {
	return defaultLogger.Load()
}

func SetLevel(level slog.Level) {
	handler := NewTerminalHandler(os.Stderr, &HandlerOptions{
		Level:      level,
		ShowSource: true,
	})
	SetDefault(slog.New(handler))
}

func With(args ...any) *slog.Logger {
	return Default().With(args...)
}

func WithGroup(name string) *slog.Logger {
	return Default().WithGroup(name)
}

func Debug(msg string, args ...any) {
	log(context.Background(), slog.LevelDebug, msg, args...)
}

func Debugf(format string, args ...any) {
	log(context.Background(), slog.LevelDebug, fmt.Sprintf(format, args...))
}

func Info(msg string, args ...any) {
	log(context.Background(), slog.LevelInfo, msg, args...)
}

func Infof(format string, args ...any) {
	log(context.Background(), slog.LevelInfo, fmt.Sprintf(format, args...))
}

func Warn(msg string, args ...any) {
	log(context.Background(), slog.LevelWarn, msg, args...)
}

func Warnf(format string, args ...any) {
	log(context.Background(), slog.LevelWarn, fmt.Sprintf(format, args...))
}

func Error(msg string, args ...any) {
	log(context.Background(), slog.LevelError, msg, args...)
}

func Errorf(format string, args ...any) {
	log(context.Background(), slog.LevelError, fmt.Sprintf(format, args...))
}

func Fatal(msg string, args ...any) {
	log(context.Background(), slog.LevelError, msg, args...)
	os.Exit(1)
}

func Fatalf(format string, args ...any) {
	log(context.Background(), slog.LevelError, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func Print(args ...any) {
	log(context.Background(), slog.LevelInfo, fmt.Sprint(args...))
}

func Println(args ...any) {
	log(context.Background(), slog.LevelInfo, fmt.Sprint(args...))
}

func Printf(format string, args ...any) {
	log(context.Background(), slog.LevelInfo, fmt.Sprintf(format, args...))
}

func log(ctx context.Context, level slog.Level, msg string, args ...any) {
	l := Default()
	if !l.Enabled(ctx, level) {
		return
	}

	// Get the caller's PC for source location (skip 3: log(), the public function, and runtime.Callers)
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])

	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(args...)

	_ = l.Handler().Handle(ctx, r)
}

type Logger struct {
	*slog.Logger
}

func NewLogger(group string) *Logger {
	return &Logger{Logger: Default().WithGroup(group)}
}

func (l *Logger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

func (l *Logger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}
