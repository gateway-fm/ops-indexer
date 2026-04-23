package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	reset   = "\033[0m"
	bold    = "\033[1m"
	dim     = "\033[2m"
	red     = "\033[31m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	blue    = "\033[34m"
	magenta = "\033[35m"
	cyan    = "\033[36m"
	white   = "\033[37m"
	gray    = "\033[90m"
)

var levelStyles = map[slog.Level]struct {
	color string
	label string
}{
	slog.LevelDebug: {color: gray, label: "DEBUG"},
	slog.LevelInfo:  {color: green, label: "INFO"},
	slog.LevelWarn:  {color: yellow, label: "WARN"},
	slog.LevelError: {color: red, label: "ERROR"},
}

type TerminalHandler struct {
	opts      *HandlerOptions
	mu        *sync.Mutex
	w         io.Writer
	attrs     []slog.Attr
	groups    []string
	useColors bool
}

type HandlerOptions struct {
	Level      slog.Leveler
	ShowSource bool
	UseColors  *bool
}

func NewTerminalHandler(w io.Writer, opts *HandlerOptions) *TerminalHandler {
	if opts == nil {
		opts = &HandlerOptions{}
	}
	if opts.Level == nil {
		opts.Level = slog.LevelInfo
	}

	useColors := true
	if opts.UseColors != nil {
		useColors = *opts.UseColors
	} else if f, ok := w.(*os.File); ok {
		fi, err := f.Stat()
		if err != nil || (fi.Mode()&os.ModeCharDevice) == 0 {
			useColors = false
		}
		if os.Getenv("NO_COLOR") != "" {
			useColors = false
		}
	}

	return &TerminalHandler{
		opts:      opts,
		mu:        &sync.Mutex{},
		w:         w,
		useColors: useColors,
	}
}

func (h *TerminalHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

// Format: LEVEL[DD-MM|HH:MM:SS] message key=value key2=value2
func (h *TerminalHandler) Handle(_ context.Context, r slog.Record) error {
	var buf strings.Builder

	style, ok := levelStyles[r.Level]
	if !ok {
		style = levelStyles[slog.LevelInfo]
	}

	timeStr := r.Time.Format("02-01|15:04:05")

	if h.useColors {
		buf.WriteString(style.color)
		buf.WriteString(bold)
		buf.WriteString(style.label)
		buf.WriteString(reset)
		buf.WriteString("[")
		buf.WriteString(timeStr)
		buf.WriteString("]")
	} else {
		buf.WriteString(style.label)
		buf.WriteString("[")
		buf.WriteString(timeStr)
		buf.WriteString("]")
	}
	buf.WriteString(" ")

	if len(h.groups) > 0 {
		prefix := strings.Join(h.groups, ".")
		if h.useColors {
			buf.WriteString(cyan)
			buf.WriteString(prefix)
			buf.WriteString(reset)
			buf.WriteString(": ")
		} else {
			buf.WriteString(prefix)
			buf.WriteString(": ")
		}
	}

	buf.WriteString(r.Message)

	allAttrs := append(h.attrs, collectAttrs(r)...)
	if len(allAttrs) > 0 {
		buf.WriteString(" ")
		h.writeAttrs(&buf, allAttrs)
	}

	if h.opts.ShowSource && r.Level >= slog.LevelError {
		if r.PC != 0 {
			fs := runtime.CallersFrames([]uintptr{r.PC})
			f, _ := fs.Next()
			if f.File != "" {
				file := filepath.Base(f.File)
				if h.useColors {
					fmt.Fprintf(&buf, " %s(%s:%d)%s", dim, file, f.Line, reset)
				} else {
					fmt.Fprintf(&buf, " (%s:%d)", file, f.Line)
				}
			}
		}
	}

	buf.WriteString("\n")

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write([]byte(buf.String()))
	return err
}

func (h *TerminalHandler) writeAttrs(buf *strings.Builder, attrs []slog.Attr) {
	for i, attr := range attrs {
		if i > 0 {
			buf.WriteString(" ")
		}
		if h.useColors {
			buf.WriteString(cyan)
			buf.WriteString(attr.Key)
			buf.WriteString(reset)
			buf.WriteString("=")
			buf.WriteString(formatValue(attr.Value))
		} else {
			buf.WriteString(attr.Key)
			buf.WriteString("=")
			buf.WriteString(formatValue(attr.Value))
		}
	}
}

func formatValue(v slog.Value) string {
	switch v.Kind() {
	case slog.KindString:
		s := v.String()
		if strings.ContainsAny(s, " \t\n") {
			return fmt.Sprintf("%q", s)
		}
		return s
	case slog.KindTime:
		return v.Time().Format(time.RFC3339)
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindGroup:
		attrs := v.Group()
		if len(attrs) == 0 {
			return "{}"
		}
		var parts []string
		for _, a := range attrs {
			parts = append(parts, fmt.Sprintf("%s=%s", a.Key, formatValue(a.Value)))
		}
		return "{" + strings.Join(parts, " ") + "}"
	default:
		return v.String()
	}
}

func collectAttrs(r slog.Record) []slog.Attr {
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	return attrs
}

func (h *TerminalHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TerminalHandler{
		opts:      h.opts,
		mu:        h.mu,
		w:         h.w,
		attrs:     append(h.attrs, attrs...),
		groups:    h.groups,
		useColors: h.useColors,
	}
}

func (h *TerminalHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &TerminalHandler{
		opts:      h.opts,
		mu:        h.mu,
		w:         h.w,
		attrs:     h.attrs,
		groups:    append(h.groups, name),
		useColors: h.useColors,
	}
}
