package grpcserver

import (
	"fmt"
	"time"
)

// parseDate accepts a "YYYY-MM-DD" string and returns the UTC midnight for that day.
func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("date is required (YYYY-MM-DD)")
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD, got %q", s)
	}
	return t.UTC(), nil
}
