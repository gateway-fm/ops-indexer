package db

import "testing"

func TestSanitizeWorkMem(t *testing.T) {
	cases := map[string]string{
		"1GB":       "1GB",
		"512MB":     "512MB",
		"64mb":      "64mb",
		"262144":    "262144",
		"  256MB  ": "256MB",
		// Invalid / unsafe inputs fall back to the default rather than being
		// interpolated into SET LOCAL.
		"":                          "1GB",
		"100PB":                     "1GB",
		"1GB'; DROP TABLE users;--": "1GB",
		"$(rm -rf /)":               "1GB",
	}
	for in, want := range cases {
		if got := sanitizeWorkMem(in); got != want {
			t.Errorf("sanitizeWorkMem(%q) = %q, want %q", in, got, want)
		}
	}
}
