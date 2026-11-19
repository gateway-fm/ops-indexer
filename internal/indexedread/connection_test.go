package indexedread

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConnectionReferenceOwnsActualNonsecretSelection(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")
	u := "postgresql://index_reader:first-password@localhost:5432/indexer?sslmode=disable"
	a, err := parseConnection(u)
	if err != nil {
		t.Fatal(err)
	}
	b, err := parseConnection(strings.Replace(u, "first-password", "another-password", 1))
	if err != nil || a.reference != b.reference || a.config.ConnConfig.Password == b.config.ConnConfig.Password {
		t.Fatal("credentials joined public identity or were not selected", err)
	}
	for _, changed := range []string{
		strings.Replace(u, "localhost", "127.0.0.1", 1),
		strings.Replace(u, "5432", "5433", 1),
		strings.Replace(u, "/indexer?", "/different?", 1),
		strings.Replace(u, "index_reader:", "other_reader:", 1),
		strings.Replace(u, "sslmode=disable", "sslmode=verify-full", 1),
	} {
		p, err := parseConnection(changed)
		if err != nil || p.reference == a.reference {
			t.Fatal("different selected endpoint/role/TLS reused reference", err)
		}
	}
	secured, err := parseConnection(strings.Replace(u, "sslmode=disable", "sslmode=verify-full", 1))
	if err != nil || secured.config.ConnConfig.TLSConfig == nil || secured.config.ConnConfig.TLSConfig.InsecureSkipVerify || secured.config.ConnConfig.TLSConfig.ServerName != "localhost" || secured.config.ConnConfig.TLSConfig.MinVersion != tls.VersionTLS12 || len(secured.config.ConnConfig.Fallbacks) != 0 {
		t.Fatal("actual TLS differs from bound selection", err)
	}
}

func TestConnectionCannotInheritRedirectOrStartupOptions(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")
	t.Setenv("PGHOST", "foreign-host")
	t.Setenv("PGDATABASE", "foreign_db")
	t.Setenv("PGUSER", "foreign_user")
	t.Setenv("PGSSLMODE", "prefer")
	t.Setenv("PGTARGETSESSIONATTRS", "primary")
	t.Setenv("PGOPTIONS", "-c search_path=foreign")
	p, err := parseConnection("postgres://index_reader:secret@localhost/indexer?sslmode=disable")
	if err != nil || p.config.ConnConfig.Host != "localhost" || p.config.ConnConfig.Port != 5432 || p.config.ConnConfig.User != "index_reader" || p.config.ConnConfig.Database != "indexer" || p.config.ConnConfig.TLSConfig != nil || len(p.config.ConnConfig.Fallbacks) != 0 || p.config.ConnConfig.ValidateConnect != nil || p.config.ConnConfig.RuntimeParams["options"] != "" {
		t.Fatal("inherited connection changed fixed purpose", err)
	}
	t.Setenv("PGSERVICE", "unserved-service")
	if _, err := parseConnection("postgres://index_reader:secret@localhost/indexer?sslmode=disable"); err != ErrInvalid {
		t.Fatal("service-file selection was admitted", err)
	}
}

func TestConnectionRejectsUnservedInputsWithoutSecretErrors(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")
	base := "postgres://index_reader:very-private@localhost:5432/indexer?sslmode=disable"
	for _, dsn := range []string{
		"host=localhost user=index_reader password=very-private",
		strings.Replace(base, "sslmode=disable", "sslmode=prefer", 1),
		strings.Replace(base, "sslmode=disable", "sslmode=require", 1),
		base + "&options=-c%20role%3Downer", base + "&sslmode=verify-full",
		base + "&service=foreign", base + "&host=foreign", base + "&sslcert=/private/key",
		strings.Replace(base, "localhost:5432", "localhost:5432,foreign:5432", 1),
		strings.Replace(base, "localhost:5432", "localhost:05432", 1),
		strings.Replace(base, "index_reader:very-private", "index_reader", 1),
		strings.Replace(base, "/indexer?", "/indexer/other?", 1),
		base + "#fragment",
	} {
		_, err := parseConnection(dsn)
		if err == nil || strings.Contains(err.Error(), "very-private") || strings.Contains(err.Error(), dsn) {
			t.Fatal("unserved selection or secret-bearing error", err)
		}
	}
}

func TestConnectionRejectsNoncertificateAndOversizedTrustFile(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")
	file := filepath.Join(t.TempDir(), "ca.pem")
	uri := "postgres://index_reader:secret@localhost/indexer?sslmode=verify-full&sslrootcert=" + file
	for _, data := range [][]byte{[]byte("not a certificate"), []byte(strings.Repeat("x", maxCABytes+1))} {
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := parseConnection(uri); err != ErrInvalid {
			t.Fatal("invalid trust file admitted", err)
		}
	}
}
