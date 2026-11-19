// Package indexedread owns fixed, read-only purposes over the existing
// indexer. Its writer and database are named operational trust dependencies.
package indexedread

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"hash"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalid = errors.New("invalid indexed read configuration")
var ErrUnavailable = errors.New("indexed read unavailable")
var ErrTooBroad = errors.New("indexed read exceeds limits")
var ErrConflict = errors.New("indexed read changed")

const maxCABytes = 1 << 20
const connectionPurpose = "ops.explorer-index-connection/v1"

type selectedConnection struct {
	config    *pgxpool.Config
	reference [32]byte
	role      string
}

func framed(h hash.Hash, p []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(p)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(p)
}

func sqlName(v string) bool {
	if len(v) == 0 || len(v) > 63 {
		return false
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func hostName(v string) bool {
	if a, err := netip.ParseAddr(v); err == nil {
		return a.Zone() == "" && !a.Is4In6() && a.String() == v
	}
	if len(v) == 0 || len(v) > 253 || strings.ToLower(v) != v {
		return false
	}
	for _, part := range strings.Split(v, ".") {
		if len(part) == 0 || len(part) > 63 || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// ConnectionReference has no network I/O. The public CA file, when selected,
// is bounded and its exact bytes join the selection. Passwords never join it.
// Only the operational pool retains reconnect credentials; errors omit them.
func ConnectionReference(dsn string) ([32]byte, error) {
	s, err := parseConnection(dsn)
	if err != nil {
		return [32]byte{}, err
	}
	return s.reference, nil
}

func parseConnection(dsn string) (*selectedConnection, error) {
	if len(dsn) == 0 || len(dsn) > 8192 || strings.ContainsAny(dsn, "\x00\r\n") {
		return nil, ErrInvalid
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "postgresql" && u.Scheme != "postgres" || u.Opaque != "" || u.User == nil || u.Fragment != "" || u.RawFragment != "" || u.ForceQuery || !hostName(u.Hostname()) {
		return nil, ErrInvalid
	}
	role := u.User.Username()
	password, present := u.User.Password()
	database := strings.TrimPrefix(u.Path, "/")
	if !sqlName(role) || !sqlName(database) || !present || len(password) == 0 || len(password) > 1024 || strings.ContainsAny(password, "\x00\r\n") || u.Path != "/"+database || u.RawPath != "" {
		return nil, ErrInvalid
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
		return nil, ErrInvalid
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || strings.Contains(u.RawQuery, ";") || len(q) < 1 || len(q) > 2 {
		return nil, ErrInvalid
	}
	for key, values := range q {
		if len(values) != 1 || key != "sslmode" && key != "sslrootcert" {
			return nil, ErrInvalid
		}
	}
	mode := q.Get("sslmode")
	if mode != "disable" && mode != "verify-full" || mode == "disable" && q.Has("sslrootcert") {
		return nil, ErrInvalid
	}
	var tc *tls.Config
	rootIdentity := []byte("system-roots")
	if mode == "verify-full" {
		tc = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
		if q.Has("sslrootcert") {
			name := q.Get("sslrootcert")
			if !filepath.IsAbs(name) || filepath.Clean(name) != name || strings.ContainsAny(name, "\x00\r\n") {
				return nil, ErrInvalid
			}
			f, err := os.Open(name)
			if err != nil {
				return nil, ErrInvalid
			}
			raw, readErr := io.ReadAll(io.LimitReader(f, maxCABytes+1))
			closeErr := f.Close()
			if readErr != nil || closeErr != nil || len(raw) == 0 || len(raw) > maxCABytes {
				return nil, ErrInvalid
			}
			pool := x509.NewCertPool()
			remaining := raw
			certs := 0
			for len(strings.TrimSpace(string(remaining))) != 0 {
				block, rest := pem.Decode(remaining)
				if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || certs >= 64 {
					return nil, ErrInvalid
				}
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, ErrInvalid
				}
				pool.AddCert(cert)
				certs++
				remaining = rest
			}
			if certs == 0 {
				return nil, ErrInvalid
			}
			tc.RootCAs = pool
			sum := sha256.Sum256(raw)
			rootIdentity = sum[:]
		}
	}
	// Do not let a libpq service file or opportunistic TLS/fallback selection
	// redirect this dedicated URI. Other inherited runtime GUCs are discarded.
	if os.Getenv("PGSERVICE") != "" || os.Getenv("PGSERVICEFILE") != "" {
		return nil, ErrInvalid
	}
	parsed := *u
	parsed.RawQuery = "sslmode=disable&target_session_attrs=any"
	cfg, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		return nil, ErrInvalid
	}
	cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Database = u.Hostname(), uint16(n), database
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	cfg.ConnConfig.TLSConfig, cfg.ConnConfig.Fallbacks = tc, nil
	cfg.ConnConfig.ValidateConnect = nil
	cfg.ConnConfig.RuntimeParams = map[string]string{"application_name": "ops-v2-index-reader", "timezone": "UTC"}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.MaxConns, cfg.MinConns = 4, 0
	h := sha256.New()
	for _, p := range [][]byte{[]byte(connectionPurpose), []byte(u.Hostname()), []byte(port), []byte(database), []byte(role), []byte(mode), []byte("tls-min1.2"), rootIdentity, []byte("fixed-indexer-read-schema/v1")} {
		framed(h, p)
	}
	var ref [32]byte
	copy(ref[:], h.Sum(nil))
	return &selectedConnection{cfg, ref, role}, nil
}
