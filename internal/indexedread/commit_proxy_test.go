package indexedread

import (
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A bounded cleartext PostgreSQL test proxy forwards real authentication and
// actual SQL. It can drop a COMMIT result after the real server completed it.
// It neither synthesizes query success nor changes the production Reader.
type commitProxy struct {
	address   string
	drop      atomic.Bool
	committed chan struct{}
}

func commitResponseProxy(t *testing.T, upstreamDSN string) *commitProxy {
	t.Helper()
	u, err := url.Parse(upstreamDSN)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &commitProxy{address: listener.Addr().String(), committed: make(chan struct{}, 1)}
	var mu sync.Mutex
	var connections []net.Conn
	stopping := false
	var workers sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		stopping = true
		for _, c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if stopping {
				mu.Unlock()
				_ = client.Close()
				return
			}
			connections = append(connections, client)
			mu.Unlock()
			server, err := net.DialTimeout("tcp", u.Host, time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			mu.Lock()
			if stopping {
				mu.Unlock()
				_ = client.Close()
				_ = server.Close()
				return
			}
			connections = append(connections, server)
			mu.Unlock()
			_ = client.SetDeadline(time.Now().Add(30 * time.Second))
			_ = server.SetDeadline(time.Now().Add(30 * time.Second))
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				defer server.Close()
				// The initial startup packet has a length header and no kind byte.
				var header [4]byte
				if _, err := io.ReadFull(client, header[:]); err != nil {
					return
				}
				n := int(binary.BigEndian.Uint32(header[:]))
				if n < 8 || n > 8192 {
					return
				}
				startup := make([]byte, n)
				copy(startup, header[:])
				if _, err := io.ReadFull(client, startup[4:]); err != nil {
					return
				}
				if _, err := server.Write(startup); err != nil {
					return
				}
				responses := make(chan struct{})
				go func() {
					defer close(responses)
					defer client.Close()
					defer server.Close()
					for {
						frame, err := readPGFrame(server)
						if err != nil {
							return
						}
						if frame[0] == 'C' && string(frame[5:]) == "COMMIT\x00" && p.drop.Load() {
							select {
							case p.committed <- struct{}{}:
							default:
							}
							return
						}
						if _, err := client.Write(frame); err != nil {
							return
						}
					}
				}()
				for {
					frame, err := readPGFrame(client)
					if err != nil {
						break
					}
					if _, err := server.Write(frame); err != nil {
						break
					}
				}
				_ = client.Close()
				_ = server.Close()
				<-responses
			}()
		}
	}()
	return p
}

func readPGFrame(r io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(header[1:]))
	if n < 4 || n > 8<<20 {
		return nil, io.ErrUnexpectedEOF
	}
	frame := make([]byte, n+1)
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[5:])
	return frame, err
}

func TestWrongReaderBindingRejectsBeforeConnect(t *testing.T) {
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dsn := "postgres://index_reader:public-test-only@" + listener.Addr().String() + "/index_test?sslmode=disable"
	reference, err := ConnectionReference(dsn)
	if err != nil {
		t.Fatal(err)
	}
	expected := testBinding(reference)
	expected.SourceReference[0] ^= 1
	reader, err := OpenReader(t.Context(), dsn, expected, DefaultLimits())
	if reader != nil || err != ErrInvalid {
		t.Fatal("wrong configured source accepted", err)
	}
	if err := listener.SetDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	c, err := listener.Accept()
	if c != nil {
		_ = c.Close()
		t.Fatal("wrong binding performed network I/O")
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("listener probe", err)
	}
	if strings.Contains(err.Error(), "public-test-only") {
		t.Fatal("secret error")
	}
}
