//go:build faults

package faultkit

import (
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// Action is what a Rule does to the connection that sends a matching statement.
type Action int

const (
	// KillBefore closes the connection instead of forwarding the matching statement: the step never runs, and
	// the open transaction, if any, rolls back (a crash, or an error before the step).
	KillBefore Action = iota
	// FailCommit lets the matching statement run, then closes the connection instead of forwarding the COMMIT
	// that follows it: the transaction rolls back and the client sees an error.
	FailCommit
	// LostAck lets the matching statement and its COMMIT (or, outside a transaction, the statement itself) run on
	// the server, then closes the connection before the client sees ReadyForQuery: it committed, the client
	// sees an error.
	LostAck
)

// Rule fires on the first statement (simple Query or Parse) containing Match, case-insensitively.
type Rule struct {
	Match  string
	Action Action
	// Times is how often it fires (0 = once).
	Times int
	// Skip lets this many matching statements through first.
	Skip    int
	fired   int
	skipped int
	// inapplicable counts firings of a FailCommit rule on a statement that ran outside a transaction: there was no
	// COMMIT to drop, so the statement committed as usual and the rule was disarmed.
	inapplicable int
}

// Proxy forwards Postgres connections to a server, applying armed rules.
type Proxy struct {
	ln     net.Listener
	target string
	u      *url.URL
	mu     sync.Mutex
	rules  []*Rule
	log    []string
	cut    bool
}

// NewProxy proxies to the database at dbURL; URL() is the same database through the proxy.
func NewProxy(t testing.TB, dbURL string) *Proxy {
	t.Helper()
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: u.Host}
	pu := *u
	pu.Host = ln.Addr().String()
	p.u = &pu
	go p.accept()
	t.Cleanup(func() { ln.Close() })
	return p
}

func (p *Proxy) URL() string { return p.u.String() }

// Arm adds a rule.
func (p *Proxy) Arm(r Rule) *Rule {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Times == 0 {
		r.Times = 1
	}
	rr := r
	p.rules = append(p.rules, &rr)
	return &rr
}

// Disarm removes every rule.
func (p *Proxy) Disarm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = nil
}

// Inapplicable reports whether a FailCommit rule only ever fired on statements outside a transaction, where it
// cannot fail anything: the caller should skip the case rather than count it as tested.
func (p *Proxy) Inapplicable(r *Rule) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.fired > 0 && r.inapplicable == r.fired
}

// Fired reports how often r fired.
func (p *Proxy) Fired(r *Rule) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.fired
}

// Log returns the actions taken, in order.
func (p *Proxy) Log() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.log...)
}

// Down makes the proxy refuse every new and existing connection until Up (the database unreachable).
func (p *Proxy) Down() { p.mu.Lock(); p.cut = true; p.mu.Unlock() }
func (p *Proxy) Up()   { p.mu.Lock(); p.cut = false; p.mu.Unlock() }

func (p *Proxy) isDown() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.cut }

func (p *Proxy) match(sql string) *Rule {
	p.mu.Lock()
	defer p.mu.Unlock()
	l := strings.ToLower(sql)
	for _, r := range p.rules {
		if r.fired < r.Times && strings.Contains(l, strings.ToLower(r.Match)) {
			if r.skipped < r.Skip {
				r.skipped++
				continue
			}
			r.fired++
			p.log = append(p.log, actionName(r.Action)+" "+r.Match)
			return r
		}
	}
	return nil
}

func actionName(a Action) string {
	return [...]string{"kill-before", "fail-commit", "lost-ack"}[a]
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.isDown() {
			c.Close()
			continue
		}
		go p.serve(c)
	}
}

type pconn struct {
	p            *Proxy
	client, srv  net.Conn
	mu           sync.Mutex
	pending      Action // armed action waiting for COMMIT / ReadyForQuery
	armed        bool
	rule         *Rule
	commitWanted bool // LostAck inside a transaction: wait for the COMMIT's ReadyForQuery
	sawCommit    bool
	stmts        map[string]string
	once         sync.Once
}

func (c *pconn) close() {
	c.once.Do(func() { c.client.Close(); c.srv.Close() })
}

func (p *Proxy) serve(client net.Conn) {
	srv, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	c := &pconn{p: p, client: client, srv: srv}
	defer c.close()
	// startup: untyped length-prefixed message(s); with sslmode=disable just the StartupMessage.
	var hdr [4]byte
	if _, err := io.ReadFull(client, hdr[:]); err != nil {
		return
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	body := make([]byte, n-4)
	if _, err := io.ReadFull(client, body); err != nil {
		return
	}
	if _, err := srv.Write(append(hdr[:], body...)); err != nil {
		return
	}
	go c.fromServer()
	c.fromClient()
}

func readMsg(r io.Reader) (byte, []byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(h[1:]))
	b := make([]byte, n-4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, nil, err
	}
	return h[0], append(h[:], b...), nil
}

func cstring(b []byte) string {
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

func (c *pconn) fromClient() {
	for {
		if c.p.isDown() {
			return
		}
		typ, msg, err := readMsg(c.client)
		if err != nil {
			return
		}
		var sql string
		switch typ {
		case 'Q':
			sql = cstring(msg[5:])
		case 'P':
			rest := msg[5:]
			name := cstring(rest)
			if c.stmts == nil {
				c.stmts = map[string]string{}
			}
			c.stmts[name] = cstring(rest[len(name)+1:])
		case 'B':
			rest := msg[5:]
			portal := cstring(rest)
			sql = c.stmts[cstring(rest[len(portal)+1:])]
		}
		if sql != "" {
			lsql := strings.ToLower(strings.TrimSpace(sql))
			c.mu.Lock()
			armed, pending := c.armed, c.pending
			c.mu.Unlock()
			if armed && (lsql == "commit" || strings.HasPrefix(lsql, "commit")) {
				if pending == FailCommit {
					return // the COMMIT never reaches the server
				}
				c.mu.Lock()
				c.sawCommit = true
				c.mu.Unlock()
			}
			if r := c.p.match(sql); r != nil {
				switch r.Action {
				case KillBefore:
					return
				case FailCommit, LostAck:
					c.mu.Lock()
					c.armed, c.pending, c.rule = true, r.Action, r
					c.mu.Unlock()
				}
			}
		}
		if _, err := c.srv.Write(msg); err != nil {
			return
		}
	}
}

func (c *pconn) fromServer() {
	defer c.close()
	for {
		typ, msg, err := readMsg(c.srv)
		if err != nil {
			return
		}
		if typ == 'Z' && len(msg) >= 6 {
			status := msg[5]
			c.mu.Lock()
			lost := c.armed && c.pending == LostAck && status == 'I'
			if c.armed && c.pending == FailCommit && status == 'I' && !c.sawCommit {
				// The statement ran outside a transaction (or the transaction rolled back): nothing to fail. Left
				// armed, the rule would kill a later, unrelated COMMIT on this pooled connection.
				c.armed = false
				c.p.mu.Lock()
				c.rule.inapplicable++
				c.p.mu.Unlock()
			}
			c.mu.Unlock()
			if lost {
				return // committed on the server; the client never hears it
			}
		}
		if _, err := c.client.Write(msg); err != nil {
			return
		}
	}
}
