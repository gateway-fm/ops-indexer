package indexedread

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"io"
	"slices"
	"sync"
)

//go:embed schema-columns.json
var schemaBytes []byte

type columnDefinition struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
	TypeMod int32  `json:"type_mod"`
	NotNull bool   `json:"not_null"`
	Select  bool   `json:"select"`
}
type keyDefinition struct {
	Kind    string   `json:"kind"`
	Columns []string `json:"columns"`
}
type tableDefinition struct {
	Name    string             `json:"name"`
	Columns []columnDefinition `json:"columns"`
	Keys    []keyDefinition    `json:"keys"`
}
type counterShapes struct {
	Columns  []string `json:"columns"`
	TypeOIDs []uint32 `json:"type_oids"`
}
type schemaDefinition struct {
	CounterShapes counterShapes     `json:"counter_shapes"`
	Version       int               `json:"version"`
	Purpose       string            `json:"purpose"`
	Schema        string            `json:"schema"`
	Tables        []tableDefinition `json:"tables"`
}

// SchemaDigest binds the compiled fixed-query purpose; it is not a database
// restore marker, row digest or assertion of canonical chain contents.
func SchemaDigest() [32]byte {
	h := sha256.New()
	framed(h, []byte("ops.explorer-index-schema/v1"))
	framed(h, schemaBytes)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

var compiledDefinition = sync.OnceValues(func() (*schemaDefinition, error) {
	var s schemaDefinition
	if json.Unmarshal(schemaBytes, &s) != nil || s.Version != 2 || s.Purpose != "existing-indexer-fixed-reads/v1" || s.Schema != "public" || len(s.Tables) != 11 {
		return nil, ErrInvalid
	}
	expectedCounters := []string{"address_stats.internal_tx_count", "address_stats.token_transfer_count", "address_stats.tx_count", "tokens.holder_count", "tokens.transfer_count"}
	if !slices.Equal(s.CounterShapes.Columns, expectedCounters) || !slices.Equal(s.CounterShapes.TypeOIDs, []uint32{20, 23}) {
		return nil, ErrInvalid
	}
	counters := 0
	for i, t := range s.Tables {
		if !sqlName(t.Name) || i > 0 && s.Tables[i-1].Name >= t.Name || len(t.Columns) == 0 || len(t.Columns) > 256 || len(t.Keys) == 0 || len(t.Keys) > 3 {
			return nil, ErrInvalid
		}
		for j, c := range t.Columns {
			if !sqlName(c.Name) || j > 0 && t.Columns[j-1].Name >= c.Name || c.TypeOID == 0 {
				return nil, ErrInvalid
			}
		}
		for _, column := range t.Columns {
			if slices.Contains(s.CounterShapes.Columns, t.Name+"."+column.Name) {
				if column.TypeOID != 23 {
					return nil, ErrInvalid
				}
				counters++
			}
		}
		for _, k := range t.Keys {
			if k.Kind != "p" && k.Kind != "u" || len(k.Columns) == 0 || len(k.Columns) > 4 {
				return nil, ErrInvalid
			}
			for _, name := range k.Columns {
				if !slices.ContainsFunc(t.Columns, func(c columnDefinition) bool { return c.Name == name }) {
					return nil, ErrInvalid
				}
			}
		}
	}
	if counters != 5 {
		return nil, ErrInvalid
	}
	return &s, nil
})

type actualColumn struct {
	Name       string `json:"name"`
	TypeSchema string `json:"type_schema"`
	TypeOID    uint32 `json:"type_oid"`
	TypeMod    int32  `json:"type_mod"`
	NotNull    bool   `json:"not_null"`
	Generated  string `json:"generated"`
	Identity   string `json:"identity"`
	Select     bool   `json:"select"`
}
type actualKey struct {
	Kind    string   `json:"kind"`
	Columns []string `json:"columns"`
	Ready   bool     `json:"ready"`
}
type actualTable struct {
	Name             string         `json:"name"`
	Kind             string         `json:"kind"`
	Persistence      string         `json:"persistence"`
	RowSecurity      bool           `json:"row_security"`
	ForceRowSecurity bool           `json:"force_row_security"`
	Inheritance      bool           `json:"inheritance"`
	Rules            bool           `json:"rules"`
	UserTriggers     bool           `json:"user_triggers"`
	Columns          []actualColumn `json:"columns"`
	Keys             []actualKey    `json:"keys"`
}

func validateSchema(raw []byte, maximum int) error {
	if len(raw) == 0 || len(raw) > maximum || maximum < 1 || maximum > 2<<20 {
		return ErrTooBroad
	}
	definition, err := compiledDefinition()
	if err != nil {
		return err
	}
	var actual []actualTable
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&actual) != nil || len(actual) != len(definition.Tables) {
		return ErrUnavailable
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrUnavailable
	}
	// Producer009 widens precisely these five counters in one migration.
	// Admit only both complete known shapes, never arbitrary numeric casts or
	// an interrupted/mixed-width catalog. All remaining structure stays exact.
	var counterOID uint32
	for i, t := range actual {
		want := definition.Tables[i]
		if t.Name != want.Name || t.Kind != "r" || t.Persistence != "p" || t.RowSecurity || t.ForceRowSecurity || t.Inheritance || t.Rules || t.UserTriggers || len(t.Columns) != len(want.Columns) || len(t.Keys) != len(want.Keys) {
			return ErrUnavailable
		}
		for j, c := range t.Columns {
			w := want.Columns[j]
			typeMatches := c.TypeOID == w.TypeOID
			if slices.Contains(definition.CounterShapes.Columns, t.Name+"."+w.Name) {
				typeMatches = slices.Contains(definition.CounterShapes.TypeOIDs, c.TypeOID)
				if counterOID != 0 && counterOID != c.TypeOID {
					return ErrUnavailable
				}
				counterOID = c.TypeOID
			}
			if c.Name != w.Name || c.TypeSchema != "pg_catalog" || !typeMatches || c.TypeMod != w.TypeMod || c.NotNull != w.NotNull || c.Generated != "" || c.Identity != "" || c.Select != w.Select {
				return ErrUnavailable
			}
		}
		for _, k := range want.Keys {
			matches := 0
			for _, a := range t.Keys {
				if a.Ready && a.Kind == k.Kind && slices.Equal(a.Columns, k.Columns) {
					matches++
				}
			}
			if matches != 1 {
				return ErrUnavailable
			}
		}
	}
	return nil
}
