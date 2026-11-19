// Package indexedquery contains neutral bounded chain-fact query types.
package indexedquery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"strconv"
)

type Network struct{ Environment, ChainID, GenesisHash, DeploymentID string }

func (n Network) Key() ([32]byte, error) {
	if n.ChainID == "" || n.ChainID[0] == '0' || len(n.GenesisHash) != 66 || n.GenesisHash[:2] != "0x" {
		return [32]byte{}, fmt.Errorf("invalid network")
	}
	if _, e := strconv.ParseUint(n.ChainID, 10, 64); e != nil {
		return [32]byte{}, e
	}
	b, e := hex.DecodeString(n.GenesisHash[2:])
	if e != nil || hex.EncodeToString(b) != n.GenesisHash[2:] {
		return [32]byte{}, fmt.Errorf("invalid network")
	}
	return sha256.Sum256([]byte(n.ChainID + ":" + n.GenesisHash)), nil
}

type Binding struct {
	Network                       Network
	SourceReference, SchemaDigest [32]byte
	ConfiguredEpoch               string
}

func (b Binding) Valid() bool {
	_, e := b.Network.Key()
	id, u := uuid.Parse(b.ConfiguredEpoch)
	return e == nil && u == nil && id != uuid.Nil && id.String() == b.ConfiguredEpoch && b.SourceReference != ([32]byte{}) && b.SchemaDigest != ([32]byte{})
}

type ReaderIdentity struct{ marker byte }

func NewReaderIdentity() *ReaderIdentity { return &ReaderIdentity{1} }
func (i *ReaderIdentity) Valid() bool    { return i != nil && i.marker == 1 }

type Metadata struct {
	Binding  Binding
	Mode     string
	Identity *ReaderIdentity
}
