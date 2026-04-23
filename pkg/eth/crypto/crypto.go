package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"hash"

	"github.com/gateway-fm/chain-indexer/pkg/eth/common"

	"golang.org/x/crypto/sha3"
)

func Keccak256(data ...[]byte) []byte {
	d := newKeccak256()
	for _, b := range data {
		d.Write(b)
	}
	return d.Sum(nil)
}

func Keccak256Hash(data ...[]byte) (h common.Hash) {
	d := newKeccak256()
	for _, b := range data {
		d.Write(b)
	}
	d.Sum(h[:0])
	return h
}

func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func newKeccak256() hash.Hash {
	return sha3.NewLegacyKeccak256()
}
