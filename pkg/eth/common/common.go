package common

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	"golang.org/x/crypto/sha3"
)

const AddressLength = 20

type Address [AddressLength]byte

func BytesToAddress(b []byte) Address {
	var a Address
	a.setBytes(b)
	return a
}

func HexToAddress(s string) Address {
	return BytesToAddress(FromHex(s))
}

func IsHexAddress(s string) bool {
	if len(s) != 42 {
		return false
	}
	if s[:2] != "0x" && s[:2] != "0X" {
		return false
	}
	for _, c := range s[2:] {
		if !isHexChar(byte(c)) {
			return false
		}
	}
	return true
}

func (a *Address) setBytes(b []byte) {
	if len(b) > AddressLength {
		b = b[len(b)-AddressLength:]
	}
	copy(a[AddressLength-len(b):], b)
}

func (a Address) Bytes() []byte {
	return a[:]
}

func (a Address) Hex() string {
	unchecksummed := hex.EncodeToString(a[:])

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(unchecksummed))
	hash := hasher.Sum(nil)

	result := []byte("0x")
	for i, c := range []byte(unchecksummed) {
		if c >= '0' && c <= '9' {
			result = append(result, c)
		} else {
			hashByte := hash[i/2]
			var nibble byte
			if i%2 == 0 {
				nibble = hashByte >> 4
			} else {
				nibble = hashByte & 0x0f
			}
			if nibble >= 8 {
				result = append(result, c-32)
			} else {
				result = append(result, c)
			}
		}
	}
	return string(result)
}

func (a Address) String() string {
	return a.Hex()
}

func (a Address) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.Hex())
}

func (a *Address) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	decoded := FromHex(s)
	if len(decoded) != AddressLength {
		return fmt.Errorf("common: invalid address length %d", len(decoded))
	}
	copy(a[:], decoded)
	return nil
}

const HashLength = 32

type Hash [HashLength]byte

func BytesToHash(b []byte) Hash {
	var h Hash
	h.setBytes(b)
	return h
}

func HexToHash(s string) Hash {
	return BytesToHash(FromHex(s))
}

func (h *Hash) setBytes(b []byte) {
	if len(b) > HashLength {
		b = b[len(b)-HashLength:]
	}
	copy(h[HashLength-len(b):], b)
}

func (h Hash) Bytes() []byte {
	return h[:]
}

func (h Hash) Hex() string {
	return "0x" + hex.EncodeToString(h[:])
}

func (h Hash) String() string {
	return h.Hex()
}

func (h Hash) Big() *big.Int {
	return new(big.Int).SetBytes(h[:])
}

func (h Hash) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.Hex())
}

func (h *Hash) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	decoded := FromHex(s)
	if len(decoded) != HashLength {
		return fmt.Errorf("common: invalid hash length %d", len(decoded))
	}
	copy(h[:], decoded)
	return nil
}

func Bytes2Hex(b []byte) string {
	return hex.EncodeToString(b)
}

func FromHex(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		s = s[2:]
	}
	if len(s) == 0 {
		return nil
	}
	if len(s)%2 != 0 {
		s = "0" + s
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

func LeftPadBytes(b []byte, size int) []byte {
	if len(b) >= size {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	out := make([]byte, size)
	copy(out[size-len(b):], b)
	return out
}

func isHexChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

var (
	_ json.Marshaler   = Address{}
	_ json.Unmarshaler = &Address{}
	_ json.Marshaler   = Hash{}
	_ json.Unmarshaler = &Hash{}
)
