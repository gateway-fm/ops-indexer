package hexutil

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

type Big big.Int

func (b *Big) ToInt() *big.Int {
	return (*big.Int)(b)
}

func (b Big) MarshalJSON() ([]byte, error) {
	i := (*big.Int)(&b)
	if i.Sign() == 0 {
		return json.Marshal("0x0")
	}
	return json.Marshal("0x" + i.Text(16))
}

func (b *Big) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("hexutil.Big: invalid JSON: %w", err)
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "0x" || s == "0x0" {
		(*big.Int)(b).SetUint64(0)
		return nil
	}
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	i, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return fmt.Errorf("hexutil.Big: invalid hex string %q", s)
	}
	*b = Big(*i)
	return nil
}

func (b *Big) String() string {
	return "0x" + (*big.Int)(b).Text(16)
}

type Uint64 uint64

func (u Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + strconv.FormatUint(uint64(u), 16))
}

func (u *Uint64) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		var n uint64
		if err2 := json.Unmarshal(data, &n); err2 != nil {
			return fmt.Errorf("hexutil.Uint64: invalid JSON: %w", err)
		}
		*u = Uint64(n)
		return nil
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "0x" || s == "0x0" {
		*u = 0
		return nil
	}
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return fmt.Errorf("hexutil.Uint64: invalid hex string %q: %w", s, err)
	}
	*u = Uint64(v)
	return nil
}

func (u Uint64) String() string {
	return "0x" + strconv.FormatUint(uint64(u), 16)
}

type Bytes []byte

func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + hex.EncodeToString(b))
}

func (b *Bytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("hexutil.Bytes: invalid JSON: %w", err)
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "0x" {
		*b = []byte{}
		return nil
	}
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	if len(s)%2 != 0 {
		s = "0" + s
	}
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("hexutil.Bytes: invalid hex string: %w", err)
	}
	*b = decoded
	return nil
}

func (b Bytes) String() string {
	return "0x" + hex.EncodeToString(b)
}
