package rlp

import (
	"fmt"
	"math/big"
	"reflect"
)

func EncodeToBytes(val any) ([]byte, error) {
	b, err := encode(reflect.ValueOf(val))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func encode(v reflect.Value) ([]byte, error) {
	for v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Ptr:
		return encodePtr(v)
	case reflect.Struct:
		return encodeStruct(v)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return encodeBytes(v.Bytes()), nil
		}
		return nil, fmt.Errorf("rlp: unsupported slice element type %s", v.Type().Elem())
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(b), v)
			return encodeBytes(b), nil
		}
		return nil, fmt.Errorf("rlp: unsupported array element type %s", v.Type().Elem())
	case reflect.Uint64:
		return encodeUint64(v.Uint()), nil
	case reflect.Bool:
		return encodeBool(v.Bool()), nil
	default:
		return nil, fmt.Errorf("rlp: unsupported type %s", v.Type())
	}
}

func encodePtr(v reflect.Value) ([]byte, error) {
	if v.Type() == reflect.TypeOf((*big.Int)(nil)) {
		if v.IsNil() {
			return []byte{0x80}, nil
		}
		return encodeBigInt(v.Interface().(*big.Int)), nil
	}

	if v.IsNil() {
		return []byte{0x80}, nil
	}
	return encode(v.Elem())
}

func encodeStruct(v reflect.Value) ([]byte, error) {
	var payload []byte
	t := v.Type()
	for i := range t.NumField() {
		if !t.Field(i).IsExported() {
			continue
		}
		b, err := encode(v.Field(i))
		if err != nil {
			return nil, fmt.Errorf("rlp: field %s: %w", t.Field(i).Name, err)
		}
		payload = append(payload, b...)
	}
	return appendListHeader(payload), nil
}

func encodeBytes(b []byte) []byte {
	if len(b) == 1 && b[0] <= 0x7f {
		return []byte{b[0]}
	}
	return append(encodeStringHeader(len(b)), b...)
}

func encodeStringHeader(length int) []byte {
	if length <= 55 {
		return []byte{byte(0x80 + length)}
	}
	lenBytes := putUintBE(uint64(length))
	header := make([]byte, 1+len(lenBytes))
	header[0] = byte(0xb7 + len(lenBytes))
	copy(header[1:], lenBytes)
	return header
}

func appendListHeader(payload []byte) []byte {
	length := len(payload)
	if length <= 55 {
		out := make([]byte, 1+length)
		out[0] = byte(0xc0 + length)
		copy(out[1:], payload)
		return out
	}
	lenBytes := putUintBE(uint64(length))
	out := make([]byte, 1+len(lenBytes)+length)
	out[0] = byte(0xf7 + len(lenBytes))
	copy(out[1:], lenBytes)
	copy(out[1+len(lenBytes):], payload)
	return out
}

func encodeBigInt(i *big.Int) []byte {
	if i.Sign() == 0 {
		return []byte{0x80}
	}
	return encodeBytes(i.Bytes())
}

func encodeUint64(v uint64) []byte {
	if v == 0 {
		return []byte{0x80}
	}
	return encodeBytes(putUintBE(v))
}

func encodeBool(b bool) []byte {
	if b {
		return []byte{0x01}
	}
	return []byte{0x80}
}

func putUintBE(v uint64) []byte {
	var buf [8]byte
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
	for i := range 8 {
		if buf[i] != 0 {
			return buf[i:]
		}
	}
	return nil
}
