package rlp

import (
	"encoding/hex"
	"math/big"
	"testing"
)

func TestEncodeToBytes_EmptyString(t *testing.T) {
	got, err := EncodeToBytes([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x80 {
		t.Errorf("empty bytes = %x, want 80", got)
	}
}

func TestEncodeToBytes_SingleByte(t *testing.T) {
	got, err := EncodeToBytes([]byte{0x42})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x42 {
		t.Errorf("single byte 0x42 = %x, want 42", got)
	}
}

func TestEncodeToBytes_SingleByteZero(t *testing.T) {
	got, err := EncodeToBytes([]byte{0x00})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x00 {
		t.Errorf("single byte 0x00 = %x, want 00 (0x00 <= 0x7f)", got)
	}
}

func TestEncodeToBytes_ShortString(t *testing.T) {
	input := []byte("dog")
	got, err := EncodeToBytes(input)
	if err != nil {
		t.Fatal(err)
	}
	want := "83646f67"
	if hex.EncodeToString(got) != want {
		t.Errorf("\"dog\" = %x, want %s", got, want)
	}
}

func TestEncodeToBytes_Uint64_Zero(t *testing.T) {
	got, err := EncodeToBytes(uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x80 {
		t.Errorf("uint64(0) = %x, want 80", got)
	}
}

func TestEncodeToBytes_Uint64_Small(t *testing.T) {
	got, err := EncodeToBytes(uint64(127))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x7f {
		t.Errorf("uint64(127) = %x, want 7f", got)
	}
}

func TestEncodeToBytes_Uint64_Medium(t *testing.T) {
	got, err := EncodeToBytes(uint64(1024))
	if err != nil {
		t.Fatal(err)
	}
	want := "820400"
	if hex.EncodeToString(got) != want {
		t.Errorf("uint64(1024) = %x, want %s", got, want)
	}
}

func TestEncodeToBytes_BigInt_Zero(t *testing.T) {
	got, err := EncodeToBytes(big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x80 {
		t.Errorf("big.Int(0) = %x, want 80", got)
	}
}

func TestEncodeToBytes_BigInt_Small(t *testing.T) {
	got, err := EncodeToBytes(big.NewInt(127))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x7f {
		t.Errorf("big.Int(127) = %x, want 7f", got)
	}
}

func TestEncodeToBytes_BigInt_Large(t *testing.T) {
	v := new(big.Int).SetBytes([]byte{0x01, 0x00})
	got, err := EncodeToBytes(v)
	if err != nil {
		t.Fatal(err)
	}
	want := "820100"
	if hex.EncodeToString(got) != want {
		t.Errorf("big.Int(256) = %x, want %s", got, want)
	}
}

func TestEncodeToBytes_BigInt_Nil(t *testing.T) {
	var i *big.Int
	got, err := EncodeToBytes(i)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x80 {
		t.Errorf("nil big.Int = %x, want 80", got)
	}
}

func TestEncodeToBytes_Bool(t *testing.T) {
	trueEnc, err := EncodeToBytes(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(trueEnc) != 1 || trueEnc[0] != 0x01 {
		t.Errorf("true = %x, want 01", trueEnc)
	}

	falseEnc, err := EncodeToBytes(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(falseEnc) != 1 || falseEnc[0] != 0x80 {
		t.Errorf("false = %x, want 80", falseEnc)
	}
}

func TestEncodeToBytes_Struct(t *testing.T) {
	type testStruct struct {
		A uint64
		B uint64
	}
	got, err := EncodeToBytes(testStruct{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xc2 {
		t.Errorf("struct list header = %x, want c2", got[0])
	}
	if got[1] != 0x01 || got[2] != 0x02 {
		t.Errorf("struct payload = %x, want 0102", got[1:])
	}
}

func TestEncodeToBytes_NestedStruct(t *testing.T) {
	type Inner struct {
		X uint64
	}
	type Outer struct {
		A uint64
		B Inner
	}
	got, err := EncodeToBytes(Outer{A: 1, B: Inner{X: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] < 0xc0 {
		t.Errorf("outer should be a list, got header %x", got[0])
	}
}

func TestEncodeToBytes_ByteArray(t *testing.T) {
	type withArray struct {
		Data [3]byte
	}
	got, err := EncodeToBytes(withArray{Data: [3]byte{0xaa, 0xbb, 0xcc}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("encoding byte array should produce output")
	}
}

func TestEncodeToBytes_UnexportedFields(t *testing.T) {
	type withPrivate struct {
		A uint64
		b uint64 //nolint:unused
	}
	got, err := EncodeToBytes(withPrivate{A: 5})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xc1 {
		t.Errorf("struct with unexported field: header = %x, want c1 (length 1)", got[0])
	}
}

func TestEncodeToBytes_LongString(t *testing.T) {
	input := make([]byte, 56)
	for i := range input {
		input[i] = byte(i)
	}
	got, err := EncodeToBytes(input)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xb8 {
		t.Errorf("56-byte string header = %x, want b8", got[0])
	}
	if got[1] != 56 {
		t.Errorf("56-byte string length = %d, want 56", got[1])
	}
}

func TestEncodeToBytes_UnsupportedType(t *testing.T) {
	_, err := EncodeToBytes("string not supported")
	if err == nil {
		t.Error("expected error for string type")
	}
}

func TestEncodeToBytes_UnsupportedSlice(t *testing.T) {
	_, err := EncodeToBytes([]int{1, 2, 3})
	if err == nil {
		t.Error("expected error for int slice")
	}
}

func TestEncodeToBytes_LongList(t *testing.T) {
	type bigStruct struct {
		Data [60]byte
	}
	var s bigStruct
	for i := range s.Data {
		s.Data[i] = byte(i)
	}
	got, err := EncodeToBytes(s)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] < 0xf7 {
		t.Errorf("long list header = %x, want >= f7", got[0])
	}
}

func TestEncodeToBytes_NilPointer(t *testing.T) {
	type withPtr struct {
		Val *uint64
	}
	got, err := EncodeToBytes(withPtr{Val: nil})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 2 {
		t.Fatal("encoding struct with nil pointer should produce output")
	}
}

func TestEncodeToBytes_NonNilPointer(t *testing.T) {
	type withPtr struct {
		Val *uint64
	}
	v := uint64(42)
	got, err := EncodeToBytes(withPtr{Val: &v})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 2 {
		t.Fatal("encoding struct with pointer should produce output")
	}
}

func TestEncodeToBytes_UnsupportedArray(t *testing.T) {
	type withIntArray struct {
		Data [3]int
	}
	_, err := EncodeToBytes(withIntArray{})
	if err == nil {
		t.Error("expected error for non-byte array")
	}
}

func TestEncodeToBytes_Interface(t *testing.T) {
	var val any = uint64(5)
	got, err := EncodeToBytes(val)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0x05 {
		t.Errorf("interface uint64(5) = %x, want 05", got)
	}
}
