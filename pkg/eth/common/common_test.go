package common

import (
	"encoding/json"
	"testing"
)

func TestFromHex(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"0x", 0},
		{"0X", 0},
		{"0xff", 1},
		{"ff", 1},
		{"0xabcd", 2},
		{"0x1", 1},
		{"invalid", 0},
	}
	for _, tt := range tests {
		got := FromHex(tt.input)
		if len(got) != tt.want {
			t.Errorf("FromHex(%q) = %d bytes, want %d", tt.input, len(got), tt.want)
		}
	}
}

func TestFromHex_OddLength(t *testing.T) {
	got := FromHex("0xf")
	if len(got) != 1 || got[0] != 0x0f {
		t.Errorf("FromHex(\"0xf\") = %x, want 0f", got)
	}
}

func TestBytes2Hex(t *testing.T) {
	got := Bytes2Hex([]byte{0xde, 0xad, 0xbe, 0xef})
	if got != "deadbeef" {
		t.Errorf("Bytes2Hex = %q, want %q", got, "deadbeef")
	}
}

func TestLeftPadBytes(t *testing.T) {
	in := []byte{0x01, 0x02}
	got := LeftPadBytes(in, 4)
	want := []byte{0x00, 0x00, 0x01, 0x02}
	if len(got) != 4 {
		t.Fatalf("LeftPadBytes length = %d, want 4", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("LeftPadBytes[%d] = %x, want %x", i, got[i], want[i])
		}
	}
}

func TestLeftPadBytes_AlreadyLarger(t *testing.T) {
	in := []byte{0x01, 0x02, 0x03}
	got := LeftPadBytes(in, 2)
	if len(got) != 3 {
		t.Errorf("LeftPadBytes should return copy of original when size < len, got %d", len(got))
	}
}

func TestIsHexAddress(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", true},
		{"0x0000000000000000000000000000000000000000", true},
		{"0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG", false},
		{"f39Fd6e51aad88F6F4ce6aB8827279cffFb92266", false},
		{"0x1234", false},
		{"", false},
	}
	for _, tt := range tests {
		got := IsHexAddress(tt.input)
		if got != tt.want {
			t.Errorf("IsHexAddress(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestAddress_HexToAddress(t *testing.T) {
	addr := HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	hex := addr.Hex()
	if hex != "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266" {
		t.Errorf("HexToAddress roundtrip = %q", hex)
	}
}

func TestAddress_EIP55Checksum(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"},
		{"0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359", "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359"},
		{"0x0000000000000000000000000000000000000000", "0x0000000000000000000000000000000000000000"},
	}
	for _, tt := range tests {
		addr := HexToAddress(tt.input)
		got := addr.Hex()
		if got != tt.want {
			t.Errorf("Hex(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestAddress_Bytes(t *testing.T) {
	addr := HexToAddress("0xff00000000000000000000000000000000000001")
	b := addr.Bytes()
	if len(b) != AddressLength {
		t.Fatalf("Address.Bytes() length = %d", len(b))
	}
	if b[0] != 0xff || b[19] != 0x01 {
		t.Errorf("Address.Bytes() = %x", b)
	}
}

func TestAddress_String(t *testing.T) {
	addr := HexToAddress("0x0000000000000000000000000000000000000000")
	if addr.String() != addr.Hex() {
		t.Error("String() should equal Hex()")
	}
}

func TestAddress_JSON(t *testing.T) {
	addr := HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	data, err := json.Marshal(addr)
	if err != nil {
		t.Fatal(err)
	}

	var got Address
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != addr {
		t.Errorf("JSON roundtrip: got %s, want %s", got.Hex(), addr.Hex())
	}
}

func TestAddress_UnmarshalJSON_Null(t *testing.T) {
	var addr Address
	if err := json.Unmarshal([]byte("null"), &addr); err != nil {
		t.Fatal(err)
	}
	if addr != (Address{}) {
		t.Error("unmarshalling null should leave address as zero")
	}
}

func TestAddress_UnmarshalJSON_InvalidLength(t *testing.T) {
	var addr Address
	err := json.Unmarshal([]byte(`"0x1234"`), &addr)
	if err == nil {
		t.Error("expected error for invalid address length")
	}
}

func TestAddress_BytesToAddress_Truncate(t *testing.T) {
	long := make([]byte, 25)
	for i := range long {
		long[i] = byte(i)
	}
	addr := BytesToAddress(long)
	if addr[0] != 5 {
		t.Errorf("BytesToAddress should take last 20 bytes, got first byte %d", addr[0])
	}
}

func TestAddress_BytesToAddress_Short(t *testing.T) {
	short := []byte{0xab, 0xcd}
	addr := BytesToAddress(short)
	if addr[18] != 0xab || addr[19] != 0xcd {
		t.Errorf("BytesToAddress short = %x, expected right-aligned", addr)
	}
}

func TestHash_HexToHash(t *testing.T) {
	h := HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	if h[31] != 1 {
		t.Errorf("HexToHash last byte = %d, want 1", h[31])
	}
}

func TestHash_Hex(t *testing.T) {
	h := HexToHash("0xabcdef0000000000000000000000000000000000000000000000000000000000")
	got := h.Hex()
	if got != "0xabcdef0000000000000000000000000000000000000000000000000000000000" {
		t.Errorf("Hash.Hex() = %q", got)
	}
}

func TestHash_Big(t *testing.T) {
	h := HexToHash("0x0000000000000000000000000000000000000000000000000000000000000100")
	got := h.Big()
	if got.Int64() != 256 {
		t.Errorf("Hash.Big() = %d, want 256", got.Int64())
	}
}

func TestHash_JSON(t *testing.T) {
	h := HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000000")
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}

	var got Hash
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Errorf("JSON roundtrip: got %s, want %s", got.Hex(), h.Hex())
	}
}

func TestHash_UnmarshalJSON_Null(t *testing.T) {
	var h Hash
	if err := json.Unmarshal([]byte("null"), &h); err != nil {
		t.Fatal(err)
	}
	if h != (Hash{}) {
		t.Error("unmarshalling null should leave hash as zero")
	}
}

func TestHash_UnmarshalJSON_InvalidLength(t *testing.T) {
	var h Hash
	err := json.Unmarshal([]byte(`"0xabcd"`), &h)
	if err == nil {
		t.Error("expected error for invalid hash length")
	}
}

func TestHash_BytesToHash_Truncate(t *testing.T) {
	long := make([]byte, 40)
	for i := range long {
		long[i] = byte(i)
	}
	h := BytesToHash(long)
	if h[0] != 8 {
		t.Errorf("BytesToHash should take last 32 bytes, got first byte %d", h[0])
	}
}

func TestHash_String(t *testing.T) {
	h := HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")
	if h.String() != h.Hex() {
		t.Error("String() should equal Hex()")
	}
}

func TestAddress_ZeroValue(t *testing.T) {
	var a Address
	if a.Hex() != "0x0000000000000000000000000000000000000000" {
		t.Errorf("zero Address.Hex() = %q", a.Hex())
	}
}

func TestHash_ZeroValue(t *testing.T) {
	var h Hash
	if h.Hex() != "0x0000000000000000000000000000000000000000000000000000000000000000" {
		t.Errorf("zero Hash.Hex() = %q", h.Hex())
	}
}

func TestAddress_UnmarshalJSON_InvalidJSON(t *testing.T) {
	var addr Address
	err := json.Unmarshal([]byte(`123`), &addr)
	if err == nil {
		t.Error("expected error for non-string JSON")
	}
}

func TestHash_UnmarshalJSON_InvalidJSON(t *testing.T) {
	var h Hash
	err := json.Unmarshal([]byte(`123`), &h)
	if err == nil {
		t.Error("expected error for non-string JSON")
	}
}
