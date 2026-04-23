package hexutil

import (
	"encoding/json"
	"math/big"
	"testing"
)

func TestBig_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		val  *big.Int
		want string
	}{
		{"zero", big.NewInt(0), `"0x0"`},
		{"one", big.NewInt(1), `"0x1"`},
		{"255", big.NewInt(255), `"0xff"`},
		{"large", big.NewInt(0x1a2b3c), `"0x1a2b3c"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Big(*tt.val)
			data, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != tt.want {
				t.Errorf("MarshalJSON = %s, want %s", data, tt.want)
			}
		})
	}
}

func TestBig_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want int64
	}{
		{"hex", `"0xff"`, 255},
		{"zero_short", `"0x0"`, 0},
		{"zero_prefix", `"0x"`, 0},
		{"empty", `""`, 0},
		{"large", `"0x100"`, 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b Big
			if err := json.Unmarshal([]byte(tt.json), &b); err != nil {
				t.Fatal(err)
			}
			got := b.ToInt().Int64()
			if got != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %d, want %d", tt.json, got, tt.want)
			}
		})
	}
}

func TestBig_UnmarshalJSON_Invalid(t *testing.T) {
	var b Big
	err := json.Unmarshal([]byte(`"0xZZZZ"`), &b)
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

func TestBig_UnmarshalJSON_InvalidJSON(t *testing.T) {
	var b Big
	err := json.Unmarshal([]byte(`123`), &b)
	if err == nil {
		t.Error("expected error for non-string JSON")
	}
}

func TestBig_Roundtrip(t *testing.T) {
	original := Big(*big.NewInt(999999))
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got Big
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ToInt().Cmp(original.ToInt()) != 0 {
		t.Errorf("roundtrip: got %s, want %s", got.ToInt(), original.ToInt())
	}
}

func TestBig_String(t *testing.T) {
	b := Big(*big.NewInt(255))
	if b.String() != "0xff" {
		t.Errorf("String() = %q, want %q", b.String(), "0xff")
	}
}

func TestUint64_MarshalJSON(t *testing.T) {
	tests := []struct {
		val  Uint64
		want string
	}{
		{0, `"0x0"`},
		{1, `"0x1"`},
		{255, `"0xff"`},
		{65536, `"0x10000"`},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.val)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != tt.want {
			t.Errorf("MarshalJSON(%d) = %s, want %s", tt.val, data, tt.want)
		}
	}
}

func TestUint64_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want uint64
	}{
		{"hex", `"0xff"`, 255},
		{"zero", `"0x0"`, 0},
		{"empty_hex", `"0x"`, 0},
		{"empty", `""`, 0},
		{"number", `42`, 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var u Uint64
			if err := json.Unmarshal([]byte(tt.json), &u); err != nil {
				t.Fatal(err)
			}
			if uint64(u) != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %d, want %d", tt.json, u, tt.want)
			}
		})
	}
}

func TestUint64_UnmarshalJSON_Invalid(t *testing.T) {
	var u Uint64
	err := json.Unmarshal([]byte(`"0xGG"`), &u)
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

func TestUint64_UnmarshalJSON_InvalidBoth(t *testing.T) {
	var u Uint64
	err := json.Unmarshal([]byte(`true`), &u)
	if err == nil {
		t.Error("expected error for boolean JSON")
	}
}

func TestUint64_Roundtrip(t *testing.T) {
	original := Uint64(123456)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got Uint64
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != original {
		t.Errorf("roundtrip: got %d, want %d", got, original)
	}
}

func TestUint64_String(t *testing.T) {
	u := Uint64(255)
	if u.String() != "0xff" {
		t.Errorf("String() = %q, want %q", u.String(), "0xff")
	}
}

func TestBytes_MarshalJSON(t *testing.T) {
	tests := []struct {
		val  Bytes
		want string
	}{
		{[]byte{}, `"0x"`},
		{[]byte{0xde, 0xad}, `"0xdead"`},
		{[]byte{0x00}, `"0x00"`},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.val)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != tt.want {
			t.Errorf("MarshalJSON(%x) = %s, want %s", []byte(tt.val), data, tt.want)
		}
	}
}

func TestBytes_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want int
	}{
		{"hex", `"0xdead"`, 2},
		{"empty", `""`, 0},
		{"empty_0x", `"0x"`, 0},
		{"odd", `"0xf"`, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b Bytes
			if err := json.Unmarshal([]byte(tt.json), &b); err != nil {
				t.Fatal(err)
			}
			if len(b) != tt.want {
				t.Errorf("UnmarshalJSON(%s) = %d bytes, want %d", tt.json, len(b), tt.want)
			}
		})
	}
}

func TestBytes_UnmarshalJSON_Invalid(t *testing.T) {
	var b Bytes
	err := json.Unmarshal([]byte(`"0xZZ"`), &b)
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

func TestBytes_UnmarshalJSON_InvalidJSON(t *testing.T) {
	var b Bytes
	err := json.Unmarshal([]byte(`123`), &b)
	if err == nil {
		t.Error("expected error for non-string JSON")
	}
}

func TestBytes_Roundtrip(t *testing.T) {
	original := Bytes([]byte{0xca, 0xfe, 0xba, 0xbe})
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got Bytes
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(original) {
		t.Fatalf("roundtrip length: got %d, want %d", len(got), len(original))
	}
	for i := range original {
		if got[i] != original[i] {
			t.Errorf("roundtrip[%d]: got %x, want %x", i, got[i], original[i])
		}
	}
}

func TestBytes_String(t *testing.T) {
	b := Bytes([]byte{0xab, 0xcd})
	if b.String() != "0xabcd" {
		t.Errorf("String() = %q, want %q", b.String(), "0xabcd")
	}
}

func TestBig_UnmarshalJSON_UppercasePrefix(t *testing.T) {
	var b Big
	if err := json.Unmarshal([]byte(`"0Xff"`), &b); err != nil {
		t.Fatal(err)
	}
	if b.ToInt().Int64() != 255 {
		t.Errorf("0X prefix: got %d, want 255", b.ToInt().Int64())
	}
}

func TestBytes_UnmarshalJSON_UppercasePrefix(t *testing.T) {
	var b Bytes
	if err := json.Unmarshal([]byte(`"0Xdead"`), &b); err != nil {
		t.Fatal(err)
	}
	if len(b) != 2 {
		t.Errorf("0X prefix: got %d bytes, want 2", len(b))
	}
}
