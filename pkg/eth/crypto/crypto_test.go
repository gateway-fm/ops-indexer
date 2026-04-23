package crypto

import (
	"encoding/hex"
	"testing"
)

func TestKeccak256_EmptyInput(t *testing.T) {
	got := hex.EncodeToString(Keccak256([]byte{}))
	want := "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	if got != want {
		t.Errorf("Keccak256(empty) = %s, want %s", got, want)
	}
}

func TestKeccak256_HelloWorld(t *testing.T) {
	got := hex.EncodeToString(Keccak256([]byte("hello")))
	want := "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
	if got != want {
		t.Errorf("Keccak256(\"hello\") = %s, want %s", got, want)
	}
}

func TestKeccak256_MultipleInputs(t *testing.T) {
	combined := Keccak256([]byte("hello"), []byte("world"))
	separate := Keccak256([]byte("helloworld"))
	if hex.EncodeToString(combined) != hex.EncodeToString(separate) {
		t.Error("Keccak256 with multiple inputs should equal concatenated input")
	}
}

func TestKeccak256Hash(t *testing.T) {
	h := Keccak256Hash([]byte("hello"))
	got := hex.EncodeToString(h[:])
	want := "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"
	if got != want {
		t.Errorf("Keccak256Hash(\"hello\") = %s, want %s", got, want)
	}
}

func TestKeccak256Hash_ReturnsHashType(t *testing.T) {
	h := Keccak256Hash([]byte("test"))
	b := h.Bytes()
	if len(b) != 32 {
		t.Errorf("Keccak256Hash result should be 32 bytes, got %d", len(b))
	}
}

func TestKeccak256_TransferEventSignature(t *testing.T) {
	sig := Keccak256([]byte("Transfer(address,address,uint256)"))
	got := hex.EncodeToString(sig)
	want := "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	if got != want {
		t.Errorf("Transfer event signature = %s, want %s", got, want)
	}
}

func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		t.Fatal("GenerateKey returned nil")
	}
	if key.PublicKey.X == nil || key.PublicKey.Y == nil {
		t.Error("generated key has nil public key coordinates")
	}
}

func TestGenerateKey_Unique(t *testing.T) {
	k1, _ := GenerateKey()
	k2, _ := GenerateKey()
	if k1.D.Cmp(k2.D) == 0 {
		t.Error("two generated keys should not be equal")
	}
}
