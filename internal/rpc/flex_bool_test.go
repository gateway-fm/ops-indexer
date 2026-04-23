package rpc

import (
	"encoding/json"
	"testing"
)

func TestFlexBool_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    bool
		wantErr bool
	}{
		{name: "bool true", input: `true`, want: true},
		{name: "bool false", input: `false`, want: false},
		{name: "string true", input: `"true"`, want: true},
		{name: "string false", input: `"false"`, want: false},
		{name: "string TRUE", input: `"TRUE"`, want: true},
		{name: "string FALSE", input: `"FALSE"`, want: false},
		{name: "string 1", input: `"1"`, want: true},
		{name: "string 0", input: `"0"`, want: false},
		{name: "string empty", input: `""`, want: false},
		{name: "invalid string", input: `"maybe"`, wantErr: true},
		{name: "number", input: `42`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fb FlexBool
			err := json.Unmarshal([]byte(tt.input), &fb)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %s, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %s: %v", tt.input, err)
			}
			if fb.Bool() != tt.want {
				t.Errorf("got %v, want %v", fb.Bool(), tt.want)
			}
		})
	}
}

func TestFlexBool_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		val  FlexBool
		want string
	}{
		{name: "true", val: FlexBool(true), want: "true"},
		{name: "false", val: FlexBool(false), want: "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.val)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(data) != tt.want {
				t.Errorf("got %s, want %s", string(data), tt.want)
			}
		})
	}
}

func TestFlexBool_InStruct(t *testing.T) {
	// Simulates the exact scenario: op-reth returns isSystemTx as a string.
	type tx struct {
		IsSystemTx *FlexBool `json:"isSystemTx,omitempty"`
	}

	tests := []struct {
		name string
		json string
		want *bool
	}{
		{
			name: "string true from op-reth",
			json: `{"isSystemTx":"true"}`,
			want: boolPtr(true),
		},
		{
			name: "bool true from geth",
			json: `{"isSystemTx":true}`,
			want: boolPtr(true),
		},
		{
			name: "omitted",
			json: `{}`,
			want: nil,
		},
		{
			name: "string false",
			json: `{"isSystemTx":"false"}`,
			want: boolPtr(false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v tx
			if err := json.Unmarshal([]byte(tt.json), &v); err != nil {
				t.Fatalf("unmarshal error: %v", err)
			}
			if tt.want == nil {
				if v.IsSystemTx != nil {
					t.Fatalf("expected nil, got %v", v.IsSystemTx.Bool())
				}
				return
			}
			if v.IsSystemTx == nil {
				t.Fatalf("expected %v, got nil", *tt.want)
			}
			if v.IsSystemTx.Bool() != *tt.want {
				t.Errorf("got %v, want %v", v.IsSystemTx.Bool(), *tt.want)
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
