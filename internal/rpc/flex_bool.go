package rpc

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FlexBool is a bool that can be unmarshaled from both JSON booleans
// (true/false) and JSON strings ("true"/"false"). Some execution clients
// (e.g. op-reth) serialize boolean transaction fields as strings.
type FlexBool bool

func (fb *FlexBool) UnmarshalJSON(data []byte) error {
	// Try bool first (handles true / false without quotes).
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		*fb = FlexBool(b)
		return nil
	}

	// Try string (handles "true" / "false").
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		switch strings.ToLower(s) {
		case "true", "1", "0x1", "0x01":
			*fb = true
			return nil
		case "false", "0", "", "0x0", "0x00":
			*fb = false
			return nil
		}
		return fmt.Errorf("FlexBool: unrecognized string %q", s)
	}

	return fmt.Errorf("FlexBool: cannot unmarshal %s", string(data))
}

func (fb FlexBool) MarshalJSON() ([]byte, error) {
	return json.Marshal(bool(fb))
}

// Bool returns the underlying bool value.
func (fb FlexBool) Bool() bool {
	return bool(fb)
}
