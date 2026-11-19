package rpc

import "testing"

func TestFlattenPreservesActualFrameType(t *testing.T) {
	for _, kind := range []string{"CALL", "CALLCODE", "DELEGATECALL", "STATICCALL", "CREATE", "CREATE2", "SELFDESTRUCT", "FUTURE_NODE_FRAME"} {
		rows := FlattenCallFrame(&CallFrame{Type: kind}, "0xhash", 1, nil)
		if len(rows) != 1 {
			t.Fatalf("frame %s disappeared", kind)
		}
		want := map[string]string{"CALL": "call", "CALLCODE": "callcode", "DELEGATECALL": "delegatecall", "STATICCALL": "staticcall", "CREATE": "create", "CREATE2": "create2", "SELFDESTRUCT": "selfdestruct", "FUTURE_NODE_FRAME": "future_node_frame"}[kind]
		if rows[0].CallType != want {
			t.Fatalf("frame %s mislabeled as %s", kind, rows[0].CallType)
		}
	}
}
