package jsonx

import "testing"

func TestMarshalUnmarshalValid(t *testing.T) {
	in := map[string]any{"a": 1, "b": "x"}
	data, err := Marshal(in)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if !Valid(data) {
		t.Fatalf("expected valid json")
	}
	if Valid([]byte("{")) {
		t.Fatalf("expected invalid json to fail")
	}
	var out map[string]any
	if err := Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if out["b"] != "x" {
		t.Fatalf("unexpected decoded value: %#v", out)
	}
}
