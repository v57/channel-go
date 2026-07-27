package channelgo

import (
	"strings"
	"testing"
)

type fakeSendCaller struct {
	value any
	err   error
}

func (f fakeSendCaller) Send(_ string, _ any, _ ...any) (any, error) {
	return f.value, f.err
}

func TestSendTypedHelper(t *testing.T) {
	value, err := Send[string](fakeSendCaller{value: "world"}, "hello", "hi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != "world" {
		t.Fatalf("unexpected value: %#v", value)
	}

	var zero int
	zero, err = Send[int](fakeSendCaller{value: nil}, "hello", nil)
	if err != nil {
		t.Fatalf("nil response should not fail: %v", err)
	}
	if zero != 0 {
		t.Fatalf("nil response should return zero value, got %d", zero)
	}

	_, err = Send[string](fakeSendCaller{value: 42}, "hello", nil)
	if err == nil || !strings.Contains(err.Error(), "is not assignable") {
		t.Fatalf("expected assignability error, got %v", err)
	}
}
