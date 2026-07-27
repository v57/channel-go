package channelgo

import (
	"testing"
)

func BenchmarkReceiveOnePostNoResponse(b *testing.B) {
	ch := NewChannel().Post("hello", func(Body) (any, error) {
		return "world", nil
	})
	sender := MakeSender(ch, &benchConn{})
	controller := &Controller{
		ResponseFn:    func(Message) {},
		SubscribeFn:   func(string) {},
		UnsubscribeFn: func(string) {},
		EventFn:       func(string, any) {},
		SenderValue:   sender,
		StateValue:    map[string]any{},
	}
	message := Message{Path: strPtr("hello")}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.ReceiveOne(message, controller)
	}
}

type benchConn struct{}

func (benchConn) Send(body any) int {
	_ = body
	return 0
}

func (benchConn) Sent(int) {}

func (benchConn) Cancel(int) bool {
	return false
}

func (benchConn) Notify(any) {}

func (benchConn) AddTopic(string, EventBody) func() bool {
	return func() bool { return true }
}

func (benchConn) Stop() {}
