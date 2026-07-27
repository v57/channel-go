package process

import (
	"testing"

	"github.com/v57/channel-go/channelgo"
)

func BenchmarkProcessSend(b *testing.B) {
	sender := Connect(channelgo.NewChannel().Post("hello", func(channelgo.Body) (any, error) {
		return "world", nil
	}))
	defer sender.Stop()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sender.Send("hello", nil); err != nil {
			b.Fatalf("send failed: %v", err)
		}
	}
}
