package channelgo

import (
	"testing"
)

// wireConnection is one of several connections that write to the same wire, like the WebSocket client's public
// and loop senders.
type wireConnection struct {
	fakeConnection
	wire any
}

func (c *wireConnection) Wire() any { return c.wire }

// A response answers a request only on the connection the request went to: ids are one counter for every
// connection, so another peer of a server could otherwise answer, feed or end what the server sent someone else.
func TestResponseFromAnotherConnectionIsIgnored(t *testing.T) {
	ch := NewChannel()
	service := MakeSender(ch, &fakeConnection{})
	attacker := MakeSender(ch, &fakeConnection{})
	attackerController := &Controller{SenderValue: attacker, StateValue: map[string]any{}}
	serviceController := &Controller{SenderValue: service, StateValue: map[string]any{}}

	var call, stream, sub []Message
	callID := *ch.MakeRequest("svc/read", nil, nil, func(m Message) { call = append(call, m) }, service).ID
	streamID := *ch.MakeStream("svc/values", nil, nil, func(m Message) { stream = append(stream, m) }, service).ID
	subID := *ch.MakeSubscription("svc/topic", nil, nil, func(m Message) { sub = append(sub, m) }, service).ID

	topic := "x"
	for id := 0; id < 256; id++ {
		ch.ReceiveOne(Message{ID: intPtr(id), Body: "forged"}, attackerController)
		ch.ReceiveOne(Message{ID: intPtr(id), Topic: &topic, Body: "forged"}, attackerController)
		ch.ReceiveOne(Message{ID: intPtr(id), Done: boolPtr(true)}, attackerController)
	}
	if len(call) != 0 || len(stream) != 0 || len(sub) != 0 || ch.RequestsSize() != 3 {
		t.Fatalf("another connection reached the requests: call=%v stream=%v sub=%v pending=%d", call, stream, sub, ch.RequestsSize())
	}

	ch.ReceiveOne(Message{ID: intPtr(callID), Body: "real"}, serviceController)
	ch.ReceiveOne(Message{ID: intPtr(streamID), Body: "real"}, serviceController)
	ch.ReceiveOne(Message{ID: intPtr(streamID), Done: boolPtr(true)}, serviceController)
	ch.ReceiveOne(Message{ID: intPtr(subID), Topic: &topic, Body: "real"}, serviceController)
	if len(call) != 1 || call[0].Body != "real" {
		t.Fatalf("expected the call's real answer, got %v", call)
	}
	if len(stream) != 2 || stream[0].Body != "real" || stream[1].Done == nil || !*stream[1].Done {
		t.Fatalf("expected the stream's real value and end, got %v", stream)
	}
	if len(sub) != 1 || sub[0].Body != "real" {
		t.Fatalf("expected the subscription's real reply, got %v", sub)
	}
	if ch.RequestsSize() != 0 {
		t.Fatalf("expected no pending requests, got %d", ch.RequestsSize())
	}
}

// Senders that share one wire answer each other's requests.
func TestResponseOnTheSameWireAnswersAnotherSendersRequest(t *testing.T) {
	ch := NewChannel()
	wire := new(int)
	public := MakeSender(ch, &wireConnection{wire: wire})
	loop := MakeSender(ch, &wireConnection{wire: wire})
	other := MakeSender(ch, &wireConnection{wire: new(int)})

	var got []Message
	id := *ch.MakeRequest("hello", nil, nil, func(m Message) { got = append(got, m) }, public).ID
	ch.ReceiveOne(Message{ID: intPtr(id), Body: "forged"}, &Controller{SenderValue: other, StateValue: map[string]any{}})
	ch.ReceiveOne(Message{ID: intPtr(id), Body: "real"}, &Controller{SenderValue: loop, StateValue: map[string]any{}})
	if len(got) != 1 || got[0].Body != "real" {
		t.Fatalf("expected the real answer through the shared wire, got %v", got)
	}
}
