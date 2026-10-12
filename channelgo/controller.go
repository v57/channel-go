package channelgo

// Controller is supplied by transports to route responses/events.
type Controller struct {
	ResponseFn    func(Message)
	SubscribeFn   func(string)
	UnsubscribeFn func(string)
	EventFn       func(string, any)
	SenderValue   *Sender
	StateValue    any
	// Async runs post and stream handlers on their own goroutine, so a slow
	// handler or a long-lived stream does not block the transport's event
	// loop (the TypeScript channel runs handlers asynchronously). ResponseFn
	// must then be safe to call from any goroutine.
	Async bool
}

func (c *Controller) Response(response Message) {
	if c != nil && c.ResponseFn != nil {
		c.ResponseFn(response)
	}
}

func (c *Controller) Subscribe(topic string) {
	if c != nil && c.SubscribeFn != nil {
		c.SubscribeFn(topic)
	}
}

func (c *Controller) Unsubscribe(topic string) {
	if c != nil && c.UnsubscribeFn != nil {
		c.UnsubscribeFn(topic)
	}
}

func (c *Controller) Event(topic string, body any) {
	if c != nil && c.EventFn != nil {
		c.EventFn(topic, body)
	}
}

func (c *Controller) Sender() *Sender {
	if c == nil {
		return nil
	}
	return c.SenderValue
}

func (c *Controller) State() any {
	if c == nil {
		return nil
	}
	return c.StateValue
}
