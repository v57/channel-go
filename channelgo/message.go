package channelgo

// Message is the wire-level request/response envelope.
type Message struct {
	ID      *int    `json:"id,omitempty"`
	Path    *string `json:"path,omitempty"`
	Body    any     `json:"body,omitempty"`
	Context any     `json:"context,omitempty"`
	Error   any     `json:"error,omitempty"`
	Cancel  *int    `json:"cancel,omitempty"`
	Sub     *string `json:"sub,omitempty"`
	Unsub   *string `json:"unsub,omitempty"`
	Topic   *string `json:"topic,omitempty"`
	Stream  *string `json:"stream,omitempty"`
	Done    *bool   `json:"done,omitempty"`
}

func intPtr(v int) *int {
	return &v
}

func strPtr(v string) *string {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func messageWithBody(id int, body any) Message {
	return Message{ID: intPtr(id), Body: body}
}

func messageWithError(id int, err any) Message {
	return Message{ID: intPtr(id), Error: err}
}
