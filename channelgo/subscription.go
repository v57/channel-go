package channelgo

import (
	"fmt"
	"sync"
)

// SubscriptionEvent is emitted to subscription publishers.
type SubscriptionEvent struct {
	Topic string `json:"topic"`
	Body  any    `json:"body"`
}

// Publisher is implemented by transports that can publish subscription events.
type Publisher interface {
	Publish(event SubscriptionEvent)
}

// Subscription defines dynamic topic and payload generation for events.
type Subscription struct {
	mu         sync.RWMutex
	publishers []Publisher
	prefix     string
	topicFn    func(any) string
	bodyFn     func(any) (any, error)
}

func NewSubscription() *Subscription {
	return &Subscription{
		topicFn: func(request any) string {
			if request == nil {
				return ""
			}
			return fmt.Sprintf("%v", request)
		},
		bodyFn: func(any) (any, error) { return nil, nil },
	}
}

func (s *Subscription) clonePublishers() []Publisher {
	s.mu.RLock()
	publishers := append([]Publisher{}, s.publishers...)
	s.mu.RUnlock()
	return publishers
}

func (s *Subscription) setPrefix(prefix string) {
	s.mu.Lock()
	s.prefix = prefix
	s.mu.Unlock()
}

func (s *Subscription) Prefix() string {
	s.mu.RLock()
	p := s.prefix
	s.mu.RUnlock()
	return p
}

func (s *Subscription) Topic(makeTopic func(request any) string) {
	if makeTopic == nil {
		return
	}
	s.mu.Lock()
	s.topicFn = makeTopic
	s.mu.Unlock()
}

// Body intentionally mirrors JS behavior where body() overwrites the topic formatter.
func (s *Subscription) Body(makeBody func(body any) any) {
	if makeBody == nil {
		return
	}
	s.mu.Lock()
	s.topicFn = func(request any) string {
		return fmt.Sprintf("%v", makeBody(request))
	}
	s.mu.Unlock()
}

func (s *Subscription) BodyFn(makeBody func(request any) (any, error)) {
	if makeBody == nil {
		return
	}
	s.mu.Lock()
	s.bodyFn = makeBody
	s.mu.Unlock()
}

func (s *Subscription) Send(request any, body ...any) {
	s.mu.RLock()
	topicFn := s.topicFn
	bodyFn := s.bodyFn
	s.mu.RUnlock()

	topic := topicFn(request)
	var value any
	if len(body) > 0 {
		value = body[0]
	} else {
		v, err := bodyFn(request)
		if err != nil {
			return
		}
		value = v
	}
	s.Publish(topic, value)
}

func (s *Subscription) Publish(topic string, body any) {
	if body == nil {
		return
	}
	prefix := s.Prefix()
	fullTopic := topic
	if prefix != "" {
		fullTopic = joinPath(prefix, topic)
	}
	event := SubscriptionEvent{Topic: fullTopic, Body: body}
	for _, publisher := range s.clonePublishers() {
		publisher.Publish(event)
	}
}

func (s *Subscription) AddPublisher(p Publisher) {
	s.mu.Lock()
	s.publishers = append(s.publishers, p)
	s.mu.Unlock()
}

func (s *Subscription) RemovePublisher(p Publisher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, existing := range s.publishers {
		if existing == p {
			s.publishers = append(s.publishers[:i], s.publishers[i+1:]...)
			return
		}
	}
}

func (s *Subscription) PublisherCount() int {
	s.mu.RLock()
	n := len(s.publishers)
	s.mu.RUnlock()
	return n
}
