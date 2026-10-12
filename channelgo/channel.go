package channelgo

import (
	"context"
	"fmt"
	"sync"
)

type pendingRequest struct {
	request  Message
	response func(Message)
	sender   *Sender
}

type postRoute struct {
	path    func(string) bool
	request Function
}

type streamRoute struct {
	path    func(string) bool
	request StreamFunction
}

// Channel is the core request/stream/subscription router.
type Channel struct {
	mu           sync.RWMutex
	id           int
	requests     map[int]pendingRequest
	postAPI      map[string]Function
	otherPost    []postRoute
	streamAPI    map[string]StreamFunction
	otherStream  []streamRoute
	onDisconnect func(state any, sender *Sender)
	eventsAPI    map[string]*Subscription
}

const disconnectedError = "disconnected"

func NewChannel() *Channel {
	return &Channel{
		requests:  make(map[int]pendingRequest),
		postAPI:   make(map[string]Function),
		streamAPI: make(map[string]StreamFunction),
	}
}

func (ch *Channel) Post(path string, request Function) *Channel {
	ch.mu.Lock()
	ch.postAPI[path] = request
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) PostOther(path func(string) bool, request Function) *Channel {
	ch.mu.Lock()
	ch.otherPost = append(ch.otherPost, postRoute{path: path, request: request})
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) Stream(path string, request StreamFunction) *Channel {
	ch.mu.Lock()
	ch.streamAPI[path] = request
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) StreamOther(path func(string) bool, request StreamFunction) *Channel {
	ch.mu.Lock()
	ch.otherStream = append(ch.otherStream, streamRoute{path: path, request: request})
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) OnDisconnect(action func(state any, sender *Sender)) *Channel {
	ch.mu.Lock()
	ch.onDisconnect = action
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) Disconnect(state any, sender *Sender) {
	ch.disconnectSender(state, sender, true, nil)
}

func (ch *Channel) DisconnectMany(state any, primary *Sender, others ...*Sender) {
	ch.disconnectSender(state, primary, true, nil)
	for _, sender := range others {
		ch.disconnectSender(state, sender, false, nil)
	}
}

func (ch *Channel) DisconnectSilent(state any, sender *Sender, skipped map[int]struct{}) {
	ch.disconnectSender(state, sender, false, skipped)
}

func (ch *Channel) DisconnectSkipped(state any, primary *Sender, skipped map[int]struct{}, others ...*Sender) {
	ch.disconnectSender(state, primary, true, skipped)
	for _, sender := range others {
		ch.disconnectSender(state, sender, false, nil)
	}
}

func (ch *Channel) disconnectSender(state any, sender *Sender, runCallback bool, skipped map[int]struct{}) {
	if sender == nil {
		return
	}

	if runCallback {
		ch.mu.RLock()
		onDisconnect := ch.onDisconnect
		ch.mu.RUnlock()

		func() {
			defer func() { _ = recover() }()
			if onDisconnect != nil {
				onDisconnect(state, sender)
			}
		}()
	}

	sender.streams.ForEach(func(_ int, stream StreamIterator) {
		func() {
			defer func() { _ = recover() }()
			_ = stream.Return()
		}()
	})

	sender.requests.ForEach(func(_ int, request *Task) {
		request.Cancel()
	})

	idsToDelete := make([]int, 0)
	pending := make([]pendingRequest, 0)
	ch.mu.RLock()
	for id, req := range ch.requests {
		if req.requestSender() == sender {
			if skipped != nil {
				if _, ok := skipped[id]; ok {
					continue
				}
			}
			idsToDelete = append(idsToDelete, id)
			pending = append(pending, req)
		}
	}
	ch.mu.RUnlock()

	if len(idsToDelete) > 0 {
		ch.mu.Lock()
		for _, id := range idsToDelete {
			delete(ch.requests, id)
		}
		ch.mu.Unlock()
	}

	for _, req := range pending {
		func() {
			defer func() { _ = recover() }()
			req.response(Message{ID: req.request.ID, Error: disconnectedError})
		}()
	}

	sender.streams.Clear()
	sender.requests.Clear()
}

func (ch *Channel) MakeRequest(path string, body any, context any, response func(Message), sender *Sender) Message {
	ch.mu.Lock()
	id := ch.id
	ch.id++
	request := Message{ID: intPtr(id), Path: strPtr(path), Body: body, Context: context}
	ch.requests[id] = pendingRequest{request: request, response: response, sender: sender}
	ch.mu.Unlock()
	return request
}

func (ch *Channel) MakeStream(stream string, body any, context any, response func(Message), sender *Sender) Message {
	ch.mu.Lock()
	id := ch.id
	ch.id++
	request := Message{ID: intPtr(id), Stream: strPtr(stream), Body: body, Context: context}
	ch.requests[id] = pendingRequest{request: request, response: response, sender: sender}
	ch.mu.Unlock()
	return request
}

func (ch *Channel) MakeSubscription(sub string, body any, context any, response func(Message), sender *Sender) Message {
	ch.mu.Lock()
	id := ch.id
	ch.id++
	request := Message{ID: intPtr(id), Sub: strPtr(sub), Body: body, Context: context}
	ch.requests[id] = pendingRequest{request: request, response: response, sender: sender}
	ch.mu.Unlock()
	return request
}

func (ch *Channel) Cancel(id int) {
	ch.mu.Lock()
	delete(ch.requests, id)
	ch.mu.Unlock()
}

func (ch *Channel) RequestsSize() int {
	ch.mu.RLock()
	n := len(ch.requests)
	ch.mu.RUnlock()
	return n
}

func (ch *Channel) Receive(some any, controller *Controller) {
	switch value := some.(type) {
	case []Message:
		for _, item := range value {
			ch.ReceiveOne(item, controller)
		}
	case []*Message:
		for _, item := range value {
			if item != nil {
				ch.ReceiveOne(*item, controller)
			}
		}
	case []any:
		for _, item := range value {
			if msg, ok := asMessage(item); ok {
				ch.ReceiveOne(msg, controller)
			}
		}
	default:
		if msg, ok := asMessage(value); ok {
			ch.ReceiveOne(msg, controller)
		}
	}
}

func (ch *Channel) ReceiveOne(some Message, controller *Controller) {
	if controller == nil || controller.Sender() == nil {
		return
	}

	if some.Path != nil {
		ch.receivePath(some, controller)
		return
	}

	if some.Stream != nil {
		ch.receiveStream(some, controller)
		return
	}

	if some.Cancel != nil {
		ch.receiveCancel(*some.Cancel, controller)
		return
	}

	if some.Sub != nil {
		ch.receiveSubscription(some, controller)
		return
	}

	if some.Unsub != nil {
		controller.Unsubscribe(*some.Unsub)
		return
	}

	if some.Topic != nil {
		if some.ID != nil {
			if request, ok := ch.takeRequest(*some.ID); ok {
				request.response(some)
			}
		}
		if isTruthy(some.Body) {
			controller.Event(*some.Topic, some.Body)
		}
		return
	}

	if some.ID != nil {
		id := *some.ID
		request, ok := ch.getRequest(id)
		if !ok {
			return
		}
		if !(request.request.Stream != nil && !(some.Done != nil && *some.Done)) {
			ch.Cancel(id)
		}
		request.response(some)
	}
}

func (ch *Channel) receivePath(some Message, controller *Controller) {
	path := *some.Path
	api := ch.lookupPost(path)
	id := some.ID

	if api == nil {
		if id != nil {
			controller.Response(messageWithError(*id, "api not found"))
		}
		return
	}

	var task *Task
	if id != nil {
		task = newTask(func() {
			controller.Response(messageWithError(*id, "cancelled"))
		})
		controller.Sender().requests.Set(*id, task)
	}

	run := func() {
		defer func() {
			if id != nil {
				controller.Sender().requests.Delete(*id)
			}
			if recovered := recover(); recovered != nil && id != nil {
				if task == nil || !task.IsCancelled() {
					controller.Response(messageWithError(*id, fmt.Sprint(recovered)))
				}
			}
		}()

		body, err := api(Body{
			Body:    some.Body,
			Context: valueOr(some.Context, map[string]any{}),
			Sender:  controller.Sender(),
			State:   controller.State(),
			Path:    path,
			Task:    task,
		})
		if id == nil {
			return
		}
		if task != nil && task.IsCancelled() {
			return
		}
		if err != nil {
			controller.Response(messageWithError(*id, fmt.Sprint(err)))
			return
		}
		controller.Response(messageWithBody(*id, body))
	}

	run()
}

func (ch *Channel) receiveStream(some Message, controller *Controller) {
	path := *some.Stream
	api := ch.lookupStream(path)
	if api == nil {
		if some.ID != nil {
			controller.Response(messageWithError(*some.ID, "api not found"))
		}
		return
	}
	if some.ID == nil {
		return
	}
	id := *some.ID

	ch.streamRequest(id, controller, path, some.Body, valueOr(some.Context, map[string]any{}), api)
}

func (ch *Channel) streamRequest(id int, controller *Controller, path string, body any, contextValue any, stream StreamFunction) {
	task := newTask(nil)
	controller.Sender().streams.Set(id, &FuncIterator{
		ReturnFn: func() error {
			task.Cancel()
			return nil
		},
	})
	defer controller.Sender().streams.Delete(id)

	emitter := &streamEmitter{
		id:         id,
		controller: controller,
		task:       task,
	}
	err := stream(Body{
		Body:    body,
		Sender:  controller.Sender(),
		State:   controller.State(),
		Path:    path,
		Context: contextValue,
		Task:    task,
	}, emitter)
	if task.IsCancelled() {
		return
	}
	if err != nil {
		controller.Response(messageWithError(id, fmt.Sprint(err)))
		return
	}
	controller.Response(Message{ID: intPtr(id), Done: boolPtr(true)})
}

func (ch *Channel) receiveCancel(id int, controller *Controller) {
	if stream := controller.Sender().streams.Get(id); stream != nil {
		_ = stream.Return()
		controller.Sender().streams.Delete(id)
	}
	if request := controller.Sender().requests.Get(id); request != nil {
		request.Cancel()
		controller.Sender().requests.Delete(id)
	}
}

type streamEmitter struct {
	id         int
	controller *Controller
	task       *Task
}

func (e *streamEmitter) Emit(value any) error {
	if e == nil || e.task == nil {
		return context.Canceled
	}
	if e.task.IsCancelled() {
		return context.Canceled
	}
	e.controller.Response(messageWithBody(e.id, value))
	return nil
}

func (e *streamEmitter) IsCancelled() bool {
	if e == nil || e.task == nil {
		return true
	}
	return e.task.IsCancelled()
}

func (ch *Channel) receiveSubscription(some Message, controller *Controller) {
	subName := *some.Sub
	subscription := ch.lookupSubscription(subName)
	if subscription == nil {
		if some.ID != nil {
			controller.Response(messageWithError(*some.ID, "subscription not found"))
		}
		return
	}

	subscription.mu.RLock()
	topicFn := subscription.topicFn
	bodyFn := subscription.bodyFn
	prefix := subscription.prefix
	subscription.mu.RUnlock()

	topic := topicFn(some.Body)
	topic = joinPath(prefix, topic)
	body := some.Body
	if bodyFn != nil {
		if evaluated, err := bodyFn(some.Body); err == nil {
			body = evaluated
		}
	}

	controller.Subscribe(topic)
	controller.Response(Message{ID: some.ID, Topic: strPtr(topic), Body: body})
}

func (ch *Channel) Events(events map[string]*Subscription) *Channel {
	return ch.EventsWithPrefix(events, "")
}

func (ch *Channel) EventsWithPrefix(events map[string]*Subscription, prefix string) *Channel {
	ch.mu.Lock()
	if ch.eventsAPI == nil {
		ch.eventsAPI = make(map[string]*Subscription)
	}
	for key, value := range events {
		if value == nil {
			continue
		}
		full := joinPathNoEmpty(prefix, key)
		value.setPrefix(full)
		ch.eventsAPI[full] = value
	}
	ch.mu.Unlock()
	return ch
}

func (ch *Channel) EventsAny(events any, prefix string) *Channel {
	parsed := make(map[string]*Subscription)
	parseSubscriptions(parsed, events, prefix)
	return ch.EventsWithPrefix(parsed, "")
}

func (ch *Channel) Merge(channel *Channel, prefix string) *Channel {
	if channel == nil {
		return ch
	}

	channel.ForEachEvent(func(key string, value *Subscription) {
		if value == nil {
			return
		}
		full := joinPathNoEmpty(prefix, key)
		ch.EventsWithPrefix(map[string]*Subscription{full: value}, "")
	})

	channel.mu.RLock()
	postAPI := make(map[string]Function, len(channel.postAPI))
	for key, value := range channel.postAPI {
		postAPI[key] = value
	}
	streamAPI := make(map[string]StreamFunction, len(channel.streamAPI))
	for key, value := range channel.streamAPI {
		streamAPI[key] = value
	}
	channel.mu.RUnlock()

	for key, value := range postAPI {
		ch.Post(joinPathNoEmpty(prefix, key), value)
	}
	for key, value := range streamAPI {
		ch.Stream(joinPathNoEmpty(prefix, key), value)
	}
	return ch
}

func (ch *Channel) ForEachEvent(fn func(string, *Subscription)) {
	if fn == nil {
		return
	}
	ch.mu.RLock()
	events := make(map[string]*Subscription, len(ch.eventsAPI))
	for key, value := range ch.eventsAPI {
		events[key] = value
	}
	ch.mu.RUnlock()

	for key, value := range events {
		fn(key, value)
	}
}

func (ch *Channel) lookupPost(path string) Function {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if fn := ch.postAPI[path]; fn != nil {
		return fn
	}
	for _, route := range ch.otherPost {
		if route.path != nil && route.path(path) {
			return route.request
		}
	}
	return nil
}

func (ch *Channel) lookupStream(path string) StreamFunction {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if fn := ch.streamAPI[path]; fn != nil {
		return fn
	}
	for _, route := range ch.otherStream {
		if route.path != nil && route.path(path) {
			return route.request
		}
	}
	return nil
}

func (ch *Channel) lookupSubscription(path string) *Subscription {
	ch.mu.RLock()
	sub := ch.eventsAPI[path]
	ch.mu.RUnlock()
	return sub
}

func (ch *Channel) getRequest(id int) (pendingRequest, bool) {
	ch.mu.RLock()
	request, ok := ch.requests[id]
	ch.mu.RUnlock()
	return request, ok
}

func (ch *Channel) takeRequest(id int) (pendingRequest, bool) {
	ch.mu.Lock()
	request, ok := ch.requests[id]
	if ok {
		delete(ch.requests, id)
	}
	ch.mu.Unlock()
	return request, ok
}

func asMessage(value any) (Message, bool) {
	switch typed := value.(type) {
	case Message:
		return typed, true
	case *Message:
		if typed == nil {
			return Message{}, false
		}
		return *typed, true
	case map[string]any:
		var msg Message
		if v, ok := asInt(typed["id"]); ok {
			msg.ID = intPtr(v)
		}
		if v, ok := typed["path"].(string); ok {
			msg.Path = strPtr(v)
		}
		if v, ok := typed["body"]; ok {
			msg.Body = v
		}
		if v, ok := typed["context"]; ok {
			msg.Context = v
		}
		if v, ok := typed["error"]; ok {
			msg.Error = v
		}
		if v, ok := asInt(typed["cancel"]); ok {
			msg.Cancel = intPtr(v)
		}
		if v, ok := typed["sub"].(string); ok {
			msg.Sub = strPtr(v)
		}
		if v, ok := typed["unsub"].(string); ok {
			msg.Unsub = strPtr(v)
		}
		if v, ok := typed["topic"].(string); ok {
			msg.Topic = strPtr(v)
		}
		if v, ok := typed["stream"].(string); ok {
			msg.Stream = strPtr(v)
		}
		if v, ok := typed["done"].(bool); ok {
			msg.Done = boolPtr(v)
		}
		return msg, true
	default:
		return Message{}, false
	}
}

func asInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	default:
		return 0, false
	}
}

func valueOr(value any, fallback any) any {
	if value == nil {
		return fallback
	}
	return value
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix + "/"
	}
	return prefix + "/" + key
}

func joinPathNoEmpty(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + "/" + key
}

func parseSubscriptions(out map[string]*Subscription, value any, prefix string) {
	switch typed := value.(type) {
	case map[string]*Subscription:
		for key, sub := range typed {
			if sub == nil {
				continue
			}
			full := joinPathNoEmpty(prefix, key)
			sub.setPrefix(full)
			out[full] = sub
		}
	case map[string]any:
		for key, next := range typed {
			if sub, ok := next.(*Subscription); ok {
				full := joinPathNoEmpty(prefix, key)
				sub.setPrefix(full)
				out[full] = sub
				continue
			}
			parseSubscriptions(out, next, joinPathNoEmpty(prefix, key))
		}
	}
}

func isTruthy(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case int:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	default:
		return true
	}
}

func (p pendingRequest) requestSender() *Sender {
	return p.sender
}
