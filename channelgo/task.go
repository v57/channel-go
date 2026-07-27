package channelgo

import "sync"

// Task represents a cancellable in-flight request.
type Task struct {
	mu            sync.Mutex
	isCancelled   bool
	cancelActions []func()
	onCancel      func()
}

func newTask(onCancel func()) *Task {
	return &Task{onCancel: onCancel}
}

func (t *Task) IsCancelled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isCancelled
}

func (t *Task) OnCancel(action func()) {
	if action == nil {
		return
	}
	t.mu.Lock()
	if t.isCancelled {
		t.mu.Unlock()
		func() {
			defer func() { _ = recover() }()
			action()
		}()
		return
	}
	t.cancelActions = append(t.cancelActions, action)
	t.mu.Unlock()
}

func (t *Task) Cancel() {
	t.mu.Lock()
	if t.isCancelled {
		t.mu.Unlock()
		return
	}
	t.isCancelled = true
	actions := append([]func(){}, t.cancelActions...)
	onCancel := t.onCancel
	t.mu.Unlock()

	for _, action := range actions {
		func() {
			defer func() { _ = recover() }()
			action()
		}()
	}
	if onCancel != nil {
		onCancel()
	}
}
