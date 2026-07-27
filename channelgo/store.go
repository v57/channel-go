package channelgo

import "sync"

type iteratorStore struct {
	mu sync.RWMutex
	m  map[int]StreamIterator
}

func newIteratorStore() *iteratorStore {
	return &iteratorStore{m: make(map[int]StreamIterator)}
}

func (s *iteratorStore) Set(id int, it StreamIterator) {
	s.mu.Lock()
	s.m[id] = it
	s.mu.Unlock()
}

func (s *iteratorStore) Get(id int) StreamIterator {
	s.mu.RLock()
	it := s.m[id]
	s.mu.RUnlock()
	return it
}

func (s *iteratorStore) Delete(id int) {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

func (s *iteratorStore) ForEach(fn func(int, StreamIterator)) {
	s.mu.RLock()
	pairs := make([]struct {
		id int
		it StreamIterator
	}, 0, len(s.m))
	for id, it := range s.m {
		pairs = append(pairs, struct {
			id int
			it StreamIterator
		}{id: id, it: it})
	}
	s.mu.RUnlock()

	for _, pair := range pairs {
		fn(pair.id, pair.it)
	}
}

func (s *iteratorStore) Clear() {
	s.mu.Lock()
	s.m = make(map[int]StreamIterator)
	s.mu.Unlock()
}

func (s *iteratorStore) Size() int {
	s.mu.RLock()
	n := len(s.m)
	s.mu.RUnlock()
	return n
}

type taskStore struct {
	mu sync.RWMutex
	m  map[int]*Task
}

func newTaskStore() *taskStore {
	return &taskStore{m: make(map[int]*Task)}
}

func (s *taskStore) Set(id int, task *Task) {
	s.mu.Lock()
	s.m[id] = task
	s.mu.Unlock()
}

func (s *taskStore) Get(id int) *Task {
	s.mu.RLock()
	t := s.m[id]
	s.mu.RUnlock()
	return t
}

func (s *taskStore) Delete(id int) {
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

func (s *taskStore) ForEach(fn func(int, *Task)) {
	s.mu.RLock()
	pairs := make([]struct {
		id int
		t  *Task
	}, 0, len(s.m))
	for id, task := range s.m {
		pairs = append(pairs, struct {
			id int
			t  *Task
		}{id: id, t: task})
	}
	s.mu.RUnlock()

	for _, pair := range pairs {
		fn(pair.id, pair.t)
	}
}

func (s *taskStore) Clear() {
	s.mu.Lock()
	s.m = make(map[int]*Task)
	s.mu.Unlock()
}

func (s *taskStore) Size() int {
	s.mu.RLock()
	n := len(s.m)
	s.mu.RUnlock()
	return n
}
