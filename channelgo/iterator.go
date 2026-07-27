package channelgo

import (
	"context"
	"errors"
	"sync"
)

// FuncIterator adapts callbacks to a StreamIterator.
type FuncIterator struct {
	NextFn   func(ctx context.Context) (any, bool, error)
	ReturnFn func() error
}

func (it *FuncIterator) Next(ctx context.Context) (any, bool, error) {
	if it == nil || it.NextFn == nil {
		return nil, true, nil
	}
	return it.NextFn(ctx)
}

func (it *FuncIterator) Return() error {
	if it == nil || it.ReturnFn == nil {
		return nil
	}
	return it.ReturnFn()
}

// SliceIterator yields a fixed set of values.
type SliceIterator struct {
	mu      sync.Mutex
	values  []any
	index   int
	closed  bool
	onYield func(any)
}

func NewSliceIterator(values ...any) *SliceIterator {
	return &SliceIterator{values: values}
}

func (it *SliceIterator) OnYield(fn func(any)) *SliceIterator {
	it.onYield = fn
	return it
}

func (it *SliceIterator) Next(_ context.Context) (any, bool, error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return nil, true, nil
	}
	if it.index >= len(it.values) {
		it.closed = true
		return nil, true, nil
	}
	value := it.values[it.index]
	it.index++
	if it.onYield != nil {
		it.onYield(value)
	}
	return value, false, nil
}

func (it *SliceIterator) Return() error {
	it.mu.Lock()
	it.closed = true
	it.mu.Unlock()
	return nil
}

// ChannelIterator receives values through channels.
type ChannelIterator struct {
	Values <-chan any
	Errors <-chan error
	cancel func()
}

func (it *ChannelIterator) Next(ctx context.Context) (any, bool, error) {
	if it == nil {
		return nil, true, nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case err, ok := <-it.Errors:
			if ok && err != nil {
				return nil, false, err
			}
			if !ok {
				it.Errors = nil
			}
		case value, ok := <-it.Values:
			if !ok {
				return nil, true, nil
			}
			return value, false, nil
		}
		if it.Values == nil && it.Errors == nil {
			return nil, true, nil
		}
	}
}

func (it *ChannelIterator) Return() error {
	if it != nil && it.cancel != nil {
		it.cancel()
		return nil
	}
	return errors.New("iterator is nil")
}
