package channelgo

import (
	"fmt"
	"reflect"
)

type SendCaller interface {
	Send(path string, body any, contextValue ...any) (any, error)
}

func Send[T any](sender SendCaller, path string, body any, contextValue ...any) (T, error) {
	var zero T

	value, err := sender.Send(path, body, contextValue...)
	if err != nil {
		return zero, err
	}
	if value == nil {
		return zero, nil
	}

	typed, ok := value.(T)
	if ok {
		return typed, nil
	}

	return zero, fmt.Errorf(
		"channel-go: response type %T is not assignable to %v",
		value,
		reflect.TypeFor[T](),
	)
}
