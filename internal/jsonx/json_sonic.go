//go:build channelgo_sonic && (darwin || linux) && (amd64 || arm64)

package jsonx

import "github.com/bytedance/sonic"

func Marshal(v any) ([]byte, error) {
	return sonic.ConfigFastest.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	return sonic.ConfigFastest.Unmarshal(data, v)
}

func Valid(data []byte) bool {
	return sonic.Valid(data)
}
