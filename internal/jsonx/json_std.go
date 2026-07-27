//go:build !channelgo_sonic || !((darwin || linux) && (amd64 || arm64))

package jsonx

import "encoding/json"

func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func Valid(data []byte) bool {
	return json.Valid(data)
}
