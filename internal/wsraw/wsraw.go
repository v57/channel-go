package wsraw

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
)

const (
	OpText  = 0x1
	OpClose = 0x8
	OpPing  = 0x9
	OpPong  = 0xA
)

var (
	ErrIncomplete            = errors.New("incomplete websocket payload")
	ErrFragmentedFrame       = errors.New("fragmented websocket frames are not supported")
	ErrContinuationFrame     = errors.New("continuation websocket frames are not supported")
	ErrInvalidHandshake      = errors.New("invalid websocket handshake")
	ErrInvalidHandshakeReply = errors.New("invalid websocket handshake reply")
)

type Frame struct {
	OpCode  byte
	Payload []byte
}

type Reader struct {
	buf []byte
}

func (r *Reader) Feed(data []byte) {
	if len(data) == 0 {
		return
	}
	r.buf = append(r.buf, data...)
}

func (r *Reader) Reset() {
	r.buf = r.buf[:0]
}

func (r *Reader) Next() (Frame, bool, error) {
	if len(r.buf) < 2 {
		return Frame{}, false, nil
	}

	first := r.buf[0]
	second := r.buf[1]
	if first&0x80 == 0 {
		return Frame{}, false, ErrFragmentedFrame
	}
	opcode := first & 0x0f
	if opcode == 0x0 {
		return Frame{}, false, ErrContinuationFrame
	}

	payloadLen := int(second & 0x7f)
	offset := 2
	switch payloadLen {
	case 126:
		if len(r.buf) < offset+2 {
			return Frame{}, false, nil
		}
		payloadLen = int(binary.BigEndian.Uint16(r.buf[offset : offset+2]))
		offset += 2
	case 127:
		if len(r.buf) < offset+8 {
			return Frame{}, false, nil
		}
		size := binary.BigEndian.Uint64(r.buf[offset : offset+8])
		if size > uint64(^uint(0)>>1) {
			return Frame{}, false, fmt.Errorf("websocket frame too large: %d", size)
		}
		payloadLen = int(size)
		offset += 8
	}

	masked := second&0x80 != 0
	maskOffset := 0
	if masked {
		maskOffset = 4
	}
	total := offset + maskOffset + payloadLen
	if len(r.buf) < total {
		return Frame{}, false, nil
	}

	payload := make([]byte, payloadLen)
	copy(payload, r.buf[offset+maskOffset:total])
	if masked {
		mask := r.buf[offset : offset+4]
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}

	copy(r.buf, r.buf[total:])
	r.buf = r.buf[:len(r.buf)-total]

	return Frame{
		OpCode:  opcode,
		Payload: payload,
	}, true, nil
}

type ServerHandshake struct {
	Path    string
	Headers map[string]string
	Key     string
}

func ParseServerHandshake(data []byte) (ServerHandshake, int, bool, error) {
	headerEnd := bytes.Index(data, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return ServerHandshake{}, 0, false, nil
	}
	consumed := headerEnd + 4
	lines := strings.Split(string(data[:headerEnd]), "\r\n")
	if len(lines) == 0 {
		return ServerHandshake{}, 0, false, ErrInvalidHandshake
	}
	parts := strings.Split(lines[0], " ")
	if len(parts) != 3 || parts[0] != "GET" || !strings.HasPrefix(parts[2], "HTTP/1.1") {
		return ServerHandshake{}, 0, false, ErrInvalidHandshake
	}
	headers := make(map[string]string)
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		index := strings.IndexByte(line, ':')
		if index <= 0 {
			return ServerHandshake{}, 0, false, ErrInvalidHandshake
		}
		key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(line[:index]))
		value := strings.TrimSpace(line[index+1:])
		headers[strings.ToLower(key)] = value
	}
	key := headers["sec-websocket-key"]
	if key == "" {
		return ServerHandshake{}, 0, false, ErrInvalidHandshake
	}
	return ServerHandshake{
		Path:    parts[1],
		Headers: headers,
		Key:     key,
	}, consumed, true, nil
}

func BuildServerHandshakeResponse(key string) []byte {
	return []byte(
		"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + AcceptKey(key) + "\r\n" +
			"\r\n",
	)
}

func ParseClientHandshakeResponse(data []byte, expectedAccept string) (int, bool, error) {
	headerEnd := bytes.Index(data, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return 0, false, nil
	}
	consumed := headerEnd + 4
	lines := strings.Split(string(data[:headerEnd]), "\r\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "HTTP/1.1 101 ") {
		return 0, false, ErrInvalidHandshakeReply
	}
	headers := make(map[string]string)
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		index := strings.IndexByte(line, ':')
		if index <= 0 {
			return 0, false, ErrInvalidHandshakeReply
		}
		key := strings.ToLower(textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(line[:index])))
		value := strings.TrimSpace(line[index+1:])
		headers[key] = value
	}
	if !strings.EqualFold(headers["upgrade"], "websocket") {
		return 0, false, ErrInvalidHandshakeReply
	}
	if !strings.EqualFold(headers["connection"], "upgrade") {
		return 0, false, ErrInvalidHandshakeReply
	}
	if headers["sec-websocket-accept"] != expectedAccept {
		return 0, false, ErrInvalidHandshakeReply
	}
	return consumed, true, nil
}

func AcceptKey(key string) string {
	hash := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(hash[:])
}

func EncodeFrame(opcode byte, payload []byte, masked bool, maskKey uint32) []byte {
	header := make([]byte, 0, 14+len(payload))
	header = append(header, 0x80|opcode)

	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case len(payload) < 126:
		header = append(header, maskBit|byte(len(payload)))
	case len(payload) <= 0xffff:
		header = append(header, maskBit|126)
		header = binary.BigEndian.AppendUint16(header, uint16(len(payload)))
	default:
		header = append(header, maskBit|127)
		header = binary.BigEndian.AppendUint64(header, uint64(len(payload)))
	}

	if !masked {
		header = append(header, payload...)
		return header
	}

	mask := [4]byte{}
	binary.BigEndian.PutUint32(mask[:], maskKey)
	header = append(header, mask[:]...)
	for i, value := range payload {
		header = append(header, value^mask[i%4])
	}
	return header
}
