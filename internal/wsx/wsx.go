package wsx

import (
	"errors"
	"io"
	"net"
	"sync"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func WriteClientText(conn net.Conn, writeMu *sync.Mutex, payload []byte) error {
	if conn == nil {
		return errors.New("websocket not connected")
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	return wsutil.WriteClientText(conn, payload)
}

func WriteServerText(conn net.Conn, writeMu *sync.Mutex, payload []byte) error {
	if conn == nil {
		return errors.New("websocket not connected")
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	return wsutil.WriteServerText(conn, payload)
}

func ReadServerText(conn net.Conn) ([]byte, error) {
	for {
		payload, op, err := wsutil.ReadServerData(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, err
		}
		if op == ws.OpText {
			return payload, nil
		}
	}
}

func ReadClientText(conn net.Conn) ([]byte, error) {
	for {
		payload, op, err := wsutil.ReadClientData(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, err
		}
		if op == ws.OpText {
			return payload, nil
		}
	}
}
