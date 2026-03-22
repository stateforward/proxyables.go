package proxyables

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// Encode serializes a value into msgpack bytes.
func Encode(value interface{}) ([]byte, error) {
	return msgpack.Marshal(value)
}

// DecodeInstruction decodes a single ProxyInstruction from msgpack bytes.
func DecodeInstruction(data []byte) (*ProxyInstruction, *ProxyError) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	return decodeFromDecoder(dec)
}

func decodeFromDecoder(dec *msgpack.Decoder) (*ProxyInstruction, *ProxyError) {
	var instr ProxyInstruction
	if err := dec.Decode(&instr); err != nil {
		return nil, &ProxyError{Message: err.Error()}
	}
	return &instr, nil
}

func decodeFromBytes(data []byte) (map[string]interface{}, error) {
	var out map[string]interface{}
	if err := msgpack.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
