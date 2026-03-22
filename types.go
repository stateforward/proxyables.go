package proxyables

import "fmt"

// ProxyInstructionKind represents instruction kinds in the DSL.
type ProxyInstructionKind uint32

// ProxyValueKind represents serialized value kinds in the DSL.
type ProxyValueKind uint32

const (
	ProxyValueKindFunction  ProxyValueKind = 0x9ed64249
	ProxyValueKindArray     ProxyValueKind = 0x8a58ad26
	ProxyValueKindString    ProxyValueKind = 0x17c16538
	ProxyValueKindNumber    ProxyValueKind = 0x1bd670a0
	ProxyValueKindBoolean   ProxyValueKind = 0x65f46ebf
	ProxyValueKindSymbol    ProxyValueKind = 0xf3fb51d1
	ProxyValueKindObject    ProxyValueKind = 0xb8c60cba
	ProxyValueKindBigInt    ProxyValueKind = 0x8a67a5ca
	ProxyValueKindUnknown   ProxyValueKind = 0x9b759fb9
	ProxyValueKindNull      ProxyValueKind = 0x77074ba4
	ProxyValueKindUndefined ProxyValueKind = 0x9b61ad43
	ProxyValueKindReference ProxyValueKind = 0x5a1b3c4d
)

const (
	ProxyInstructionKindLocal     ProxyInstructionKind = 0x9c436708
	ProxyInstructionKindGet       ProxyInstructionKind = 0x540ca757
	ProxyInstructionKindSet       ProxyInstructionKind = 0xc6270703
	ProxyInstructionKindApply     ProxyInstructionKind = 0x24bc4a3b
	ProxyInstructionKindConstruct ProxyInstructionKind = 0x40c09172
	ProxyInstructionKindExecute   ProxyInstructionKind = 0xa01e3d98
	ProxyInstructionKindThrow     ProxyInstructionKind = 0x7a78762f
	ProxyInstructionKindReturn    ProxyInstructionKind = 0x85ee37bf
	ProxyInstructionKindNext      ProxyInstructionKind = 0x5cb68de8
	ProxyInstructionKindRelease   ProxyInstructionKind = 0x1a2b3c4d
)

// ProxyInstruction is the wire format instruction.
type ProxyInstruction struct {
	ID       string      `msgpack:"id,omitempty"`
	Kind     uint32      `msgpack:"kind"`
	Data     interface{} `msgpack:"data"`
	Metadata interface{} `msgpack:"metadata,omitempty"`
}

// ProxyError is the wire error format.
type ProxyError struct {
	Message string      `msgpack:"message"`
	Cause   *ProxyError `msgpack:"cause,omitempty"`
}

func (e *ProxyError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s", e.Message, e.Cause.Error())
	}
	return e.Message
}

// ProxyableResults is a tuple-like result for internal APIs.
type ProxyableResults[T any] struct {
	Error *ProxyError
	Value T
}

func ok[T any](value T) ProxyableResults[T] {
	return ProxyableResults[T]{Value: value}
}

func errResult[T any](err *ProxyError) ProxyableResults[T] {
	return ProxyableResults[T]{Error: err}
}
