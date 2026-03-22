package proxyables

import (
	"context"
	"net"
	"sync"

	"github.com/hashicorp/yamux"
	"github.com/vmihailenco/msgpack/v5"
)

// ExportedProxyable hosts a root object and serves requests.
type ExportedProxyable struct {
	session     *yamux.Session
	root        interface{}
	registry    *ObjectRegistry
	streamPool  *StreamPool
	closeOnce   sync.Once
	closed      chan struct{}
	poolSize    int
	poolReuse   bool
}

// NewExportedProxyable creates and starts a server-side proxyable.
func NewExportedProxyable(conn net.Conn, root interface{}, opts *ExportOptions) (*ExportedProxyable, error) {
	session, err := yamux.Server(conn, nil)
	if err != nil {
		return nil, err
	}

	registry := NewObjectRegistry()
	exported := &ExportedProxyable{
		session:    session,
		root:       root,
		registry:   registry,
		closed:     make(chan struct{}),
		poolSize:   opts.StreamPoolSize,
		poolReuse:  opts.StreamPoolReuse,
	}
	exported.streamPool = NewStreamPool(session, opts.StreamPoolSize, opts.StreamPoolReuse)

	go exported.acceptLoop()
	return exported, nil
}

// Close shuts down the exported proxyable.
func (e *ExportedProxyable) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.closed)
		err = e.session.Close()
		e.streamPool.Close()
	})
	return err
}

func (e *ExportedProxyable) acceptLoop() {
	for {
		stream, err := e.session.Accept()
		if err != nil {
			return
		}
		go e.handleStream(stream)
	}
}

func (e *ExportedProxyable) handleStream(conn net.Conn) {
	defer conn.Close()
	dec := msgpack.NewDecoder(conn)
	enc := msgpack.NewEncoder(conn)

	for {
		var instr ProxyInstruction
		if err := dec.Decode(&instr); err != nil {
			return
		}

		response := e.handleInstruction(instr)
		_ = enc.Encode(&response)
	}
}

func (e *ExportedProxyable) handleInstruction(instr ProxyInstruction) ProxyInstruction {
	kind := ProxyInstructionKind(instr.Kind)
	switch kind {
	case ProxyInstructionKindExecute:
		instructions, parseErr := parseInstructionSlice(instr.Data)
		if parseErr != nil {
			return CreateThrowInstruction(parseErr)
		}
		result, err := e.evaluateInstructions(instructions)
		if err != nil {
			return CreateThrowInstruction(err)
		}
		return CreateReturnInstruction(result)
	case ProxyInstructionKindRelease:
		refID, ok := parseReleaseID(instr.Data)
		if ok {
			e.registry.Delete(refID)
		}
		return CreateReturnInstruction(createUndefinedValue())
	default:
		return CreateThrowInstruction(&ProxyError{Message: "unsupported instruction"})
	}
}

func (e *ExportedProxyable) evaluateInstructions(instructions []ProxyInstruction) (ProxyInstruction, *ProxyError) {
	stack := make([]ProxyInstruction, 0, len(instructions))
	for _, instr := range instructions {
		if ProxyValueKind(instr.Kind) == ProxyValueKindReference {
			stack = append(stack, instr)
			continue
		}

		var target interface{} = e.root
		if len(stack) > 0 {
			last := stack[len(stack)-1]
			if ProxyValueKind(last.Kind) == ProxyValueKindReference {
				stack = stack[:len(stack)-1]
				if refID, ok := last.Data.(string); ok {
					if value, found := e.registry.Get(refID); found {
						target = value
					}
				}
			}
		}

		result, err := e.applyInstruction(target, instr)
		if err != nil {
			return ProxyInstruction{}, err
		}
		stack = append(stack, result)
	}

	if len(stack) == 0 {
		return ProxyInstruction{}, &ProxyError{Message: "no result"}
	}
	return stack[0], nil
}

func (e *ExportedProxyable) applyInstruction(target interface{}, instr ProxyInstruction) (ProxyInstruction, *ProxyError) {
	switch ProxyInstructionKind(instr.Kind) {
	case ProxyInstructionKindGet:
		key, err := parseGetKey(instr.Data)
		if err != nil {
			return ProxyInstruction{}, err
		}
		value, found, getErr := getProperty(target, key)
		if getErr != nil {
			return ProxyInstruction{}, getErr
		}
		if !found {
			return createUndefinedValue(), nil
		}
		return e.createValue(value), nil
	case ProxyInstructionKindApply:
		args, err := parseArgs(instr.Data)
		if err != nil {
			return ProxyInstruction{}, err
		}
		value, callErr := e.callTarget(target, args)
		if callErr != nil {
			return ProxyInstruction{}, callErr
		}
		return e.createValue(value), nil
	case ProxyInstructionKindConstruct:
		args, err := parseArgs(instr.Data)
		if err != nil {
			return ProxyInstruction{}, err
		}
		value, callErr := e.callTarget(target, args)
		if callErr != nil {
			return ProxyInstruction{}, callErr
		}
		return e.createValue(value), nil
	case ProxyInstructionKindRelease:
		refID, ok := parseReleaseID(instr.Data)
		if ok {
			e.registry.Delete(refID)
		}
		return createUndefinedValue(), nil
	default:
		return ProxyInstruction{}, &ProxyError{Message: "unsupported instruction"}
	}
}

func (e *ExportedProxyable) createValue(value interface{}) ProxyInstruction {
	if isPrimitive(value) {
		return createPrimitiveValue(value)
	}
	if value == nil {
		return createNullValue()
	}
	refID := e.registry.Register(value)
	return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindReference), Data: refID}
}

func (e *ExportedProxyable) callTarget(target interface{}, args []interface{}) (interface{}, *ProxyError) {
	return callValue(target, args, e)
}

// Execute implements Executor for callback proxy cursors created on the exported side.
func (e *ExportedProxyable) Execute(ctx context.Context, instructions []ProxyInstruction) (interface{}, *ProxyError) {
	resp, err := executeRemote(ctx, e.streamPool, instructions, e.registry)
	if err != nil {
		return nil, err
	}
	return unwrapReturn(resp, e)
}
