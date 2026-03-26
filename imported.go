package proxyables

import (
	"context"
	"net"
	"runtime"
	"sync"

	"github.com/hashicorp/yamux"
	"github.com/vmihailenco/msgpack/v5"
)

// ImportedProxyable connects to a remote exported proxyable.
type ImportedProxyable struct {
	session    *yamux.Session
	registry   *ObjectRegistry
	streamPool *StreamPool
	closed     chan struct{}
	closeOnce  sync.Once
}

// NewImportedProxyable creates and starts a client-side proxyable.
func NewImportedProxyable(conn net.Conn, opts *ImportOptions) (*ImportedProxyable, error) {
	session, err := yamux.Client(conn, nil)
	if err != nil {
		return nil, err
	}
	imported := &ImportedProxyable{
		session:    session,
		registry:   opts.Registry,
		streamPool: NewStreamPool(session, opts.StreamPoolSize, opts.StreamPoolReuse),
		closed:     make(chan struct{}),
	}
	if imported.registry == nil {
		imported.registry = NewObjectRegistry()
	}
	go imported.acceptLoop()
	return imported, nil
}

// Cursor returns the remote root cursor for users who prefer PascalCase symmetry.
func (i *ImportedProxyable) Cursor() *ProxyCursor {
	return NewProxyCursor(i, nil)
}

// Close shuts down the imported proxyable.
func (i *ImportedProxyable) Close() error {
	var err error
	i.closeOnce.Do(func() {
		close(i.closed)
		err = i.session.Close()
		i.streamPool.Close()
	})
	return err
}

func (i *ImportedProxyable) acceptLoop() {
	for {
		stream, err := i.session.Accept()
		if err != nil {
			return
		}
		go i.handleStream(stream)
	}
}

func (i *ImportedProxyable) handleStream(conn net.Conn) {
	defer conn.Close()
	dec := msgpack.NewDecoder(conn)
	enc := msgpack.NewEncoder(conn)

	for {
		var instr ProxyInstruction
		if err := dec.Decode(&instr); err != nil {
			return
		}
		response := i.handleInstruction(instr)
		_ = enc.Encode(&response)
	}
}

func (i *ImportedProxyable) handleInstruction(instr ProxyInstruction) ProxyInstruction {
	kind := ProxyInstructionKind(instr.Kind)
	switch kind {
	case ProxyInstructionKindExecute:
		instructions, parseErr := parseInstructionSlice(instr.Data)
		if parseErr != nil {
			return CreateThrowInstruction(parseErr)
		}
		result, err := i.evaluateInstructions(instructions)
		if err != nil {
			return CreateThrowInstruction(err)
		}
		return CreateReturnInstruction(result)
	case ProxyInstructionKindRelease:
		refID, ok := parseReleaseID(instr.Data)
		if ok {
			i.registry.Delete(refID)
		}
		return CreateReturnInstruction(createUndefinedValue())
	default:
		return CreateThrowInstruction(&ProxyError{Message: "unsupported instruction"})
	}
}

func (i *ImportedProxyable) evaluateInstructions(instructions []ProxyInstruction) (ProxyInstruction, *ProxyError) {
	stack := make([]ProxyInstruction, 0, len(instructions))
	for _, instr := range instructions {
		if ProxyValueKind(instr.Kind) == ProxyValueKindReference {
			stack = append(stack, instr)
			continue
		}

		var target interface{}
		if len(stack) > 0 {
			last := stack[len(stack)-1]
			if ProxyValueKind(last.Kind) == ProxyValueKindReference {
				stack = stack[:len(stack)-1]
				if refID, ok := last.Data.(string); ok {
					if value, found := i.registry.Get(refID); found {
						target = value
					}
				}
			}
		}

		result, err := i.applyInstruction(target, instr)
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

func (i *ImportedProxyable) applyInstruction(target interface{}, instr ProxyInstruction) (ProxyInstruction, *ProxyError) {
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
		return i.createValue(value), nil
	case ProxyInstructionKindApply:
		args, err := parseArgs(instr.Data)
		if err != nil {
			return ProxyInstruction{}, err
		}
		value, callErr := i.callTarget(target, args)
		if callErr != nil {
			return ProxyInstruction{}, callErr
		}
		return i.createValue(value), nil
	case ProxyInstructionKindConstruct:
		args, err := parseArgs(instr.Data)
		if err != nil {
			return ProxyInstruction{}, err
		}
		value, callErr := i.callTarget(target, args)
		if callErr != nil {
			return ProxyInstruction{}, callErr
		}
		return i.createValue(value), nil
	case ProxyInstructionKindRelease:
		refID, ok := parseReleaseID(instr.Data)
		if ok {
			i.registry.Delete(refID)
		}
		return createUndefinedValue(), nil
	default:
		return ProxyInstruction{}, &ProxyError{Message: "unsupported instruction"}
	}
}

func (i *ImportedProxyable) createValue(value interface{}) ProxyInstruction {
	if isPrimitive(value) {
		return createPrimitiveValue(value)
	}
	if value == nil {
		return createNullValue()
	}
	refID := i.registry.Register(value)
	return ProxyInstruction{ID: MakeID(), Kind: uint32(ProxyValueKindReference), Data: refID}
}

func (i *ImportedProxyable) callTarget(target interface{}, args []interface{}) (interface{}, *ProxyError) {
	return callValue(target, args, i)
}

// Execute implements Executor for client-side proxy cursors.
func (i *ImportedProxyable) Execute(ctx context.Context, instructions []ProxyInstruction) (interface{}, *ProxyError) {
	resp, err := executeRemote(ctx, i.streamPool, instructions, i.registry)
	if err != nil {
		return nil, err
	}
	return unwrapReturn(resp, i)
}

func (i *ImportedProxyable) registerFinalizer(cursor *ProxyCursor, refID string) {
	runtime.SetFinalizer(cursor, func(_ *ProxyCursor) {
		go i.releaseReference(refID)
	})
}

func (i *ImportedProxyable) releaseReference(refID string) {
	stream, err := i.streamPool.Acquire()
	if err != nil {
		return
	}
	defer i.streamPool.Release(stream)
	enc := msgpack.NewEncoder(stream)
	dec := msgpack.NewDecoder(stream)
	_ = enc.Encode(CreateReleaseInstruction(refID))
	var resp ProxyInstruction
	_ = dec.Decode(&resp)
}

func (i *ImportedProxyable) RegistrySnapshot() ObjectRegistrySnapshot {
	return i.registry.Snapshot()
}
