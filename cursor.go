package proxyables

import (
	"context"
)

// Result is the async execution result.
type Result struct {
	Value interface{}
	Error *ProxyError
}

// Executor executes instruction batches.
type Executor interface {
	Execute(ctx context.Context, instructions []ProxyInstruction) (interface{}, *ProxyError)
}

// ProxyCursor builds batched instructions.
type ProxyCursor struct {
	executor     Executor
	instructions []ProxyInstruction
}

func NewProxyCursor(executor Executor, instructions []ProxyInstruction) *ProxyCursor {
	return &ProxyCursor{executor: executor, instructions: instructions}
}

// Get appends a get instruction.
func (c *ProxyCursor) Get(key string) *ProxyCursor {
	instr := CreateInstructionUnsafe(ProxyInstructionKindGet, []interface{}{key})
	return NewProxyCursor(c.executor, append(append([]ProxyInstruction{}, c.instructions...), instr))
}

// Apply appends an apply instruction.
func (c *ProxyCursor) Apply(args ...interface{}) *ProxyCursor {
	instr := CreateInstructionUnsafe(ProxyInstructionKindApply, args)
	return NewProxyCursor(c.executor, append(append([]ProxyInstruction{}, c.instructions...), instr))
}

// Construct appends a construct instruction.
func (c *ProxyCursor) Construct(args ...interface{}) *ProxyCursor {
	instr := CreateInstructionUnsafe(ProxyInstructionKindConstruct, args)
	return NewProxyCursor(c.executor, append(append([]ProxyInstruction{}, c.instructions...), instr))
}

// Await executes the batch asynchronously and returns a channel for select usage.
func (c *ProxyCursor) Await(ctx context.Context) <-chan Result {
	ch := make(chan Result, 1)
	go func() {
		value, err := c.executor.Execute(ctx, c.instructions)
		ch <- Result{Value: value, Error: err}
		close(ch)
	}()
	return ch
}

// Exec executes the batch synchronously.
func (c *ProxyCursor) Exec(ctx context.Context) (interface{}, *ProxyError) {
	return c.executor.Execute(ctx, c.instructions)
}

func (c *ProxyCursor) referenceID() (string, bool) {
	if len(c.instructions) != 1 {
		return "", false
	}
	instr := c.instructions[0]
	if ProxyValueKind(instr.Kind) != ProxyValueKindReference {
		return "", false
	}
	if id, ok := instr.Data.(string); ok {
		return id, true
	}
	return "", false
}
