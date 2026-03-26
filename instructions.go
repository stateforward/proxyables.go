package proxyables

// CreateInstructionUnsafe builds a proxy instruction without validation.
func CreateInstructionUnsafe(kind ProxyInstructionKind, data interface{}) ProxyInstruction {
	return ProxyInstruction{
		ID:   MakeID(),
		Kind: uint32(kind),
		Data: data,
	}
}

// CreateThrowInstruction wraps an error for the wire protocol.
func CreateThrowInstruction(err *ProxyError) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindThrow, err)
}

// CreateGetInstruction looks up a property on the current target.
func CreateGetInstruction(key string) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindGet, []interface{}{key})
}

// CreateApplyInstruction invokes the current target with positional arguments.
func CreateApplyInstruction(args ...interface{}) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindApply, args)
}

// CreateConstructInstruction constructs the current target with positional arguments.
func CreateConstructInstruction(args ...interface{}) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindConstruct, args)
}

// CreateExecuteInstruction wraps a batch of instructions for execution.
func CreateExecuteInstruction(instructions []ProxyInstruction) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindExecute, instructions)
}

// CreateReturnInstruction wraps a value for the wire protocol.
func CreateReturnInstruction(value interface{}) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindReturn, value)
}

// CreateReleaseInstruction requests remote reference release.
func CreateReleaseInstruction(refID string) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindRelease, []interface{}{refID})
}
