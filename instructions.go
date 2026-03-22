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

// CreateReturnInstruction wraps a value for the wire protocol.
func CreateReturnInstruction(value interface{}) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindReturn, value)
}

// CreateReleaseInstruction requests remote reference release.
func CreateReleaseInstruction(refID string) ProxyInstruction {
	return CreateInstructionUnsafe(ProxyInstructionKindRelease, []interface{}{refID})
}
