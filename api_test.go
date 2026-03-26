package proxyables

import "testing"

func TestInstructionParityConstants(t *testing.T) {
	if ProxyValueKindReference != 0x5a1b3c4d {
		t.Fatalf("reference kind drifted: got %#x", ProxyValueKindReference)
	}
	if ProxyInstructionKindRelease != 0x1a2b3c4d {
		t.Fatalf("release kind drifted: got %#x", ProxyInstructionKindRelease)
	}
}

func TestRootPackageSurface(t *testing.T) {
	var exported *ExportedProxyable
	var imported *ImportedProxyable
	var cursor *ProxyCursor

	if exported != nil || imported != nil || cursor != nil {
		t.Fatalf("unexpected non-nil zero values")
	}
}

func TestDSLInstructionShapes(t *testing.T) {
	if got := CreateGetInstruction("key").Data.([]interface{}); len(got) != 1 || got[0] != "key" {
		t.Fatalf("unexpected get payload: %#v", got)
	}

	if got := CreateApplyInstruction(1, 2).Data.([]interface{}); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unexpected apply payload: %#v", got)
	}

	if got := CreateConstructInstruction(1, 2).Data.([]interface{}); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unexpected construct payload: %#v", got)
	}

	if got := CreateReleaseInstruction("ref-1").Data.([]interface{}); len(got) != 1 || got[0] != "ref-1" {
		t.Fatalf("unexpected release payload: %#v", got)
	}

	if CreateExecuteInstruction([]ProxyInstruction{CreateGetInstruction("key")}).Kind != uint32(ProxyInstructionKindExecute) {
		t.Fatalf("unexpected execute kind")
	}
}
