package proxyables

import "github.com/stateforward/hsm.go/muid"

// MakeID returns a new monotonically unique ID string.
func MakeID() string {
	return muid.MakeString()
}
