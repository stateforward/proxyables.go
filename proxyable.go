package proxyables

import (
	"net"
)

const (
	defaultStreamPoolSize  = 8
	defaultStreamPoolReuse = true
)

// ExportOptions configures exported proxyables.
type ExportOptions struct {
	StreamPoolSize  int
	StreamPoolReuse bool
}

// ImportOptions configures imported proxyables.
type ImportOptions struct {
	StreamPoolSize  int
	StreamPoolReuse bool
}

// Export creates an exported proxyable bound to a yamux server session.
func Export(conn net.Conn, object interface{}, opts *ExportOptions) (*ExportedProxyable, error) {
	if opts == nil {
		opts = &ExportOptions{StreamPoolSize: defaultStreamPoolSize, StreamPoolReuse: defaultStreamPoolReuse}
	}
	exported, err := NewExportedProxyable(conn, object, opts)
	if err != nil {
		return nil, err
	}
	return exported, nil
}

// ImportFrom creates an imported proxyable root cursor bound to a yamux client session.
func ImportFrom(conn net.Conn, opts *ImportOptions) (*ProxyCursor, *ImportedProxyable, error) {
	if opts == nil {
		opts = &ImportOptions{StreamPoolSize: defaultStreamPoolSize, StreamPoolReuse: defaultStreamPoolReuse}
	}
	imported, err := NewImportedProxyable(conn, opts)
	if err != nil {
		return nil, nil, err
	}
	return NewProxyCursor(imported, nil), imported, nil
}
