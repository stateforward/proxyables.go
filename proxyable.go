package proxyables

import (
	"context"
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
	return imported.Root(), imported, nil
}

// ExecuteAsync is a helper to execute a cursor and return a channel for select usage.
func ExecuteAsync(ctx context.Context, cursor *ProxyCursor) <-chan Result {
	return cursor.Await(ctx)
}

// CreateExportedProxyable is a naming-aligned helper for Export.
func CreateExportedProxyable(conn net.Conn, object interface{}, opts *ExportOptions) (*ExportedProxyable, error) {
	return Export(conn, object, opts)
}

// CreateImportedProxyable is a naming-aligned helper for ImportFrom.
func CreateImportedProxyable(conn net.Conn, opts *ImportOptions) (*ProxyCursor, *ImportedProxyable, error) {
	return ImportFrom(conn, opts)
}
