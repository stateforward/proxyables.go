package proxyables

import (
	"errors"
	"net"
	"sync"
)

type streamPoolRequest struct {
	ch chan net.Conn
}

// StreamPool reuses yamux streams for multiple calls.
type StreamPool struct {
	session Session
	max     int
	reuse   bool

	mu       sync.Mutex
	open     int
	idle     []net.Conn
	pending  []streamPoolRequest
	shutdown bool
}

type Session interface {
	Open() (net.Conn, error)
}

func NewStreamPool(session Session, max int, reuse bool) *StreamPool {
	if max < 1 {
		max = 1
	}
	return &StreamPool{session: session, max: max, reuse: reuse}
}

func (p *StreamPool) Acquire() (net.Conn, error) {
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		return nil, errors.New("stream pool closed")
	}
	if n := len(p.idle); n > 0 {
		stream := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return stream, nil
	}
	if p.open < p.max {
		p.open++
		p.mu.Unlock()
		stream, err := p.session.Open()
		if err != nil {
			p.mu.Lock()
			p.open--
			p.mu.Unlock()
			return nil, err
		}
		return stream, nil
	}
	req := streamPoolRequest{ch: make(chan net.Conn, 1)}
	p.pending = append(p.pending, req)
	p.mu.Unlock()
	stream := <-req.ch
	if stream == nil {
		return nil, errors.New("stream pool closed")
	}
	return stream, nil
}

func (p *StreamPool) Release(stream net.Conn) {
	if stream == nil {
		return
	}
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		_ = stream.Close()
		return
	}
	if len(p.pending) > 0 {
		req := p.pending[0]
		p.pending = p.pending[1:]
		p.mu.Unlock()
		req.ch <- stream
		return
	}
	if !p.reuse {
		p.open--
		p.mu.Unlock()
		_ = stream.Close()
		return
	}
	p.idle = append(p.idle, stream)
	p.mu.Unlock()
}

func (p *StreamPool) Close() {
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		return
	}
	p.shutdown = true
	idle := p.idle
	pending := p.pending
	p.idle = nil
	p.pending = nil
	p.mu.Unlock()

	for _, stream := range idle {
		_ = stream.Close()
	}
	for _, req := range pending {
		req.ch <- nil
		close(req.ch)
	}
}
