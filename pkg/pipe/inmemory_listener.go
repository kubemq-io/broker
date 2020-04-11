package pipe

import (
	"errors"
	"net"
	"sync"
)

var DefaultInMemoryListener = &Options{
	MaxAcceptConn: 1024 * 10,
	MaxPipeBuffer: 1024,
}

// ErrInmemoryListenerClosed indicates that the InmemoryListener is already closed.
var ErrInmemoryListenerClosed = errors.New("InmemoryListener is already closed: use of closed network connection")

// InmemoryListener provides in-memory dialer<->net.Listener implementation.
//
// It may be used either for fast in-process client<->server communications
// without network stack overhead or for client<->server tests.
type Options struct {
	MaxAcceptConn int
	MaxPipeBuffer int
}
type InmemoryListener struct {
	lock   sync.Mutex
	closed bool
	conns  chan acceptConn
	opts   *Options
}

type acceptConn struct {
	conn     net.Conn
	accepted chan struct{}
}

// NewInmemoryListener returns new in-memory dialer<->net.Listener.
func NewInmemoryListener(opts *Options) *InmemoryListener {
	if opts == nil {
		opts = DefaultInMemoryListener
	}
	l := &InmemoryListener{
		lock:   sync.Mutex{},
		closed: false,
		conns:  make(chan acceptConn, opts.MaxAcceptConn),
		opts:   opts,
	}
	return l
}

// Accept implements net.Listener's Accept.
//
// It is safe calling Accept from concurrently running goroutines.
//
// Accept returns new connection per each Dial call.
func (ln *InmemoryListener) Accept() (net.Conn, error) {
	c, ok := <-ln.conns
	if !ok {
		return nil, ErrInmemoryListenerClosed
	}
	close(c.accepted)
	return c.conn, nil
}

// Close implements net.Listener's Close.
func (ln *InmemoryListener) Close() error {
	var err error

	ln.lock.Lock()
	if !ln.closed {
		close(ln.conns)
		ln.closed = true
	} else {
		err = ErrInmemoryListenerClosed
	}
	ln.lock.Unlock()
	return err
}

// Addr implements net.Listener's Addr.
func (ln *InmemoryListener) Addr() net.Addr {
	return &net.UnixAddr{
		Name: "In-memoryListener",
		Net:  "memory",
	}
}

// Dial creates new client<->server connection.
// Just like a real Dial it only returns once the server
// has accepted the connection.
//
// It is safe calling Dial from concurrently running goroutines.
func (ln *InmemoryListener) Dial() (net.Conn, error) {
	pc := NewPipeConns(ln.opts.MaxPipeBuffer)
	cConn := pc.Conn1()
	sConn := pc.Conn2()
	ln.lock.Lock()
	accepted := make(chan struct{})
	if !ln.closed {
		ln.conns <- acceptConn{sConn, accepted}
		// Wait until the connection has been accepted.
		<-accepted
	} else {
		sConn.Close() //nolint:errcheck
		cConn.Close() //nolint:errcheck
		cConn = nil
	}
	ln.lock.Unlock()

	if cConn == nil {
		return nil, ErrInmemoryListenerClosed
	}
	return cConn, nil
}
