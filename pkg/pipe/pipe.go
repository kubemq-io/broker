package pipe

import (
	"net"
)

type Pipe struct {
	name    string
	address string
	*InmemoryListener
	isShutdown bool
}

func NewPipe(name string) *Pipe {
	p := &Pipe{
		name:             name,
		InmemoryListener: NewInmemoryListener(DefaultInMemoryListener),
	}

	return p
}

func (p *Pipe) Dial(network, address string) (net.Conn, error) {
	p.address = address
	return p.InmemoryListener.Dial()
}

func (p *Pipe) Name() string {
	return p.name
}

func (p *Pipe) Address() string {
	return p.address
}
func (p *Pipe) SetShutdown() {
	p.isShutdown = true

}
func (p *Pipe) IsClosed() bool {
	return p.isShutdown
}
