package server

import (
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/kubemq-io/broker/server/gnatsd/logger"
	"net"
	"strconv"
)

func (s *Server) StartWithPipe(mp *pipe.Pipe) {
	s.pipeListener = mp
	s.Start()
}

// AcceptLoop is exported for easier testing.
func (s *Server) AcceptLoop(clr chan struct{}) {
	// If we were to exit before the listener is setup properly,
	// make sure we close the channel.
	defer func() {
		if clr != nil {
			close(clr)
		}
	}()

	// Snapshot server options.
	opts := s.getOpts()

	hp := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	l, e := net.Listen("tcp", hp)
	if e != nil {
		s.Fatalf("Error listening on port: %s, %q", hp, e)
		return
	}
	s.Noticef("Listening for client connections on %s",
		net.JoinHostPort(opts.Host, strconv.Itoa(l.Addr().(*net.TCPAddr).Port)))

	// Alert of TLS enabled.
	if opts.TLSConfig != nil {
		s.Noticef("TLS required for client connections")
	}

	s.Noticef("Server id is %s", s.info.ID)
	s.Noticef("Server is ready")

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.listener = l

	// If server was started with RANDOM_PORT (-1), opts.Port would be equal
	// to 0 at the beginning this function. So we need to get the actual port
	if opts.Port == 0 {
		// Write resolved port back to options.
		opts.Port = l.Addr().(*net.TCPAddr).Port
	}

	// Now that port has been set (if it was set to RANDOM), set the
	// server's info Host/Port with either values from Options or
	// ClientAdvertise. Also generate the JSON byte array.
	if err := s.setInfoHostPortAndGenerateJSON(); err != nil {
		s.Fatalf("Error setting server INFO with ClientAdvertise value of %s, err=%v", s.opts.ClientAdvertise, err)
		s.mu.Unlock()
		return
	}
	// Keep track of client connect URLs. We may need them later.
	s.clientConnectURLs = s.getClientConnectURLs()
	s.mu.Unlock()

	// Let the caller know that we are ready
	close(clr)
	clr = nil

	tmpDelay := ACCEPT_MIN_SLEEP

	// Check if we have a in-memory pipe ready and start using it
	if s.pipeListener != nil {
		s.startGoRoutine(func() {
			s.runPipeListener(s.pipeListener)
			s.grWG.Done()
		})
	} else {
		s.Noticef("No in-memory pipe initiated, client server connection is local TCP")
	}

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if s.isLameDuckMode() {
				// Signal that we are not accepting new clients
				s.ldmCh <- true
				// Now wait for the Shutdown...
				<-s.quitCh
				return
			}
			tmpDelay = s.acceptError("Client", err, tmpDelay)
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		s.startGoRoutine(func() {

			s.createClient(conn)
			s.grWG.Done()
		})
	}
	s.done <- true
}

// Shutdown will shutdown the server instance by kicking out the AcceptLoop
// and closing all associated clients.
func (s *Server) Shutdown() {
	// Shutdown the eventing system as needed.
	// This is done first to send out any messages for
	// account status. We will also clean up any
	// eventing items associated with accounts.

	s.shutdownEventing()

	s.mu.Lock()
	// Prevent issues with multiple calls.
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	s.Noticef("Initiating Shutdown...")

	opts := s.getOpts()

	s.shutdown = true
	s.running = false
	s.grMu.Lock()
	s.grRunning = false
	s.grMu.Unlock()

	conns := make(map[uint64]*client)

	// Copy off the clients
	for i, c := range s.clients {
		conns[i] = c
	}
	// Copy off the connections that are not yet registered
	// in s.routes, but for which the readLoop has started
	s.grMu.Lock()
	for i, c := range s.grTmpClients {
		conns[i] = c
	}
	s.grMu.Unlock()
	// Copy off the routes
	for i, r := range s.routes {
		conns[i] = r
	}
	// Copy off the gateways
	s.getAllGatewayConnections(conns)

	// Copy off the leaf nodes
	for i, c := range s.leafs {
		conns[i] = c
	}

	// Number of done channel responses we expect.
	doneExpected := 0

	// Kick client AcceptLoop()
	if s.listener != nil {
		doneExpected++
		s.listener.Close()
		s.listener = nil
	}

	// Stop pipe listener
	if s.pipeListener != nil {
		s.pipeListener.SetShutdown()
		s.pipeListener.Close()
		s.pipeListener = nil
	}

	// Kick leafnodes AcceptLoop()
	if s.leafNodeListener != nil {
		doneExpected++
		s.leafNodeListener.Close()
		s.leafNodeListener = nil
	}

	// Kick route AcceptLoop()
	if s.routeListener != nil {
		doneExpected++
		s.routeListener.Close()
		s.routeListener = nil
	}

	// Kick Gateway AcceptLoop()
	if s.gatewayListener != nil {
		doneExpected++
		s.gatewayListener.Close()
		s.gatewayListener = nil
	}

	// Kick HTTP monitoring if its running
	if s.http != nil {
		doneExpected++
		s.http.Close()
		s.http = nil
	}

	// Kick Profiling if its running
	if s.profiler != nil {
		doneExpected++
		s.profiler.Close()
	}

	s.mu.Unlock()

	// Release go routines that wait on that channel
	close(s.quitCh)

	// Close client and route connections
	for _, c := range conns {
		c.setNoReconnect()
		c.closeConnection(ServerShutdown)
	}

	// Block until the accept loops exit
	for doneExpected > 0 {
		<-s.done
		doneExpected--
	}

	// Wait for go routines to be done.
	s.grWG.Wait()

	if opts.PortsFileDir != _EMPTY_ {
		s.deletePortsFile(opts.PortsFileDir)
	}

	s.Noticef("Server Exiting..")
	// Close logger if applicable. It allows tests on Windows
	// to be able to do proper cleanup (delete log file).
	s.logging.RLock()
	log := s.logging.logger
	s.logging.RUnlock()
	if log != nil {
		if l, ok := log.(*logger.Logger); ok {
			l.Close()
		}
	}

	// Notify that the shutdown is complete
	close(s.shutdownComplete)
}

func (s *Server) runPipeListener(p *pipe.Pipe) {
	defer func() {
		_ = p.Close()
		s.Noticef("In-Memory client server connection pipe: %s ended", p.Name())
	}()
	s.Noticef("In-Memory client server connection pipe: %s started", p.Name())
	for s.isRunning() {
		conn, err := p.Accept()
		if err != nil {
			if !p.IsClosed() {
				s.Errorf("In-Memory client server connection pipe error: %s", err.Error())
			}
			return
		}
		s.startGoRoutine(func() {
			s.createClient(conn)
			s.grWG.Done()
		})
	}
}

func (s *Server) MemoryPipe() *pipe.Pipe {
	return s.pipeListener
}
