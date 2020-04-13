package server



func (s *StanServer) RaftState() int32  {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.raft!= nil {
		return int32(s.raft.Raft.State())
	}
	return -1
}
