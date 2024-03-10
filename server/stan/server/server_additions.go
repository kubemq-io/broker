package server

import (
	"fmt"
	"time"
)

func (s *StanServer) DeleteChannel(name string) error {
	channelToDelete := s.channels.get(name)
	if channelToDelete == nil {
		return fmt.Errorf("channel not found")
	}
	s.channels.Lock()
	lastMaxInactivity := channelToDelete.activity.maxInactivity
	channelToDelete.activity.maxInactivity = time.Second
	s.channels.Unlock()
	s.sendDeleteChannelRequest(channelToDelete)
	channelToDelete.resetDeleteTimer(lastMaxInactivity)
	return nil
}

func (s *StanServer) HasChannel(name string) bool {
	return s.channels.get(name) != nil
}
