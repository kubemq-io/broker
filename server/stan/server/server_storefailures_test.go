// Copyright 2016-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/broker/client/stan/pb"
	"github.com/kubemq-io/broker/server/stan/spb"
	"github.com/kubemq-io/broker/server/stan/stores"
)

type mockedStore struct {
	stores.Store
}

type mockedMsgStore struct {
	stores.MsgStore
	sync.RWMutex
	fail bool
}

type mockedSubStore struct {
	stores.SubStore
	sync.RWMutex
	fail          bool
	failFlushOnce bool
	ch            chan bool
}

func (ms *mockedStore) CreateChannel(name string) (*stores.Channel, error) {
	cs, err := ms.Store.CreateChannel(name)
	if err != nil {
		return nil, err
	}
	cs.Msgs = &mockedMsgStore{MsgStore: cs.Msgs}
	cs.Subs = &mockedSubStore{SubStore: cs.Subs}
	return cs, nil
}

func (ms *mockedMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return nil, errOnPurpose
	}
	return ms.MsgStore.Lookup(seq)
}

func (ms *mockedMsgStore) FirstSequence() (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.FirstSequence()
}

func (ms *mockedMsgStore) LastSequence() (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.LastSequence()
}

func (ms *mockedMsgStore) FirstAndLastSequence() (uint64, uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, 0, errOnPurpose
	}
	return ms.MsgStore.FirstAndLastSequence()
}

func (ms *mockedMsgStore) GetSequenceFromTimestamp(startTime int64) (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.GetSequenceFromTimestamp(startTime)
}

func TestStartPositionFailures(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	cs := channelsGet(t, s.channels, "foo")
	mms := cs.store.Msgs.(*mockedMsgStore)
	mms.Lock()
	mms.fail = true
	mms.Unlock()

	// New only
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Last received
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartWithLastReceived()); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Time delta
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAtTimeDelta(time.Second)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Sequence start
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAtSequence(1)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// First
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAt(pb.StartPosition_First)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
}

type checkErrorLogger struct {
	dummyLogger
	checkErrorStr string
	gotError      bool
}

func (l *checkErrorLogger) Errorf(format string, args ...interface{}) {
	l.log(format, args...)
	l.Lock()
	if strings.Contains(l.msg, l.checkErrorStr) {
		l.gotError = true
	}
	l.Unlock()
}

func TestMsgLookupFailures(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "looking up"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rcvCh := make(chan bool)
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		rcvCh <- true
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	cs := channelsGet(t, s.channels, "foo")
	mms := cs.store.Msgs.(*mockedMsgStore)
	mms.Lock()
	mms.fail = true
	mms.Unlock()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	select {
	case <-rcvCh:
		t.Fatal("Should not have received the message")
	case <-time.After(100 * time.Millisecond):
		// we waited "long enoug" and did not receive anything, which is good
	}
	logger.Lock()
	gotErr := logger.gotError
	logger.Unlock()
	if !gotErr {
		t.Fatalf("Did not capture error about lookup")
	}
	mms.Lock()
	mms.fail = false
	mms.Unlock()
	sub.Unsubscribe()

	// Create subscription, manual ack mode, don't ack, wait for redelivery
	rdlvCh := make(chan bool)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if !m.Redelivered {
			rcvCh <- true
		} else if m.RedeliveryCount == 3 {
			rdlvCh <- true
			m.Ack()
		}
	}, stan.DeliverAllAvailable(), stan.SetManualAckMode(), stan.AckWait(ackWaitInMs(50)))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := Wait(rcvCh); err != nil {
		t.Fatal("Did not get our message")
	}
	// Activate store failure
	mms.Lock()
	mms.fail = true
	logger.Lock()
	logger.checkErrorStr = "Error getting message for redelivery"
	logger.gotError = false
	logger.Unlock()
	mms.Unlock()
	// Make sure message is not redelivered and we capture the error
	select {
	case <-rcvCh:
		t.Fatal("Should not have received the message")
	case <-time.After(100 * time.Millisecond):
		// we waited more than redelivery time and did not receive anything, which is good
	}
	logger.Lock()
	gotErr = logger.gotError
	logger.Unlock()
	if !gotErr {
		t.Fatalf("Did not capture error about redelivery")
	}
	mms.Lock()
	mms.fail = false
	mms.Unlock()

	// Now make sure that we do get it redelivered when the error clears
	select {
	case <-rdlvCh:
	case <-time.After(time.Second):
		t.Fatal("Redelivery should have continued until error cleared")
	}
	sub.Unsubscribe()
}

func (ss *mockedSubStore) CreateSub(sub *spb.SubState) error {
	ss.RLock()
	fail := ss.fail
	ch := ss.ch
	ss.RUnlock()
	if ch != nil {
		// Wait for notification that we can continue
		<-ch
	}
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.CreateSub(sub)
}

func (ss *mockedSubStore) AddSeqPending(subid, seq uint64) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.AddSeqPending(subid, seq)
}

func (ss *mockedSubStore) UpdateSub(sub *spb.SubState) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.UpdateSub(sub)
}

func (ss *mockedSubStore) DeleteSub(subid uint64) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.DeleteSub(subid)
}

func (ss *mockedSubStore) Flush() error {
	ss.RLock()
	fail := ss.failFlushOnce
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.Flush()
}

func TestDeleteSubFailures(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "deleting subscription"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create a plain sub
	psub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a queue sub
	qsub, err := sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a durable queue sub with manual ack and does not ack message
	ch := make(chan bool)
	dqsub1, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {
		ch <- true
	}, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Produce a message to this durable queue sub
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create 2 more durable queue subs
	dqsub2, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {},
		stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Ensure subscription is processed
	waitForNumSubs(t, s, clientName, 5)

	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()

	// Check that server reported an error
	checkError := func() {
		logger.Lock()
		gotIt := logger.gotError
		logger.gotError = false
		logger.Unlock()
		if !gotIt {
			stackFatalf(t, "Server did not log error on unsubscribe")
		}
	}

	// Now unsubscribe
	if err := psub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}
	// Wait for unsubscribe to be processed
	waitForNumSubs(t, s, clientName, 4)
	checkError()

	// Unsubscribe queue sub
	if err := qsub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}
	// Wait for unsubscribe to be processed
	waitForNumSubs(t, s, clientName, 3)
	checkError()

	// Close 1 durable queue sub
	if err := dqsub2.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Wait for close to be processed
	waitForNumSubs(t, s, clientName, 2)
	checkError()

	// Now check that when closing qsub1 that has an unack message,
	// server logs an error when trying to move the message to remaining
	// queue member
	logger.Lock()
	logger.checkErrorStr = "transfer message"
	logger.Unlock()
	if err := dqsub1.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Wait for close to be processed
	waitForNumSubs(t, s, clientName, 1)
	checkError()
}

func TestUpdateSubFailure(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "add subscription"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	dur, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	dur.Close()
	waitForNumSubs(t, s, clientName, 0)

	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur")); err == nil {
		t.Fatal("Expected subscription to fail")
	}
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatalf("Server did not log error on subscribe")
	}
}

func TestCloseClientWithDurableSubs(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "flushing store"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.QueueSubscribe("foo", "bar",
		func(_ *stan.Msg) {},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)

	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.failFlushOnce = true
	mss.Unlock()

	// Close client, we should get failure trying to update the queue sub record
	sc.Close()
	waitForNumClients(t, s, 0)

	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatalf("Server did not log error on close client")
	}
}

func TestSendMsgToSubStoreFailure(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "add pending message"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)

	// Cause failure on AddSeqPending
	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Check error was logged.
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not log error about updating subscription")
	}
}

func TestClientStoreError(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "unregistering client"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	s.clients.Lock()
	s.clients.store = &clientStoreErrorsStore{Store: s.clients.store}
	s.clients.Unlock()

	// Client should not fail to close
	if err := sc.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// However, server should have logged something about an error closing client
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not report error about closing client")
	}
	// Verify that client is gone though
	if c := s.clients.lookup(clientName); c != nil {
		t.Fatalf("Unexpected client in server: %v", c)
	}

	logger.Lock()
	logger.gotError = false
	logger.checkErrorStr = "registering client"
	logger.Unlock()

	if _, err := stan.Connect(clusterName, clientName); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Expected error on connect, got %v", err)
	}
	logger.Lock()
	gotIt = logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not report error about registering client")
	}
	if c := s.clients.lookup(clientName); c != nil {
		t.Fatalf("Unexpected client in server: %v", c)
	}
}

type delChannStore struct {
	stores.Store
	sync.RWMutex
	fail bool
	ch   chan bool
}

func (ms *delChannStore) DeleteChannel(name string) error {
	ms.RLock()
	fail := ms.fail
	ch := ms.ch
	ms.RUnlock()
	defer func() { ch <- true }()
	if fail {
		return errOnPurpose
	}
	return ms.Store.DeleteChannel(name)
}

func TestDeleteChannelStoreError(t *testing.T) {
	opts := GetDefaultOptions()
	logger := &checkErrorLogger{checkErrorStr: "deleting channel"}
	opts.CustomLogger = logger
	opts.StoreLimits.MaxInactivity = 100 * time.Millisecond
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	s.channels.Lock()
	ms := &delChannStore{Store: s.channels.store, ch: make(chan bool)}
	s.channels.store = ms
	s.channels.Unlock()

	testDeleteChannel = true
	defer func() { testDeleteChannel = false }()

	c, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	if c.activity == nil {
		t.Fatalf("Activity not created")
	}
	time.Sleep(2 * opts.StoreLimits.MaxInactivity)
	// Check for possible lookup while channel is being deleted.
	if _, err := s.lookupOrCreateChannel("foo"); err != ErrChanDelInProgress {
		t.Fatalf("Expected error %v, got %v", ErrChanDelInProgress, err)
	}
	// Wait to be notified that channel has been deleted
	if err := Wait(ms.ch); err != nil {
		t.Fatal("Channel was not deleted")
	}
	// Channel should have been deleted and no longer be in map
	if s.channels.get("foo") != nil {
		t.Fatalf("Channel should have been removed")
	}
	// Check that timer is off
	s.channels.RLock()
	dip := c.activity.deleteInProgress
	tset := c.activity.timerSet
	s.channels.RUnlock()
	if !dip {
		t.Fatalf("DeleteInProgress not expected to have been reset")
	}
	if tset {
		t.Fatalf("Timer should have been stopped")
	}

	// Don't sleep anymore
	testDeleteChannel = false

	// Now introduce failure
	ms.Lock()
	ms.fail = true
	ms.Unlock()

	// Create new channel
	c, err = s.lookupOrCreateChannel("bar")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	// Wait to be notified that store tried to delete channel
	if err := Wait(ms.ch); err != nil {
		t.Fatal("Channel was not deleted")
	}
	// We get the notification after the mock DeleteChannel returns the
	// error, but the server logs the error after that call, so make
	// sure we give it a chance.
	waitFor(t, time.Second, 15*time.Millisecond, func() error {
		logger.Lock()
		gotIt := logger.gotError
		logger.Unlock()
		if !gotIt {
			return fmt.Errorf("No error about deleting channel was logged")
		}
		return nil
	})
	// Check that the activity struct has been reset properly
	s.channels.RLock()
	dip = c.activity.deleteInProgress
	tset = c.activity.timerSet
	s.channels.RUnlock()
	if dip {
		t.Fatalf("DeleteInProgress should have been reset")
	}
	if !tset {
		t.Fatalf("Timer should be active")
	}
	// Remove failure
	ms.Lock()
	ms.fail = false
	ms.Unlock()
	// Wait for deletion
	if err := Wait(ms.ch); err != nil {
		t.Fatal("Channel was not deleted")
	}
	// Channel should have been deleted and no longer be in map
	if s.channels.get("foo") != nil {
		t.Fatalf("Channel should have been removed")
	}
	// Check that timer is off
	s.channels.RLock()
	dip = c.activity.deleteInProgress
	tset = c.activity.timerSet
	s.channels.RUnlock()
	if !dip {
		t.Fatalf("DeleteInProgress not expected to have been reset")
	}
	if tset {
		t.Fatalf("Timer should have been stopped")
	}
}

func TestCreateChannelError(t *testing.T) {
	opts := GetDefaultOptions()
	logger := &checkErrorLogger{checkErrorStr: "Creating channel \"foo\" failed: " + errOnPurpose.Error()}
	opts.CustomLogger = logger
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	s.channels.Lock()
	ms := &testChannelStoreFailStore{Store: s.channels.store}
	s.channels.store = ms
	s.channels.Unlock()

	if _, err := s.channels.createChannel(s, "foo"); err == nil {
		t.Fatal("Expected error, got none")
	}
	if !logger.gotError {
		t.Fatal("Did not log the expected error")
	}
}

type noSrvStateLogger struct {
	dummyLogger
	ch chan string
}

func (l *noSrvStateLogger) Warnf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "not recovered") {
		select {
		case l.ch <- msg:
		default:
		}
	}
}

func TestFileStoreServerStateMissing(t *testing.T) {
	l := &noSrvStateLogger{ch: make(chan string, 1)}

	// Force the storage to be FILE, regardless of persistent_store value.
	opts := GetDefaultOptions()
	opts.CustomLogger = l
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	opts.FileStoreOpts.BufferSize = 1024
	if err := os.RemoveAll(defaultDataStore); err != nil {
		t.Fatalf("Error cleaning up datastore: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(defaultDataStore); err != nil {
			t.Fatalf("Error cleaning up datastore: %v", err)
		}
	}()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	if _, err := s.channels.createChannel(s, "foo"); err != nil {
		t.Fatal("Expected error, got none")
	}

	s.Shutdown()
	os.Remove(filepath.Join(defaultDataStore, "server.dat"))

	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	if c := s.channels.get("foo"); c == nil {
		t.Fatal("Expected channel to be recovered, was not")
	}

	select {
	case <-l.ch: // OK
	default:
		t.Fatal("Did not get warning about non recovered server state")
	}
}

func TestSQLStoreServerStateMissing(t *testing.T) {
	// If user doesn't want to run any SQL tests, we need to bail.
	if !doSQL {
		t.Skip()
	}
	// Force persistent store to be SQL for this test.
	orgps := persistentStoreType
	persistentStoreType = stores.TypeSQL
	defer func() { persistentStoreType = orgps }()

	cleanupDatastore(t)
	defer cleanupDatastore(t)

	l := &noSrvStateLogger{ch: make(chan string, 1)}

	opts := getTestDefaultOptsForPersistentStore()
	opts.CustomLogger = l
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	if _, err := s.channels.createChannel(s, "foo"); err != nil {
		t.Fatal("Expected error, got none")
	}

	s.Shutdown()

	db, err := sql.Open(testSQLDriver, testSQLSource)
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("DELETE FROM ServerInfo"); err != nil {
		t.Fatalf("Error deleting server info: %v", err)
	}
	db.Close()

	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	if c := s.channels.get("foo"); c == nil {
		t.Fatal("Expected channel to be recovered, was not")
	}

	select {
	case <-l.ch: // OK
	default:
		t.Fatal("Did not get warning about non recovered server state")
	}
}
