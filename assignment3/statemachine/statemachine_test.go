package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"testing"
	"errors"
	"time"
)

// start the servers
func init() {
	go main()
}

func TestRaft(t *testing.T) {
	commit_err := errors.New("Entry not committed")
	time.Sleep(1*time.Second)  //let the servers boot-up

	// Test on Leader's commit channel
	raft.Send(serverMap[4].ThisServerId, "set abc 10 0 10")
	msg1 := raft.Receive(serverMap[4].ThisServerId)
	if !msg1.Log_commit{t.Error(commit_err)}

	raft.Send(serverMap[4].ThisServerId, "set xyz 10 0 10\r\n")
	msg2 := raft.Receive(serverMap[4].ThisServerId)
	if !msg2.Log_commit{t.Error(commit_err)}
}
