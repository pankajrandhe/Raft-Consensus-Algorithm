package main

import (
	"errors"
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"testing"
	"time"
)

// start the servers
func init() {
	go main()
}

func TestRaft(t *testing.T) {
	commit_err := errors.New("Entry not committed")
	time.Sleep(1 * time.Second) //let the servers boot-up

	// Test on Leader's commit channel
	raft.Send(raft.RaftMap[4].ThisServerId, "set abc 10 0 10\r\n")
	msg1 := raft.Receive(raft.RaftMap[4].ThisServerId)
	if !msg1.Log_commit {
		t.Error(commit_err)
	}
	//time.Sleep(1*time.Second)
	raft.Send(raft.RaftMap[4].ThisServerId, "set x/y/z 8 0 0\r\n")
	msg2 := raft.Receive(raft.RaftMap[4].ThisServerId)
	if !msg2.Log_commit {
		t.Error(commit_err)
	}
}
