package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"sync"
	"testing"
	"time"
)

// start the servers
func init() {
	go main()
}

func TestRaft(t *testing.T) {
	time.Sleep(1 * time.Second) //let the servers boot-up

	var w sync.WaitGroup
	var leader int
	var flag = false
	var commitCount1, commitCount2 int = 0, 0
	var response1, response2 raft.LogStruct

	// check who is the current leader
	for {
		for s := 0; s < 5; s++ {
			leader = raft.RaftMap[s].LeaderId //check if any of the server has info. about the leader
			if leader != -1 {
				flag = true
				break
			}
		}
		if flag {
			break
		}
	}
	// Leader is legitimate or not (there should be only one leader OR HB not yet received i.e. -1 status)
	for s := 0; s < 5; s++ {
		if (leader == raft.RaftMap[s].LeaderId) || (raft.RaftMap[s].LeaderId == -1) {
		} else {
			t.Error("More than one leader for the term")
		}
	}

	/***********************/

	// Now Append at leader's log and check if the logentry pops out at each server's commit channel
	raft.RaftMap[leader].ClientSend("set country India\r\n")
	time.Sleep(3 * time.Second)
	for s := 0; s < 5; s++ {
		w.Add(1)
		go func(m int) {
			response1 = <-raft.RaftMap[m].CommitCh
			commitCount1++
			w.Done()
		}(s)
	}
	w.Wait()
	if commitCount1 < 3 {
		t.Error("Logentry not coming out of the commit channel of the majority servers")
	}

	/***********************/

	// Send another Append and check if logs are ordered
	raft.RaftMap[leader].ClientSend("set country Sweden\r\n")
	time.Sleep(3 * time.Second)
	for s := 0; s < 5; s++ {
		w.Add(1)
		go func(m int) {
			response2 = <-raft.RaftMap[m].CommitCh
			commitCount2++
			w.Done()
		}(s)
	}
	w.Wait()
	if commitCount2 < 3 {
		t.Error("Logentry not coming out of the commit channel of the majority servers")
	}
	if response1.Log_lsn >= response2.Log_lsn {
		t.Error("Logentries are not monotonically increasing")
	}

	/***********************/

	// Now bring down the original leader and check if another leader is elected
	raft.RaftMap[leader].EventCh <- raft.KillServer{}
	time.Sleep(2 * time.Second)
	// check who is the leader
	for {
		for s := 0; s < 5; s++ {
			leader = raft.RaftMap[s].LeaderId //check if any of the server has info. about the leader
			if leader != -1 {
				flag = true
				break
			}
		}
		if flag {
			break
		}
	}
	// New Leader should be elected or not (there should be only one leader OR HB not yet received i.e. -1 status)
	for s := 0; s < 5; s++ {
		if (leader == raft.RaftMap[s].LeaderId) || (raft.RaftMap[s].LeaderId == -1) {
		} else {
			t.Error(raft.RaftMap[s].LeaderId)
		}
	}
}
