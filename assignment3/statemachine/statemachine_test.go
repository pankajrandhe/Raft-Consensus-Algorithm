package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"testing"
	"time"
	"sync"
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
	var commitCount = 0
	
	// check who is the leader
	for{
		for s:=0; s<5;s++{
			leader = raft.RaftMap[s].LeaderId  //check if any of the server has info. about the leader
			if leader != -1{
				flag = true
				break
			}
		}
		if flag{
			break
		}
	}

	// Leader is legitimate or not (there should be only one leader OR leader HB not yet received)
	for s:=0; s<5;s++{
		if (leader == raft.RaftMap[s].LeaderId) || (raft.RaftMap[s].LeaderId == -1){
		} else{
			t.Error("More than one leader for the term")
		}

	}
	
	// Now try to append at leader's log and check if the logentry pops out at each server's commit channel
	raft.RaftMap[leader].ClientSend("set x/y/z 10 0 10\r\n")
	time.Sleep(3*time.Second)
	for s:=0; s<5;s++ {
		w.Add(1)
		go func(m int){
			<- raft.RaftMap[m].CommitCh
			commitCount++
			w.Done()
		}(s)
	}
	w.Wait()	
	if commitCount <3{
		t.Error("Logentry not coming out of the commit channel of the majority servers")
	}
	
}
