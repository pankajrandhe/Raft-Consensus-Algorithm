package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"sync"
	"time"
)

const serverCount int = 5

func main() {
	var servers = make([]raft.ServerConfig, 5, 5)
	var cluster = raft.ClusterConfig{"/tmp/log_file", servers}
	w := &sync.WaitGroup{}

	raft.RaftMap = make(map[int]*raft.Raft)

	// Configure the 5 servers in the cluster
	for i := 0; i < serverCount; i++ {
		// Define the sever parameters while booting-up
		id := i
		eventCh := make(chan raft.Event)
		commitCh := make(chan raft.LogStruct)
		term := 0
		lastLogTerm := 0
		lastLogIndex := 0
		VoteHistory := make([]bool, 10) //Change later on, voteHistrory can't be limited ... (CHECK)
		commitIndex := 0
		lastApplied := 0
		log := make(map[int]*raft.LogStruct)

		serverConfig := raft.ServerConfig{id, eventCh, commitCh, term, lastLogTerm, lastLogIndex, VoteHistory, commitIndex, lastApplied, log}
		servers[i] = serverConfig
	}
	// Spawn five servers as GO routines
	for _, server := range cluster.Servers {
		leaderId := -1 // since leader is not elected thus leaderid has invalid value... (CHECK)
		//Initialize the Raft Instance for each server
		raftInst, _ := raft.NewRaft(&cluster, server.Id, leaderId, serverCount)
		// Store the raft instances of each server in the Map
		raft.RaftMap[server.Id] = raftInst
		w.Add(1)
		go raftInst.Loop(w)
	}

	time.Sleep(1 * time.Second)

	go func() {
		// Test on Leader's commit channel
		raft.Send(raft.RaftMap[4].ThisServerId, "set abc 10 0 10\r\n")
		//msg1 := raft.Receive(serverMap[4].ThisServerId)
		//fmt.Println(msg1)
		//time.Sleep(10*time.Millisecond)
		raft.Send(raft.RaftMap[4].ThisServerId, "set x/y/z 8 0 0\r\n")
		//msg2 := raft.Receive(serverMap[4].ThisServerId)
		//time.Sleep(10*time.Millisecond)
		raft.Send(raft.RaftMap[4].ThisServerId, "set p/q/r 8 0 0\r\n")
		//time.Sleep(10*time.Millisecond)
		//raft.Send(raft.RaftMap[4].ThisServerId, "set p/q/r 8 0 0\r\n")*/
	}()
	w.Wait()
}
