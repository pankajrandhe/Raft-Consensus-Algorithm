package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"sync"
)

const serverCount int = 5

func main() {
	var servers = make([]raft.ServerConfig, 5, 5)
	var cluster = []raft.ClusterConfig{
		{"/tmp/log_file0", servers},
		{"/tmp/log_file1", servers},
		{"/tmp/log_file2", servers},
		{"/tmp/log_file3", servers},
		{"/tmp/log_file4", servers},
	}
	w := &sync.WaitGroup{}

	// Configure the 5 servers in the cluster
	for i := 0; i < serverCount; i++ {
		// Define the sever parameters while booting-up
		id := i
		// client port to be added
		// log port to be added

		serverConfig := raft.ServerConfig{id}
		servers[i] = serverConfig
	}

	for k := 0; k < serverCount; k++ {
		leaderId := -1 // since leader is not elected thus leaderid has invalid value... (CHECK)
		eventCh := make(chan interface{})
		commitCh := make(chan raft.LogStruct)
		currentTerm := 0
		lastLogTerm := 0
		lastLogIndex := 0
		VotedFor := -1
		commitIndex := 0
		lastApplied := 0
		log := make(map[int]*raft.LogStruct)
		//Initialize the Raft Instance for each server
		raftInst, _ := raft.NewRaft(&cluster[k], k, leaderId, serverCount, eventCh, commitCh,
			currentTerm, lastLogTerm, lastLogIndex, VotedFor, commitIndex, lastApplied, log)
		raft.RaftMap[k] = raftInst
	}

	for m := 0; m < serverCount; m++ {
		w.Add(1)
		go raft.RaftMap[m].Loop(w)
	}
	w.Wait()
}
