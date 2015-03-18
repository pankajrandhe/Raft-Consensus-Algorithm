package main

import (
	"github.com/pankajrandhe/cs733/assignment3/raft"
	"sync"
)

const serverCount int = 5

var w sync.WaitGroup

func main() {

	var servers = make([]raft.ServerConfig, 5, 5)
	var cluster = raft.ClusterConfig{servers}
	var j int = 0

	// Configure the 5 servers in the cluster
	for i := 0; i < serverCount; i++ {

		// Define the sever parameters while booting-up
		id := i
		hostname := "localhost"
		clientPort := 9000
		eventCh := make(chan raft.Event)
		term := 0
		lastLogTerm := 0
		lastLogIndex := 0
		VoteHistory := make([]bool, 10) //Change later on, voteHistrory can't be limited ... (CHECK)

		serverConfig := raft.ServerConfig{id, hostname, clientPort, eventCh, term, lastLogTerm, lastLogIndex, VoteHistory}
		servers[i] = serverConfig
		clientPort++
	}

	// Spawn five servers as GO routines
	for _, _ = range cluster.Servers {
		thisServerId := j
		j++
		leaderId := -1 // since leader is not elected thus leaderid has invalid value... (CHECK)
		//Initialize the Raft Instance for each server
		raftInst, _ := raft.NewRaft(&cluster, thisServerId, leaderId, serverCount)
		w.Add(1)
		go raftInst.Loop(thisServerId, w)
	}
	w.Wait()
}
