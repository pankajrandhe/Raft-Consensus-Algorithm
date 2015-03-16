package main

import (
    "time"
    "math/rand"
    "github.com/pankajrandhe/cs733/assignment3/raft"
)


var cluster raft.ClusterConfig
var servers int = 5
var serverId int
var currentTerm int = 0
const follower = 0
const candidate = 1
const leader = 2
var i int = 0
var j int = 0

type raft_type raft.Raft


func main(){ 

    // Configure the servers in the cluster
    for _,server := range cluster.Servers{

        // Define the sever parameters while boting-up
        id := i
        hostname := "localhost"
        clientPort := 9000
        voteCh:= make(chan raft.VoteReq)
        responseCh := make(chan string)
        term := 0
        lastLogTerm := 0
        lastLogIndex := 0

        serverConfig := raft.ServerConfig{id,hostname,clientPort,voteCh,responseCh,term,lastLogTerm,lastLogIndex}
        cluster.Servers[i] = serverConfig
        i++
        clientPort++
    }

    // Spawn five servers as GO routines
    for _,server := range cluster.Servers{

        thisServerId := j
        j++
        leaderId := -1  // since leader is not elected yet it is invalid value
        //Initialize the Raft Instance for each server
        raftInst, err := raft.NewRaft(&cluster, thisServerId, leaderId, servers)    
        raftInst.loop(thisServerId)
    }
}

