package main

import (
    "main"
    "time"
    "math/rand"
    "github.com/pankajrandhe/cs733/assignment3/raft"
)


var raft *Raft
var currentTerm int = 0
var servers int = 5

type voteReq struct{
    term int 
    candidateId int
    lastLogIndex int
    lastLogTerm int
}

func main(){ 

    // Spawn five servers as five GO routines
    for _,_ = range servers {

        go func (raft *Raft) {
        state := follower; // begin life as a follower

            for {
                switch (state)  {
                case follower: 
                    state = raft.follower()
                case candidate: 
                    state = raft.candidate()
                case leader: 
                    state = raft.leader()
                default: return
                }
            }
        }()
    }
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func (raft *Raft) follower() string {

    eventCh := make(chan string)
    T := 150  // T is 150ms
    election_timeout := random(T, 2*T) // This should be the random value between T, 2T
    timer := time.NewTimer(election_timeout * Millisecond) // to become candidate if no append reqs
    <- timer.C
    eventCh <- "Timeout"  // Generate the Timeout event on timer expiry
    
    go func() {
        for {
            event := <- eventCh
        
            switch event {
            case ClientAppend:
                // Do not handle clients in follower mode. Send it back up the
                // pipe with committed = false
                ev.logEntry.commited = false
                commitCh <- ev.logentry
            case VoteRequest:
                msg = event.msg
                if msg.term < currentTerm{   
                    voteChan <- currentTerm  //Responds with current term
                }
                if msg.term > currentTerm{
                    currentTerm = msg.term  // Updates the currentTerm
                }
                if !voteHistory[term]{  //not already voted in my term
                    reset timer
                    reply ok to event.msg.serverid
                    remember term, leader id (either in log or in separate file)
                } 
                    
            case AppendRPC:
                reset timer
                if msg.term < currentterm, ignore
                reset heartbeat timer
                upgrade to event.msg.term if necessary
                if prev entries of my log and event.msg match
                   add to disk log
                   flush disk log
                   respond ok to event.msg.serverid
                else
                   respond err.
            case Timeout : return candidate  // new state back to loop()
            }
        }
    }
}


func (raft *Raft) candidate() string {

    var votesReceived int = 0
    var voterequest voteReq

    currentTerm = currentTerm + 1  // increment the current term

    // Parallerly send the Vote Requests to all the servers 
    for _, _ = range servers {
        go func(){
            term, voteGranted = RequestVote(voteChan, voteReq)
            if voteGranted{
                votesReceived = votesReceived +1
                isMajority = checkMajority(votesReceived)
                if isMajority {
                    return leader   // return to leader state on getting the majority
                }
            }
        }()
    }
}

func (raft *Raft) leader() {

    // Send regular heartbeats (empty Append RPCs)
}