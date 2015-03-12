import main

package {
    "main"
    "time"
    "math/rand"
}

int servers := 5

// Spawn five servers as five GO routines
for i = 0; i < servers; i++ {

    go func (raft *Raft) loop() {
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
    }
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

raft.follower() {

    T := 150  // T is 150ms
    election_timeout := random(T, 2*T) // This should be the random value between T, 2T
    timer := time.NewTimer(election_timeout * Millisecond) // to become candidate if no append reqs
    <- timer.C
    
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
            if msg.term < currentterm, respond with 
            if msg.term > currentterm, upgrade currentterm
            if not already voted in my term
                reset timer
                reply ok to event.msg.serverid
                remember term, leader id (either in log or in separate file)
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

raft.candidate() {
}

raft.leader() {

    // Send regular heartbeats (empty Append RPCs)

}