package raft

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	follower  int = 1
	candidate int = 2
	leader    int = 3
)

const serverDown int = 9999 // just for testing
type KillServer struct {} //just for testing

var T int = 150 // Timeout between T and 2T
var threshold = 3 // Threshold value for majority

var RaftMap = make(map[int]*Raft)
//type Lsn uint64 //Log sequence number, unique for all time.

//type ErrRedirect int // See Log.Append. Implements Error interface.

type LogStruct struct {
	Log_lsn    int
	Log_term   int
	Log_data   string
	Log_commit bool
}

type Timeout struct{
}

type ClientAppend struct{
	msg string
}

type VoteRequest struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	SenderId int
	Term     int
	Vote     bool
}

type AppendRPC struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //Lsn
	PrevLogTerm  int
	Msg          string
	LeaderCommit int
}

type AppendResponse struct {
	Term     int
	Index    int //Lsn
	ServerId int
	Success  bool
}

func (x LogStruct) Lsn() int {
	return x.Log_lsn
}

func (x LogStruct) Data() string {
	return x.Log_data
}

func (x LogStruct) Committed() bool {
	return x.Log_commit
}

func (x LogStruct) Term() int {
	return x.Log_term
}
// Raft setup
type ServerConfig struct {
	Id           int // Id of server. Must be unique
	//client port
	// log port
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	Cluster      *ClusterConfig
	ThisServerId int
	LeaderId     int
	ServersCount int
	EventCh      chan interface{}
	CommitCh     chan LogStruct
	CurrentTerm  int
	LastLogTerm  int
	LastLogIndex int
	VotedFor  int
	CommitIndex  int
	LastApplied  int
	Log          map[int]*LogStruct
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique lsn. 
	// The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data string) (LogStruct, error)
	RequestVote(serverId int, voteAppeal VoteRequest)
	AppendEntries(receiverId int,term int,leaderId int,prevLogIndex int,prevLogTerm int,msg string,leaderCommit int)
}

type LogEntry interface {
	Lsn() int
	Term() int
	Data() byte
	Committed() bool
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed

//func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
func NewRaft(config *ClusterConfig, thisServerId int, leaderId int, servers int, eventCh chan interface{},
			commitCh chan LogStruct,term int,lastLogTerm  int, lastLogIndex int, votedFor  int,commitIndex  int,
			lastApplied int, log map[int]*LogStruct) (*Raft, error) {
	
	var raft Raft
	raft = Raft{config, thisServerId, leaderId, servers, eventCh, commitCh, term, lastLogTerm, lastLogIndex,
				votedFor, commitIndex, lastApplied, log}
	var err error = nil
	return &raft, err
}

func (raft *Raft) Loop(w *sync.WaitGroup) {
	state := follower // begin life as a follower
	for {
		switch state {
		case follower:
			state = raft.follower()
		case candidate:
			state = raft.candidate()
		case leader:
			state = raft.leader()
		case serverDown:		// Just the dummy case to kill the server for testing
			return
		default:
			return
		}
	}
	w.Done()
}

func (raft *Raft) follower() int {
	var w sync.WaitGroup
	//fmt.Println("inside follower " + strconv.Itoa(thisServerId))
	resetCh := make(chan interface{})
	stopCh := make(chan interface{})

	//start the timer
	electionTOut := random(T, 2*T)
	timer := time.NewTimer(time.Duration(electionTOut) * time.Millisecond) // to become candidate if no append reqs
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			select {
			case <-timer.C:
				raft.EventCh <- Timeout{} // Generate the Timeout event on timer expiry
			case <-resetCh:
				_ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
			case <-stopCh:
				_ = timer.Stop()
				return
			}
		}
	}()

	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case ClientAppend:
			// Do not handle clients in follower mode. Send it back up the pipe with committed = false
			//fmt.Println("got from client")
			/*ev.logEntry.commited = false
			  commitCh <- ev.logentry**/
			msg := ev.(ClientAppend).msg
			raft.CommitCh <- LogStruct{0, 0, msg, false}
		case VoteRequest:
			//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:F event:VoteReq_recd from "+strconv.Itoa(ev.(VoteRequest).CandidateId))
			msg := ev.(VoteRequest) 
			toServer := raft.Cluster.Servers[msg.CandidateId].Id
			if msg.Term < raft.CurrentTerm {
				//Responds with current term for candidate to update its own term
				raft.Send(toServer,VoteResponse{raft.ThisServerId, raft.CurrentTerm, false})
			}
			cond1 := ((msg.LastLogTerm > raft.LastLogTerm)&&(raft.VotedFor == -1))
			cond2 := ((msg.LastLogTerm == raft.LastLogTerm)&&(msg.LastLogIndex >= raft.LastLogIndex)&& (raft.VotedFor == -1))
			if cond1 || cond2{
				/*remember term, leader id (either in log or in separate file)*/
				raft.VotedFor = msg.CandidateId
				raft.CurrentTerm = msg.Term 
			 	resetCh <- true
				raft.Send(toServer, VoteResponse{raft.ThisServerId, -1, true}) //reply ok to event.msg.serverids
				//fmt.Println("voted for "+strconv.Itoa(msg.CandidateId))
			}
		case AppendRPC:
			resetCh <- true //reset timer
			msg := ev.(AppendRPC)
			if msg.Msg == "" {
				//fmt.Println(strconv.Itoa(raft.ThisServerId) + "AppendEntriesRPC(HB) received")
				// Update the Leader ID in server's own Raft object
				raft.LeaderId = msg.LeaderId
				if raft.CommitIndex < msg.LeaderCommit{
					raft.CommitIndex = msg.LeaderCommit
					go func(){
						if raft.CommitIndex > raft.LastApplied{
							// put the log-entry on the commit channel
							raft.CommitCh <- *raft.Log[raft.LastApplied+1]
							raft.LastApplied = raft.LastApplied + 1
						}
					}()
				}	
			} else {
				//fmt.Println(strconv.Itoa(raft.ThisServerId) + "AppendEntriesRPC received")
				//prev entries of my log and event.msg  should match
				if raft.LastLogIndex == 0 || (raft.LastLogIndex == msg.PrevLogIndex && raft.LastLogTerm==msg.PrevLogTerm){
					//Prepare the logentry and insert it into map
					logentry := LogStruct{raft.LastLogIndex + 1, msg.Term, msg.Msg, false}
					raft.Log[raft.LastLogIndex+1] = &logentry
					raft.LastLogIndex = raft.LastLogIndex + 1
					//add to disk log
					filename := (raft.Cluster).Path
					err := writeToFile(filename, logentry)
					//flush disk log
					//respond ok to event.msg.serverid
					ok := AppendResponse{raft.LastLogTerm,raft.LastLogIndex, raft.ThisServerId, true}
					if err == nil{
						raft.Send(msg.LeaderId, ok)
						//fmt.Println(strconv.Itoa(raft.ThisServerId)+": AppendResponse sent to"+strconv.Itoa(raft.LeaderId))
					}
					if raft.CommitIndex < msg.LeaderCommit{
						raft.CommitIndex = msg.LeaderCommit
						go func(){
							if raft.CommitIndex > raft.LastApplied{
							// put the log-entry on the commit channel
							raft.CommitCh <- *raft.Log[raft.LastApplied+1]
							raft.LastApplied = raft.LastApplied + 1
							}
						}()
					}
				} else {
					//respond err
					err := AppendResponse{raft.CurrentTerm, raft.LastLogIndex, raft.ThisServerId, false}
					raft.Send(msg.LeaderId, err)
				}
			}
			/*if msg.Term < raft.Cluster.Servers[raft.ThisServerId].Term//, ignore
			  reset heartbeat timer
			  upgrade to event.msg.term if necessary*/
		case Timeout:
			stopCh <- true
			//fmt.Println("follower " + strconv.Itoa(raft.ThisServerId) + " timeout")
			return candidate // new state back to loop()
		}
	}
}

func (raft *Raft) candidate() int {
	var w1, w2 sync.WaitGroup
	//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": inside candidate")
	votesReceived := 0
	resetCh := make(chan interface{})
	stopCh := make(chan interface{})

	lastlogterm := raft.LastLogTerm
	lastlogindex := raft.LastLogIndex

	// increment the current term
	raft.CurrentTerm = raft.CurrentTerm + 1
	// vote for self
	votesReceived = votesReceived + 1
	raft.VotedFor = raft.ThisServerId //set votedfor to candidate id
	// reset election timer
	electionTOut := random(T, 2*T)
	timer := time.NewTimer(time.Duration(electionTOut) * time.Millisecond)
	//Prepare the vote request and send RequestVote RPC to all other servers
	voteAppeal := VoteRequest{raft.ThisServerId, raft.CurrentTerm, lastlogterm, lastlogindex}
	w1.Add(1)
	go func() {
		defer w1.Done()
		for _, server := range raft.Cluster.Servers {
			if server.Id != raft.ThisServerId {
				raft.RequestVote(server.Id, voteAppeal)
				//fmt.Println(strconv.Itoa(raft.ThisServerId) + " Sending vote request to " + strconv.Itoa(server.Id))
			}
		}
	}()

	w2.Add(1)
	go func() {
		defer w2.Done()
		for {
			select {
			case <-timer.C:
				raft.EventCh <- Timeout{} // Generate the Timeout event on timer expiry
			case <-resetCh:
				_ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
			case <-stopCh:
				_ = timer.Stop()
				return
			}
		}
	}()

	//Keep listening on event channel for votes OR AppendEntriesRPC from new leader OR election timeout
	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case VoteResponse:
			msg := ev.(VoteResponse) 
			if msg.Vote {
				//fmt.Println(strconv.Itoa(raft.ThisServerId) + "vote recd")
				votesReceived = votesReceived + 1 // increment the voteCount
				if checkMajority(votesReceived) {
					return leader
				}
			} else {
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				return follower
			}
		case AppendRPC:
			// Check for leader's term, Return to follower state if the leader is legitimate
			msg := ev.(AppendRPC)
			if msg.Term > raft.CurrentTerm{
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				return follower
			}
		case VoteRequest:
			// Check for leader's term, Return to follower state if the leader is legitimate
			msg := ev.(VoteRequest)
			if msg.Term > raft.CurrentTerm{
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				return follower
			}
		case Timeout:
			// start new election
			//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state=C event=Timeout")
			return candidate
		}
	}
}

func (raft *Raft) leader() int {
	raft.LeaderId = raft.ThisServerId
	var w sync.WaitGroup
	responseCount := make(map[int]int)
	lastIndex := raft.LastLogIndex
	//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:L event:leader elected")
	//fmt.Println(raft.CurrentTerm)
	// nextIndex contains next log entry to be sent to each server
	nextIndex := []int{lastIndex+1,lastIndex+1,lastIndex+1,lastIndex+1,lastIndex+1} 
	//matchIndex contains index of highest log entry known to be replicated for each server
	matchIndex := []int{0,0,0,0,0} 

	w.Add(1)
	go func() {
		defer w.Done()
		for {
			for _, server := range raft.Cluster.Servers {
				CommitIndex := raft.CommitIndex
				if server.Id != raft.LeaderId {
					prevLogIndex := nextIndex[server.Id] - 1
					prevLogTerm := raft.LastLogTerm 				//(CHECK...)
					// Check if we have got some data to send to follower server
					if nextIndex[server.Id] <= raft.LastLogIndex{
						msg := raft.Log[nextIndex[server.Id]].Log_data
						raft.AppendEntries(server.Id,raft.CurrentTerm,raft.LeaderId,prevLogIndex,prevLogTerm,msg,CommitIndex)
						//fmt.Println("AppendEntriesRPC Sent")
						nextIndex[server.Id] = nextIndex[server.Id] + 1 
					}else{
						// Else send the HeartBeat to mark the presence of the leader
						msg := ""
						raft.AppendEntries(server.Id,raft.CurrentTerm,raft.LeaderId,prevLogIndex,prevLogTerm,msg,CommitIndex)
						//fmt.Println("hb sent")
					}  
				}
			}
			time.Sleep(100 * time.Millisecond) //Sending HB every 100ms
		}
	}()

	//Keep listening on event channel client requests
	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case ClientAppend:
			msg := ev.(ClientAppend).msg
			//fmt.Println("Leader got:" + msg)
			logentry,err := raft.Append(msg) // Append entry to local log
			raft.LastLogIndex = raft.LastLogIndex+1
			if err==nil{responseCount[logentry.Lsn()] = 1} //Since the leader has put the entry successfully in its log	
		case AppendRPC:
			// Check for leader's term, Return to follower state if the leader is legitimate
			msg := ev.(AppendRPC)
			if msg.Term > raft.CurrentTerm{
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				//fmt.Println(strconv.Itoa(raft.ThisServerId)+"leader returning to follower(higher term in AppendRPC)=================================")
				return follower
			}
		/*case VoteRequest:
			// Check for leader's term, Return to follower state if the leader is legitimate
			fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:L event:VoteReq_recd from "+strconv.Itoa(ev.(VoteRequest).CandidateId))
			msg := ev.(VoteRequest)
							fmt.Println(msg.Term)
				fmt.Println(raft.CurrentTerm)
			if msg.Term > raft.CurrentTerm{
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				fmt.Println("leader returning to follower (higher term in Voterequest)===================")
				return follower
			}*/
		case AppendResponse:
			// Check the append response and commit the leader's log entry on success
			msg := ev.(AppendResponse)
			if msg.Success {
				// Icrement the matchIndex for the follower server
				//nextIndex[msg.ServerId] = nextIndex[msg.ServerId] + 1
				matchIndex[msg.ServerId] = matchIndex[msg.ServerId] + 1
				responseCount[msg.Index] = responseCount[msg.Index] + 1 // to check for majority
				if checkMajority(responseCount[msg.Index]) && responseCount[msg.Index] == 3 {
					// if got majority commit the logentry at leader
					raft.Log[msg.Index].Log_commit = true
					// Increment the CommitIndex
					raft.CommitIndex=raft.CommitIndex+1
					go func(){ // is this position correct? (CHECK........)
						if raft.CommitIndex > raft.LastApplied{
						// put the log-entry on the commit channel
						raft.CommitCh <- *raft.Log[raft.LastApplied+1]
						raft.LastApplied = raft.LastApplied + 1
						} 
					}()
				}
			}else{
					// leader can fail in appending entry to follower due to log inconsistency
					// Decrement the nextIndex and retry
					nextIndex[msg.ServerId] = nextIndex[msg.ServerId] - 1
			} 
			case KillServer: 
				raft.LeaderId = -1 //since the server is no more leader now
				return serverDown
		}
	}
	w.Wait()
	return leader
}

// RequestVote RPC
func (raft *Raft) RequestVote(serverId int, voteAppeal VoteRequest) {
	// Request the vote on vote channel of the follower
	//raft.Cluster.Servers[serverId].EventCh <- voteAppeal
	raft.Send(serverId,voteAppeal)
	/** NOTE: In case of real RPC it will return either term or vote **/
}

func (raft *Raft) AppendEntries(receiverId int,term int,leaderId int,prevLogIndex int,prevLogTerm int,msg string,leaderCommit int){
	// Attach other metadata with log data and replicate to other servers
	appendEntry := AppendRPC{term,leaderId,prevLogIndex,prevLogTerm,msg,leaderCommit}
	raft.Send(receiverId, appendEntry)
}

func (raft *Raft) Append(data string) (LogStruct, error) {
	// Prepare the log entry
	index := raft.LastLogIndex
	logentry := LogStruct{ index + 1, raft.CurrentTerm, data, false}
	// Put the entry in leader's log and update the leader.LastLogIndex
	raft.Log[logentry.Lsn()] = &logentry
	// Also write the entry to the disk file
	filename := (raft.Cluster).Path
	err := writeToFile(filename, logentry)
	return logentry, err
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func checkMajority(responses int) bool {
	if responses > 2 {
		return true
	} else {
		return false
	}
}


func writeToFile(filename string, logentry LogStruct) error {
	// Writing to Files - https://gobyexample.com/writing-files
	//Appending to Files - http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1
	//Check whether file already exists - http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		_, err := os.Create(filename)
		if err != nil {
			return err
		}
	}
	logFile, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer logFile.Close()
	if _, err = logFile.WriteString("\n"+strconv.Itoa(logentry.Lsn())+" "+strconv.Itoa(logentry.Term())+" " +logentry.Data()); err != nil {
		return err
	}
	return nil
}

func (raft *Raft) Send(toServer int, msg interface{}){
	delay_prob := random(1,100)
    if delay_prob < 30{
    	RaftMap[toServer].Receive(raft.ThisServerId,msg) //send directly
	} else if delay_prob > 30 && delay_prob <60 {
    	_ = time.AfterFunc(5 * time.Millisecond, func(){RaftMap[toServer].Receive(raft.ThisServerId,msg)})
    }else {//if delay_prob > 60 && delay_prob <90 {
    	_ = time.AfterFunc(10 * time.Millisecond, func(){RaftMap[toServer].Receive(raft.ThisServerId,msg)})
    }// else{
		// drop the packet
	//}*/
}
        
func (raft *Raft) Receive(fromServer int, msg interface{}){
	// Put the msg on receiver's channel
	raft.EventCh <- msg
}


func (raft *Raft) ClientSend(msg string) {
	// Send the message on the EventCh of the destination server
	raft.EventCh <- ClientAppend{msg}
}


// ErrRedirect as an Error object
/*func (e ErrRedirect) Error string {
	return "ERR_REDIRECT " + raft.Cluster.Servers[int(e)].Hostname + " " + strconv.Itoa(raft.Cluster.Servers[int(e)].ClientPort) + "\r\n"
}*/

/*** TO DO LIST **/
/* 1. PrevLogTerm problem
   4. Check definitions of lastlogindex and lastlogterm
*/     