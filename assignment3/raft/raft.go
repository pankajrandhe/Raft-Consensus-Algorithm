package raft

import (
	"fmt"
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

var T int = 150 // Timeout between T and 2T
var threshold = 3 // Threshold value for majority
var raft Raft
var RaftMap map[int]*Raft

//type Lsn uint64 //Log sequence number, unique for all time.

type ErrRedirect int // See Log.Append. Implements Error interface.

type LogStruct struct {
	Log_lsn    int
	Log_term   int
	Log_data   string
	Log_commit bool
}

type Event struct {
	evType string
	data   interface{}
}

type VoteReq struct {
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

type Entry struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //Lsn
	PrevLogTerm  int
	Msg          string
	CommitIndex int
}

type AppendResponse struct {
	Term     int
	Index    int //Lsn
	ServerId int
	Success  bool
}

type CommitResponse struct {
	Index    int
	Committed bool
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

// Raft setup
type ServerConfig struct {
	Id           int // Id of server. Must be unique
	EventCh      chan Event
	CommitCh     chan LogStruct
	Term         int
	LastLogTerm  int
	LastLogIndex int
	VoteHistory  []bool
	CommitIndex  int
	LastApplied  int
	Log          map[int]*LogStruct
	//Hostname   string // name or ip of host
	//ClientPort int    // port at which server listens to client messages.
	//LogPort    int    // tcp port for inter-replica protocol messages.
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
	//MajorityMap   map[string]int
	//CmdHistMap    map[string]LogEntry
	//ConnectionMap map[string]net.Conn
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data string) (LogStruct, error)
	RequestVote(VoteChan chan VoteReq, ResponseChan chan string, voteAppeal VoteReq) (int, bool)
	AppendEntries(msg string) LogStruct
}

type LogEntry interface {
	Lsn() int
	Data() byte
	Committed() bool
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed

//func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
func NewRaft(config *ClusterConfig, thisServerId int, leaderId int, servers int) (*Raft, error) {

	//raft = Raft{config, thisServerId, LeaderId, commitCh, MajorityMap, CmdHistMap, ConnectionMap, serversCount}
	raft = Raft{config, thisServerId, leaderId, servers}

	var err error = nil
	return &raft, err
}

func (raft Raft) Loop(w *sync.WaitGroup) {
	state := follower // begin life as a follower
	for {
		switch state {
		case follower:
			state = raft.follower()
		case candidate:
			state = raft.candidate()
		case leader:
			state = raft.leader()
		default:
			return
		}
	}
	w.Done()
}

func (raft Raft) follower() int {
	var w sync.WaitGroup
	thisServerId := raft.ThisServerId
	currentTerm := raft.Cluster.Servers[thisServerId].Term
	//fmt.Println("inside follower " + strconv.Itoa(thisServerId))
	resetCh := make(chan interface{})
	stopCh := make(chan interface{})

	//start the timer
	electionTOut := random(T, 2*T)
	//fmt.Println(electionTOut)
	timer := time.NewTimer(time.Duration(electionTOut) * time.Millisecond) // to become candidate if no append reqs
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			select {
			case <-timer.C:
				raft.Cluster.Servers[thisServerId].EventCh <- Event{"Timeout", nil} // Generate the Timeout event on timer expiry
				//fmt.Println("follower " + strconv.Itoa(thisServerId) + " timeout")
			case <-resetCh:
				_ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
			case <-stopCh:
				_ = timer.Stop()
				return
			}
		}
	}()

	for {
		ev := <-raft.Cluster.Servers[thisServerId].EventCh
		event := ev.evType
		switch event {
		case "ClientAppend":
			// Do not handle clients in follower mode. Send it back up the pipe with committed = false
			//fmt.Println("got from client")
			/*ev.logEntry.commited = false
			  commitCh <- ev.logentry**/
			msg := ev.data.(string)
			raft.Cluster.Servers[raft.ThisServerId].CommitCh <- LogStruct{0, 0, msg, false}
		case "VoteRequest":
			//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:F event:VoteReq_recd")
			msg := ev.data.(VoteReq) // type assertion
			sender := raft.Cluster.Servers[msg.CandidateId]

			if msg.Term < currentTerm {
				//Responds with current term
				sender.EventCh <- Event{"VoteResponse", VoteResponse{thisServerId, currentTerm, false}}
			}
			if msg.Term > currentTerm {
				currentTerm = msg.Term // Updates the currentTerm
			}
			if !raft.Cluster.Servers[thisServerId].VoteHistory[currentTerm] { //not already voted in my term
				resetCh <- true
				sender.EventCh <- Event{"VoteResponse", VoteResponse{thisServerId, -1, true}} //reply ok to event.msg.serverid
				/*remember term, leader id (either in log or in separate file)*/
				raft.Cluster.Servers[thisServerId].VoteHistory[currentTerm] = true
			}
		case "AppendRPC":
			resetCh <- true //reset timer
			msg := ev.data.(Entry)
			if msg.Msg == "" {
				//fmt.Println(strconv.Itoa(thisServerId) + ":state=F event=HB_recd")
				// Update the Leader ID in server's own Raft object
				raft.LeaderId = msg.LeaderId
			} else {
				f := raft.Cluster.Servers[raft.ThisServerId]
				fmt.Println(strconv.Itoa(thisServerId) + "AppendEntriesRPC received")
				//prev entries of my log and event.msg  should match
				if f.LastLogIndex == 0 || (f.LastLogIndex == msg.PrevLogIndex && f.LastLogTerm==msg.PrevLogTerm){
					//Prepare the logentry and insert it into map
					logentry := LogStruct{f.LastLogIndex + 1, msg.Term, msg.Msg, false}
					raft.Cluster.Servers[raft.ThisServerId].Log[f.LastLogIndex+1] = &logentry
					raft.Cluster.Servers[raft.ThisServerId].LastLogIndex = raft.Cluster.Servers[raft.ThisServerId].LastLogIndex + 1
					//add to disk log
					filename := (raft.Cluster).Path
					err := writeToFile(filename, logentry)
					//flush disk log
					//respond ok to event.msg.serverid
					ok := Event{"AppendResponse", AppendResponse{raft.Cluster.Servers[raft.ThisServerId].LastLogTerm, 
													raft.Cluster.Servers[raft.ThisServerId].LastLogIndex, 
													raft.ThisServerId, 
													true}}
					if err == nil{raft.Cluster.Servers[msg.LeaderId].EventCh <- ok}
					fmt.Println(strconv.Itoa(f.Id)+": AppendResponse sent to"+strconv.Itoa(raft.LeaderId))
				} else {
					//respond err
					err := Event{"AppendResponse", AppendResponse{f.Term, f.LastLogIndex, f.Id, false}}
					raft.Cluster.Servers[msg.LeaderId].EventCh <- err
				}
			}
			/*if msg.Term < raft.Cluster.Servers[raft.ThisServerId].Term//, ignore
			  reset heartbeat timer
			  upgrade to event.msg.term if necessary*/
		case "CommitResponse":
			fmt.Println("Got commit response")
			msg := ev.data.(CommitResponse)
			raft.Cluster.Servers[raft.ThisServerId].Log[msg.Index].Log_commit = true
		case "Timeout":
			stopCh <- true
			return candidate // new state back to loop()
		}
	}
}

func (raft Raft) candidate() int {
	var w1, w2 sync.WaitGroup
	thisServerId := raft.ThisServerId
	//fmt.Println(strconv.Itoa(thisServerId) + ": inside candidate")
	votesReceived := 0
	resetCh := make(chan interface{})
	stopCh := make(chan interface{})

	lastlogterm := raft.Cluster.Servers[thisServerId].LastLogTerm
	lastlogindex := raft.Cluster.Servers[thisServerId].LastLogIndex

	// increment the current term
	currentTerm := raft.Cluster.Servers[thisServerId].Term + 1
	// vote for self
	votesReceived = votesReceived + 1
	// reset election timer
	electionTOut := random(T, 2*T)
	//fmt.Println(electionTOut)
	timer := time.NewTimer(time.Duration(electionTOut) * time.Millisecond)

	//Prepare the vote request and send RequestVote RPC to all other servers
	voteAppeal := Event{"VoteRequest", VoteReq{thisServerId, currentTerm, lastlogterm, lastlogindex}}
	w1.Add(1)
	go func() {
		defer w1.Done()
		for _, server := range raft.Cluster.Servers {
			if server.Id != thisServerId {
				raft.RequestVote(server.Id, voteAppeal)
				//fmt.Println(strconv.Itoa(thisServerId) + " Sending vote request to " + strconv.Itoa(server.Id))
			}
		}
	}()

	w2.Add(1)
	go func() {
		defer w2.Done()
		for {
			select {
			case <-timer.C:
				raft.Cluster.Servers[thisServerId].EventCh <- Event{"Timeout", nil} // Generate the Timeout event on timer expiry
				//fmt.Println("candidate " + strconv.Itoa(thisServerId) + " timeout")
				//return
			case <-resetCh:
				_ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
				//return
			case <-stopCh:
				_ = timer.Stop()
				return
			}
		}
	}()

	//Keep listening on event channel for votes OR AppendEntriesRPC from new leader OR election timeout
	for {
		ev := <-raft.Cluster.Servers[thisServerId].EventCh
		event := ev.evType
		switch event {
		case "VoteResponse":
			msg := ev.data.(VoteResponse) // type assertion
			if msg.Vote {
				fmt.Println(strconv.Itoa(raft.ThisServerId) + "vote recd")
				votesReceived = votesReceived + 1 // increment the voteCount
				if checkMajority(votesReceived) {
					return leader
				}
			} else {
				raft.Cluster.Servers[thisServerId].Term = msg.Term
				return follower
			}
		case "AppendRPC":
			// Return to follower state since new leader is elected
			//fmt.Println(strconv.Itoa(thisServerId) + ":state=C event=HB_recd")
			return follower
		case "Timeout":
			// start new election
			fmt.Println(strconv.Itoa(thisServerId) + ": state=C event=Timeout")
			return candidate
		}
	}
}

func (raft Raft) leader() int {
	raft.LeaderId = raft.ThisServerId
	var w sync.WaitGroup
	responseCount := make(map[int]int)
	//CommitIndex := 0
	lastIndex := raft.Cluster.Servers[raft.LeaderId].LastLogIndex
	//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:L event:leader elected")
	// nextIndex contains next log entry to be sent to each server
	nextIndex := []int{lastIndex+1,lastIndex+1,lastIndex+1,lastIndex+1,lastIndex+1} 
	//matchIndex contains index of highest log entry known to be replicated for each server
	matchIndex := []int{0,0,0,0,0} 
	//commitSent contains index of the commited logentry acknowledged to followers
	commitSent := []int{0,0,0,0,0} 
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			for _, server := range raft.Cluster.Servers {
				CommitIndex := raft.Cluster.Servers[raft.ThisServerId].CommitIndex
				if server.Id != raft.LeaderId {
					prevLogIndex := nextIndex[server.Id] - 1
					prevLogTerm := raft.Cluster.Servers[raft.ThisServerId].LastLogTerm
					term := raft.Cluster.Servers[raft.LeaderId].Term
					// Check if we have got some data to send to follower server
					if nextIndex[server.Id] <= raft.Cluster.Servers[raft.LeaderId].LastLogIndex{
						fmt.Println("sending AppendEntriesRPC")
						msg := raft.Cluster.Servers[raft.LeaderId].Log[nextIndex[server.Id]].Log_data
						raft.AppendEntries(server.Id,term,raft.LeaderId,prevLogIndex,prevLogTerm,msg,CommitIndex)
						nextIndex[server.Id] = nextIndex[server.Id] + 1 
					}else{
						// Else send the HeartBeat to mark the presence of the leader
						msg := ""
						raft.AppendEntries(server.Id,term,raft.LeaderId,prevLogIndex,prevLogTerm,msg,CommitIndex)
						fmt.Println("Sent HB to " + strconv.Itoa(server.Id))
					}  
				}
			}
			time.Sleep(100 * time.Millisecond) //Sending HB every 100ms
		}
	}()

	// This GO routine will send the Commit response to the follower servers once the logentry is committed at leader
	w.Add(1)
	go func(){
		defer w.Done()
		for{
			for _, server := range raft.Cluster.Servers{
				CommitIndex := raft.Cluster.Servers[raft.ThisServerId].CommitIndex
				if server.Id != raft.LeaderId{
					if CommitIndex > commitSent[server.Id] && matchIndex[server.Id] > commitSent[server.Id]{
						response := CommitResponse{raft.Cluster.Servers[raft.LeaderId].Log[commitSent[server.Id] + 1].Log_lsn,true}
						server.EventCh <- Event{"CommitResponse",response}
						commitSent[server.Id] = commitSent[server.Id] + 1
					}		
				}
			}
			time.Sleep(100 * time.Millisecond) //Sending HB every 100ms
		}
	}()

	//Keep listening on event channel client requests
	for {
		ev := <-raft.Cluster.Servers[raft.ThisServerId].EventCh
		event := ev.evType
		switch event {
		case "ClientAppend":
			msg := ev.data.(string)
			fmt.Println("Leader got:" + msg)
			logentry,err := raft.Append(msg)
			raft.Cluster.Servers[raft.ThisServerId].LastLogIndex = raft.Cluster.Servers[raft.ThisServerId].LastLogIndex+1
			if err==nil{responseCount[logentry.Lsn()] = 1} //Since the leader has put the entry successfully in its log	
		case "AppendResponse":
			// Check the append response and commit the leader's log entry on success
			msg := ev.data.(AppendResponse)
			if msg.Success {
				// Icrement the matchIndex for the follower server
				matchIndex[msg.ServerId] = matchIndex[msg.ServerId] + 1
				responseCount[msg.Index] = responseCount[msg.Index] + 1 // to check for majority
				if checkMajority(responseCount[msg.Index]) && responseCount[msg.Index] == 3 {
					// if got majority commit the logentry at leader
					raft.Cluster.Servers[raft.ThisServerId].Log[msg.Index].Log_commit = true
					// Increment the CommitIndex
					raft.Cluster.Servers[raft.ThisServerId].CommitIndex=raft.Cluster.Servers[raft.ThisServerId].CommitIndex+1 
					// Task: Put the entry on Leader's commitCh
					// Task: Increment the lastApplied after putting entry on commitCh
					// Task: Communicate with followers to commit this logentry
				}
			}
		}
	}
	w.Wait()
	return leader
}

// RequestVote RPC
func (raft Raft) RequestVote(serverId int, voteAppeal Event) {
	// Request the vote on vote channel of the follower
	raft.Cluster.Servers[serverId].EventCh <- voteAppeal
	/** NOTE: In case of real RPC it will return either term or vote **/
}

func (raft Raft) AppendEntries(receiverId int,term int,leaderId int,prevLogIndex int,prevLogTerm int,msg string,leaderCommit int){
	// Attach other metadata with log data and replicate to other servers
	appendEntry := Event{"AppendRPC", Entry{term,leaderId,prevLogIndex,prevLogTerm,msg,leaderCommit}}
	raft.Cluster.Servers[receiverId].EventCh <- appendEntry
}

func (raft Raft) Append(data string) (LogStruct, error) {
	prevLogIndex := raft.Cluster.Servers[raft.ThisServerId].LastLogIndex
	prevLogTerm := raft.Cluster.Servers[raft.ThisServerId].LastLogTerm
	// Prepare the log entry
	logentry := LogStruct{prevLogIndex + 1, prevLogTerm + 1, data, false}
	// Put the entry in leader's log and update the leader.LastLogIndex
	raft.Cluster.Servers[raft.ThisServerId].Log[logentry.Lsn()] = &logentry
	// Also write the entry to the disk file
	filename := (raft.Cluster).Path
	err := writeToFile(filename, logentry)
	return logentry, err
}

func Send(serverId int, msg string) {
	// Send the message on the EventCh of the destination server
	raft.Cluster.Servers[serverId].EventCh <- Event{"ClientAppend", msg}
}

func Receive(serverId int) LogStruct {
	response := <-raft.Cluster.Servers[serverId].CommitCh
	return response
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
	//rand.Seed(time.Now().Unix())
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
	if _, err = logFile.WriteString("\n " + strconv.Itoa(logentry.Lsn()) + " " + logentry.Data()); err != nil {
		return err
	}
	return nil
}

// ErrRedirect as an Error object
/*func (e ErrRedirect) Error() string {
	return "ERR_REDIRECT " + raft.Cluster.Servers[int(e)].Hostname + " " + strconv.Itoa(raft.Cluster.Servers[int(e)].ClientPort) + "\r\n"
}*/
