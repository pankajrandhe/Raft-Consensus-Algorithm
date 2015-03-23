package raft

import (
	"math/rand"
	"sync"
	"time"
	//"os"
)

const (
	follower  int = 1
	candidate int = 2
	leader    int = 3
)

var T int = 150 // Timeout between T and 2T
var raft Raft

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
}

type AppendResponse struct {
	Term     int
	Index    int //Lsn
	ServerId int
	Success  bool
}

type CommitMsg struct {
	Index    int
	LeaderId int
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
				//fmt.Println(strconv.Itoa(thisServerId) + "AppendEntriesRPC received")
				//prev entries of my log and event.msg  should match
				if f.LastLogIndex == 0 || (f.LastLogIndex == msg.PrevLogIndex && f.Log[f.LastLogIndex].Log_data == msg.Msg) {
					//Prepare the logentry and insert it into map
					f.Log[f.LastLogIndex+1] = &LogStruct{f.LastLogIndex + 1, msg.Term, msg.Msg, false}
					f.LastLogIndex = msg.PrevLogIndex + 1
					//add to disk log
					//flush disk log
					//respond ok to event.msg.serverid
					ok := Event{"AppendResponse", AppendResponse{f.Term, f.LastLogIndex, f.Id, true}}
					raft.Cluster.Servers[msg.LeaderId].EventCh <- ok
				} else {
					//respond err
					err := Event{"AppendResponse", AppendResponse{f.Term, f.LastLogIndex, f.Id, false}}
					raft.Cluster.Servers[msg.LeaderId].EventCh <- err
				}
			}
			/*if msg.Term < raft.Cluster.Servers[raft.ThisServerId].Term//, ignore
			  reset heartbeat timer
			  upgrade to event.msg.term if necessary*/
		case "CommitMsg":
			//msg := ev.data.(CommitMsg)
			//raft.Cluster.Servers[raft.ThisServerId].Log[msg.Index].Log_commit = true
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
				//fmt.Println(strconv.Itoa(raft.ThisServerId) + "vote recd")
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
			//fmt.Println(strconv.Itoa(thisServerId) + ": state=C event=Timeout")
			return candidate
		}
	}
}

func (raft Raft) leader() int {
	var w sync.WaitGroup
	responseCount := make(map[int]int)
	//fmt.Println(strconv.Itoa(raft.ThisServerId) + ": state:L event:leader elected")
	prevLogIndex := raft.Cluster.Servers[raft.ThisServerId].LastLogIndex
	prevLogTerm := raft.Cluster.Servers[raft.ThisServerId].LastLogTerm
	// Initialize the nextIndex[], matchIndex[]
	//nextIndex := make([]int,5,5) //contains next log entry to be sent to each server
	//matchIndex := make([]int,5,5) // contains index of highest log entry known to be replicated for each server
	HB := Event{"AppendRPC", Entry{raft.Cluster.Servers[raft.ThisServerId].Term, raft.ThisServerId, prevLogIndex, prevLogTerm, ""}}
	w.Add(1)
	go func() {
		defer w.Done()
		for {
			for _, server := range raft.Cluster.Servers {
				if server.Id != raft.ThisServerId {
					server.EventCh <- HB
					//fmt.Println("Sent HB to " + strconv.Itoa(server.Id))
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
			//fmt.Println("Leader got:" + msg)
			//logentry, err := raft.Append(msg)
			logentry := raft.AppendEntries(msg)
			// Put the response in response count
			responseCount[logentry.Lsn()] = 1 //Since the leader has put the entry successfully in its log
		case "AppendResponse":
			// Check the append response and commit the leader's log entry on success
			//fmt.Println("AppendResponse received")
			msg := ev.data.(AppendResponse)
			//fmt.Println(msg)
			if msg.Success {
				responseCount[msg.Index] = responseCount[msg.Index] + 1
				if checkMajority(responseCount[msg.Index]) && responseCount[msg.Index] == 3 {
					// if got majority commit the logentry at server
					raft.Cluster.Servers[raft.ThisServerId].Log[msg.Index].Log_commit = true
					// Put the entry on Leader's commitCh
					leader := raft.Cluster.Servers[raft.ThisServerId]
					logentry := leader.Log[msg.Index]
					leader.CommitCh <- *logentry
					// Increment the CommitIndex
					// Also increment the lastApplied after putting entry on commitCh
					// Also communicate with followers to commit this logentry
					/*for _, server := range raft.Cluster.Servers {
						if server.Id != raft.ThisServerId{
							server.EventCh <- Event{"CommiMsg",CommitMsg{msg.Index,raft.ThisServerId}}
						}
					}*/
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

func (raft Raft) AppendEntries(msg string) LogStruct {
	leader := raft.Cluster.Servers[raft.ThisServerId]
	prevLogIndex := leader.LastLogIndex
	prevLogTerm := leader.LastLogTerm
	// Prepare the log entry
	//fmt.Println(prevLogIndex)
	logentry := LogStruct{prevLogIndex + 1, prevLogTerm + 1, msg, false}
	// Put the entry in leader's log and update the leader.LastLogIndex
	leader.Log[logentry.Lsn()] = &logentry
	// Attach other metadata with log data and replicate to other servers
	entry := Event{"AppendRPC", Entry{raft.Cluster.Servers[raft.ThisServerId].Term, raft.ThisServerId, prevLogIndex, prevLogTerm, msg}}
	for _, server := range raft.Cluster.Servers {
		if server.Id != leader.Id {
			server.EventCh <- entry
			//fmt.Println("logentry sent to: " + strconv.Itoa(server.Id))
		}
	}
	return logentry
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

/*
func (raft Raft) Append(data string) (LogStruct, error) {
		fmt.Println("inside append")
		// Writing to Files - https://gobyexample.com/writing-files
		//Appending to Files - http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1
		//Check whether file already exists - http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
		leader := raft.Cluster.Servers[raft.ThisServerId]
		prevLogIndex := leader.LastLogIndex
		prevLogTerm := leader.LastLogTerm
		// Prepare the log entry
		fmt.Println(prevLogIndex)
		logentry := LogStruct{prevLogIndex + 1, prevLogTerm + 1, data, false}
		// Put the entry in leader's log and update the leader.LastLogIndex
		leader.Log[logentry.Lsn()] = &logentry
		// Also write the entry to the disk file
		filename := (raft.Cluster).Path
		var e error
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			_, err := os.Create(filename)
			if err != nil {
				e = err
			}
		}
		logFile, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			e = err
		}
		defer logFile.Close()
		if _, err = logFile.WriteString("\n "+strconv.Itoa(logentry.Lsn())+" "+logentry.Data());
		err != nil {
			e = err
		}
		return logentry,e
}
*/
func Send(serverId int, msg string) {
	// Send the message on the EventCh of the destination server
	raft.Cluster.Servers[serverId].EventCh <- Event{"ClientAppend", msg}
}

func Receive(serverId int) LogStruct {
	response := <-raft.Cluster.Servers[serverId].CommitCh
	return response
}

// ErrRedirect as an Error object
/*func (e ErrRedirect) Error() string {
	return "ERR_REDIRECT " + raft.Cluster.Servers[int(e)].Hostname + " " + strconv.Itoa(raft.Cluster.Servers[int(e)].ClientPort) + "\r\n"
}*/
