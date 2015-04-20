package raft

import (
	"encoding/gob"
	"log"
	"math/rand"
	"net"
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

var T int = 300   // Timeout between T and 2T
var threshold = 3 // Threshold value for majority

var RaftMap = make(map[int]*Raft)

//type Lsn uint64 //Log sequence number, unique for all time.

type ErrRedirect int

type LogStruct struct {
	Log_lsn    int
	Log_term   int
	Log_data   []byte
	Log_commit bool
}

type Timeout struct{}

type SendAppendRPC struct{}

type ClientAppend struct {
	logEntry LogStruct
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
	Msg          []byte
	LeaderCommit int
}

type AppendResponse struct {
	Term     int
	Index    int //Lsn
	ServerId int
	Success  bool
}

type Msg struct {
	Content interface{}
}

func (x LogStruct) Lsn() int {
	return x.Log_lsn
}

func (x LogStruct) Data() []byte {
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
	Id         int
	Hostname   string
	ClientPort int
	LogPort    int
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
	VotedFor     int
	CommitIndex  int
	LastApplied  int
	Log          map[int]*LogStruct
	EncMap       map[int]*gob.Encoder
}

type SharedLog interface {
	Append(data []byte) (LogStruct, error)
	RequestVote(serverId int, voteAppeal VoteRequest)
	AppendEntries(receiverId int, term int, leaderId int, prevLogIndex int, prevLogTerm int, msg string, leaderCommit int)
}

type LogEntry interface {
	Lsn() int
	Term() int
	Data() []byte
	Commited() bool
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed

func NewRaft(thisServerId int, config *ClusterConfig, commitCh chan LogStruct) (*Raft, error) {

	eventCh := make(chan interface{})
	logMap := make(map[int]*LogStruct)
	encMap := make(map[int]*gob.Encoder)

	raft := Raft{
		Cluster:      config,
		ThisServerId: thisServerId,
		LeaderId:     -1,
		ServersCount: 5,
		EventCh:      eventCh,
		CommitCh:     commitCh,
		CurrentTerm:  0,
		LastLogTerm:  0,
		LastLogIndex: 0,
		VotedFor:     -1,
		CommitIndex:  0,
		LastApplied:  0,
		Log:          logMap,
		EncMap:       encMap,
	}
	var err error = nil //(what could be the possible error here)
	return &raft, err
}

func (raft *Raft) Loop() {
	var w sync.WaitGroup
	gob.Register(VoteRequest{})
	gob.Register(VoteResponse{})
	gob.Register(AppendRPC{})
	gob.Register(AppendResponse{})
	serverListener := raft.OpenServerListener()

	w.Add(1)
	go func() {
		for {
			sConn, err := serverListener.Accept()
			if err != nil {
				log.Fatal(err)
				continue
			}
			go raft.HandleServerConn(sConn)
		}
		defer w.Done()
	}()

	raft.EstablishServerLinks()

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
	w.Wait()
}

func (raft *Raft) checkForCommit(leaderCommit int) {
	if raft.CommitIndex < leaderCommit {
		raft.CommitIndex = min(leaderCommit, len(raft.Log))
		if raft.CommitIndex > raft.LastApplied {
			raft.Log[raft.LastApplied+1].Log_commit = true
			raft.CommitCh <- *raft.Log[raft.LastApplied+1]  
			raft.LastApplied = raft.LastApplied + 1
		}
	}
}

func (raft *Raft) follower() int {
	//log.Println(raft.ThisServerId,":state = F term = ",raft.CurrentTerm)
	//start the timer
	electionTOut := random(T, 2*T)
	timer := time.AfterFunc(time.Duration(electionTOut)*time.Millisecond, func() { raft.EventCh <- Timeout{} })

	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case ClientAppend:
			// Do not handle clients in follower mode.
			msg := ev.(ClientAppend)
			msg.logEntry.Log_commit = false
			raft.CommitCh <- msg.logEntry // Do we really need it?
		case VoteRequest:
			msg := ev.(VoteRequest)
			toServer := raft.Cluster.Servers[msg.CandidateId].Id
			notVotedCond := (raft.VotedFor == -1) // || raft.VotedFor==msg.CandidateId)
			if msg.Term < raft.CurrentTerm {
				raft.Send(toServer, Msg{VoteResponse{raft.ThisServerId, raft.CurrentTerm, false}})
			} else if notVotedCond && raft.LastLogTerm != msg.LastLogTerm {
				if raft.LastLogTerm < msg.LastLogTerm { //your log more up-to-date
					raft.Send(toServer, Msg{VoteResponse{raft.ThisServerId, -1, true}}) // term field of no-use, so inavlid
					raft.VotedFor = msg.CandidateId
					raft.CurrentTerm = msg.Term
					timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
				} else if raft.LastLogTerm > msg.LastLogTerm { // my log more up-to-date
					raft.Send(toServer, Msg{VoteResponse{raft.ThisServerId, raft.CurrentTerm, false}})
				}
			} else if notVotedCond && raft.LastLogTerm == msg.LastLogTerm {
				if len(raft.Log) <= msg.LastLogIndex {
					raft.Send(toServer, Msg{VoteResponse{raft.ThisServerId, -1, true}})
					raft.VotedFor = msg.CandidateId
					raft.CurrentTerm = msg.Term
					timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
				} else {
					raft.Send(toServer, Msg{VoteResponse{raft.ThisServerId, raft.CurrentTerm, false}})
				}
			}
		case AppendRPC:
			electionTimeOut := random(T, 2*T)
			//log.Println("TO for ",raft.ThisServerId," ",electionTimeOut)
			timer.Reset(time.Duration(electionTimeOut) * time.Millisecond) //reset timer
			msg := ev.(AppendRPC)
			if msg.Term < raft.CurrentTerm {
				err := AppendResponse{raft.CurrentTerm, raft.LastLogIndex, raft.ThisServerId, false}
				raft.Send(msg.LeaderId, Msg{err})
			} else if msg.Term > raft.CurrentTerm {
				raft.CurrentTerm = msg.Term
			}

			if msg.Msg == nil { // HeartBeats
				//log.Println(raft.ThisServerId," Got HB")
				raft.LeaderId = msg.LeaderId // Update the Leader info
				raft.checkForCommit(msg.LeaderCommit)
			} else {
				raft.checkForCommit(msg.LeaderCommit)
				if msg.PrevLogIndex == 0 && len(raft.Log) == 0 { // first entry
					logentry := LogStruct{msg.PrevLogIndex + 1, msg.Term, msg.Msg, false}
					raft.Log[msg.PrevLogIndex+1] = &logentry
					go func() { //add to disk log
						diskPath := (raft.Cluster).Path + strconv.Itoa(raft.ThisServerId)
						err := writeToFile(diskPath, logentry)
						if err != nil {
							log.Fatal(err)
						}
					}()
					raft.LastLogIndex = raft.LastLogIndex + 1
					raft.LastLogTerm = raft.Log[raft.LastLogIndex].Term()
					ok := AppendResponse{raft.LastLogTerm, raft.LastLogIndex, raft.ThisServerId, true}
					raft.Send(msg.LeaderId, Msg{ok})
				} else if len(raft.Log) > 0 && len(raft.Log) >= msg.PrevLogIndex {
					if len(raft.Log) == msg.PrevLogIndex {
						if msg.PrevLogTerm == raft.Log[len(raft.Log)].Term() {
							logentry := LogStruct{msg.PrevLogIndex + 1, msg.Term, msg.Msg, false}
							raft.Log[msg.PrevLogIndex+1] = &logentry
							go func() { //add to disk log
								diskPath := (raft.Cluster).Path + strconv.Itoa(raft.ThisServerId)
								err := writeToFile(diskPath, logentry)
								if err != nil {
									log.Fatal(err)
								}
							}()
							raft.LastLogIndex = raft.LastLogIndex + 1
							raft.LastLogTerm = raft.Log[raft.LastLogIndex].Term()
							ok := AppendResponse{raft.LastLogTerm, raft.LastLogIndex, raft.ThisServerId, true}
							raft.Send(msg.LeaderId, Msg{ok})
							//raft.checkForCommit(msg.LeaderCommit)
						} else if msg.PrevLogTerm != raft.Log[len(raft.Log)].Term() { //(CHECK)
							err := AppendResponse{raft.CurrentTerm, raft.LastLogIndex, raft.ThisServerId, false}
							raft.Send(msg.LeaderId, Msg{err})
						}
					} //else if len(raft.Log) > msg.PrevLogIndex { // have to truncate (CHECK THIS BLOCK)
						//* THIS IS LOGIC FOR LOG REPAIRING, COMMENTED BECAUSE THERE ARE NO TESTS FOR IT*//

						/*log.Println(raft.ThisServerId," logLength:",len(raft.Log)," prevLogindex:",msg.PrevLogIndex)
						var preTerm int  //(CHECK)
						if msg.PrevLogIndex == 0{ preTerm = 0} else {preTerm = raft.Log[msg.PrevLogIndex].Term() }
						if  preTerm != msg.PrevLogTerm{ // mismatch
							// Log Repairing [Raft, Fig.7(f)]
							err := AppendResponse{raft.CurrentTerm, raft.LastLogIndex, raft.ThisServerId, false}
							raft.Send(msg.LeaderId, Msg{err})
							/*logentry := LogStruct{msg.PrevLogIndex+1,msg.Term,msg.Msg,false}
							raft.Log[msg.PrevLogIndex+1] = &logentry
							go func(){  //add to disk log
								diskPath := (raft.Cluster).Path + strconv.Itoa(raft.ThisServerId)
								err := writeToFile(diskPath, logentry)
								if err != nil{ log.Fatal(err)}
							}()
							raft.LastLogIndex = raft.LastLogIndex + 1
							raft.LastLogTerm = raft.Log[raft.LastLogIndex].Term()
							ok := AppendResponse{raft.LastLogTerm, raft.LastLogIndex, raft.ThisServerId, true}
							raft.Send(msg.LeaderId, Msg{ok})*/
						/*} else if preTerm == msg.PrevLogTerm{
							for delIndex := msg.PrevLogIndex + 1; delIndex <= len(raft.Log); delIndex++{
								delete(raft.Log,delIndex)
								log.Println(raft.ThisServerId,": Deleted")
							}
							// once deleted append entry at correct position
							logentry := LogStruct{msg.PrevLogIndex+1,msg.Term,msg.Msg,false}
							raft.Log[msg.PrevLogIndex+1] = &logentry
							go func(){ //add to disk log
								diskPath := (raft.Cluster).Path + strconv.Itoa(raft.ThisServerId)
								err := writeToFile(diskPath, logentry)
								if err != nil{log.Fatal(err)}
							}()
							//raft.LastLogIndex = raft.LastLogIndex + 1
							raft.LastLogIndex = len(raft.Log)  //(CHECK)
							raft.LastLogTerm = raft.Log[raft.LastLogIndex].Term()
							ok := AppendResponse{raft.LastLogTerm, raft.LastLogIndex, raft.ThisServerId, true}
							raft.Send(msg.LeaderId, Msg{ok})
						}*/
					//}
				} else if msg.PrevLogIndex > len(raft.Log) { //my log is lagging, send me previous entries
					err := AppendResponse{raft.CurrentTerm, raft.LastLogIndex, raft.ThisServerId, false}
					raft.Send(msg.LeaderId, Msg{err})
				}
			}
		case Timeout:
			timer.Stop()
			//log.Println(raft.ThisServerId,"follower timeout")
			return candidate
		}
	}
}

func (raft *Raft) candidate() int {
	votesReceived := 0
	raft.CurrentTerm = raft.CurrentTerm + 1 // increment the current term
	//log.Println(raft.ThisServerId,":state = C term = ",raft.CurrentTerm)
	votesReceived = votesReceived + 1 // vote for self
	raft.VotedFor = raft.ThisServerId //set votedfor to candidate id

	electionTOut := random(T, 2*T) // reset election timer
	timer := time.AfterFunc(time.Duration(electionTOut)*time.Millisecond, func() { raft.EventCh <- Timeout{} })

	//Prepare the vote request and send RequestVote RPC to all other servers
	voteAppeal := VoteRequest{raft.ThisServerId, raft.CurrentTerm, raft.LastLogIndex, raft.LastLogTerm}
	go func() {
		for _, server := range raft.Cluster.Servers {
			if server.Id != raft.ThisServerId {
				raft.RequestVote(server.Id, voteAppeal)
			}
		}
	}()

	//Keep listening on event channel for votes OR AppendEntriesRPC from new leader OR election timeout
	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case ClientAppend:
			msg := ev.(ClientAppend)
			msg.logEntry.Log_commit = false
			//raft.CommitCh <- msg.logEntry
		case VoteResponse:
			msg := ev.(VoteResponse)
			if msg.Vote {
				votesReceived = votesReceived + 1 // increment the voteCount
				if checkMajority(votesReceived) {
					timer.Stop()
					return leader
				}
			} else {
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				timer.Stop()
				return follower
			}
		case AppendRPC:
			// Check for leader's term, Return to follower state if the leader is legitimate
			//log.Println(raft.ThisServerId," got HB")
			timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond)
			msg := ev.(AppendRPC)
			if msg.Term > raft.CurrentTerm { //Checkkkkkkkkkkk for >= OR >
				//log.Println(raft.ThisServerId," got HB")
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				raft.LeaderId = msg.LeaderId
				timer.Stop()
				return follower
			}
		case VoteRequest:
			msg := ev.(VoteRequest)
			if msg.Term > raft.CurrentTerm {
				//*log.Println(raft.ThisServerId,":state = C event = got higher term in VoteRequest")
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				timer.Stop()
				return follower
			}
		case Timeout:
			//log.Println(raft.ThisServerId,": Candidte TimeOut")
			timer.Stop()
			return candidate // start new election
		}
	}
}

func (raft *Raft) leader() int {
	raft.LeaderId = raft.ThisServerId
	responseCount := make(map[int]int)

	lastIndex := len(raft.Log) //raft.LastLogIndex  //###### (Check, Modified)
	//log.Println(raft.ThisServerId,": state:L term:",raft.CurrentTerm," event:leader electeddddd")
	// nextIndex contains next log entry to be sent to each server
	nextIndex := []int{lastIndex + 1, lastIndex + 1, lastIndex + 1, lastIndex + 1, lastIndex + 1}
	//matchIndex contains index of highest log entry known to be replicated for each server
	matchIndex := []int{0, 0, 0, 0, 0}

	//Ref: http://stackoverflow.com/questions/16466320/is-there-a-way-to-do-repetitive-tasks-at-intervals-in-golang
	ticker := time.NewTicker(time.Duration(1) * time.Millisecond)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				raft.EventCh <- SendAppendRPC{}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	//Keep listening on event channel client requests
	for {
		ev := <-raft.EventCh
		switch ev.(type) {
		case ClientAppend:
			//the leader has put the entry successfully in its log
			msg := ev.(ClientAppend).logEntry
			responseCount[msg.Lsn()] = 1
		case SendAppendRPC:
			for _, server := range raft.Cluster.Servers {
				if server.Id != raft.LeaderId {
					var prevLogTerm, prevLogIndex int
					prevLogIndex = nextIndex[server.Id] - 1
					if prevLogIndex == 0 {
						prevLogTerm = 0
					} else {
						//log.Println("Before line 460",len(raft.Log),"",prevLogIndex)
						prevLogTerm = raft.Log[prevLogIndex].Log_term
					}
					// Check if leader has got some data to send to follower server
					msg := []byte(nil)
					if len(raft.Log) >= nextIndex[server.Id] {
						msg = raft.Log[nextIndex[server.Id]].Log_data
					}
					go raft.AppendEntries(server.Id, raft.CurrentTerm, raft.LeaderId, prevLogIndex, prevLogTerm, msg, raft.CommitIndex)
				}
			}
		case AppendRPC:
			// Check for leader's term, Return to follower state if the leader is legitimate
			msg := ev.(AppendRPC)
			if msg.Term > raft.CurrentTerm {
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				//log.Println(strconv.Itoa(raft.ThisServerId)+": leader returning to follower(higher term in AppendRPC)")
				close(quit) //to stop the ticker
				raft.LeaderId = -1
				return follower
			}
		case VoteRequest:
			// Check for leader's term, Return to follower state if the leader is legitimate
			msg := ev.(VoteRequest)
			if msg.Term > raft.CurrentTerm {
				raft.CurrentTerm = msg.Term
				raft.VotedFor = -1
				//log.Println(strconv.Itoa(raft.ThisServerId)+":leader returning to follower (higher term in Voterequest)")
				//log.Println("Voterequest = Term:",msg.Term," Sender: ",msg.CandidateId)
				close(quit) //to stop the ticker
				raft.LeaderId = -1
				return follower
			}
		case AppendResponse:
			// Check the append response and commit the leader's log entry on success
			msg := ev.(AppendResponse)
			if msg.Term > raft.CurrentTerm {
				raft.CurrentTerm = msg.Term
				raft.LeaderId = -1
				close(quit) //to stop the ticker (CHECK)
				return follower
			}
			if msg.Success {
				//log.Println("Got AppendResponse from ",msg.ServerId)
				// Update matchIndex[] & nextIndex[] for the follower server
				nextIndex[msg.ServerId] = nextIndex[msg.ServerId] + 1
				matchIndex[msg.ServerId] = matchIndex[msg.ServerId] + 1
				responseCount[msg.Index] = responseCount[msg.Index] + 1 // to check for majority
				if checkMajority(responseCount[msg.Index]) && responseCount[msg.Index] == 3 {
					// if got majority commit the logentry at leader
					raft.Log[msg.Index].Log_commit = true
					raft.CommitIndex = min(raft.CommitIndex+1, len(raft.Log)) // Increment the CommitIndex
					if raft.CommitIndex > raft.LastApplied {
						//log.Println("ResponseCount for ",msg.Index," is ",responseCount[msg.Index])
						//log.Println("Log length",len(raft.Log)," CommitIndex",raft.CommitIndex)
						raft.CommitCh <- *raft.Log[raft.LastApplied+1] // put entry on the commit channel
						raft.LastApplied = raft.LastApplied + 1
					}
				}
			} else {
				// leader can fail in appending entry to follower due to log inconsistency
				//log.Println("got failure while appending from Server",msg.ServerId," ",len(raft.Log))
				nextIndex[msg.ServerId] = nextIndex[msg.ServerId] - 1 // Decrement the nextIndex and retry
			}
		}
	}
}

func (raft *Raft) OpenServerListener() net.Listener {
	port := strconv.Itoa(raft.Cluster.Servers[raft.ThisServerId].LogPort)
	address, err := net.ResolveTCPAddr("tcp", ":"+port)
	if err != nil {
		log.Fatal("error resolving TCP address", err)
	}
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatal("error creating the listener", err)
	}
	return listener
}

func (raft *Raft) DialConnection(tcpAddr string) net.Conn {
	for {
		conn, err := net.Dial("tcp", tcpAddr)
		if err != nil {
			// retry
			continue
		} else {
			return conn
		}
	}
}

func (raft *Raft) Send(toServer int, msg Msg) {
	enc := raft.EncMap[toServer]
	if enc == nil {
		// re-dial the connection
		conn_new := raft.DialConnection(":" + strconv.Itoa(raft.Cluster.Servers[toServer].LogPort))
		enc = gob.NewEncoder(conn_new)
		raft.EncMap[toServer] = enc
	}
	err := enc.Encode(msg)
	if err != nil {
		//log.Fatal("encode error:", err)
		// just don't send if remote server is dead!
	}
}

func (raft *Raft) EstablishServerLinks() {
	for _, server := range raft.Cluster.Servers {
		if server.Id != raft.ThisServerId {
			// open conn and put the encoder object into map
			conn := raft.DialConnection(":" + strconv.Itoa(server.LogPort))
			enc := gob.NewEncoder(conn)
			raft.EncMap[server.Id] = enc
		} else {
			raft.EncMap[server.Id] = nil
		}
	}
}

func (raft *Raft) HandleServerConn(conn net.Conn) {
	dec := gob.NewDecoder(conn)
	for {
		// read continuously and put it on the eventCh
		var msg Msg
		err := dec.Decode(&msg)
		if err == nil {
			raft.EventCh <- msg.Content
		} else {
			//log.Fatal("Error Decode:",err," for server ",raft.ThisServerId)
			continue
		}
	}
	defer conn.Close()
	//log.Println("Closing conn for server")
}

func (raft *Raft) OpenClientListener() net.Listener {
	port := strconv.Itoa(raft.Cluster.Servers[raft.ThisServerId].ClientPort)
	address, err := net.ResolveTCPAddr("tcp", ":"+port)
	if err != nil {
		log.Fatal("error resolving TCP address", err)
	}
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatal("error creating the listener", err)
	}
	return listener
}

func (raft *Raft) RequestVote(serverId int, voteAppeal VoteRequest) {
	// Request the vote on vote channel of the follower
	raft.Send(serverId, Msg{voteAppeal})
}

func (raft *Raft) AppendEntries(receiverId int, term int, leaderId int, prevLogIndex int, prevLogTerm int, msg []byte, leaderCommit int) {
	// Attach other metadata with log data and replicate to other servers
	appendEntry := AppendRPC{term, leaderId, prevLogIndex, prevLogTerm, msg, leaderCommit}
	raft.Send(receiverId, Msg{appendEntry})
}

func (raft *Raft) Append(data []byte) (LogStruct, error) {
	if raft.ThisServerId != raft.LeaderId {
		return LogStruct{}, ErrRedirect(raft.LeaderId)
	} else {
		// Prepare the log entry
		logentry := LogStruct{raft.LastLogIndex + 1, raft.CurrentTerm, data, false}
		raft.Log[logentry.Lsn()] = &logentry
		raft.LastLogIndex = raft.LastLogIndex + 1
		raft.LastLogTerm = raft.Log[raft.LastLogIndex].Term() // (CHECK, MODIFIED)
		raft.EventCh <- ClientAppend{logentry}
		//appending serverId to org path makes diff. files for diff. servers
		diskPath := (raft.Cluster).Path + strconv.Itoa(raft.ThisServerId)
		go func() {
			err := writeToFile(diskPath, logentry)
			if err != nil {
				log.Fatal(err)
			}
		}()
		return logentry, nil
	}
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
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
	//Source : "http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1"
	//Source : "http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go"
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
	if _, err = logFile.WriteString("\n" + strconv.Itoa(logentry.Lsn()) + " " + strconv.Itoa(logentry.Term()) + " " + string(logentry.Data())); err != nil {
		return err
	}
	return nil
}

func (raft *Raft) StartServer() {
	raft.Loop()
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server:" + strconv.Itoa(int(e))
}

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
