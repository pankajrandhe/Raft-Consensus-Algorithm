package raft

import (
	"math/rand"
	//"net"
	//"os"
	"strconv"
	"time"
	"fmt"
	"sync"
)

const follower int = 1
const candidate int = 2
const leader int = 3

var raft Raft
//type Lsn uint64 //Log sequence number, unique for all time.

//type ErrRedirect int // See Log.Append. Implements Error interface.

/*type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}*/

type Event struct{
    evType string
    data interface{}
}

type VoteReq struct{
    CandidateId int
    Term int
    LastLogIndex int
    LastLogTerm int
}

type VoteResponse struct{
	SenderId int
	Term int
	Vote bool
}
/*
func (x LogStruct) Lsn() Lsn {

	return x.Log_lsn
}

func (x LogStruct) Data() []byte {

	return x.Log_data
}

func (x LogStruct) Committed() bool {

	return x.Log_commit
}
*/
type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	//Append(data []byte) (LogEntry, error)
	RequestVote(VoteChan chan VoteReq,ResponseChan chan string,voteAppeal VoteReq) (int, bool)
	Loop(thisServerId int)
	follower(thisServerId int, currentTerm int) int
	candidate(thisServerId int, currentTerm int) int
	leader(thisServerId int, currentTerm int) int
}

// Raft setup
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	EventCh chan Event
	Term int
	LastLogTerm int
	LastLogIndex int
	VoteHistory []bool
	//LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	//Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	Cluster       *ClusterConfig
	ThisServerId  int
	LeaderId      int
	//CommitCh      chan LogEntry
	//MajorityMap   map[string]int
	//CmdHistMap    map[string]LogEntry
	//ConnectionMap map[string]net.Conn
	ServersCount  int
}

/*
type LogStruct struct {
	Log_lsn    Lsn
	Log_data   []byte
	Log_commit bool
}
*/
// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed

//func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
func NewRaft(config *ClusterConfig, thisServerId int, leaderId int, servers int ) (*Raft, error) {

	//var LeaderId = 0 //we have fixed leader 0th server, since we are not implementing the Leader Elections
	//MajorityMap := make(map[string]int)
	//CmdHistMap := make(map[string]LogEntry)
	//ConnectionMap := make(map[string]net.Conn)
	//serversCount := 0

	//Count number of Servers in the Cluster
	//for _, _ = range config.Servers {
	//	serversCount = serversCount + 1
	//}

	//raft = Raft{config, thisServerId, LeaderId, commitCh, MajorityMap, CmdHistMap, ConnectionMap, serversCount}
	raft = Raft{config, thisServerId, leaderId, servers}
	//Each Raft Object will have a Majority Map and a CmdHistMap, to be used only by the leader. Created for all to handle leader changes.

	var err error = nil
	return &raft, err
}

// ErrRedirect as an Error object
/*func (e ErrRedirect) Error() string {
	return "ERR_REDIRECT " + raft.Cluster.Servers[int(e)].Hostname + " " + strconv.Itoa(raft.Cluster.Servers[int(e)].ClientPort) + "\r\n"
}*/
/*
func (raft Raft) Append(data []byte) (LogEntry, error) {

	if raft.ThisServerId != raft.LeaderId {

		return nil, ErrRedirect(raft.LeaderId)

	} else {

		// Prepare the LogEntry
		var log_instance LogStruct
		lsn := Lsn(uint64(rand.Int63()))
		log_instance = LogStruct{lsn, data, false}

		//Initialize number of ack for the lsn to 0
		string_lsn := strconv.Itoa(int(log_instance.Lsn()))
		raft.MajorityMap[string_lsn] = 0
		raft.CmdHistMap[string_lsn] = log_instance

		
		// Writing to Files - https://gobyexample.com/writing-files
		//Appending to Files - http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1
		//Check whether file already exists - http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
		

		filename := (raft.Cluster).Path

		if _, err := os.Stat(filename); os.IsNotExist(err) {
			_, err := os.Create(filename)
			if err != nil {
				panic(err)
			}
		}

		logFile, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}

		defer logFile.Close()

		// ########NOTE: DUMMY value for commit, remember to correct it!
		if _, err = logFile.WriteString("\n" + strconv.Itoa(int(log_instance.Log_lsn)) + " " + string(log_instance.Log_data) + "false"); err != nil {
			panic(err)
		}

		// Take the raft object and broadcast the log-entry
		// Lets send the logentry to each of the servers in the cluster

		//Read back the servers as JSON objects
		servers := raft.Cluster.Servers

		for _, server := range servers {

			//host := server.Hostname
			port := server.LogPort

			//now establish the TCP connection and send the data to the follower servers
			connection, err := net.Dial("tcp", ":"+strconv.Itoa(port))
			if err != nil {
				continue
			} else {
				_, _ = connection.Write([]byte(strconv.Itoa(int(log_instance.Lsn())) + " " + string(log_instance.Data()) + " false"))
			}
		}
		// Prepare the log entry and return it
		return log_instance, nil
	}
}
*/

func (raft Raft) Loop(w sync.WaitGroup) {
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
    w.Done()
}

func (raft Raft) follower() int {
	thisServerId := raft.ThisServerId
	currentTerm := raft.Cluster.Servers[thisServerId].Term
	var w sync.WaitGroup
	fmt.Println("inside follower "+strconv.Itoa(thisServerId))
    eventCh := raft.Cluster.Servers[thisServerId].EventCh
    T := 15  // T is 150ms for example
    var timer *(time.Timer)

    w.Add(1)
    go func(){
        election_timeout := random(T, 2*T) // This should be the random value between T, 2T
        fmt.Println(election_timeout)
        timer = time.NewTimer(time.Duration(election_timeout) * time.Second) // to become candidate if no append reqs
        <-timer.C
        eventCh <- Event{"Timeout",nil}  // Generate the Timeout event on timer expiry
        fmt.Println("follower"+strconv.Itoa(thisServerId)+"timeout, put on channel")
		w.Done()	
    }()

    for {
            ev := <- raft.Cluster.Servers[thisServerId].EventCh
            event := ev.evType
            switch event {
            case "ClientAppend":
                /* Do not handle clients in follower mode. Send it back up the
                // pipe with committed = false
                ev.logEntry.commited = false
                commitCh <- ev.logentry**/
            case "VoteRequest":
            	fmt.Println("voterequest received")
                msg := ev.data.(VoteReq)	// type assertion
                sender := raft.Cluster.Servers[msg.CandidateId]

                if msg.Term < currentTerm{   
                	//Responds with current term
                    sender.EventCh <- Event{"VoteResponse",VoteResponse{thisServerId,currentTerm,false}}  
                }
                if msg.Term > currentTerm{
                    currentTerm = msg.Term  // Updates the currentTerm
                }
                if !raft.Cluster.Servers[thisServerId].VoteHistory[currentTerm]{  //not already voted in my term
                    _ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond) //reset timer with new random timeout
                    sender.EventCh <- Event{"VoteResponse",VoteResponse{thisServerId,-1,true}} //reply ok to event.msg.serverid
                    /*remember term, leader id (either in log or in separate file)*/
                } 
                    
            case "AppendRPC":
                _ = timer.Reset(time.Duration(random(T, 2*T)) * time.Millisecond) //reset timer
                /*if msg.term < currentterm, ignore
                reset heartbeat timer
                upgrade to event.msg.term if necessary
                if prev entries of my log and event.msg match
                   add to disk log
                   flush disk log
                   respond ok to event.msg.serverid
                else
                   respond err.*/
            case "Timeout" : return candidate  // new state back to loop()
            }
    }
}

func (raft Raft) candidate() int {

	thisServerId := raft.ThisServerId
	//var w sync.WaitGroup
	fmt.Println("inside candidate"+strconv.Itoa(thisServerId))
    votesReceived := 0
    eventCh := raft.Cluster.Servers[thisServerId].EventCh
    
    lastlogterm := raft.Cluster.Servers[thisServerId].LastLogTerm
    lastlogindex := raft.Cluster.Servers[thisServerId].LastLogIndex

    // increment the current term
    currentTerm := raft.Cluster.Servers[thisServerId].Term + 1  
    // vote for self
    votesReceived = votesReceived + 1
    // reset election timer

     
    //Prepare the vote request and send RequestVote RPC to all other servers
    voteAppeal := Event{"VoteRequest",VoteReq{thisServerId, currentTerm, lastlogterm, lastlogindex}}  
    go func(){

    	for _, server := range raft.Cluster.Servers {
        //go func() {
        	if server.Id != thisServerId{
        		fmt.Println(strconv.Itoa(thisServerId)+" Sending vote request to "+strconv.Itoa(server.Id))
	            raft.RequestVote(server.Id, voteAppeal)  
        	}
        //}()
	    }
	}()
    
    //Keep listening on event channel for votes OR AppendEntriesRPC from new leader OR election timeout
    for{
    	ev := <- eventCh
    	switch ev.evType{
    	case "VoteResponse":
    		msg := ev.data.(VoteResponse)  // type assertion
    		if msg.Vote{
    			votesReceived = votesReceived +1 // increment the voteCount
				if checkMajority(votesReceived){
				return leader
				}
    		} else{
    			currentTerm = msg.Term
    			return follower
    		}
 
    	case "AppendRPC":
    		// Return to follower state since new leader is elected
    		return follower
    	case "Timeout":
    		// start new election
    	}
    }
}

func (raft Raft) leader() int {
	thisServerId := raft.ThisServerId
	fmt.Println("leader is "+strconv.Itoa(thisServerId))	//Just for testing
    // Send regular heartbeats (empty Append RPCs)
    for _, server := range raft.Cluster.Servers{
    	server.EventCh <- Event{"AppendRPC",nil}
    }
    return leader //check
}

// RequestVote RPC
func (raft Raft) RequestVote(serverId int,voteAppeal Event){
	// Request the vote on vote channel of the follower
	raft.Cluster.Servers[serverId].EventCh <- voteAppeal
	/** NOTE: In case of real RPC it will return either term or vote **/	 
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
    //rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func checkMajority(votesReceived int) bool{
	if votesReceived >= 3{
		return true
	} else {
		return false
	}
}