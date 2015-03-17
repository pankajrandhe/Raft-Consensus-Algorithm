package raft

import (
	"math/rand"
	//"net"
	//"os"
	//"strconv"
	"time"
	"fmt"
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

// RequestVote RPC
func (raft Raft) RequestVote(eventCh chan Event,voteAppeal Event) (int, bool){

	// Request the vote on vote channel of the follower
	eventCh <- voteAppeal
	ev := <- raft.Cluster.Servers[raft.ThisServerId].EventCh
	msg := ev.data.(VoteResponse)
	
	return msg.Term,msg.Vote 	 
}

func (raft Raft) Loop(thisServerId int) {
    state := follower; // begin life as a follower
       
    for {
                switch (state)  {
                case follower: 
                    state = raft.follower(thisServerId,0)
                case candidate: 
                    state = raft.candidate(thisServerId)
                case leader: 
                    state = raft.leader(thisServerId)
                default: return
                }
    }
}

func (raft Raft) follower(thisServerId int, currentTerm int) int {

    // Alias for follower's own event channel
    eventCh := raft.Cluster.Servers[thisServerId].EventCh

    T := 150  // T is 150ms for example
    var timer *(time.Timer)

    go func(){
        election_timeout := random(T, 2*T) // This should be the random value between T, 2T
        timer = time.NewTimer(time.Duration(election_timeout) * time.Millisecond) // to become candidate if no append reqs
        <- timer.C
        eventCh <- Event{"Timeout",nil}  // Generate the Timeout event on timer expiry
    }()

    go func() (int){
        for {

            ev := <- raft.Cluster.Servers[thisServerId].EventCh
            event := ev.evType
            //event := <- eventCh
            switch event {
            /*case ClientAppend:
                // Do not handle clients in follower mode. Send it back up the
                // pipe with committed = false
                ev.logEntry.commited = false
                commitCh <- ev.logentry**/
            case "VoteRequest":
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
                    
            /*case AppendRPC:
                _ = timer.Reset(random(T, 2*T)) //reset timer
                if msg.term < currentterm, ignore
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
    }()
    return candidate
}

func (raft Raft) candidate(thisServerId int) int {

    var votesReceived int = 0
    temp := make(chan bool)

    candidateId := thisServerId
    currentTerm := raft.Cluster.Servers[candidateId].Term + 1  // increment the current term
    lastlogterm := raft.Cluster.Servers[candidateId].LastLogTerm
    lastlogindex := raft.Cluster.Servers[candidateId].LastLogIndex
     
    //Prepare the vote request 
    voteAppeal := Event{"VoteRequest",VoteReq{candidateId, currentTerm, lastlogterm, lastlogindex}} 

    // Parallerly send the Vote Requests to all the servers 
    for _, server := range raft.Cluster.Servers {
        go func() {
            _, voteGranted := raft.RequestVote(server.EventCh, voteAppeal)  //term to be defined
            if voteGranted {
                votesReceived = votesReceived +1 
            }
        }()
    }

    go func() {
    	for {
    		isMajority := checkMajority(votesReceived)
                if isMajority {
                	temp <- true
                    break   
                } else {
                	continue
                }
    	}
    // return to leader state on getting the majority //(CHECK)
    }()
    
    <-temp
    return leader 
}

func (raft Raft) leader(thisServerId int) int {

    fmt.Println("Leader Elected is:") //Just for testing
    fmt.Println(thisServerId)	//Just for testing
    // Send regular heartbeats (empty Append RPCs)
    for _, server := range raft.Cluster.Servers{
    	server.EventCh <- Event{"AppendRPC",nil}
    }
    return leader //check
}

// Source: "http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html"
func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func checkMajority(votesReceived int) bool{

	if votesReceived >= 3{
		return true
	} else {
		return false
	}
}