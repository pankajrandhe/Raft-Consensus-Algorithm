package raft

import(
	"os"
	//"fmt"
	//"errors"
	"strconv"
	"net"
	//"log"
	//"time"
	"math/rand"
	//"fmt"
)

var raft Raft

type bytearray []byte

type Lsn uint64  //Log sequence number, unique for all time.

type ErrRedirect int  // See Log.Append. Implements Error interface.

type LogEntry interface {
	Lsn() Lsn
	Data([]byte) []byte
	Committed() bool
}

//################Add the logic for unique seq no.
func (x LogStruct) Lsn() Lsn { 

	return Lsn(uint64(rand.Int63()))
	//return Lsn(uint64(time.Now().UnixNano() / uint64(time.Millisecond)))
}

func (x LogStruct) Data(data []byte) []byte {

	return data
}

func (x LogStruct) Committed() bool{

	return false
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data []byte) (LogEntry, error)
}

// --------------------------------------
// Raft setup
type ServerConfig struct {
	Id int // Id of server. Must be unique
	Hostname string // name or ip of host
	ClientPort int 	// port at which server listens to client messages.
	LogPort int	// tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path string 	// Directory for persistent log
	Servers []ServerConfig	// All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {

	Cluster *ClusterConfig
	ThisServerId int
	LeaderId int
	CommitCh chan LogEntry
	MajorityMap map[Lsn]int
}

type LogStruct struct {

	Log_lsn Lsn
	Log_data []byte
	Log_commit bool
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {

	var LeaderId = 0 	//we have fixed leader 0th server, since we are not implementing the Leader Elections
	MajorityMap := make(map[Lsn]int)
	raft = Raft{config,thisServerId,LeaderId,commitCh, MajorityMap}
	//Each Raft Object will have a Majority Map, to be used only by the leader

	var err error = nil
	return &raft, err
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "ERR_REDIRECT " + raft.Cluster.Servers[int(e)].Hostname + " " + strconv.Itoa(raft.Cluster.Servers[int(e)].ClientPort)+"\r\n"
}

func (raft Raft) Append(data []byte) (LogEntry, error){

	if raft.ThisServerId != raft.LeaderId{

		return nil, ErrRedirect(raft.LeaderId)

	} else {


		// Prepare the LogEntry
		var log_instance LogStruct
		log_instance = LogStruct{log_instance.Lsn(),log_instance.Data(data),log_instance.Committed()}


		//Initialize number of ack for the lsn to 0


 
		/*
		* Write the received log entry - data byte to a file in the local disk 
		*
		* References:
		* Writing to Files - https://gobyexample.com/writing-files
		* Appending to Files - http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1
		* Check whether file already exists - http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
		*/

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
		if _, err = logFile.WriteString("\n"+strconv.Itoa(int(log_instance.Log_lsn))+" "+string(log_instance.Log_data)+"false"); err != nil {
		    panic(err)
		}

		// Take the raft object and broadcast the log-entry
		// Lets send the logentry to each of the servers in the cluster

		//Read back the servers as JSON objects
		servers := raft.Cluster.Servers

		for _, server := range servers{

			//host := server.Hostname
			port := server.LogPort

			//now establish the TCP connection and send the data to the follower servers
			connection, _ := net.Dial("tcp",":"+strconv.Itoa(port))
			//fmt.Println("error here:",err1)
			_, _ = connection.Write([]byte(strconv.Itoa(int(log_instance.Log_lsn))+" "+string(log_instance.Log_data)+"false"))
			//fmt.Println("error writing: ",err3)
		}
		// Prepare the log entry and return it 
		return log_instance,nil
	}
}

