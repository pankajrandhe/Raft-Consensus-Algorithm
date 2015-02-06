package raft

import(
	"os"
	"errors"
	"strconv"
	"net"
)

var raft Raft

type bytearray []byte

type Lsn uint64  //Log sequence number, unique for all time.

type ErrRedirect int  // See Log.Append. Implements Error interface.

type LogEntry interface {
	LSN() Lsn
	Data() []byte
	Committed() bool
}

func (x bytearray) LSN() Lsn { 

	seqno := 10	//temporary
	return Lsn(seqno)
}

func (x bytearray) Data() []byte {

	return x
}

func (x bytearray) Committed() bool{

	return true
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
	CommitCh chan LogEntry
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {

	raft = Raft{config,thisServerId,commitCh}
	var err error = nil
	return &raft, err
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func (raft Raft) Append(data []byte) (LogEntry, error){

	// first we have to check if the server is the leader
	var leaderId int = 0 //dummy

	err := errors.New("Leader is Server"+string(leaderId))
	
	if raft.ThisServerId != leaderId{
		//Server is not the leader
		return nil,err
	} else {
 
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

		if _, err = logFile.WriteString("\n"+string(data)); err != nil {
		    panic(err)
		}

		// Take the raft object and broadcast the log-entry
		// Lets send the logentry to each of the servers in the cluster

		//Read back the servers as JSON objects
		servers := raft.Cluster.Servers

		for _, server := range servers{

			host := server.Hostname
			port := server.LogPort

			//now establish the TCP connection and send the data to the server
			connection, err1 := net.Dial("tcp", host+":"+strconv.Itoa(port))
			panic(err1)

			_, err3 := connection.Write(data)
			panic(err3)

		}

		/*
		* Code for receving response from other servers; if majority, write to KV Store and respond to Client.
		*/

		// Prepare the log entry and return it else error
		return bytearray(data),nil
	}
}
