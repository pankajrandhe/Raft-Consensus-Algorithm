package raft


type Lsn uint64  //Log sequence number, unique for all time.

type ErrRedirect int  // See Log.Append. Implements Error interface.

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
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

	cluster ClusterConfig
	thisServerId int
	commitCh chan LogEntry
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {

	
	return nil, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func (raft Raft) Append(data []byte) (LogEntry, error){

	// first we have to check if the server is the leader
	if raft.ServerId != leaderID{
		return ErrRedirect
	} else {
 
		/*
		* Write the received log entry - data byte to a file in the local disk 
		*
		* References:
		* Writing to Files - https://gobyexample.com/writing-files
		* Appending to Files - http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1
		* Check whether file already exists - http://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
		*/
		filename := ClusterConfig.path

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

	}
}
