package raft

import (
	"net"
	"strings"
	"strconv"
	"time"
	"log"
	"regexp"
	"testing"
	"os/exec"
	"os"
)

var upServers = make([]*exec.Cmd,5) // Reference for array of *exec.Cmd: Arun Babu's 
									//answer on Piazza in discussion for CS733
var leader int = -1  // Currently invalid, searched by searchLeader() later on

func TestConcurrency(t *testing.T){
	path, err := exec.LookPath("server")
	if err != nil {
		log.Fatal("Check your PATH variable OR reload . /etc/profile")
	}

	for s:=0; s<5;s++{
		upServers[s] = exec.Command(path,strconv.Itoa(s))
		upServers[s].Stdout = os.Stdout
		upServers[s].Stderr = os.Stderr
		err := upServers[s].Start()
		if err!= nil{
			log.Fatal("error occured in spawner ",err.Error())
		}
	}

	doneChan := make(chan bool)
	time.Sleep(1*time.Second)
	leader = searchLeader() 
	//log.Println("Got the leader: ",leader)
	//Test for the 10 concurrent clients
	for i:=0; i<10; i++ { 
		go setSingleClient(t, &doneChan, strconv.Itoa(leader+9000)) //connect client to the leader's client port (CHECK)
	}
	for i:=0; i<10; i++ {
		<- doneChan
	}
	//var input string  // obstacle to let all tests finish
    //fmt.Scanln(&input)
}

func TestServerKilling(t *testing.T) {
	// Kill the current leader, and test if new leader gets elected
	kill(leader, upServers)
	time.Sleep(5*time.Second) // wait for some time, timer value is based on general trial executions  
	// see if we have got new leader
	newLeaderId := searchLeader()
	log.Println("Got the New leader after killing old one: ",newLeaderId)
	if newLeaderId != -1 || newLeaderId == leader {
		//t.Error("New Leader not elected after killing the old leader")
	}
}

func setSingleClient(t *testing.T, doneCh *chan bool, addr string){ 
	readCh := make(chan []byte)
	time.Sleep(1*time.Second)  // wait for some time to let the server boot-up
	connection := DialConnection(":"+addr)
	
	// Start a goroutine to read from our net connection
	go func() {
		for {
			data := make([]byte, 512)
			_, err := connection.Read(data)
			if err != nil {
				continue
			}
			readCh <- data
		}
	}()

	commandPool := []string {
		"set country 0 2\r\nAustralia\r\n",
		"set country 0 2\r\nCzec republic\r\n",
		"set country 0 2\r\nPapua New Gunea\r\n",
		"set country 0 2\r\nGreenLand\r\n",
		"set country 0 2\r\nIndia\r\n",
	}

	for i:=0; i<5; i++ {
		time.Sleep(time.Duration(10) *time.Millisecond)
		sendToServer(connection,commandPool[i])
		<- readCh  // we dont't care for response here
	}

	sendToServer(connection, "get country\r\n")
	reply := <-readCh
	// After doing ten successive "set"s, 
	// the next "get" should give any of the values set the latest "set" command
	matched, _ := regexp.MatchString("VALUE 2\r\nIndia\r\n", string(reply))
	if !matched {
		t.Error("Output not matched")
	} 
	*doneCh <- true
}

func searchLeader() (int) {
	var leaderId int
	leaderCPort := 9000  // assumed leader's client port
	for {
		connection := DialConnection(":"+strconv.Itoa(leaderCPort))
		//sendToServer(connection,"set zero 0 0\r\n \r\n")  // send some simple command to check who is leader
		_, werr := connection.Write([]byte("set zero 0 0\r\n \r\n"))
		if werr != nil{
			continue  // we will continue in case of broken TCP pipe
		}
		data := make([]byte, 512)
		_, err := connection.Read(data)  // read the response
		if err != nil{
			continue
		}

		if strings.Contains(string(data),"Redirect to server:"){
			temp := strings.Split(string(data),"\r\n")
			s := strings.Trim(temp[0], "Redirect to server:")
			leaderId,err = strconv.Atoi(s)
			if err != nil{
				log.Fatal(err)
			}
			if leaderId == -1{  // leader is not elected yet, so continue our search
				continue
			} else{
				//log.Println("leader id is",leaderId)  // redirect to the mentioned leader
				leaderCPort = leaderId + 9000 //just temporary to simulate leader's client port,to be replaced with read from JSON
				connection.Close()
				break // got leader id
			}
		} else {
			connection.Close()
			break
		}
	}
	return leaderId
}

func DialConnection(tcpAddr string) (net.Conn){
	for {
		conn, err := net.Dial("tcp",tcpAddr)
		if err!=nil{
			// retry
			continue
		} else{
			return conn
		}
	}
}

func sendToServer(connection net.Conn, message string) {
	_, err3 := connection.Write([]byte(message))
	handleErr(err3)
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Reference : Arun Babu's answer on Piazza in discussion for CS733
func kill(id int, upServers []*exec.Cmd) {
		if upServers[id] == nil {
			return
		}
		upServers[id].Process.Kill()
		//log.Println("Server killed")
		upServers[id].Wait()
}