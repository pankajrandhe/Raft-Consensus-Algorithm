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
	//"fmt"
)

func TestConcurrency(t *testing.T){
	path, err := exec.LookPath("server")
	if err != nil {
		log.Fatal("Check your PATH variable OR reload . /etc/profile")
	}

	for s:=0; s<5;s++{
		cmd := exec.Command(path,strconv.Itoa(s))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err!= nil{
			log.Fatal("error occured in spawner ",err.Error())
		}
	}

	completionChan := make(chan bool)
	time.Sleep(1*time.Second)
	leaderAddr := findLeader() 
	//Test for the 10 concurrent clients
	for i:=0; i<10; i++ {
		go setSingleClient(t, &completionChan, strconv.Itoa(leaderAddr)) //connect client to the leader's client port
	}
	for i:=0; i<10; i++ {
		<- completionChan
	}
	//var input string  // obstacle to let all tests finish
    //fmt.Scanln(&input)
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
	matched, _ := regexp.MatchString("VALUE 2\r\nIndia\r\n", string(reply))
	if !matched {
		t.Error("Output not matched")
	} 
	*doneCh <- true
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

func findLeader() (int) {
	leaderCPort := 9000  // assumed leader's client port
	for {
		connection := DialConnection(":"+strconv.Itoa(leaderCPort))
		sendToServer(connection,"set zero 0 0\r\n \r\n")  // send some simple command to check who is leader
		data := make([]byte, 512)
		_, err := connection.Read(data)  // read the response
		if err != nil{
			continue
		}

		if strings.Contains(string(data),"Redirect to server:"){
			temp := strings.Split(string(data),"\r\n")
			s := strings.Trim(temp[0], "Redirect to server:")
			leaderId,err := strconv.Atoi(s)
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
	return leaderCPort
}

/*func killServer(id int) {
		if liveServers[id] == nil {
			return
		}
		liveServers[id].Process.Kill()
		liveServers[id].Wait()
}*/