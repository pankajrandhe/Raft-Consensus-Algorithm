package main

import (
	"net"
	//"strings"
	//"strconv"
	"time"
	"fmt"
	"log"
	"os"
)

func main() {
	clientPort := os.Args[1]

	//Test for the single client
	for {
		time.Sleep(1*time.Second)
		setSingleClient(clientPort)
	}
	var input string
    fmt.Scanln(&input)
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

func setSingleClient(addr string){ 
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
			/*if strings.Contains(string(data),"Redirect to server:"){
				temp := strings.Split(string(data),"\r\n")
				s := strings.Trim(temp[0], "Redirect to server:")
				leaderId,erri := strconv.Atoi(s)
				if erri!=nil{
					log.Fatal(erri)
				}
				if leaderId == -1{
					continue
				} else{
					log.Println("leader id is",leaderId)
					leaderCPort := leaderId + 9000 //just temporary to create leader's client port, to be replaced
					//leaderRedirector(connection,":"+strconv.Itoa(leaderCPort))
					tempConnection := DialConnection(":"+strconv.Itoa(leaderCPort))
					connection = tempConnection
					log.Println("redirected to leader:",strconv.Itoa(leaderCPort))
					tempConnection.Close()
					log.Println("Conn closed")
				}
			} else {*/
				log.Println(string(data))
			//}
		}
	}()

	for {
		time.Sleep(1*time.Second)
		connection.Write([]byte("set one 0 10\r\npankaj\r\n"))
	}
}

func leaderRedirector(conn net.Conn, leaderAddr string){
	// replace the existing connection with conn to the leader
	toLeaderConn := DialConnection(leaderAddr)
	var temp net.Conn
	temp = conn
	conn =toLeaderConn
	temp.Close()
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func sendToServer(connection net.Conn, message string) {
	_, err3 := connection.Write([]byte(message))
	handleErr(err3)
}

		  
