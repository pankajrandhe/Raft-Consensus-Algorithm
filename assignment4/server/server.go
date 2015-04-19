package main

import (
	"github.com/pankajrandhe/cs733/assignment4/raft"
	"log"
	"strconv"
	"io/ioutil"
	"encoding/json"
	"os"
	"sync"
	"net"
	"strings"
)

var ResponseCh = make(chan Resp)  
var responseMap = make(map[int]net.Conn)
var dummyLsn = 0

// this comes from the clientHandler
type CmdWrapper struct{
	CmdType string
	Key string
	Exptime int
	Numbytes int
	Noreply bool
	Value []byte
	Version int
}

func main() {
	thisServerId, err := strconv.Atoi(os.Args[1])
	handleError(err)

	//Read the json file and get the server configurations
	var cluster raft.ClusterConfig
	server_configs, err := ioutil.ReadFile("/home/pankaj/go/src/github.com/pankajrandhe/cs733/assignment4/servers.json")
	handleError(err)
	err_json := json.Unmarshal(server_configs, &cluster)
	handleError(err_json)

	//Initialize the Raft Instance for the server
	commitCh := make(chan raft.LogStruct)
	raftInst, err_raft := raft.NewRaft(thisServerId, &cluster,commitCh)
	handleError(err_raft)
	
	go raftInst.StartServer()  		// start the raft-side of the server
	go KVloop(commitCh) 			// start the KVstore backend
	go toClientReplier(ResponseCh) 	// listen from the reply from KV backend 

	var w sync.WaitGroup
	clientListener := raftInst.OpenClientListener() // start listening for the clients
	w.Add(1)
	go func() {
		for {
			clientConn, err := clientListener.Accept()
			if err != nil {
				//log.Fatal(err)
				continue
			}
			go HandleClientConn(clientConn, raftInst)
		}
		defer w.Done()
	}()
	w.Wait()
}

func toClientReplier(ResponseCh chan Resp){
	for{
		reply := <- ResponseCh
		// find lsn and respective conn. from map to reply back to client
		sendReply(responseMap[reply.lsn],reply.data)
	}
}

func sendReply(conn net.Conn, data []byte){
	_,err := conn.Write(data)
	if err != nil{
		// just neglect the error
	}
}

func HandleClientConn(conn net.Conn, raftInst *raft.Raft){
	for {
		data := getClientCommand(conn)	
		if data != nil{   // we don't get data back if data is erroneous
			command := cmdParser(data)  // Encode the data bytes into CmdWrapper
			logentry, err := raftInst.Append(command) // Pass the command bytes to Append() 
			if err != nil{ 
				sendReply(conn, []byte(err.Error()+"\r\n"))
			} else {
				log.Println("Command Logged at Lsn:", logentry.Lsn()) 
				responseMap[logentry.Lsn()] = conn  // store the conn object to reply later
			}
		}
		/*
		// Temporarily bypassing Raft to test KVStore
		// Prepare the dummy logentry and put to commitCh
		if data != nil{
			var dummyLogEntry raft.LogStruct
			dummyLogEntry = raft.LogStruct{
				Log_lsn : dummyLsn,
				Log_term : 0,
				Log_data : command,
				Log_commit : true,
			}
			raftInst.CommitCh <- dummyLogEntry
			responseMap[dummyLogEntry.Lsn()] = conn
			dummyLsn = dummyLsn + 1
		}*/
	}  
	//defer conn.Close()   // to safely close the connection
}

func cmdParser(data []byte) ([]byte){
	// Note: -99 represents invalid Value
	var command CmdWrapper
	input := string(data)
	rows := strings.Split(input,"\r\n")
	if (len(rows)-1)==2 {  // we have SET or CAS command
		cmd := rows[0]
		Value := rows[1]
		attributes := strings.Split(cmd ," ")
		if attributes[0]== "set" { 
			et,_ := strconv.Atoi(attributes[2])
			nb,_ := strconv.Atoi(attributes[3])
			if len(attributes) == 5 { //with Noreply
				command = CmdWrapper {
						CmdType : attributes[0],
						Key : attributes[1],
						Exptime : et,
						Numbytes : nb,
						Noreply : true,
						Value : []byte(Value),
						Version : -99,
					}
			} else if len(attributes) == 4 { //without Noreply
				command = CmdWrapper {
						CmdType : attributes[0],
						Key : attributes[1],
						Exptime : et,
						Numbytes : nb,
						Noreply : false,
						Value : []byte(Value),
						Version : -99,
					}
			}
		} else if attributes[0]== "cas" { // without Noreply
			et,_ := strconv.Atoi(attributes[2])
			nb,_ := strconv.Atoi(attributes[4])
			v, _ := strconv.Atoi(attributes[3])
			if len(attributes) == 5 { //with Noreply
				command = CmdWrapper {
						CmdType : attributes[0],
						Key : attributes[1],
						Exptime : et,
						Numbytes : nb,
						Noreply : true,
						Value : []byte(Value),
						Version : v,
					}
			} else if len(attributes) == 4{ //without Noreply
				command = CmdWrapper {
						CmdType : attributes[0],
						Key : attributes[1],
						Exptime : et,
						Numbytes : nb, 
						Noreply : false,
						Value : []byte(Value),
						Version : v, 
				}
			}
		}
	} else if (len(rows)-1)==1 { // we have DELETE or GETM or GET command
		cmd := rows[0]
		attributes := strings.Split(cmd," ")
		if attributes[0]=="get" || attributes[0]=="getm" || attributes[0]=="delete"{
			command = CmdWrapper {
					CmdType : attributes[0],
					Key : attributes[1],
					Exptime : -99,
					Numbytes : -99,
					Noreply : false,
					Value : []byte(nil),
					Version : -99,
			}
		}		
	}	
	// Now encode the command 
	c, err := json.Marshal(command)
	handleError(err)
	return c
}

func getClientCommand(conn net.Conn) (command []byte) {
	var err error
	var length_r, length_temp int
	var newline_count int = 0
	buffer := make([]byte, 512)
	//buffer_temp := make([]byte, 512)
	// Read the first packet to decide the course of the action
	length_r, err = conn.Read(buffer[0:])
	if err != nil {
		//continue
	}
	newline_count = strings.Count(string(buffer[0:length_r]), "\r\n")
	CmdType := strings.Split(string(buffer[0:length_r]), " ")[0]

	if !(CmdType=="set"||CmdType=="get"||CmdType=="getm"||CmdType=="cas"||CmdType=="delete"){
		sendReply(conn, []byte("ERR_CMD_ERR"))
	} else if newline_count==1 && (CmdType =="get"||CmdType == "getm"||CmdType == "delete"){
		/*if !validate(CmdType){   // erroneous command
			sendReply(conn, []byte("ERR_CMD_ERR"))
		} else*/{
			command = buffer[0:length_r]  // we have got complete get or getm or delete cmd
			// yet to handle partial command case
		}		
	} else if CmdType =="set"||CmdType == "cas"{
		/*if !validate(CmdType){
			sendReply(conn, []byte("ERR_CMD_ERR"))
		} else */if newline_count==2 {
			command = buffer[0:length_r]  // we have got complete set or cas cmd
		} else if newline_count < 2 {
			// parse the "numbytes" and read corresponding value from next line
			var numbytes int
			input := strings.Split(string(buffer),"\r\n")[0]
			if CmdType == "set"{ 
				numbytes,_ = strconv.Atoi(strings.Split(input," ")[3])  // in SET it is at position 3
			} else if CmdType == "cas"{
				numbytes,_ = strconv.Atoi(strings.Split(input," ")[4])  // in CAS it is at position 4
			}

			for {
				buffer_temp := make([]byte, numbytes)
				length_temp, err = conn.Read(buffer_temp[0:])
				if err != nil {
					continue
				}
				buffer = append(buffer[0:length_r], buffer_temp[0:length_temp]...) //create complete set or cas entity
				length_r = length_r + length_temp
				command = buffer[0:length_r]
				break
			}	
		}
	}
	return command	
}

func handleError(err error){
	if err!= nil{
		log.Fatal(err)
	}
}

/*
	// check whether if we have got \r\n and command is not set or cas command
	if strings.Contains(string(buffer[0:length_r]), "\r\n") && !(CmdType =="set"|| CmdType == "cas"){
		command = buffer[0:length_r]
	} else if !(strings.Contains(string(buffer[0:length_r]), "\r\n")) && !(CmdType =="set"||CmdType =="cas"){
		// In this case read the packets until CmdType==we get the "\r\n"
		for {
			length_temp, err = conn.Read(buffer_temp[0:])
			if err != nil {
				continue
			}
			buffer = append(buffer[0:length_r], buffer_temp[0:length_temp]...)
			length_r = length_r + length_temp
			if strings.Contains(string(buffer), "\r\n") {
				break
			} else {
				continue
			}
		}
		command = buffer[0:length_r]
	} else {
		if newline_count == 2 {
			command = buffer[0:length_r]
		} else {
			// Otherwise go on reading until we get two "\r\n" characters
			for {
				if newline_count < 2 {
					buffer_temp := make([]byte, 1024)
					length_temp, err = conn.Read(buffer_temp[0:])
					if err != nil {
						continue
					}
					buffer = append(buffer[0:length_r], buffer_temp[0:length_temp]...)
					length_r = length_r + length_temp
					if strings.Contains(string(buffer), "\r\n") {
						newline_count++
					}
				} else {
					break
				}
			}
			command = buffer[0:length_r]
		}
	}
	return command*/