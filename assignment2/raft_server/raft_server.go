package main

import (
	"log"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"github.com/pankajrandhe/cs733/assignment2/raft"
	"bytes"
)

// Lets first create the structure which will hold the value and all other meta-data for the key
type value_and_metadata struct {
	value    string
	exptime  int64
	numbytes string
	version  int64
}

// Create the MAP to hold the keys, values and the associated meta-data
var kvmap = struct {
	sync.RWMutex
	key_values map[string]*value_and_metadata
}{key_values: make(map[string]*value_and_metadata)}

//Using WaitGroup, so that the goroutine get time for execution
var wg sync.WaitGroup

func main(){

	// Pass the server id as the parameter from command line
	serverId, err := strconv.Atoi(os.Args[1])
	handleError(err)

	//Read the json file and get the server configurations 
	var cluster_config raft.ClusterConfig
	server_configs, err := ioutil.ReadFile("/home/avp/GO/src/github.com/pankajrandhe/cs733/assignment2/servers_json.json")
	if err != nil{
		panic(err)
	}

	err_json := json.Unmarshal(server_configs, &cluster_config)
	if err_json != nil{
		panic(err_json)
	} 
	
	commitCh := make(chan raft.LogEntry)

	//Initialize Raft Object with the cluster configuration, server id and commit channel.
	var raft_instance *(raft.Raft)
	raft_instance, err = raft.NewRaft(&cluster_config, serverId, commitCh)
	
	//create the TCP address to listen to servers
	server_tcpAddress, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(raft_instance.Cluster.Servers[serverId].LogPort))
	handleError(err)

	//now create the listener to listen on the above tcp address
	server_listener, err := net.ListenTCP("tcp", server_tcpAddress)
	handleError(err)

	//create the TCP address to listen cliets
	client_tcpAddress, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(raft_instance.Cluster.Servers[serverId].ClientPort))
	handleError(err)

	//now create the listener to listen on the above tcp address
	client_listener, err := net.ListenTCP("tcp", client_tcpAddress)
	handleError(err)

	fmt.Println("Server "+ strconv.Itoa(serverId) +" listening on ClientPort:"+strconv.Itoa(raft_instance.Cluster.Servers[serverId].ClientPort))
	fmt.Println("Server "+ strconv.Itoa(serverId) +" listening on LogPort:"+strconv.Itoa(raft_instance.Cluster.Servers[serverId].LogPort))

	//keep listening for acks from follower servers if the server the Leader
	wg.Add(1)
	go func(){

		for {

			server_connection, err := server_listener.Accept()

			if err != nil {
				handleError(err)
				continue
			}

			wg.Add(1)

			go handleServerConnection(server_connection,raft_instance ,serverId)
			defer server_connection.Close()
		}
	defer wg.Done()
	}()

	wg.Add(1)
	//keep listening for the client requests
	go func (){
		for {

			client_connection, err1 := client_listener.Accept()

			if err1 != nil {
				handleError(err1)
				continue
			}

			wg.Add(1)

			go handleClientConnection(client_connection, raft_instance, kvmap.key_values)
			defer client_connection.Close()
		}
		defer wg.Done()
	}()
	
	wg.Wait()
}

func handleServerConnection(server_connection net.Conn, raft_instance *raft.Raft, serverId int){

	//Case1: If the server is the Leader it will listen only for ACKs from the follower servers
	//Case2: If the server is the follower it will listen for the Log Entries on the connection and reply back with ACK if commits

	ch := make(chan []byte)
	eCh := make(chan error)
	var data []byte

	wg.Add(1)

	// Start a goroutine to read from our server-to-server connections
	go func(ch chan []byte, eCh chan error) {
		for {

			response := make([]byte, 512)
			_, err := server_connection.Read(response)

			if err != nil {
				// send an error if it's encountered
				eCh <- err
				return
			}
			// send data if we read some.
			ch <- response
		}

		defer wg.Done()
	}(ch,eCh)

	// Keep reading from the channel for the data
	for {

		data = <-ch
		n := bytes.Index(data, []byte{0})
		string_data := string(data[:n])
		//if the server is the leader then only it has to distinguish between the ACK or Log Broadcasted
		if isACK(string_data) {

			// possibilities are : Logentry OR ACK
			
				fmt.Println("Ack Received by Leader. LSN: "+ strings.Fields(string(data))[0])
				lsn := strings.Split(string_data," ")[0]

					raft_instance.MajorityMap[lsn] = raft_instance.MajorityMap[lsn] + 1
					//Currently assuming majority to 2
					if raft_instance.MajorityMap[lsn] >= 2 {
					fmt.Println("Majority")
					fmt.Println("Command to be exec on State Machine :" + raft_instance.CmdHistMap[lsn])
					}


			

		} else {

			fmt.Println("Log Entry Received at Server "+ strconv.Itoa(serverId))


                                //Send back the ACK (ACK=Lsn) to the leader
                                ack := strings.Split(string_data," ")[0]
                                fmt.Println(serverId," Sending Ack:",ack)
                                Leader_port := raft_instance.Cluster.Servers[raft_instance.LeaderId].LogPort
                                Leader_connection, _ := net.Dial("tcp",":"+strconv.Itoa(Leader_port))
                                _, err3 := Leader_connection.Write([]byte(ack))
                                handleError(err3)
                }
                

			// only possibility is the logentry
			 
	}

	defer wg.Done()
}

func isACK(data string) (response bool){

	count := strings.Count(data, " ")
	if count>1 {
		response = false
	} else {
		response = true
	}
	return response
}


func handleClientConnection(connection net.Conn, raft_instance *raft.Raft, key_values map[string]*value_and_metadata) {

	buffer := make([]byte, 512)
	buffer_temp := make([]byte, 512)
	var err error
	var ver int64 = 1

	for {

		var client_command []byte
		var case_parameter string
		var length_r, length_temp int
		var newline_count int = 0

		// Read the first packet to decide the course of the action
		length_r, err = connection.Read(buffer[0:])
		handleError(err)

		newline_count = strings.Count(string(buffer[0:length_r]), "\r\n")

		case_parameter = strings.Split(string(buffer[0:length_r]), " ")[0]

		// check whether if we have got \r\n and command is not set or cas command
		if strings.Contains(string(buffer[0:length_r]), "\r\n") && !(case_parameter == "set" || case_parameter == "cas") {

			client_command = buffer[0:length_r]

		} else if !(strings.Contains(string(buffer[0:length_r]), "\r\n")) && !(case_parameter == "set" || case_parameter == "cas") {

			// In this case read the packets until we get the "\r\n"
			for {
				length_temp, err = connection.Read(buffer_temp[0:])
				handleError(err)

				buffer = append(buffer[0:length_r], buffer_temp[0:length_temp]...)
				length_r = length_r + length_temp

				if strings.Contains(string(buffer), "\r\n") {
					break
				} else {
					continue
				}
			}
			client_command = buffer[0:length_r]
		} else {

			if newline_count == 2 {
				client_command = buffer[0:length_r]

			} else {

				// Otherwise go on reading until we get two "\r\n" characters
				for {

					if newline_count < 2 {
						buffer_temp := make([]byte, 1024)
						length_temp, err = connection.Read(buffer_temp[0:])
						handleError(err)

						buffer = append(buffer[0:length_r], buffer_temp[0:length_temp]...)
						length_r = length_r + length_temp

						if strings.Contains(string(buffer), "\r\n") {
							newline_count++
						}
					} else {
						break
					}
				}
				client_command = buffer[0:length_r]
			}
		}

		// Once the client command is ready call the Append()
		//var logentry raft.LogStruct
		//logentry, err := raft_instance.Append(client_command)
		 _, err := raft_instance.Append(client_command)
		 	//fmt.Println(logentry)
		
		if err!= nil{

			s := err.Error()
			connection.Write([]byte(s)) 
			//fmt.Println(err)
			/* 
			Error would be genearted in case if the server is not the leader, 
			redirect to the server in this case by using the error returned
			*/
			
			//connection.Close()	//for now, add redirect code here
			continue
		
		}		

		switch case_parameter {

		case "set":
			handleSet(string(client_command), connection, ver)

		case "get":
			handleGet(string(client_command), connection)

		case "getm":
			handleGetm(string(client_command), connection)

		case "cas":
			handleCas(string(client_command), connection, ver)

		case "delete":
			handleDelete(string(client_command), connection)

		default:
			_, err = connection.Write([]byte("ERRCMDERR \r\n"))
			handleError(err)
		}

		if err != nil {
			handleError(err)
			return
		}

	}

	defer wg.Done()
}

func handleSet(client_command string, connection net.Conn, ver int64) {
	var arg1 int64
	var arg2 string
	var val string
	var set_noreply string
	var err error

	command_split := strings.Split(client_command, " ")
	//Generate the version
	ver = ver + 1
	arg1, err = strconv.ParseInt(command_split[2], 10, 64)
	handleError(err)

	if strings.Contains(command_split[3], "\r\n") {
		arg := strings.Trim(command_split[3], "\r\n")
		arg2 = (strings.Split(arg, "\r\n"))[0]
		val = (strings.Split(arg, "\r\n"))[1]

	} else {

		arg2 = command_split[3] //(strings.Split(arg, "\r\n"))[0]
		arg := strings.Trim(command_split[4], "\r\n")
		set_noreply = (strings.Split(arg, "\r\n"))[0]
		val = (strings.Split(arg, "\r\n"))[1]
	}

	instance := value_and_metadata{val, arg1, arg2, ver}
	ref_instance := &(instance)

	kvmap.Lock()
	kvmap.key_values[string(command_split[1])] = ref_instance
	kvmap.Unlock()

	if set_noreply != "noreply" {

		kvmap.RLock()
		ver := strconv.FormatInt(kvmap.key_values[string(command_split[1])].version, 10)
		kvmap.RUnlock()
		_, err = connection.Write([]byte("OK " +ver+ "\r\n")) //buffer[0:length_r])
		handleError(err)
	}

	kvmap.RLock()
	if kvmap.key_values[string(command_split[1])].exptime != 0 {
		kvmap.RUnlock()
		//start the timer taking the KEY as an argument
		go exp_timer(string(command_split[1]), kvmap.key_values)
	} else {
		kvmap.RUnlock()
	}
}

func handleGet(client_command string, connection net.Conn) {

	var err error
	command_split := strings.Split(client_command, " ")
	arg1 := strings.Trim(command_split[1], "\r\n")

	kvmap.RLock()
	_, presence := kvmap.key_values[arg1]

	if presence {

		//acquire the lock for reading the key-value only, once we get the Reference release it
		instance := kvmap.key_values[arg1]
		kvmap.RUnlock()

		_, err = connection.Write([]byte("VALUE " + instance.numbytes + "\r\n" + instance.value + "\r\n"))
		handleError(err)
	} else {
		kvmap.RUnlock()
		_, err := connection.Write([]byte("ERRNOTFOUND\r\n"))
		handleError(err)
	}
}

func handleGetm(client_command string, connection net.Conn) {

	var err error
	command_split := strings.Split(client_command, " ")
	arg1 := strings.Trim(command_split[1], "\r\n")
	kvmap.RLock()
	_, presence := kvmap.key_values[arg1]

	if presence {

		instance := kvmap.key_values[arg1]
		_, err = connection.Write([]byte("VALUE " + strconv.FormatInt(instance.version, 10) + " " + strconv.FormatInt(instance.exptime, 10) + " " + instance.numbytes + "\r\n" + instance.value + "\r\n"))
		kvmap.RUnlock()

	} else {

		kvmap.RUnlock()
		_, err = connection.Write([]byte("ERRNOTFOUND\r\n"))
		handleError(err)
	}
}

func handleCas(client_command string, connection net.Conn, ver int64) {

	var arg1 int64 //version to be provided
	var arg2 int64
	var arg3 string
	var val string
	var cas_noreply string
	var err error
	command_split := strings.Split(client_command, " ")

	arg1, err = strconv.ParseInt(command_split[2], 10, 64)
	handleError(err)

	arg2, err = strconv.ParseInt(command_split[3], 10, 64)
	handleError(err)

	if strings.Contains(command_split[4], "\r\n") {
		arg := strings.Trim(command_split[4], "\r\n")
		arg3 = (strings.Split(arg, "\r\n"))[0]
		val = (strings.Split(arg, "\r\n"))[1]

	} else {

		arg3 = strings.Trim(command_split[4], " ")
		cas_noreply = strings.Trim(command_split[5], "\r\n")

	}

	kvmap.RLock()
	if arg1 == kvmap.key_values[string(command_split[1])].version {

		kvmap.RUnlock()
		kvmap.Lock()
		// Update the old values
		kvmap.key_values[string(command_split[1])].exptime = arg2
		kvmap.key_values[string(command_split[1])].numbytes = arg3
		kvmap.key_values[string(command_split[1])].version = arg1
		kvmap.key_values[string(command_split[1])].value = val
		kvmap.Unlock()

		if cas_noreply != "noreply" {
			kvmap.RLock()
			_, err = connection.Write([]byte("OK " + strconv.FormatInt(kvmap.key_values[string(command_split[1])].version, 10) + "\r\n")) //buffer[0:length_r])
			kvmap.RUnlock()
			handleError(err)
		}

		kvmap.RLock()
		if kvmap.key_values[string(command_split[1])].exptime != 0 {
			//start the timer taking the KEY as an argument
			kvmap.RUnlock()
			go exp_timer(string(command_split[1]), kvmap.key_values)
		} else {
			kvmap.RUnlock()
		}
	} else {

		_, err = connection.Write([]byte("ERR_VERSION\r\n"))
		handleError(err)
		kvmap.RUnlock()
	}

}

func handleDelete(client_command string, connection net.Conn) {

	var err error
	command_split := strings.Split(client_command, " ")
	arg1 := strings.Trim(command_split[1], "\r\n")

	kvmap.RLock()
	_, presence := kvmap.key_values[arg1]
	kvmap.RUnlock()

	if presence {
		kvmap.Lock()
		delete(kvmap.key_values, arg1)
		kvmap.Unlock()

		_, err = connection.Write([]byte("DELETED\r\n"))
		handleError(err)
	} else {

		_, err = connection.Write([]byte("ERRNOTFOUND\r\n"))
		handleError(err)
	}
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func exp_timer(key string, key_values map[string]*value_and_metadata) {

	ticker := time.NewTicker(time.Second * 1)
	record := key_values[key]
	time_limit := record.exptime

	go func() {

		for range ticker.C {

			record.exptime = record.exptime - 1
		}
	}()
	time.Sleep(time.Duration(time_limit) * time.Second)
	ticker.Stop()
	// as soon as the timer expires delete the record
	delete(key_values, key)
}

