package main

import (
	"net"
	"strconv"
	"strings"
	"time"
	"sync"
	"log"
)

// Lets first create the structure which will hold the value and all other meta-data for the key
type value_and_metadata struct {
	value    string 
	exptime  int64
	numbytes string
	version  int64
}

// Create the MAP to hold the keys, values and the associated meta-data
var kvmap = struct{
    sync.RWMutex
    key_values map[string]*value_and_metadata
}{key_values: make(map[string]*value_and_metadata)}

func main() {

	//create the TCP address to listen on
	tcpAddress, err := net.ResolveTCPAddr("tcp", ":9000")
	handleError(err)

	//now create the listener to listen on the above tcp address
	listener, err := net.ListenTCP("tcp", tcpAddress)
	handleError(err)

	//keep listening for the client request
	for {

		//if the request arrives accept the request from the client and create the connection object
		connection, err1 := listener.Accept()

		if err1 != nil {
			handleError(err1)
			continue
		}

		// Now handle the connection
		go handleConnection(connection, kvmap.key_values)
		defer connection.Close()
	}
}

func handleConnection(connection net.Conn, key_values map[string]*value_and_metadata) {

	buffer := make([]byte, 512)
	buffer_temp := make([]byte, 512)
	var err error
	var ver int64 = 1

	for {

		var client_command string
		var command_split []string
		var case_parameter string
		var length_r, length_temp int
		var newline_count int = 0

		// Read the first packet to decide the course of the action
		length_r, err = connection.Read(buffer[0:])
		handleError(err)

		newline_count = strings.Count(string(buffer[0:length_r]),"\r\n")
	
		case_parameter = strings.Split(string(buffer[0:length_r]), " ")[0]

		// check whether if we have got \r\n and command is not set or cas command
		if strings.Contains(string(buffer[0:length_r]),"\r\n") && !(case_parameter == "set" || case_parameter == "cas"){

			client_command = string(buffer[0:length_r])

		} else if !(strings.Contains(string(buffer[0:length_r]),"\r\n")) && !(case_parameter == "set" || case_parameter == "cas"){
		
			// In this case read the packets until we get the "\r\n"
			for {
					length_temp, err = connection.Read(buffer_temp[0:])
					handleError(err)

					buffer = append(buffer[0:length_r],buffer_temp[0:length_temp]...)
					length_r = length_r + length_temp

					if strings.Contains(string(buffer),"\r\n"){
						break
					} else {
						continue
					}
			}
			client_command = string(buffer[0:length_r])
		} else {
				
			if newline_count == 2{
				client_command = string(buffer[0:length_r])

			} else {

				// Otherwise go on reading until we get two "\r\n" characters
				for {

					if newline_count <2 {
						buffer_temp := make([]byte,1024)
						length_temp, err = connection.Read(buffer_temp[0:])
						handleError(err)

						buffer = append(buffer[0:length_r],buffer_temp[0:length_temp]...)
						length_r = length_r + length_temp

						if strings.Contains(string(buffer),"\r\n") {
							newline_count ++ 
						}
					} else {
							break
						}
				}
				client_command = string(buffer[0:length_r])
			}			
		}

		//client_command = string(buffer)
		command_split = strings.Split(client_command," ")

		// Using the SWITCH-CASE statements to perform the operation based on the command

		switch case_parameter {

		case "set":
			var arg1 int64
			var arg2 string
			var val string
			var set_noreply string

			//Generate the version
			ver = ver + 1

			arg1, err = strconv.ParseInt(command_split[2], 10, 64)
			handleError(err)

			if strings.Contains(command_split[3], "\r\n") {
				arg := strings.Trim(command_split[3], "\r\n")
				arg2 = (strings.Split(arg, "\r\n"))[0]
				val = (strings.Split(arg, "\r\n"))[1]

			} else {

				arg2 = command_split[3]//(strings.Split(arg, "\r\n"))[0]
				arg := strings.Trim(command_split[4], "\r\n")
				set_noreply = (strings.Split(arg,"\r\n" ))[0]
				val = (strings.Split(arg,"\r\n" ))[1]
			}

			instance := value_and_metadata{val, arg1, arg2, ver}
			ref_instance := &(instance)

			kvmap.Lock()
			kvmap.key_values[string(command_split[1])] = ref_instance
			kvmap.Unlock()

			if set_noreply != "noreply" {
				_, err = connection.Write([]byte("OK " + strconv.FormatInt(key_values[string(command_split[1])].version, 10) + "\r\n")) //buffer[0:length_r])
				handleError(err)
			} 

			kvmap.RLock()
			if kvmap.key_values[string(command_split[1])].exptime != 0 {
				kvmap.RUnlock()
				//start the timer taking the KEY as an argument
				go exp_timer(string(command_split[1]), kvmap.key_values)
			} else{
				kvmap.RUnlock()
			}

		case "get":

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

		case "getm":

			arg1 := strings.Trim(command_split[1], "\r\n")
			kvmap.RLock()
			_, presence := kvmap.key_values[arg1]

			if presence {

				instance := kvmap.key_values[arg1]
				_, err = connection.Write([]byte("VALUE " + strconv.FormatInt(instance.version, 10) + " " + strconv.FormatInt(instance.exptime, 10) + " " + instance.numbytes + "\r\n" + instance.value + "\r\n"))
				kvmap.RUnlock()

			} else {

				kvmap.RUnlock()
				_, err := connection.Write([]byte("ERRNOTFOUND\r\n"))
				handleError(err)
			}

		case "cas":
			var arg1 int64 //version to be provided
			var arg2 int64
			var arg3 string
			var val string
			var cas_noreply string
			var err error

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
					go exp_timer(string(command_split[1]), key_values)
				}else{
					kvmap.RUnlock()
				}
			} else{

				_, err = connection.Write([]byte("ERR_VERSION\r\n"))
				handleError(err)
				kvmap.RUnlock()
			}

		case "delete":
			arg1 := strings.Trim(command_split[1], "\r\n")

			// lets use the Comma-OK function (value, present = m[key]) to delete the key-value pair
			kvmap.RLock()
			_, presence := kvmap.key_values[arg1]
			kvmap.RUnlock()

			if presence {
				//map[arg1] = 1, false
				kvmap.Lock()
				delete(kvmap.key_values, arg1)
				kvmap.Unlock()
				
				_, err = connection.Write([]byte("DELETED\r\n"))
				handleError(err)
			} else {

				_, err = connection.Write([]byte("ERRNOTFOUND\r\n"))
				handleError(err)
			}

		default:
			_, err = connection.Write([]byte("ERRCMDERR \r\n"))
			handleError(err)
		}

		if err != nil {
			handleError(err)
			return
		}

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

	
