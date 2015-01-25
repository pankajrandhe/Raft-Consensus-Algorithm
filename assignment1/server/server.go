package main

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
	"sync"
)

// Lets first create the structure which will hold the value and all other meta-data for the key
type value_and_metadata struct {
	value    string //[]byte
	exptime  int64
	numbytes string
	version  int64
}

// Create the MAP to hold the keys, values and the associated meta-data
//var key_values map[string]*value_and_metadata

var kvmap = struct{
    sync.RWMutex
    key_values map[string]*value_and_metadata
}{key_values: make(map[string]*value_and_metadata)}

func main() {

	//var key_values map[string] *value_and_metadata
	//key_values = make(map[string]*value_and_metadata)

	//create the TCP address to listen on
	tcpAddress, err := net.ResolveTCPAddr("tcp", ":9000")
	handleError(err)

	//now create the listener to listen on the above tcp address
	listener, err := net.ListenTCP("tcp", tcpAddress)
	handleError(err)

	//keep listening for the client request
	for {

		fmt.Println("Server listening...")

		//if the request arrives accept the request from the client and create the connection object
		connection, err1 := listener.Accept()
		fmt.Println("req accepted")

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

	var buffer [1024]byte
	var err error

	for {

		var client_command string
		var case_parameter string
		var command_split []string
		var length_r int

		length_r, err = connection.Read(buffer[0:])
		handleError(err)

		client_command = string(buffer[0:length_r])
		command_split = strings.Split(client_command, " ")

		case_parameter = command_split[0]
		fmt.Println("Client says:", client_command)

		// Using the SWITCH-CASE statements to perform the operation based on the command

		switch case_parameter {

		case "set":
			var arg1 int64
			var arg2 string
			var val string
			var set_noreply string

			//Generate the random number as the version
			var ver int64 = rand.Int63()

			arg1, err = strconv.ParseInt(command_split[2], 10, 64)
			handleError(err)

			if strings.Contains(command_split[3], "\r\n") {
				arg := strings.Trim(command_split[3], "\r\n")
				arg2 = (strings.Split(arg, "\r\n"))[0]
				val = (strings.Split(arg, "\r\n"))[1]

			} else {

				arg2 = strings.Trim(command_split[3], " ")
				set_noreply = strings.Trim(command_split[4], "\r\n")
			}

			/*keeping aside the delay condition for now */
			// Read the second line from the client

			//val_length, err = connection.Read(buffer1[0:])
			//handleError(err)

			//val = strings.Trim(string(buffer1[0:val_length]), "\r\n")

			instance := value_and_metadata{val, arg1, arg2, ver} //val
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
				if false {
					fmt.Println("VALUE " + instance.numbytes + "\r\n" + instance.value + "\r\n")
				}
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

			/* Descarding the delay condition for now*/
			//val_length, err := connection.Read(buffer1[0:])
			//handleError(err)

			//val = strings.Trim(string(buffer1[0:val_length]), "\r\n")

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
				kvmap.RUnlock()
			}

		case "delete":
			arg1 := strings.Trim(command_split[1], "\r\n")
			fmt.Println(arg1)

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
			fmt.Println("ERRCMDERR\r\n")
			_, err = connection.Write([]byte("ERRCMDERR \r\n"))
			handleError(err)
		}

		if err != nil {
			handleError(err)
			return
		}

	}
	//connection.Close()
	//return
}

func handleError(err error) {
	if err != nil {
		fmt.Println("Error occured:", err.Error())
	}
}

func exp_timer(key string, key_values map[string]*value_and_metadata) {

	ticker := time.NewTicker(time.Second * 1)
	record := key_values[key]
	time_limit := record.exptime

	go func() {

		for t := range ticker.C {

			record.exptime = record.exptime - 1
			if false {
				fmt.Println("Tick at", t)
			}
		}
	}()
	time.Sleep(time.Duration(time_limit) * time.Second)
	ticker.Stop()
	// as soon as the timer expires delete the record
	delete(key_values, key)
}
