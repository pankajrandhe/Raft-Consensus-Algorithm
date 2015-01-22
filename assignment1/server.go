package main

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

// Lets first create the structure which will hold the value and all other meta-data for the key
type value_and_metadata struct {
	value    string
	exptime  int64
	numbytes string
	version  int64
}

// Create the MAP to hold the keys, values and the associated meta-data
var key_values map[string]*value_and_metadata


var buffer_key string
var buffer_noreply string

var cas_buffer_key string
var cas_buffer_noreply string

func main() {


	//var key_values map[string] *value_and_metadata
	key_values = make(map[string]*value_and_metadata)

	//create the TCP address to listen on
	tcpAddress, err := net.ResolveTCPAddr("tcp", ":9000")
	handleError(err)

	//now create the listener to listen on the above tcp address
	listener, err := net.ListenTCP("tcp", tcpAddress)
	handleError(err)

	//keep listening for the client request
	for {
		//var wait_flag bool = false
		fmt.Println("Server listening...")

		//if the request arrives accept the request from the client and create the connection object
		connection, err1 := listener.Accept()
		fmt.Println("req accepted")

		if err1 != nil {
			fmt.Println("err1")
			continue
		}

		// Now handle the connection
		go handleConnection(connection, key_values)
	}
}

func handleConnection(connection net.Conn, key_values map[string]*value_and_metadata) {

	var buffer [1024]byte
	var wait_flag bool = false
	var cas_wait_flag bool = false

	for {

		var client_command string
		var value string
		var case_parameter string
		var command_split []string

		length_r, err2 := connection.Read(buffer[0:])
		handleError(err2)

		if wait_flag == false && cas_wait_flag == false {

			client_command = string(buffer[0:length_r])
			command_split = strings.Split(client_command, " ")
			case_parameter = command_split[0]
			fmt.Println("Client says: ", client_command)

		} else {

			if wait_flag == true {
				value = string(buffer[0:length_r])
				case_parameter = "set"
				fmt.Println("Client says (V): ", value)
			}

			if cas_wait_flag == true {
				fmt.Println("here CAS")
				value = string(buffer[0:length_r])
				case_parameter = "cas"
				fmt.Println("Client says (V): ", value)
			}

		}


		// Using the SWITCH-CASE statements to perform the operation based on the command

		switch case_parameter {

		case "set":

			fmt.Println("Command is: SET")

			var arg1 int64
			var arg2 string
			var err error
			var ver int64 = rand.Int63n(50000)

			if wait_flag == false {

				arg1, err = strconv.ParseInt(command_split[2], 10, 64)
				handleError(err)

				if strings.Contains(command_split[3], "\r\n") {

					arg2 = strings.Trim(command_split[3], "\r\n")
					handleError(err)
				} /*else {

					arg2 = strings.Trim(command_split[3], " ")
					handleError(err)

					buffer_noreply = strings.Trim(command_split[4], "\r\n")
					handleError(err)
				}*/

				//block this thread until we receive the value
				//length_r, err2 := connection.Read(buffer[0:])
				//handleError(err2)

				instance := value_and_metadata{"", arg1, arg2, ver}
				ref_instance := &(instance)

				key_values[string(command_split[1])] = ref_instance

				//make the wait_flag TRUE so that we can get the VALUE
				buffer_key = command_split[1]
				wait_flag = true
				continue
			}

			if wait_flag == true {

				val := value
				key_values[buffer_key].value = val

				//now reply back to the client
				if buffer_noreply != "noreply" {

					length_w, err3 := connection.Write([]byte("OK " + strconv.FormatInt(key_values[buffer_key].version, 10) + "\r\n")) //buffer[0:length_r])
					if err3 == nil {
						fmt.Println("Replied to client", length_w)
					} else {
						handleError(err3)
					}
				}

				if key_values[buffer_key].exptime != 0 {

					//start the timer taking the KEY as an argument
					go exp_timer(buffer_key, key_values)
				}

				buffer_key = ""
				buffer_noreply = ""
				wait_flag = false
			}
			//continue

		case "get":
			arg1 := strings.Trim(command_split[1], "\r\n")
			_, presence := key_values[arg1]

			if presence {
				instance := key_values[arg1]
				_, err1 := connection.Write([]byte("VALUE "+instance.numbytes+"\r\n"+instance.value+"\r\n"))
				handleError(err1)
			} else {
				_, err1 := connection.Write([]byte("ERRNOTFOUND\r\n"))
				handleError(err1)
			}

		case "getm":
			arg1 := strings.Trim(command_split[1], "\r\n")
			_, presence := key_values[arg1]

			if presence {
				instance := key_values[arg1]
				_, err1 := connection.Write([]byte("VALUE "+strconv.FormatInt(instance.version, 10)+" "+strconv.FormatInt(instance.exptime, 10)))
				handleError(err1)
				_, err2 := connection.Write([]byte(" "+instance.numbytes+"\r\n"+instance.value+"\r\n"))
				handleError(err2)

			} else {

				_, err1 := connection.Write([]byte("ERRNOTFOUND\r\n"))
				handleError(err1)
			}

		case "cas":
			fmt.Println("Command is: CAS")

			var arg1 int64	//version to be provided
			var arg2 int64 
			var arg3 string

			var err error

			if cas_wait_flag == false {

				arg1, err = strconv.ParseInt(command_split[2], 10, 64)
				handleError(err)

				arg2, err = strconv.ParseInt(command_split[3], 10, 64)
				handleError(err)

				if strings.Contains(command_split[4], "\r\n") {

					arg3 = strings.Trim(command_split[4], "\r\n")
					handleError(err)
				} else {

					arg3 = strings.Trim(command_split[4], " ")
					handleError(err)

					cas_buffer_noreply = strings.Trim(command_split[5], "\r\n")
					handleError(err)
				}

				if arg1 == key_values[string(command_split[1])].version {

					// Update the old values
					key_values[string(command_split[1])].exptime = arg2
					key_values[string(command_split[1])].numbytes = arg3
					key_values[string(command_split[1])].version = arg1

					//make the wait_flag TRUE so that we can get the VALUE
					cas_buffer_key = command_split[1]
					cas_wait_flag = true
					continue
				}

			}

			if cas_wait_flag == true {

				val := value
				key_values[cas_buffer_key].value = val

				//now reply back to the client
				if cas_buffer_noreply != "noreply" {
					length_w, err3 := connection.Write([]byte("OK " + strconv.FormatInt(key_values[cas_buffer_key].version, 10) + "\r\n")) //buffer[0:length_r])
					if err3 == nil {
						fmt.Println("Replied to client", length_w)
					} else {
						handleError(err3)
					}
				}

				if key_values[cas_buffer_key].exptime != 0 {

					//start the timer taking the KEY as an argument
					go exp_timer(cas_buffer_key, key_values)
				}

				cas_buffer_key = ""
				cas_buffer_noreply = ""
				cas_wait_flag = false
			}

		case "delete":
			fmt.Println("Command is: DELETE")

			arg1 := strings.Trim(command_split[1], "\r\n")
			fmt.Println(arg1)

			// lets use the Comma-OK function (value, present = m[key]) to delete the key-value pair
			_, presence := key_values[arg1]
			if presence {

				//map[arg1] = 1, false
				delete(key_values, arg1)
				_, err1 := connection.Write([]byte("DELETED\r\n"))
				handleError(err1)
			} else {

				_, err1 := connection.Write([]byte("ERRNOTFOUND\r\n"))
				handleError(err1)
			}

		default:
			fmt.Println("ERRCMDERR\r\n")
			_, err1 := connection.Write([]byte("ERRCMDERR \r\n"))
			handleError(err1)
		}

		if err2 != nil {
			handleError(err2)
			return
		}

	}
	//connection.Close()
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

