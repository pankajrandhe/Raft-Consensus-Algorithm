package main

import (
	"fmt"
	"net"
	"regexp"
	"testing"
	"time"
	"errors"
)

func TestClient(t *testing.T) {

	go main() 

	time.Sleep(time.Duration(1) * time.Second)

	var eChr chan error = make(chan error)

	//Test for the single client
	err_stat_single := setSingleClient()
	if err_stat_single != nil{
		t.Error("Error: output not matched")
	}

	//Test 10 concurrent clients
	for i:=0; i<10; i++{

		go setConcurrentClient(eChr)
	}

	errstat_stat_concurrent := <-eChr
	
	if errstat_stat_concurrent != nil{
		t.Error(errstat_stat_concurrent)
	}

	var input string
    fmt.Scanln(&input)
}

func setSingleClient() (match_err error){

	connection, err1 := net.Dial("tcp", ":9000")
	handleErr(err1)

	ch := make(chan []byte)
	eCh := make(chan error)
	var data []byte
	var matched bool

	match_err = errors.New("not matched")

	// Start a goroutine to read from our net connection
	go func(ch chan []byte, eCh chan error) {
		for {
			// try to read the data
			data := make([]byte, 512)
			_, err := connection.Read(data)
			if err != nil {
				// send an error if it's encountered
				eCh <- err
				return
			}
			// send data if we read some.
			ch <- data
		}
	}(ch,eCh)

	//testcase#1 SET command
	sendToServer(connection, "set one 0 10\r\nval#one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		return match_err
	}

	//testcase#2 SET command
	sendToServer(connection, "set two 0 10\r\nval#two\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		return match_err
	}

	//testcase#3 SET command
	sendToServer(connection, "set three 0 10\r\nval#three\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		return match_err
	}

	//testcase#4 GET command
	sendToServer(connection, "get one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("VALUE 10\r\nval#one\r\n", string(data))
	if !matched {
		return match_err
	}

	//testcase#5 GETM command
	sendToServer(connection, "getm one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#one\r\n", string(data))
	if !matched {
		return match_err
	}

	//testcase#6 DELETE command
	sendToServer(connection, "delete two\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("DELETED\r\n", string(data))
	if !matched {
		return match_err
	}

	//testcase#7 DELETE command
	sendToServer(connection, "delete one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("DELETED\r\n", string(data))
	if !matched {
		return match_err
	}

	//testcase#7 CAS command
	sendToServer(connection, "cas three 0 123453 10\r\nval#threethree")
	data = <-ch
	matched,_ = regexp.MatchString("ERR_VERSION\r\n", string(data))
	if !matched {
		return match_err
	}

	return nil
}

func setConcurrentClient(eChr chan error){

	connection, err1 := net.Dial("tcp", ":9000")
	handleErr(err1)

	ch := make(chan []byte)
	eCh := make(chan error)
	var data []byte
	var matched bool
	var matched1 bool

	match_err := errors.New("Output not matched")

	// Start a goroutine to read from our net connection
	go func(ch chan []byte, eCh chan error) {
		for {
			// try to read the data
			data := make([]byte, 512)
			_, err := connection.Read(data)
			if err != nil {
				// send an error if it's encountered
				eCh <- err
				return
			}
			// send data if we read some.
			ch <- data
		}
	}(ch,eCh)

	//testcase#1 SET command
	sendToServer(connection, "set one 0 10\r\nval#one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		eChr <- match_err
	}

	//testcase#2 SET command
	sendToServer(connection, "set two 0 10\r\nval#two\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		eChr <- match_err
	}

	//testcase#3 GET command
	sendToServer(connection, "get one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("VALUE 10\r\nval#one\r\n", string(data))
	if !matched {
		eChr <- match_err
	}

	//testcase#4 GETM command
	sendToServer(connection, "getm one\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#one\r\n", string(data))
	if !matched {
		eChr <- match_err
	}

	//testcase#5 SET command
	sendToServer(connection, "set three 0 10\r\nval#three\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("OK.*",string(data))
	if !matched {
		eChr <- match_err
	}

	//testcase#6 GETM command
	sendToServer(connection, "getm three\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#three\r\n", string(data))
	matched1,_ = regexp.MatchString("ERRNOTFOUND\r\n", string(data))
	if !(matched || matched1) {
		eChr <- match_err
	}

	//testcase#7 DELETE command
	sendToServer(connection, "delete three\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("DELETED\r\n", string(data))
	matched1,_ = regexp.MatchString("ERRNOTFOUND\r\n", string(data))
	if !(matched || matched1) {
		eChr <- match_err
	}

	//testcase#8 GETM command
	sendToServer(connection, "getm three\r\n")
	data = <-ch
	matched,_ = regexp.MatchString("ERRNOTFOUND", string(data))
	if !matched {
		eChr <- match_err
	}

	eChr <- nil
}


func handleErr(err error) {
	if err != nil {
		fmt.Println("Error occured:", err.Error())
	}
}

func sendToServer(connection net.Conn, message string) {
	_, err3 := connection.Write([]byte(message))
	handleErr(err3)
}

		  
