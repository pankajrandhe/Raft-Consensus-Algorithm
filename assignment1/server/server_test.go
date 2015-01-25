package main

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"testing"
	"time"
)

func TestSet(t *testing.T) {

	go main()
	time.Sleep(time.Duration(1) * time.Second)

	connection, err1 := net.Dial("tcp", ":9000")
	handleErr(err1)

	ch := make(chan []byte)
	eCh := make(chan error)
	var data []byte
	var match_err error
	var matched bool

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
	}(ch, eCh)

	//testcase#1 SET command
	sendToServer(connection, "set one 0 10\r\nval#one\r\n")
	data = <-ch
	matched, match_err = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#2 SET command
	sendToServer(connection, "set two 0 10\r\nval#two\r\n")
	data = <-ch
	matched, match_err = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#3 GET command
	sendToServer(connection, "get one\r\n")
	data = <-ch
	matched, match_err = regexp.MatchString("VALUE 10\r\nval#one\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#4 GETM command
	sendToServer(connection, "getm one\r\n")
	data = <-ch
	matched, match_err = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#one\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#5 DELETE command
	sendToServer(connection, "delete two\r\n")
	data = <-ch
	matched, match_err = regexp.MatchString("DELETED\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}
}

func handleErr(err error) {
	if err != nil {
		fmt.Println("Error occured:", err.Error())
		os.Exit(1)
	}
}

func sendToServer(connection net.Conn, message string) {
	_, err3 := connection.Write([]byte(message))
	handleErr(err3)
}
