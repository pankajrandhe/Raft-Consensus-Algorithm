package main

import (
	"errors"
	"log"
	"net"
	"regexp"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {

	//Test for the single client
	//time.Sleep(time.Duration(1) * time.Second)
	setSingleClient(t)
}

func setSingleClient(t *testing.T) {

	connection, err1 := net.Dial("tcp", "localhost:9000")
	//handleErr(err1)
	if err != nil {
		t.Error(err1)
	}

	ch := make(chan []byte)
	eCh := make(chan error)
	var data []byte
	var matched bool

	match_err := errors.New("not matched")

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
	matched, _ = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
	}
	time.Sleep(time.Duration(1) * time.Second)

	//testcase#2 SET command

	sendToServer(connection, "set two 0 10\r\n")
	time.Sleep(time.Duration(1) * time.Second)
	sendToServer(connection, "val#two\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#3 SET command
	sendToServer(connection, "set three ")
	sendToServer(connection, "0 10\r\nval#three\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#4 GET command
	sendToServer(connection, "get one\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("VALUE 10\r\nval#one\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#5 GETM command
	sendToServer(connection, "getm one\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#one\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#6 DELETE command
	sendToServer(connection, "delete two\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("DELETED\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#7 DELETE command
	sendToServer(connection, "delete one\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("DELETED\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#7 CAS command
	sendToServer(connection, "cas three 0 123453 10\r\nval#three\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("ERR_VERSION\r\n", string(data))
	if !matched {
		t.Error(match_err)
	}

	//testcase#8 SET command
	sendToServer(connection, "set five 2 10\r\nval#five\r\n")
	data = <-ch
	matched, _ = regexp.MatchString("OK.*", string(data))
	if !matched {
		t.Error(match_err)
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
