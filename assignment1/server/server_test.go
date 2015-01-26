package main

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"testing"
	"time"
	"errors"
)

func ClientTest(t *testing.T) {

	time.Sleep(time.Duration(1) * time.Second)

	var eCh error

	//Test for the single client
	eCh = setClient()

	//Test 10 concurrent clients
	for i:=0; i<10; i++{

		go func(){
			eCh = setClient()
		}() 
	}
	if eCh!= nil{
		t.Error(eCh)
	}
	var input string
    fmt.Scanln(&input)
    

	
}

func setClient() (eChr error) {

	connection, err1 := net.Dial("tcp", ":9000")
	handleErr(err1)

	ch := make(chan []byte)
	eCh := make(chan error)
	//eChr = make(chan error)
	var data []byte
	//var match_err error
	var matched bool

	err := errors.New("not matched")

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
	fmt.Println(string(data))
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		fmt.Println(err)
		return err
	}

	//testcase#2 SET command
	sendToServer(connection, "set two 0 10\r\nval#two\r\n")
	data = <-ch
	fmt.Println(string(data))
	matched,_ = regexp.MatchString("OK.*", string(data))
	if !matched {
		fmt.Println(err)
		return err
	}

	//testcase#3 GET command
	sendToServer(connection, "get one\r\n")
	data = <-ch
	fmt.Println(string(data))
	matched,_ = regexp.MatchString("VALUE 10\r\nval#one\r\n", string(data))
	if !matched {
		fmt.Println(err)
		return err
	}

	//testcase#4 GETM command
	sendToServer(connection, "getm one\r\n")
	data = <-ch
	fmt.Println(string(data))
	matched,_ = regexp.MatchString("VALUE ([0-9]*) 0 10\r\nval#one\r\n", string(data))
	if !matched {
		fmt.Println(err)
		return err
	}

	return nil
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

		  
