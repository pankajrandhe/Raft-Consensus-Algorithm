package main

import (
	"testing"
	"fmt"
	"net"
	"strings"
	"time"
	"os"
)



func TestSet(t *testing.T){

	go main()
	time.Sleep(time.Duration(1)*time.Second)

	connection, err1 := net.Dial("tcp",":9000")
	handleErr(err1)

	var client_command string = "set one 0 10"
	var value string = "val#one"

	_, err2 := connection.Write([]byte(client_command))
	handleErr(err2)
	time.Sleep(time.Duration(2)*time.Second)
	_, err3 := connection.Write([]byte(value))
	handleErr(err3)

	//for{
		var read_buffer [1024]byte
		_,err3 = connection.Read(read_buffer[0:])
		time.Sleep(time.Duration(1)*time.Second)
		handleErr(err3)
		if strings.Fields(string(read_buffer[0:]))[0] != "OK"{
			t.Error("Error")
		}
	//}
	connection.Close()
}

func handleErr(err error) {
	if err != nil {
		fmt.Println("Error occured:", err.Error())
		os.Exit(1)
	}
}