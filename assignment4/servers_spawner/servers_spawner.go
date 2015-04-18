package main

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
)

var wg sync.WaitGroup

func main() {

	path, err := exec.LookPath("server")
	if err != nil {
		log.Fatal("Check your PATH variable")
	}

	for s:=0; s<5;s++{
		wg.Add(1)
		go func(id int) {
			cmd := exec.Command(path,strconv.Itoa(id))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			if err!= nil{
				log.Println("error occured in spawner")
				log.Fatal(err.Error())
			}
			defer wg.Done()
		}(s)
	}
	wg.Wait()
}
