package main

import (
	"log"
	"os/exec"
	"sync"
)

var wg sync.WaitGroup

func main() {

	path, err1 := exec.LookPath("raft_server")
	if err1 != nil {
		log.Fatal("Check your PATH variable")
	}

		wg.Add(1)
		go func(){

			cmd := exec.Command(path, "0")
			_ = cmd.Run()
			//if err != nil {panic(err)}
			defer wg.Done()
		}()
		
		wg.Add(1)
		go func(){

			cmd := exec.Command(path, "1")
			_ = cmd.Run()
			//if err != nil {panic(err)}
			defer wg.Done()
		}()

		wg.Add(1)
		go func(){
			cmd := exec.Command(path, "2")
			_ = cmd.Run()
			//if err != nil {panic(err)}
			defer wg.Done()
		}()
		
	wg.Wait()	
}

	
