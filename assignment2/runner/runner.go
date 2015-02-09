package main

import (
	"log"
	"os"
	"os/exec"
	"sync"
	//"fmt"
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
			cmd.Stdout = os.Stdout
		    cmd.Stderr = os.Stderr
			_ = cmd.Run()

			//if err != nil {panic(err)}
			defer wg.Done()
		}()
		
		wg.Add(1)
		go func(){

			cmd := exec.Command(path, "1")
			cmd.Stdout = os.Stdout
		    cmd.Stderr = os.Stderr
			_ = cmd.Run()
			//if err != nil {panic(err)}
			defer wg.Done()
		}()

		wg.Add(1)
		go func(){
			cmd := exec.Command(path, "2")
			cmd.Stdout = os.Stdout
		    cmd.Stderr = os.Stderr
			_ = cmd.Run()

			//if err != nil {panic(err)}
			defer wg.Done()
		}()

		wg.Add(1)
		go func(){

			cmd := exec.Command(path, "3")
			cmd.Stdout = os.Stdout
		    cmd.Stderr = os.Stderr
			_ = cmd.Run()

			//if err != nil {panic(err)}
			defer wg.Done()
		}()
		
	wg.Wait()	
}

	
