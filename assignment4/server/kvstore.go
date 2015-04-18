package main

import (
	"github.com/pankajrandhe/cs733/assignment4/raft"
	"encoding/json"
	"strconv"
	"sync"
	"time"
	"log"
)

var ver int = 0

// Map to hold Keys, values and associated meta-data
var kvmap = struct {
	sync.RWMutex
	key_values map[string]*kvEntity
}{key_values: make(map[string]*kvEntity)}

// Map to hold the timer objects
var t = struct {
	sync.RWMutex
	timerMap map[string]*time.Timer
}{timerMap: make(map[string]*time.Timer)}

const(
	Set string = "set"
	Get string = "get"
	Getm string = "getm"
	Cas string = "cas"
	Delete string = "delete"
)

// this goes into the map
type kvEntity struct {
	value    []byte
	exptime  int
	numbytes int
	version  int
}

// this is passed to the cmd-handler (lsn reqd. for replying back)
type kvCommand struct{
	lsn int
	data CmdWrapper
}

type Resp struct{
	lsn int 
	data []byte
}

func KVloop(commitCh chan raft.LogStruct) {
	// keep listening on the commit channel
	for{
		logentry := <- commitCh
		if logentry.Committed(){
			log.Println("Got commited entry at KV backend with Lsn:",logentry.Lsn(),"#####################")
			parser(logentry)
		} else{
			continue
		}
	}
}

func parser(logentry raft.LogStruct){
	cmdLsn := (logentry).Lsn()
	// Decode the cmdData of type []byte to type CmdWrapper
	var cmdData CmdWrapper 
	err := json.Unmarshal(logentry.Data(),&cmdData)
	if err != nil{
		log.Fatal(err)
	} else {
		// pack the data and send to command handler
		commandHandler(kvCommand{cmdLsn,cmdData})
	}
}

func commandHandler(command kvCommand) {
	switch command.data.CmdType {
	case Set:
		handleSet(command)
	case Get:
		handleGet(command)
	case Getm:
		handleGetm(command)
	case Cas:
		handleCas(command)
	case Delete:
		handleDelete(command)
	default:
		ResponseCh <- Resp{command.lsn,[]byte("ERR_CMD_ERR\r\n")}
	}
}

func handleSet(cmd kvCommand) {
	instance := kvEntity{
		value : cmd.data.Value,
		exptime : cmd.data.Exptime,
		numbytes : cmd.data.Numbytes,
		version : setVersion(),
	}
	// Put the info into kvstore	
	kvmap.Lock()
	kvmap.key_values[cmd.data.Key] = &instance
	kvmap.Unlock()

	if !cmd.data.Noreply{ // when noreply is false
		kvmap.RLock()
		ResponseCh <- Resp{cmd.lsn,[]byte("OK "+strconv.Itoa(kvmap.key_values[cmd.data.Key].version)+"\r\n")}
		kvmap.RUnlock()
	}
	
	kvmap.RLock()
	if kvmap.key_values[cmd.data.Key].exptime != 0 {
		kvmap.RUnlock()
		//go exp_timer(cmd.data.Key, kvmap.Key_values)  // old one!
		k := cmd.data.Key
		timer := time.AfterFunc(time.Duration(cmd.data.Exptime) * time.Second,
			func (){delete(kvmap.key_values,k)})
		t.Lock()
		t.timerMap[k] = timer  // so as to reset the timer later on
		t.Unlock()
	} else {
		kvmap.RUnlock()
	}
}

func handleCas(cmd kvCommand) {
	kvmap.RLock()
	if cmd.data.Version == kvmap.key_values[cmd.data.Key].version { // compare the version
		kvmap.RUnlock()
		// Update the old values
		kvmap.Lock()
		kvmap.key_values[cmd.data.Key].value = cmd.data.Value
		kvmap.key_values[cmd.data.Key].exptime = cmd.data.Exptime
		kvmap.key_values[cmd.data.Key].numbytes = cmd.data.Numbytes
		kvmap.key_values[cmd.data.Key].version = cmd.data.Version
		kvmap.Unlock()

		kvmap.RLock()
		if kvmap.key_values[cmd.data.Key].exptime != 0 {
			kvmap.RUnlock()
			//go exp_timer(string(cmd.data.Key), kvmap.Key_values)  // OLD APPROACH
			t.Lock()
			t.timerMap[cmd.data.Key].Reset(time.Duration(cmd.data.Exptime) * time.Second)  //Reset the timer taking the Key as an argument
			t.Unlock()
		} else {
			kvmap.RUnlock()
		}

		if !cmd.data.Noreply { //need to reply
			kvmap.RLock()
			ResponseCh <- Resp{cmd.lsn,
				[]byte("OK "+strconv.Itoa(kvmap.key_values[cmd.data.Key].version)+"\r\n"),
			}
			kvmap.RUnlock()
		}
	} else {
		kvmap.RUnlock()
		ResponseCh <- Resp{cmd.lsn,[]byte("ERR_VERSION\r\n")}
	}

}

func handleGet(cmd kvCommand) {
	kvmap.RLock()
	_, presence := kvmap.key_values[cmd.data.Key]
	if presence {
		instance := kvmap.key_values[cmd.data.Key]
		kvmap.RUnlock()
		ResponseCh <- Resp{cmd.lsn, []byte("VALUE "+strconv.Itoa(instance.numbytes)+"\r\n")}
		ResponseCh <- Resp{cmd.lsn, instance.value}
	} else {
		kvmap.RUnlock()
		ResponseCh <- Resp{cmd.lsn,[]byte("ERR_NOTFOUND\r\n")}
	}
}

func handleGetm(cmd kvCommand) {
	kvmap.RLock()
	_, presence := kvmap.key_values[cmd.data.Key]
	if presence {
		instance := kvmap.key_values[cmd.data.Key]
		ResponseCh <- Resp{cmd.lsn,
			[]byte("VALUE "+strconv.Itoa(instance.version)+" "+strconv.Itoa(instance.exptime)+" "+strconv.Itoa(instance.numbytes)+"\r\n"),
		}
		ResponseCh <- Resp{cmd.lsn,instance.value}
		kvmap.RUnlock()
	} else {
		kvmap.RUnlock()
		ResponseCh <- Resp{cmd.lsn,[]byte("ERR_NOTFOUND\r\n")}
	}
}

func handleDelete(cmd kvCommand) {
	kvmap.RLock()
	_, presence := kvmap.key_values[cmd.data.Key]
	kvmap.RUnlock()

	if presence {
		kvmap.Lock()
		arg := "one"
		delete(kvmap.key_values, arg )//cmd.data.Key)
		kvmap.Unlock()
		ResponseCh <-Resp{cmd.lsn,[]byte("DELETED\r\n")}
	} else {
		ResponseCh <- Resp{cmd.lsn,[]byte("ERR_NOTFOUND\r\n")}
	}
}

func setVersion() (int) {
	ver = ver+1
	version := ver
	return version
}

// TO DO list 
// 2. Replying back mechanism to be checked ==> \r\n, errors
// 3. Error handling (KV Errors) ==> validation