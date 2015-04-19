Assignment 4: Raft Consensus Algorithm
=============================

###Overview###
Raft is the consensus algorithm and manages the replicated log in consistent state. This project is the implementation of the Replicated Key-Value Store on top of the Raft Consensus Protocol.

### Project Structure ###
The project at its core has two parallels, one is the Raft side and other is the Key-Value Store. These parallels run as Go-routines and communicate among themselves via channel.

### Execution Instructions ###
```
go get github.com/pankajrandhe/cs733/assignment4/
go test github.com/pankajrandhe/cs733/assignment4 -v 
```

### BUGS ###
* The project has not yet tested for the log-reapair test cases.


  
   
   
 
 
 
