Assignment 4: Raft Consensus Algorithm
=============================

###Overview###
[Raft](https://ramcloud.stanford.edu/raft.pdf) is a consensus algorithm for managing the replicated log over a cluster of servers in a consistent state. This project is an implementation of the Replicated Key-Value Store on top of the Raft Consensus Protocol.

### Project Structure ###
The project at its core has two parallels, one is the Raft side and other is the Key-Value Store. These parallels run as Go-routines and communicate among themselves via channel.
The complete description of the [Raft package can be found here.](https://godoc.org/github.com/pankajrandhe/cs733/assignment4/raft) 

### Execution Instructions ###
To build the project use following command.

`go get github.com/pankajrandhe/cs733/assignment4/`

To test the project execute the following command.

`go test github.com/pankajrandhe/cs733/assignment4  -v `

### Usage ###
Following are the commands which Key-Value store accepts from the client.

* SET command 

`set <key> <exptime> <numbytes> [noreply]\r\n`

`<value bytes>\r\n`

Server responds with:

`OK <version>\r\n`

* GET command

`get <key>\r\n`

Server responds with:

`VALUE <numbytes>\r\n`
`<value bytes>\r\n`

* GETM command

`getm <key>\r\n`

Server responds with:

`VALUE <version> <exptime> <numbytes>\r\n`
`<value bytes>\r\n`

* CAS command

`cas <key> <exptime> <version> <numbytes> [noreply]\r\n`
`<value bytes>\r\n`

Server responds with:

`OK <version>\r\n`

* DELETE command

`delete <key>\r\n`

Server responds with:

`DELETED\r\n`

### BUGS ###
* The project has not yet been tested for the log-reapair test cases.
 
### References ###
* [In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/raft.pdf) by Diego Ongaro and John Ousterhout, Stanford University.
* [Generating random number in a given range](http://golangcookbook.blogspot.in/2012/11/generate-random-number-in-given-range.html)
* [File Append Operations](http://stackoverflow.com/questions/7151261/append-to-a-file-in-go?lq=1)
* [Ticker Implementation](http://stackoverflow.com/questions/16466320/is-there-a-way-to-do-repetitive-tasks-at-intervals-in-golang)
* Discussions with Prof. Sriram Srinivasan
* Help from friends: Abhishek Potnis, Bhavesh Singh, Dushyant Sabharwal, Saurabh Hote and discussions on Piazza forum for CS733.
  
   
 
 
 
