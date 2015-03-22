Assignment 3: Raft State Machine
=============================

###Overview###
This project handles the tasks concerned with Raft side implementation of the Replicated KV Store. The implementation includes Leader election, handling client requests, logging data, log replication and putting logentry on commit channel using two-way commit.

### Program Flow ###

* On boting up servers perform the election to elect the leader for the cluster.
* The Client sends a command to the server.
* If the server happens to be leader it logs the command in its own log and brodcasts the entry to all servers.   
* After validation servers send back an acknowledgement to the Leader. 
* On receiving ACKs from majority servers Leader commits the entry in its own log and asks the follower servers 
to commit the entry.
* On committing the log-entry leader sends it over its commit channel so as to execute the client command on 
the state machine.
 
### Execution Instructions ###


### TO DO ###
* To add the test cases to achieve leader change.
* To store log on disk file, currently storing it only in maps.


  
   
   
 
 
 
