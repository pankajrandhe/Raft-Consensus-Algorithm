# cs733

**Execution Instructions** 
```
go get github.com/pankajrandhe/cs733/assignment1/server 
go test github.com/pankajrandhe/cs733/assignment1/server  
```

**Commands:**
```
1. SET command 
Client sends command:
set <key> <exptime> <numbytes> [noreply]\r\n
<value bytes>\r\n
Server responds with:
OK <version>\r\n  

2. GET command
Client sends command:
get <key>\r\n
Server responds with:
VALUE <numbytes>\r\n
<value bytes>\r\n

3. GETM command
Client sends command:
getm <key>\r\n
Server responds with:
VALUE <version> <exptime> <numbytes>\r\n
<value bytes>\r\n

4. CAS command
Client sends command:
cas <key> <exptime> <version> <numbytes> [noreply]\r\n
<value bytes>\r\n
Server responds with:
OK <version>\r\n

5. DELETE command
Client sends command:
delete <key>\r\n
Server responds with:
DELETED\r\n

```

TO DO Tasks<br>
1. Test cases for the concurrent multiple clients for CAS commands using the automated testing framework.<br>
2. Yet the Error Handling is not robust enough, will sort out soon.<br>
