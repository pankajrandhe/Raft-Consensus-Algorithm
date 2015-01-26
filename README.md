# cs733

Execution Instructions <br>
go get github.com/pankajrandhe/cs733/assignment1/server <br>
go test github.com/pankajrandhe/cs733/assignment1/server <br>  

Commands:<br>
1. SET command <br>
Client sends command:<br>
set <key> <exptime> <numbytes> [noreply]\r\n<br>
<value bytes>\r\n<br>
Server responds with:<br>
OK <version>\r\n  

2. GET command<br>
Client sends command:<br>
get <key>\r\n<br>
Server responds with:<br>
VALUE <numbytes>\r\n<br>
<value bytes>\r\n<br>

3. GETM command<br>
Client sends command:<br>
getm <key>\r\n<br>
Server responds with:<br>
VALUE <version> <exptime> <numbytes>\r\n<br>
<value bytes>\r\n

4. CAS command<br>
Client sends command:<br>
cas <key> <exptime> <version> <numbytes> [noreply]\r\n<br>
<value bytes>\r\n<br>
Server responds with:<br>
OK <version>\r\n

5. DELETE command<br>
Client sends command:<br>
delete <key>\r\n<br>
Server responds with:<br>
DELETED\r\n<br>

TO DO Tasks<br>
1. Test cases for the concurrent multiple clients for DELETE, CAS commands using the automated testing framework.
3. Yet the Error Handling is not robust enough, will sort out soon.
