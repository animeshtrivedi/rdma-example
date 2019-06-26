# RDMA exmaple

A simple RDMA server client example. The code contains a lot of comments. Here is the workflow that happens in the example: 

Client: 
  1. setup RDMA resources   
  2. connect to the server 
  3. receive server side buffer information via send/recv exchange 
  4. do an RDMA write to the server buffer from a (first) local buffer. The content of the buffer is the string passed with the `-s` argument. 
  5. do an RDMA read to read the content of the server buffer into a second local buffer. 
  6. compare the content of the first and second buffers, and match them. 
  7. disconnect 

Server: 
  1. setup RDMA resources 
  2. wait for a client to connect 
  3. allocate and pin a server buffer
  4. accept the incoming client connection 
  5. send information about the local server buffer to the client 
  6. wait for disconnect

###### How to run      
```text
git clone https://github.com/animeshtrivedi/rdma-example.git
cd ./rdma-example
cmake .
make
``` 
 
###### server
```text
./bin/rdma_server
```
###### client
```text
atr@atr:~/rdma-example$ ./bin/rdma_client -a 127.0.0.1 -s textstring 
Passed string is : textstring , with count 10 
Trying to connect to server at : 127.0.0.1 port: 20886 
The client is connected successfully 
---------------------------------------------------------
buffer attr, addr: 0x5629832e22c0 , len: 10 , stag : 0x1617b400 
---------------------------------------------------------
...
SUCCESS, source and destination buffers match 
Client resource clean up is complete 
atr@atr:~/rdma-example$ 

```

## Does not have an RDMA device?
In case you do not have an RDMA device to test the code, you can setup SofitWARP software RDMA device on your Linux machine. Follow instructions here: [https://github.com/animeshtrivedi/blog/blob/master/post/2019-06-26-siw.md](https://github.com/animeshtrivedi/blog/blob/master/post/2019-06-26-siw.md).
