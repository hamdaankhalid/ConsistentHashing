# Consistent Hashing Implemented In Golang

Accompanying
Sharding Stateful Services From Modular Hashing To Consistent Hashing (Implementation)
[https://hamdaan-rails-personal.herokuapp.com/articles/25](https://hamdaan-rails-personal.herokuapp.com/articles/25)

## Proxy and Node Server Architecture:

incoming requests get key && upload key value

```
                            Node server 1  
 Client ----> Proxy ----->  Node server 2
                            Node server N
```

An incoming request is first intercepted by the proxy. The proxy inspects the sharding key, and using the consistent
hashing algorithm that it maintains, it makes the request to the right node server from its cluster pool, once this
it takes the response from the node server it then relays back the response to the client

```
                  <-------- Node server 1  
Client <---- Proxy          Node server 2
                            Node server N
```
