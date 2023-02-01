# Consistent Hashing Implemented In Golang

## Accompanying Article
Sharding Stateful Services From Modular Hashing To Consistent Hashing (Implementation)
[https://hamdaan-rails-personal.herokuapp.com/articles/25](https://hamdaan-rails-personal.herokuapp.com/articles/25)

## The case to scale
Issues with throughput? Replicate and try out master slave architecture. Shoot but what if it's the memory that's the
bottleneck.

## Setting the stage
You have a stateful service that runs on a single node. The size of your state stored on a single node is getting out of
hand, maybe it's a sqlite server that has hit its OS file size limits, or maybe it's an in memory state for a game
server. It is in your hands to scale horizontally. Go!

## A proposed solution
Put up a proxy, load balance with an algorithm that maintains a mapping of state, server, and requests. 
Our choice of algorithm is consistent hashing. (Read the accompanying article to understand why not modular hashing).

## Proxy and Node Server Architecture:
Incoming requests get key && upload key value

```
                            Node server A  
 Client ----> Proxy ----->  Node server B
                            Node server C
```

An incoming request is first intercepted by the proxy. The proxy inspects the sharding key, and using the consistent
hashing algorithm that it maintains, it makes the request to the right node server from its cluster pool, once this
it takes the response from the node server it then relays back the response to the client

```
                  <-------- Node server A
Client <---- Proxy          Node server B
                            Node server C
```

## What does the architecture look like from the eyes of the algorithm?
The proxy maintains the algorithm. All the nodes are mapped onto the consistent hashing ring. The proxy is not a part of
the ring. It is the one that controls additions, and subtractions from this ring.

![nodes in consistent hashing ring](https://www.researchgate.net/publication/236149101/figure/fig6/AS:669985961672724@1536748509654/Figure-Consistent-hashing-maps-nodes-and-data-items-into-the-same-ring-for.png)

Incoming requests have a sharding key attached to it

## Bottlenecks
If your proxy is really being choked by large upload get requests -> implement DSR (Direct server return) although in 
most request response system proxies are not the bottleneck. The proxy is IO intensive and not cpu intensive, working 
with state can often be cpu intensive so separating the components into proxy and node servers does not usually cause
choking issues. The proxy could also be implemented in the form of a client library that creates a logical proxy and not
its own separate process altogether.
