package main

import (
	"github.com/gorilla/mux"
	"github.com/hamdaankhalid/consistenthashing/consistenthashing"
	"github.com/hamdaankhalid/consistenthashing/proxy"
	"github.com/hamdaankhalid/consistenthashing/servers"
	"github.com/hamdaankhalid/consistenthashing/systemtesting"
	"hash/fnv"
	"net/http"
	"os"
)

/*
Usage:
To run test demo

INSTANTIATE NODE SERVERS
go run main.go 8040 node
go run main.go 8060 node
go run main.go 8080 node

INSTANTIATE PROXY SERVER
go run main.go 8020 proxy

SET-OFF DEMO TEST
go run main.go test localhost:8020 localhost:8040 localhost:8060 localhost:8080
*/
func main() {
	var r *mux.Router
	if os.Args[2] == "proxy" {
		hash := func(s string) int {
			h := fnv.New32a()
			_, _ = h.Write([]byte(s))
			return int(h.Sum32())
		}
		hmp := consistenthashing.New(
			"/keys",
			"/key",
			"/key",
			"/key",
			hash,
			360,
		)
		r = proxy.New(hmp)
	} else if os.Args[2] == "node" {
		r = servers.GetApp()
	} else if os.Args[1] == "test" {
		masterAddress := os.Args[2]
		nodes := os.Args[3:]
		systemtesting.Run(200, masterAddress, nodes)
		return
	}

	err := http.ListenAndServe(":"+os.Args[1], r)
	if err != nil {
		return
	}
}
