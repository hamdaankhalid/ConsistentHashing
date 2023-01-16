package main

import (
	"github.com/gorilla/mux"
	"github.com/hamdaankhalid/consistenthashing/consistenthashing"
	"github.com/hamdaankhalid/consistenthashing/loadtesting"
	"github.com/hamdaankhalid/consistenthashing/proxy"
	"github.com/hamdaankhalid/consistenthashing/servers"
	"hash/fnv"
	"net/http"
	"os"
)

func main() {
	var r *mux.Router
	if os.Args[2] == "proxy" {
		hash := func(s string) int {
			h := fnv.New32a()
			h.Write([]byte(s))
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
		loadtesting.Run(100, masterAddress, nodes)
		return
	}

	http.ListenAndServe(":"+os.Args[1], r)
}
