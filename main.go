package main

import (
	"github.com/gorilla/mux"
	"github.com/hamdaankhalid/consistenthashing/proxy"
	"github.com/hamdaankhalid/consistenthashing/servers"
	"net/http"
	"os"
)

func main() {
	var r *mux.Router
	if os.Args[2] == "proxy" {
		r = proxy.New()
	} else if os.Args[2] == "node" {
		r = servers.GetApp()
	}

	http.ListenAndServe(":"+os.Args[1], r)
}
