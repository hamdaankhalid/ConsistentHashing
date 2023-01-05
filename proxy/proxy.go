package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/hamdaankhalid/consistenthashing/consistenthashing"
	"hash/fnv"
	"io"
	"log"
	"net/http"
)

func New() *mux.Router {
	r := mux.NewRouter()
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

	// UPLOAD KEY VAL
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		buf, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		reqCopy := io.NopCloser(bytes.NewBuffer(buf))
		request.Body = reqCopy
		
		data := make(map[string]string)

		_ = json.Unmarshal(buf, &data)

		log.Println(data)

		shard, err := hmp.GetShard(data["key"])
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Proxy
		url := fmt.Sprintf("%s://%s%s", "http", shard, request.RequestURI)
		proxyRequest(writer, request, url)
	}).Methods(http.MethodPost)

	// GET BY KEY
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Query()["key"][0]
		log.Println(key)

		shard, err := hmp.GetShard(key)

		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Proxy
		url := fmt.Sprintf("%s://%s%s", "http", shard, request.RequestURI)
		proxyRequest(writer, request, url)
	}).Methods(http.MethodGet)

	// Add member
	r.HandleFunc("/add-member", func(writer http.ResponseWriter, request *http.Request) {
		servers := request.URL.Query()["srv"]
		for _, server := range servers {
			err := hmp.AddMember(server)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		writer.WriteHeader(http.StatusOK)
	}).Methods(http.MethodGet)

	// Remove member
	r.HandleFunc("/remove-member", func(writer http.ResponseWriter, request *http.Request) {
		servers := request.URL.Query()["srv"]
		for _, server := range servers {
			hmp.RemoveMember(server)
		}

		writer.WriteHeader(http.StatusOK)
	}).Methods(http.MethodGet)

	return r
}

func proxyRequest(w http.ResponseWriter, req *http.Request, newUrl string) {
	log.Println("Proxying to ", newUrl)

	proxyReq, err := http.NewRequest(req.Method, newUrl, req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	proxyReq.Header = req.Header
	proxyReq.Header = make(http.Header)

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	resp.Body.Close()
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
