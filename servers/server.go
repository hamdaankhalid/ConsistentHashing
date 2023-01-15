package servers

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sync"
)

var store = map[string]string{}

var mu sync.Mutex

type uploadReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func GetApp() *mux.Router {
	// allKeysRoute, removeKeyRoute, addKeyRoute, getKeyRoute
	r := mux.NewRouter()

	// UPLOAD KEY VAL
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		data := uploadReq{}
		_ = json.NewDecoder(request.Body).Decode(&data)

		key := data.Key
		value := data.Value

		log.Println("Upload Req: ", data)
		mu.Lock()
		defer mu.Unlock()
		store[key] = value

		writer.WriteHeader(http.StatusCreated)
	}).Methods("POST")

	// GET ALL KEYS
	r.HandleFunc("/keys", func(writer http.ResponseWriter, request *http.Request) {
		data := make(map[string][]string)
		data["keys"] = []string{}
		mu.Lock()
		defer mu.Unlock()
		for key := range store {
			data["keys"] = append(data["keys"], key)
		}
		body, _ := json.Marshal(data)
		writer.WriteHeader(http.StatusOK)
		writer.Write(body)
	}).Methods("GET")

	// GET BY KEY
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Query()["key"][0]

		mu.Lock()
		val, found := store[key]
		mu.Unlock()

		if !found {
			log.Println("Val not found")
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		data := make(map[string]string)
		data["key"] = key
		data["value"] = val

		log.Print("Write back ", data)

		resp, _ := json.Marshal(data)

		writer.WriteHeader(http.StatusOK)
		writer.Write(resp)
	}).Methods("GET")

	// DELETE BY KEY
	r.HandleFunc("/key/:key", func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Query()["key"][0]
		mu.Lock()
		delete(store, key)
		mu.Unlock()

		writer.WriteHeader(http.StatusOK)
	}).Methods("DELETE")
	return r
}
