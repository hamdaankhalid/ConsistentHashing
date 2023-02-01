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
		mu.Lock()
		defer mu.Unlock()
		data := uploadReq{}
		_ = json.NewDecoder(request.Body).Decode(&data)

		key := data.Key
		value := data.Value

		log.Println("Upload Req: ", data)
		store[key] = value

		writer.WriteHeader(http.StatusCreated)
	}).Methods(http.MethodPost)

	// GET ALL KEYS
	r.HandleFunc("/keys", func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		data := make(map[string][]string)
		data["keys"] = []string{}
		for key := range store {
			data["keys"] = append(data["keys"], key)
		}
		body, _ := json.Marshal(data)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(body)
	}).Methods(http.MethodGet)

	// GET BY KEY
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		log.Println("RequestURL: ", request.URL.Query()["key"])

		key := request.URL.Query()["key"][0]

		val, found := store[key]

		if !found {
			log.Println("Val not found")
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		data := make(map[string]string)
		data["key"] = key
		data["value"] = val

		log.Print("Write back ", data)

		resp, _ := json.Marshal(data)

		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(resp)
	}).Methods(http.MethodGet)

	// DELETE BY KEY
	r.HandleFunc("/key", func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		log.Println("RequestURL: ", request.URL.Query()["key"])

		key := request.URL.Query()["key"][0]
		log.Printf("Deleting key %s \n", key)
		delete(store, key)

		writer.WriteHeader(http.StatusOK)
	}).Methods(http.MethodDelete)

	return r
}
