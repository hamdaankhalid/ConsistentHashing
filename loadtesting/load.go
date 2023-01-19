package loadtesting

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"reflect"
)

type keyVal struct {
	Key, Value string
}

/*
Run must be preceded by a static pool of node servers, and the master server being up and running.
*/
func Run(dataPoints int, master string, nodes []string) {
	activeNodes, inactiveNodes := setup(master, nodes)
	log.Println("Initial Active Nodes: ", activeNodes)
	log.Println("Initial In-active Nodes: ", inactiveNodes)
	var uploaded []keyVal
	for i := 0; i < dataPoints; i++ {
		kv := keyVal{randSeq(8), randSeq(8)}
		err := upload(master, &kv)
		if err != nil {
			log.Printf("Fail: error during upload key val %s \n", err.Error())
			continue
		}
		uploaded = append(uploaded, kv)

		// randomly remove a server
		if len(activeNodes) > 1 && rand.Intn(100) > 50 {
			removeIdx := rand.Intn(len(activeNodes))
			removedSrv := activeNodes[removeIdx]
			err := removeServer(master, removedSrv)
			if err != nil {
				log.Printf("Fail: error during remove server %s \n", err.Error())
			}
			inactiveNodes = append(inactiveNodes, removedSrv)
			activeNodes = append(activeNodes[:removeIdx], activeNodes[removeIdx+1:]...)
		}

		// randomly add back a server
		if len(inactiveNodes) > 1 && rand.Intn(100) > 50 {
			addIdx := rand.Intn(len(inactiveNodes))
			addedSrv := inactiveNodes[addIdx]
			err := addServer(master, addedSrv)
			if err != nil {
				log.Printf("Fail: error during add server %s \n", err.Error())
			}
			activeNodes = append(activeNodes, addedSrv)
			inactiveNodes = append(inactiveNodes[:addIdx], inactiveNodes[addIdx+1:]...)
		}

		// randomly query uploaded data
		if len(uploaded) > 0 && rand.Intn(100) > 50 {
			keyValIdx := rand.Intn(len(uploaded))
			candidate := uploaded[keyValIdx]

			result, err := getKey(master, candidate.Key)
			if err != nil {
				log.Printf("Fail: error during get key %s \n", err.Error())
				continue
			}

			if !reflect.DeepEqual(candidate, *result) {
				log.Printf("Fail: Expect %v, Got %v \n", candidate, *result)
			}
		}

		log.Println("Active Nodes: ", activeNodes)
		log.Println("In-active Nodes: ", inactiveNodes)
	}
}

func setup(master string, nodes []string) ([]string, []string) {
	var activeNodes []string
	// active nodes start with an arbitrary number of nodes being added
	// num between 1 and len(nodes)
	initNumServers := rand.Intn(len(nodes)) + 1
	for i := 0; i < initNumServers; i++ {
		// select random number in range 0, to last idx of nodes that have not been added already
		candidateIdx := rand.Intn(len(nodes))
		selectedNode := nodes[candidateIdx]
		err := addServer(master, selectedNode)
		if err != nil {
			log.Fatal("Failed to add server during setup")
		}
		activeNodes = append(activeNodes, selectedNode)
		// remove selectedNode from nodes
		nodes = append(nodes[:candidateIdx], nodes[candidateIdx+1:]...)
	}
	// nodes that remain in initial nodes list are inactive/unselected for initial configuration
	inactiveNodes := nodes
	return activeNodes, inactiveNodes
}

func addServer(master string, addr string) error {
	// addr example localhost:6969
	url := "http://" + master + "/add-member?srv=" + addr
	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	log.Println("Add server status ", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to add server got")
	}

	return nil
}

func removeServer(master string, addr string) error {
	url := "http://" + master + "/remove-member?srv=" + addr
	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	log.Println("Remove server status ", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to remove server data got")
	}

	return nil
}

func upload(master string, kv *keyVal) error {
	data, err := json.Marshal(kv)
	if err != nil {
		return err
	}

	url := "http://" + master + "/key"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		return err
	}
	log.Println("Upload status ", resp.StatusCode)
	if resp.StatusCode != http.StatusCreated {
		return errors.New("upload request failed")
	}

	return nil
}

func getKey(master string, key string) (*keyVal, error) {
	log.Printf("Trying to get %s \n", key)
	result := &keyVal{}
	url := "http://" + master + "/key?key=" + key
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	log.Println("Get status ", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("get request failed")
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
