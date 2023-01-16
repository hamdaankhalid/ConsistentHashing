package loadtesting

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"reflect"
)

type keyVal struct {
	key, val string
}

/*
Run is preceded by a static pool of node servers being instantiated and being up and running
*/
func Run(dataPoints int, master string, nodes []string) {
	// read through upload data file and upload all data to
	// cluster while we read through our data and insert via
	// master we randomly introduce adding and removing of
	// nodes from cluster we also randomly query key value in
	// the iteration
	activeNodes, inactiveNodes := setup(master, nodes)

	var uploaded []keyVal
	for i := 0; i < dataPoints; i++ {
		// At each point we are uploading a random key val string pair
		kv := keyVal{randSeq(8), randSeq(8)}
		err := upload(master, kv)
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
			result, err := getKey(master, candidate.key)
			if err != nil {
				log.Printf("Fail: error during get key %s \n", err.Error())
			}

			if !reflect.DeepEqual(candidate, result) {
				log.Printf("Fail: Expect %v, Got %v \n", candidate, result)
			}
		}
	}
}

func setup(master string, nodes []string) ([]string, []string) {
	var activeNodes []string
	// active nodes start with an arbitrary number of nodes being added
	// num between 1 and len(nodes)
	initNumServers := rand.Intn(len(nodes)) + 1 // TODO: CHECK THIS OFF BY ONE
	for i := 0; i < initNumServers; i++ {
		// select random number in range
		candidateIdx := rand.Intn(len(nodes) - 1)
		selectedNode := nodes[candidateIdx]
		err := addServer(master, selectedNode)
		if err != nil {
			log.Fatal("Failed to add server during setup")
		}
		activeNodes = append(activeNodes, selectedNode)
		// remove selectedNode from nodes
		nodes = append(nodes[:candidateIdx], nodes[candidateIdx+1:]...)
	}
	// nodes that remain in initial nodes are inactive
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

	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to remove server data got")
	}

	return nil
}

// TODO
func upload(master string, kv keyVal) error {
	url := "http://" + master + "/key"
	http.Post(url)
	return nil
}

func getKey(master string, key string) (keyVal, error) {
	return keyVal{}, nil
}

func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
