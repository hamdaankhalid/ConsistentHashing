package consistenthashing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

// HashingFunc lets me compose ConsistentHashing struct object with a plethora of different hashing algorithms
type HashingFunc func(string) int

type ConsistentHashing struct {
	sync.Mutex

	// These routes are the endpoints exposed by every server in cluster to move data around during redistribution
	allKeysRoute, removeKeyRoute, addKeyRoute, getKeyRoute string
	hashFunc                                               HashingFunc
	ringSize                                               int
	ring                                                   *ring
}

func New(allKeysRoute string,
	removeKeyRoute string,
	addKeyRoute string,
	getKeyRoute string,
	hashFunc HashingFunc,
	ringSize int,
) *ConsistentHashing {
	return &ConsistentHashing{
		allKeysRoute:   allKeysRoute,
		removeKeyRoute: removeKeyRoute,
		getKeyRoute:    getKeyRoute,
		addKeyRoute:    addKeyRoute,
		hashFunc:       hashFunc,
		ringSize:       ringSize,
		ring:           &ring{size: ringSize},
	}
}

/*
GetShard will find the first server where the shardKey's mapped keyId is greater than the serverPosition, then return the address of the
next server while satisfying circular ring constraints
*/
func (ch *ConsistentHashing) GetShard(shardKey string) (string, error) {
	ch.Lock()
	defer ch.Unlock()
	keyPos := ch.hashFunc(shardKey) % ch.ringSize
	log.Printf("Getting owning server for key with pos: %d \n", keyPos)
	owner, err := ch.ring.getOwner(keyPos)
	if err != nil {
		return "", err
	}
	log.Printf("Owner: %v \n", owner)

	return owner.address, nil
}

/*
AddMember Adds a server into our cluster while preserving consistent hashing constraints. It will iterate over the ring
and find the first server where the nodeId is greater than current servers newly mapped id/position. Then we will insert
a new server entity between the previous server and then server containing the value greater than newly mapped
id/position.
*/
func (ch *ConsistentHashing) AddMember(serverAddr string) error {
	ch.Lock()
	defer ch.Unlock()

	log.Println("Adding new server to cluster members")

	nodePos := ch.hashFunc(serverAddr) % ch.ringSize
	newNode := &ringMember{address: serverAddr, position: nodePos}

	log.Println("Adding new server into ring, new server pos: ", nodePos)

	// Insert in ring, and get the next node after it post-insertion
	newInsertedAt := ch.ring.insert(newNode)

	if ch.ring.numServers() == 1 {
		return nil
	}
	next := ch.ring.getNextRingMember(newInsertedAt)

	log.Printf("%v inserted at %d redistributing from server %v \n", newNode, newInsertedAt, next)

	err := ch.redistribute(next, newNode, false)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (ch *ConsistentHashing) RemoveMember(serverAddr string) error {
	ch.Lock()
	defer ch.Unlock()

	removeIdx := ch.ring.find(serverAddr)

	log.Printf("Removing %s server from idx %d \n", serverAddr, removeIdx)

	if removeIdx == -1 {
		return errors.New("no server with address in cluster")
	}

	// move everything from current node into the next node
	currNode, err := ch.ring.get(removeIdx)
	if err != nil {
		return err
	}

	successor := ch.ring.getNextRingMember(removeIdx)

	err = ch.redistribute(currNode, successor, true)
	if err != nil {
		log.Printf("Error redistributing %s \n", err.Error())
		return err
	}

	// now remove currNode from the ring
	err = ch.ring.remove(removeIdx)
	if err != nil {
		return err
	}

	return nil
}

func (ch *ConsistentHashing) PrintTopology() {
	ch.Lock()
	defer ch.Unlock()
	log.Println("----Topology----")
	for idx, member := range ch.ring.partitionsRing {
		log.Printf("idx %d: server %s with pos %d\n", idx, member.address, member.position)
	}
	log.Println("---------------")
}

func (ch *ConsistentHashing) redistribute(from *ringMember, to *ringMember, isRemoval bool) error {
	resp, err := http.Get("http://" + from.address + ch.allKeysRoute)
	if err != nil {
		return err
	}
	var decodedResp allKeysResponse
	err = json.NewDecoder(resp.Body).Decode(&decodedResp)
	if err != nil {
		return err
	}
	fmt.Println("redistributing from ", from, ", to ", to)

	var wg sync.WaitGroup
	for _, key := range decodedResp.Keys {
		keyId := ch.hashFunc(key) % ch.ringSize
		correctPlacement, err := ch.ring.getOwner(keyId)
		if err != nil {
			log.Println(err)
			continue
		}
		if (correctPlacement.address == to.address) || isRemoval {
			log.Println("Moving key ", key, " from ", from, ", to ", to)
			wg.Add(1)
			go func(wg *sync.WaitGroup, key string, removeKeyRoute string, addKeyRoute string, getKeyRoute string) {
				defer wg.Done()

				client := &http.Client{}

				// Get Key Val from fromMem
				getKeyUrl := "http://" + from.address + getKeyRoute + "?key=" + key
				resp, err := client.Get(getKeyUrl)
				if err != nil {
					log.Println("Error getting key")
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("Get key response unsuccessful got %d for request to %s \n", resp.StatusCode, getKeyUrl)
					return
				}
				buf, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				respBody := bytes.NewBuffer(buf)

				// Add key val to toMem
				resp, err = client.Post("http://"+to.address+addKeyRoute, resp.Header.Get("Content-Type"), respBody)
				if err != nil {
					log.Println("Error adding key")
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusCreated {
					log.Print("Post key val response unsuccessful")
					return
				}

				// remove key val from fromMem
				removeUrl := "http://" + from.address + removeKeyRoute + "?key=" + key
				req, err := http.NewRequest(http.MethodDelete, removeUrl, nil)
				if err != nil {
					log.Println(err)
					return
				}
				resp, err = client.Do(req)
				if err != nil {
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("Delete response unsuccessful got %d on request to %s \n", resp.StatusCode, removeUrl)
					return
				}
			}(&wg, key, ch.removeKeyRoute, ch.addKeyRoute, ch.getKeyRoute)
		}
	}
	wg.Wait()
	return nil
}

type allKeysResponse struct {
	Keys []string `json:"keys"`
}
