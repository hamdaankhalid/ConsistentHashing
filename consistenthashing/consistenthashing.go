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
	keyId := ch.hashFunc(shardKey) % ch.ringSize
	owner, err := ch.ring.getOwner(keyId)
	if err != nil {
		return "", err
	}
	return owner.address, nil
}

/*
AddMember Adds a server into our cluster while preserving consistent hashing constraints. It will iterate over the ring
and find the first server where the nodeId is greater than current servers newly mapped id/position. Then we will insert
a new server entity between the previous server and then server containing the value greater than newly mapped
id/position.
*/
func (ch *ConsistentHashing) AddMember(serverAddr string) error {
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
	removeIdx := ch.ring.find(serverAddr)
	if removeIdx == -1 {
		return errors.New("so server with address in cluster")
	}

	// move everything from current node into the next node
	currNode, err := ch.ring.get(removeIdx)
	if err != nil {
		return err
	}
	successor := ch.ring.getNextRingMember(removeIdx)

	err = ch.redistribute(currNode, successor, true)
	if err != nil {
		log.Println(err)
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
	for idx, member := range ch.ring.partitionsRing {
		log.Printf("idx %d: server %s with pos %d\n", idx, member.address, member.position)
	}
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
	fmt.Println("redistributing: ", decodedResp, "to ", to)

	var wg sync.WaitGroup
	for _, key := range decodedResp.Keys {
		keyId := ch.hashFunc(key)
		if keyId < to.position || isRemoval {
			wg.Add(1)
			go func(wg *sync.WaitGroup, key string, removeKeyRoute string, addKeyRoute string, getKeyRoute string) {
				defer wg.Done()
				client := &http.Client{}

				// Get Key Val from fromMem
				resp, err := client.Get("http://" + from.address + getKeyRoute + "?key=" + key)
				if err != nil {
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Print("Get key response unsuccessful")
					return
				}
				buf, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				respBody := bytes.NewBuffer(buf)

				// Add key val to toMem
				resp, err = client.Post("http://"+to.address+addKeyRoute, resp.Header.Get("Content-Type"), respBody)
				if err != nil {
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusCreated {
					log.Print("Post key val response unsuccessful")
					return
				}

				// remove key val from fromMem
				req, err := http.NewRequest("http://"+http.MethodDelete, from.address+removeKeyRoute, nil)
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
					log.Print("Delete response unsuccessful")
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
