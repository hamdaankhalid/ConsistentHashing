package consistenthashing

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
)

// ringMember is a glorified circularly ringed list
type ringMember struct {
	address string
	// position is decided by hashing address
	position int
	next     *ringMember
}

// HashingFunc lets me compose ConsistentHashing struct object with a plethora of different hashing algorithms
type HashingFunc func(string) int

type ConsistentHashing struct {
	// These routes are the endpoints exposed by every server in cluster to move data around during redistribution
	allKeysRoute, removeKeyRoute, addKeyRoute, getKeyRoute string

	hashFunc  HashingFunc
	ringSize  int
	ringStart *ringMember
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
		ringStart:      nil,
	}
}

/*
GetShard will find the first server where the shardKey's mapped keyId is greater than the serverPosition, then return the address of the
next server while satisfying circular ring constraints
*/
func (ch *ConsistentHashing) GetShard(shardKey string) (string, error) {
	if ch.ringStart == nil {
		return "", errors.New("no members in cluster")
	}
	keyId := ch.hashFunc(shardKey) % ch.ringSize
	prev := findPrevInRing(keyId, gt, ch.ringStart)
	return prev.next.address, nil
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
	if ch.ringStart == nil {
		firstNode := &ringMember{address: serverAddr, position: nodePos}
		firstNode.next = firstNode
		ch.ringStart = firstNode
		log.Println("Head added")
		return nil
	}

	log.Println("Adding new server into ring")

	// get first node with position greater than nodePos
	prev := findPrevInRing(nodePos, gt, ch.ringStart)

	// insert new node between prev and previous' next!
	next := prev.next
	newNode := &ringMember{address: serverAddr, position: nodePos, next: next}
	prev.next = newNode

	err := ch.redistribute(next, newNode)

	if err != nil {
		return err
	}

	return nil
}

func (ch *ConsistentHashing) RemoveMember(serverAddr string) error {
	nodePos := ch.hashFunc(serverAddr) % ch.ringSize
	prev := findPrevInRing(nodePos, eq, ch.ringStart)
	err := ch.redistribute(prev.next, prev.next.next)
	if err != nil {
		return err
	}

	// now remove prev.next
	prev.next = prev.next.next

	return nil
}

func (ch *ConsistentHashing) redistribute(from *ringMember, to *ringMember) error {
	resp, err := http.Get("http://" + from.address + ch.allKeysRoute)
	if err != nil {
		return err
	}
	var decodedResp allKeysResponse
	err = json.NewDecoder(resp.Body).Decode(&decodedResp)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, key := range decodedResp.keys {
		keyId := ch.hashFunc(key)
		if keyId < to.position {
			wg.Add(1)
			go func(wg *sync.WaitGroup, key string, removeKeyRoute string, addKeyRoute string, getKeyRoute string) {
				defer wg.Done()
				client := &http.Client{}

				// Get Key Val from fromMem
				resp, err := client.Get("http://" + from.address + getKeyRoute + "?" + key)
				if err != nil {
					log.Println(err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Print("Get key response unsuccessful")
					return
				}
				buf, err := io.ReadAll(resp.Body)
				defer resp.Body.Close()
				respBody := bytes.NewBuffer(buf)

				// Add key val to toMem
				resp, err = client.Post("http://"+from.address+addKeyRoute, resp.Header.Get("Content-Type"), respBody)
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
	keys []string
}

func lt(a int, b int) bool {
	return a < b
}

func gt(a int, b int) bool {
	return a > b
}

func eq(a int, b int) bool {
	return a == b
}

func findPrevInRing(keyId int, condFunc func(int, int) bool, member *ringMember) *ringMember {
	var prevNode *ringMember = member
	currNode := member
	firstIterDone := false
	for currNode != member || !firstIterDone {
		if !firstIterDone {
			firstIterDone = true
		}

		if condFunc(keyId, currNode.position) {
			return prevNode
		}

		prevNode = currNode
		currNode = currNode.next
	}

	return prevNode
}
