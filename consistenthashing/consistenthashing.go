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
	prev := findInsertionForPos(keyId, ch.ringStart)
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

	log.Println("Adding new server into ring, new server pos: ", nodePos)

	// get first node with position greater than nodePos
	prev := findInsertionForPos(nodePos, ch.ringStart)

	log.Println("Inserting new node after: ", prev)

	// insert new node between prev and previous' next!
	next := prev.next
	newNode := &ringMember{address: serverAddr, position: nodePos, next: next}
	prev.next = newNode

	err := ch.redistribute(next, newNode, false)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (ch *ConsistentHashing) RemoveMember(serverAddr string) error {
	nodePos := ch.hashFunc(serverAddr) % ch.ringSize
	prev, err := findInRing(nodePos, ch.ringStart)
	if err != nil {
		log.Println(err)
		return err
	}
	err = ch.redistribute(prev.next, prev.next.next, true)
	if err != nil {
		log.Println(err)
		return err
	}

	// now remove prev.next
	prev.next = prev.next.next

	return nil
}

func (ch *ConsistentHashing) PrintTopology() {
	currNode := ch.ringStart
	firstIterDone := false
	for currNode != ch.ringStart || !firstIterDone {
		if !firstIterDone {
			firstIterDone = true
		}

		log.Println("Ring Member: ", currNode)

		currNode = currNode.next
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

func findInRing(keyId int, member *ringMember) (*ringMember, error) {
	prevNode := member
	currNode := member
	firstIterDone := false
	for currNode != member || !firstIterDone {
		if !firstIterDone {
			firstIterDone = true
		}

		if keyId == currNode.position {
			return prevNode, nil
		}

		prevNode = currNode
		currNode = currNode.next
	}

	return nil, errors.New("no node with key Id")
}

// Find the first node larger than pos? return the previous?
func findInsertionForPos(pos int, member *ringMember) *ringMember {
	var prevNode *ringMember = nil
	currNode := member
	isFirstIterDone := true
	for currNode != member || isFirstIterDone {
		if isFirstIterDone {
			isFirstIterDone = false
		}

		if pos > currNode.position {
			if prevNode == nil {
				return getLastNode(member)
			}
			return currNode
		}
		prevNode = currNode
		currNode = currNode.next
	}

	// By now we would have done a full loop, and previous would hold one before the ringstart
	return prevNode
}

func getLastNode(member *ringMember) *ringMember {
	var lastNode *ringMember
	node := member
	isFirstIteration := true

	for node != member || isFirstIteration {
		if isFirstIteration {
			isFirstIteration = false
		}

		lastNode = node
		node = node.next
	}
	return lastNode
}
