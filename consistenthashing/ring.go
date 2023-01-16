package consistenthashing

import "errors"

type ringMember struct {
	address string
	// position is decided by hashing address
	position int
}

type ring struct {
	size           int
	partitionsRing []*ringMember
}

func (r *ring) insert(newNode *ringMember) int {
	insertionIdx := 0
	for idx, member := range r.partitionsRing {
		if newNode.position > member.position {
			insertionIdx = idx + 1
		}
	}

	leftPart := r.partitionsRing[:insertionIdx]
	rightPart := r.partitionsRing[insertionIdx:]

	r.partitionsRing = append(leftPart, append([]*ringMember{newNode}, rightPart...)...)

	return insertionIdx
}

func (r *ring) getNextRingMember(idx int) *ringMember {
	return r.partitionsRing[(idx+1)%len(r.partitionsRing)]
}

func (r *ring) get(idx int) (*ringMember, error) {
	if idx >= len(r.partitionsRing) {
		return nil, errors.New("out of range")
	}
	return r.partitionsRing[idx], nil
}

func (r *ring) find(nodeAddr string) int {
	for idx, member := range r.partitionsRing {
		if nodeAddr == member.address {
			return idx
		}
	}
	return -1
}

func (r *ring) remove(idx int) error {
	if idx < 0 || idx > len(r.partitionsRing) {
		return errors.New("invalid argument range")
	}

	r.partitionsRing = append(r.partitionsRing[0:idx], r.partitionsRing[idx+1:]...)
	return nil
}

func (r *ring) getOwner(dataPos int) (*ringMember, error) {
	if len(r.partitionsRing) == 0 {
		return nil, errors.New("no servers")
	}

	var pre int
	for pre < len(r.partitionsRing) && dataPos > r.partitionsRing[pre].position {
		pre++
	}

	return r.partitionsRing[pre%len(r.partitionsRing)], nil
}

func (r *ring) numServers() int {
	return len(r.partitionsRing)
}
