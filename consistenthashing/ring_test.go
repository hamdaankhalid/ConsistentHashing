package consistenthashing

import (
	"math/rand"
	"testing"
)

func TestConsistenhashing_RingInsert(t *testing.T) {
	testRing := &ring{size: 800}

	for i := 0; i < 100; i++ {
		testRing.insert(&ringMember{address: "", position: rand.Intn(600)})
	}

	for i := 1; i < 100; i++ {
		curr, err := testRing.get(i)
		if err != nil {
			t.Error()
		}
		prev, err := testRing.get(i - 1)
		if err != nil {
			t.Error()
		}

		if prev.position > curr.position {
			t.Fail()
		}
	}
}

func TestConsistenhashing_RingGetOwner(t *testing.T) {
	testRing := &ring{
		size: 800,
		partitionsRing: []*ringMember{
			{address: "", position: 20},
			{address: "", position: 160},
			{address: "", position: 190},
			{address: "", position: 220},
		},
	}

	res, err := testRing.getOwner(80)
	if err != nil || res.position != 160 {
		t.Fail()
	}

	res, err = testRing.getOwner(300)
	if err != nil || res.position != 20 {
		t.Fail()
	}

	res, err = testRing.getOwner(3)
	if err != nil || res.position != 20 {
		t.Fail()
	}

	res, err = testRing.getOwner(160)
	if err != nil || res.position != 160 {
		t.Fail()
	}
}

func TestConsistentHashing_RingRemove(t *testing.T) {
	testRing := &ring{size: 270}
	for i := 0; i < 100; i++ {
		testRing.insert(&ringMember{address: "", position: i})
	}

	for i := 0; i < 60; i++ {
		if rand.Intn(100) > 50 {
			testRing.remove(rand.Intn(len(testRing.partitionsRing)))
		}
	}

	for i := 1; i < len(testRing.partitionsRing); i++ {
		curr, err := testRing.get(i)
		if err != nil {
			t.Error()
		}
		prev, err := testRing.get(i - 1)
		if err != nil {
			t.Error()
		}

		if prev.position > curr.position {
			t.Fail()
		}
	}
}
