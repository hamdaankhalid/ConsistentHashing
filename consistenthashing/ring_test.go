package consistenthashing

import (
	"log"
	"math/rand"
	"testing"
)

func TestConsistenhashing_Ringinsert(t *testing.T) {
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

		log.Printf("%d, %d \n", prev.position, curr.position)

		if curr.position < prev.position {
			t.Fail()
		}
	}
}
