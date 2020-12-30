package mydynamo

import (
	"strconv"
)

type VectorClock struct {
	//each element is a list of pair<nodeId, version>
	Clocks []int
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	vc := VectorClock {
		Clocks: make([]int, 50),
	}
	for i := 0; i < 50; i+=1 {
		vc.Clocks[i] = 0
	}

	return vc
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
	for index, elem := range otherClock.Clocks {
		if elem < (s).Clocks[index] {
			return false
		} 
	}
	return true
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {

	foundLess := false
	foundGreater := false
	for index, elem := range otherClock.Clocks {
		if (s).Clocks[index] < elem {
			foundLess = true
		} else if (s).Clocks[index] > elem {
			foundGreater = true
		}
	} 
	if foundGreater && foundLess {
		return true
	}
	return false
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	val, _ := strconv.Atoi(nodeId)
	(*s).Clocks[val] += 1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	if len(clocks) < 1 {
		return
	}

	clock := clocks[0]

	for _, cl := range clocks {
		for i := 0; i < len(cl.Clocks); i++ {
			if clock.Clocks[i] < cl.Clocks[i] {
				clock.Clocks[i] = cl.Clocks[i]
			}
		}
	}

	(*s) = clock		
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	for index, elem := range otherClock.Clocks {
		if (*s).Clocks[index] != elem {
			return false
		} 
	}
	return true
}
