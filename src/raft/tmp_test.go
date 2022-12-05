package raft

import (
	"fmt"
	"testing"
)

func TestOverflow(t *testing.T) {
	var arr = [0]int{}
	fmt.Println(arr[:0])

}
