package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type Send struct {
	taskType          byte // 0 - shakehand		1 - mapDone		2 - reduceRPC read
	mapNumber         int
	intermediateFiles []string

	reduceNumber int
}

type Reply struct {
	taskType byte // 1 - map		2 - reduce		other - exit/go on

	// reply for mapTask
	mapNumber int
	inputFile string
	NReduce   byte

	// reply for reduceTask the locations which RPC bases on
	intermediateFiles []string //locations

	// what can i do ?
	// reply for reduceTask the RPC request
	//bufferedData []byte
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
