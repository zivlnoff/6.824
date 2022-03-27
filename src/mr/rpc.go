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
	// sendType
	taskType byte // 0 - shakehand		1 - mapDone		2 - reduceRPC read		3 - reduceDone

	mapNumber       int
	reducePartition []string
	reduceNumber    int
}

type Reply struct {
	taskType byte // 1 - runMap		2 - runReduce		other - exit/go on

	// reply for mapTask
	mapNumber int
	inputFile string
	NReduce   int

	// reply for reduceTask the locations which RPC bases on
	intermediateFiles []string //locations
	reduceNumber      int

	// what can i do ?
	// reply for reduceTask the RPC request
	bufferedData []byte //serialization can help us, right?
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
