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
	// messageType
	// 0 - idle request for ...
	// 1 - map task completed
	// 2 - reduceRPC read request
	// 3 - reduce task completed
	messageType      byte
	mTNumber         int
	rTNumber         int
	reducePartitions []string
}

type Reply struct {
	// replyType
	// 1 - work for map task
	// 2 - work for reduce task
	// other - exit/go on/neverMind (decided by proto/scene)
	replyType byte

	// reply for mapTask
	mTNumber  int
	NReduce   int
	inputFile string

	// reply for reduceTask the locations which RPC bases on
	rTNumber          int
	intermediateFiles []string //locations

	// reply for reduceTask the RPC request
	bufferedData []byte
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
