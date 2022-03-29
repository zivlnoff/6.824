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
	// MessageType
	// 0 - idle request for ...
	// 1 - map task completed
	// 2 - reduceRPC read request
	// 3 - reduce task completed
	MessageType      byte
	MtNumber         int
	RtNumber         int
	ReducePartitions []string
}

type Reply struct {
	// ReplyType
	// 1 - forward
	// 2 - work for map task
	// 3 - work for reduce task
	// 0 - exit/neverMind (decided by proto/scene)
	ReplyType byte

	// reply for mapTask
	MtNumber  int
	NReduce   int
	InputFile string

	// reply for reduceTask the locations which RPC bases on
	RtNumber          int
	IntermediateFiles []string //locations

	// reply for reduceTask the RPC request
	BufferedData []KeyValue
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
