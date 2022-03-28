package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IdleRequest     byte = 0 // MessageType
	MapCompleted    byte = 1
	RpcRead         byte = 2
	ReduceCompleted byte = 3

	Forward   byte = 1 // ReplyType
	RunMap    byte = 2
	RunReduce byte = 3
	Exit      byte = 0

	Done     byte = 0 // jobStatus
	Forking  byte = 1
	Mapping  byte = 2
	Reducing byte = 3
)

type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	mMap       int
	nReduce    int

	// record mapTask status information
	mTIdle      map[int]bool
	mTInProcess map[int]*time.Timer
	mTCompleted map[int]bool

	// the locations of the buffered pairs on the local disk
	intermediateFiles [][]string

	// record reduceTask status information
	rTIdle      map[int]bool
	rTInProcess map[int]*time.Timer
	rTCompleted map[int]bool

	// job status
	jobStatus byte
}

// Your code here -- RPC handlers for the worker to call.

// MapReduce
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) MapReduce(send *Send, reply *Reply) error {
	// watch MessageType
	switch send.MessageType {
	case IdleRequest:
		// watch jobStatus
		switch c.jobStatus {
		case Done:
			return nil
		case Forking:
			c.jobStatus = Mapping
			reply.ReplyType = Forward
			c.mTInProcess = make(map[int]*time.Timer)
		case Mapping:
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			reply.ReplyType = RunMap
			reply.MtNumber = len(c.mTIdle) - 1
			reply.InputFile = c.inputFiles[reply.MtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.mTIdle, reply.MtNumber)
			c.mTInProcess[reply.MtNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		case Reducing:
			reply.ReplyType = RunReduce
			reply.RtNumber = len(c.rTIdle) - 1
			reply.IntermediateFiles = c.intermediateFiles[reply.RtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.rTIdle, reply.RtNumber)
			c.rTInProcess[reply.RtNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		}
	case MapCompleted:
		// fill reply
		reply.ReplyType = Forward

		// change mapTasks status
		delete(c.mTInProcess, send.MtNumber)
		if len(c.mTInProcess) == 0 {
			c.jobStatus = Reducing
			c.rTInProcess = make(map[int]*time.Timer)
		}
		c.mTCompleted[send.MtNumber] = true

		// reserve for redirect the locations information
		for index, v := range send.ReducePartitions {
			c.intermediateFiles[index][send.MtNumber] = v
		}
	case RpcRead:
		// fill reply
		for i := 0; i < c.mMap; i++ {
			file, err := os.Open(c.intermediateFiles[send.RtNumber][i])
			if err != nil {
				log.Fatalf("cannot open %v", reply.InputFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.InputFile)
			}

			err = file.Close()
			if err != nil {
				log.Fatalf("cannot close %v", reply.InputFile)
			}

			// append []byte
			reply.BufferedData = append(reply.BufferedData, content...)
		}
	case ReduceCompleted:
		// fill reply
		reply.ReplyType = Forward // why am not Exit

		// change reduceTask status
		delete(c.rTInProcess, send.RtNumber)
		if len(c.rTInProcess) == 0 {
			c.jobStatus = Done
		}
		c.rTCompleted[send.RtNumber] = true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//// Register publishes the receiver's methods in the DefaultServer.
	fmt.Println("rpc.Register")
	rpc.Register(c)

	fmt.Println("rpc.HandleHTTP")
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)

	fmt.Println("net.Listen")
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Println("http.Serve")
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.jobStatus == Done {
		ret = true
	}

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files //map pieces number == len(inputFiles)
	c.mMap = len(files)
	c.nReduce = nReduce
	c.jobStatus = Forking

	// initialize mapTask status
	c.mTIdle = make(map[int]bool)
	for i := 0; i < len(files); i++ {
		c.mTIdle[i] = true
	}

	// initialize IntermediateFiles array
	c.intermediateFiles = make([][]string, nReduce)
	for i := 0; i < len(files); i++ {
		c.intermediateFiles[i] = make([]string, len(files))
	}

	// initialize reduceTask status
	c.rTIdle = make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		c.rTIdle[i] = true
	}

	c.server()
	return &c
}
