package mr

import (
	"6.824/tools"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IdleRequest     byte = 0 // MessageType
	MapCompleted    byte = 1
	RpcReadCall     byte = 2
	ReduceCompleted byte = 3

	Forward       byte = 1 // ReplyType
	RunMap        byte = 2
	RunReduce     byte = 3
	RpcReadResult byte = 4
	Exit          byte = 0

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
	mTIdle      *tools.ConcurrentMap
	mTInProcess *tools.ConcurrentMap
	mTCompleted *tools.ConcurrentMap

	// the locations of the buffered pairs on the local disk
	intermediateFiles [][]string

	// record reduceTask status information
	rTIdle      *tools.ConcurrentMap
	rTInProcess *tools.ConcurrentMap
	rTCompleted *tools.ConcurrentMap

	// job status
	rwMutex   *sync.RWMutex
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
			c.rwMutex.Lock()
			c.jobStatus = Mapping
			c.rwMutex.Unlock()

			reply.ReplyType = Forward
			c.mTInProcess = tools.NewConcurrentMap()
			c.mTCompleted = tools.NewConcurrentMap()
		case Mapping:
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			reply.ReplyType = RunMap
			reply.MtNumber = c.mTIdle.Size() - 1
			reply.InputFile = c.inputFiles[reply.MtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			c.mTIdle.Delete(reply.MtNumber)
			c.mTInProcess.Store(reply.MtNumber, time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			}))
		case Reducing:
			reply.ReplyType = RunReduce
			reply.RtNumber = c.rTIdle.Size() - 1

			reply.IntermediateFiles = c.intermediateFiles[reply.RtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			c.rTIdle.Delete(reply.RtNumber)
			c.rTInProcess.Store(reply.RtNumber, time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			}))
		}
	case MapCompleted:
		// fill reply
		reply.ReplyType = Forward

		// change mapTasks status
		c.mTInProcess.Delete(send.MtNumber)
		c.mTCompleted.Store(send.MtNumber, true)
		if c.mTCompleted.Size() == c.mMap {
			c.rwMutex.Lock()
			c.jobStatus = Reducing
			c.rwMutex.Unlock()

			c.rTInProcess = tools.NewConcurrentMap()
			c.rTCompleted = tools.NewConcurrentMap()
		}

		// reserve for redirect the locations information
		for index, v := range send.ReducePartitions {
			c.intermediateFiles[index][send.MtNumber] = v
		}
	case RpcReadCall:
		// fill reply
		reply.ReplyType = RpcReadResult
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
		c.rTInProcess.Delete(send.RtNumber)
		c.rTCompleted.Store(send.RtNumber, true)
		if c.rTCompleted.Size() == c.nReduce {
			c.rwMutex.Lock()
			c.jobStatus = Done
			c.rwMutex.Unlock()
		}
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
	c.rwMutex.RLock()
	if c.jobStatus == Done {
		ret = true
	}
	c.rwMutex.RUnlock()

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
	c.rwMutex = new(sync.RWMutex)
	c.jobStatus = Forking

	// initialize mapTask status
	c.mTIdle = tools.NewConcurrentMap()
	for i := 0; i < len(files); i++ {
		c.mTIdle.Store(i, true)
	}

	// initialize IntermediateFiles array
	c.intermediateFiles = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[i] = make([]string, len(files))
	}

	// initialize reduceTask status
	c.rTIdle = tools.NewConcurrentMap()
	for i := 0; i < nReduce; i++ {
		c.rTIdle.Store(i, true)
	}

	c.server()
	return &c
}
