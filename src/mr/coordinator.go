package mr

import (
	"6.824/tools"
	"encoding/json"
	"fmt"
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
		c.rwMutex.RLock()
		jobStatus := c.jobStatus
		c.rwMutex.RUnlock()
		switch jobStatus {
		case Done:
			return nil
		case Forking:
			// watch out for order
			c.mTInProcess = tools.NewConcurrentMap()
			c.mTCompleted = tools.NewConcurrentMap()

			c.rwMutex.Lock()
			c.jobStatus = Mapping
			c.rwMutex.Unlock()

			reply.ReplyType = Forward
		case Mapping:
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			mtNumber := c.mTIdle.Random()
			if mtNumber == nil {
				reply.ReplyType = Forward
				break
			}

			reply.ReplyType = RunMap
			reply.MtNumber = mtNumber.(int)

			reply.InputFile = c.inputFiles[reply.MtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks jobStatus
			c.mTIdle.Delete(reply.MtNumber)
			c.mTInProcess.Store(reply.MtNumber, time.AfterFunc(10*time.Second, func() {
				c.mTIdle.Store(reply.MtNumber, true)
				c.mTInProcess.Delete(reply.MtNumber)
			}))
		case Reducing:
			rtNumber := c.rTIdle.Random()
			if rtNumber == nil {
				reply.ReplyType = Forward
				break
			}

			reply.ReplyType = RunReduce
			reply.RtNumber = rtNumber.(int)

			reply.IntermediateFiles = c.intermediateFiles[reply.RtNumber]
			reply.NReduce = c.nReduce

			// change reduceTasks jobStatus
			c.rTIdle.Delete(reply.RtNumber)
			c.rTInProcess.Store(reply.RtNumber, time.AfterFunc(10*time.Second, func() {
				c.rTIdle.Store(reply.RtNumber, true)
				c.rTInProcess.Delete(reply.RtNumber)
			}))
		}
	case MapCompleted:
		// fill reply
		reply.ReplyType = Forward

		// change mapTasks jobStatus
		//todo solve machine recover question, maybe machine UID
		timer, _ := c.mTInProcess.Load(send.MtNumber)
		if timer != nil {
			timer.(*time.Timer).Stop()
		} else {
			break
		}
		c.mTInProcess.Delete(send.MtNumber)
		c.mTCompleted.Store(send.MtNumber, true)
		if c.mTCompleted.Size() == c.mMap {
			c.rTInProcess = tools.NewConcurrentMap()
			c.rTCompleted = tools.NewConcurrentMap()

			c.rwMutex.Lock()
			c.jobStatus = Reducing
			c.rwMutex.Unlock()
		}

		// reserve for redirect the locations information
		for index, v := range send.ReducePartitions {
			c.intermediateFiles[index][send.MtNumber] = v
		}
	case RpcReadCall:
		// fill reply
		reply.ReplyType = RpcReadResult

		decoder := make([]*json.Decoder, c.mMap)
		for i := 0; i < c.mMap; i++ {
			file, err := os.Open(c.intermediateFiles[send.RtNumber][i])
			if err != nil {
				log.Fatalf("cannot open %v", reply.InputFile)
			}
			decoder[i] = json.NewDecoder(file)
			for decoder[i].More() {
				kva := KeyValue{}
				err = decoder[i].Decode(&kva)
				if err != nil {
					fmt.Println("decode failed, err=", err)
				}
				reply.BufferedData = append(reply.BufferedData, kva)
			}
			file.Close()
		}
	case ReduceCompleted:
		// fill reply
		reply.ReplyType = Forward // why am not Exit

		// change reduceTask jobStatus
		//todo solve machine recover question, maybe machine UID
		timer, _ := c.rTInProcess.Load(send.RtNumber)
		if timer != nil {
			timer.(*time.Timer).Stop()
		} else {
			break
		}
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
	//fmt.Println("rpc.Register")
	rpc.Register(c)

	//fmt.Println("rpc.HandleHTTP")
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)

	//fmt.Println("net.Listen")
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	//fmt.Println("http.Serve")
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
