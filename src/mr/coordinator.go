package mr

import (
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
	mapCompleted    byte = 1
	rpcRead         byte = 2
	reduceCompleted byte = 3

	NeverMind byte = 0 // ReplyType
	runMap    byte = 1
	runReduce byte = 2

	done     byte = 0 // jobStatus
	forking  byte = 1
	mapping  byte = 2
	reducing byte = 3
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

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) mapReduce(send *Send, reply *Reply) error {
	// watch MessageType
	switch send.MessageType {
	case IdleRequest:
		// watch jobStatus
		switch c.jobStatus {
		case done:
			return nil
		case forking:
			c.jobStatus = mapping
		case mapping:
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			reply.ReplyType = runMap
			reply.MtNumber = len(c.mTIdle) - 1
			reply.InputFile = c.inputFiles[reply.MtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.mTIdle, reply.MtNumber)
			c.mTInProcess[reply.MtNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		case reducing:
			reply.ReplyType = runReduce
			reply.RtNumber = len(c.rTIdle) - 1
			reply.IntermediateFiles = c.intermediateFiles[reply.RtNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.rTIdle, reply.RtNumber)
			c.rTInProcess[reply.RtNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		}
	case mapCompleted:
		// fill reply
		reply.ReplyType = NeverMind

		// change mapTasks status
		delete(c.mTInProcess, send.MtNumber)
		if len(c.mTInProcess) == 0 {
			c.jobStatus = reducing
		}
		c.mTCompleted[send.MtNumber] = true

		// reserve for redirect the locations information
		for index, v := range send.ReducePartitions {
			c.intermediateFiles[index][send.MtNumber] = v
		}
	case rpcRead:
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
	case reduceCompleted:
		// fill reply
		reply.ReplyType = NeverMind

		// change reduceTask status
		delete(c.rTInProcess, send.RtNumber)
		if len(c.rTInProcess) == 0 {
			c.jobStatus = done
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
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.jobStatus == done {
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
	c.jobStatus = forking

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
