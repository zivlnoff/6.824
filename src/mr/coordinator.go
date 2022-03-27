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
	IdleRequest     byte = 0 // messageType
	mapCompleted    byte = 1
	rpcRead         byte = 2
	reduceCompleted byte = 3

	NeverMind byte = 0 // replyType
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
	// watch messageType
	switch send.messageType {
	case IdleRequest:
		// watch jobStatus
		switch c.jobStatus {
		case done:
			return nil
		case forking:
			c.jobStatus = mapping
		case mapping:
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			reply.replyType = runMap
			reply.mTNumber = len(c.mTIdle) - 1
			reply.inputFile = c.inputFiles[reply.mTNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.mTIdle, reply.mTNumber)
			c.mTInProcess[reply.mTNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		case reducing:
			reply.replyType = runReduce
			reply.rTNumber = len(c.rTIdle) - 1
			reply.intermediateFiles = c.intermediateFiles[reply.rTNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.rTIdle, reply.rTNumber)
			c.rTInProcess[reply.rTNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		}
	case mapCompleted:
		// fill reply
		reply.replyType = NeverMind

		// change mapTasks status
		delete(c.mTInProcess, send.mTNumber)
		if len(c.mTInProcess) == 0 {
			c.jobStatus = reducing
		}
		c.mTCompleted[send.mTNumber] = true

		// reserve for redirect the locations information
		for index, v := range send.reducePartitions {
			c.intermediateFiles[index][send.mTNumber] = v
		}
	case rpcRead:
		// fill reply
		for i := 0; i < c.mMap; i++ {
			file, err := os.Open(c.intermediateFiles[send.rTNumber][i])
			if err != nil {
				log.Fatalf("cannot open %v", reply.inputFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.inputFile)
			}

			err = file.Close()
			if err != nil {
				log.Fatalf("cannot close %v", reply.inputFile)
			}

			// append []byte
			reply.bufferedData = append(reply.bufferedData, content...)
		}
	case reduceCompleted:
		// fill reply
		reply.replyType = NeverMind

		// change reduceTask status
		delete(c.rTInProcess, send.rTNumber)
		if len(c.rTInProcess) == 0 {
			c.jobStatus = done
		}
		c.rTCompleted[send.rTNumber] = true
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
	for i := 0; i < len(files); i++ {
		c.mTIdle[i] = true
	}

	// initialize intermediateFiles array
	c.intermediateFiles = make([][]string, nReduce)
	for i := 0; i < len(files); i++ {
		c.intermediateFiles[i] = make([]string, len(files))
	}

	// initialize reduceTask status
	for i := 0; i < nReduce; i++ {
		c.rTIdle[i] = true
	}

	c.server()
	return &c
}
