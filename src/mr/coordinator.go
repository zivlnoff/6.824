package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	curIndex   int
	mapStatus  []byte // 0 - idle		1 - in process		2 - done

	intermediateFiles [][]string
	nReduce           byte
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) mapReduce(send *Send, reply *Reply) error {
	switch send.taskType {
	// 0 - shakehand 	1 - mapDone		2 - reduceRPC read
	case 0:
		reply.NReduce = c.nReduce
		reply.mapNumber = c.curIndex
		reply.inputFile = c.inputFiles[c.curIndex]
		c.mapStatus[c.curIndex] = 1
		c.curIndex++
	case 1:
		c.mapStatus[send.mapNumber] = 2
		c.intermediateFiles[send.mapNumber] = send.intermediateFiles
	case 2:

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
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files //map pieces number == len(inputFiles)
	c.curIndex = 0
	c.mapStatus = make([]byte, len(files))
	c.nReduce = byte(nReduce)
	c.intermediateFiles = make([][]string, len(files))
	for i := 0; i < len(files); i++ {
		c.intermediateFiles[i] = make([]string, len(files))
	}

	c.server()
	return &c
}
