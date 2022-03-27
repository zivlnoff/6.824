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
	NeverMind = byte(0)
)

type Coordinator struct {
	// Your definitions here.
	inputFiles []string
	mMap       int
	nReduce    int

	// record mapTask status information
	mTReady   map[int]bool
	mTProcess map[int]*time.Timer
	mTEnd     map[int]bool

	// the locations of the buffered pairs on the local disk
	intermediateFiles [][]string

	// record reduceTask status information
	rTReady   map[int]bool
	rTProcess map[int]time.Timer
	rTEnd     map[int]bool

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
	switch send.taskType {
	// 0 - request
	// 1 - mapDone
	// 2 - reduceRPC read
	// 3 - reduceDone
	case 0:
		// assign task by case
		if len(c.mTReady) != 0 {
			// like 4, 3, 2, 1, 0 out in turn, fill reply first
			reply.mapNumber = len(c.mTReady) - 1
			reply.inputFile = c.inputFiles[reply.mapNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.mTReady, reply.mapNumber)
			c.mTProcess[reply.mapNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		} else {
			reply.reduceNumber = len(c.rTReady) - 1
			reply.intermediateFiles = c.intermediateFiles[reply.reduceNumber]
			reply.NReduce = c.nReduce

			// change mapTasks status
			delete(c.rTReady, reply.reduceNumber)
			c.rTProcess[reply.reduceNumber] = time.AfterFunc(10*time.Second, func() {
				//todo   you can call it worker failure
			})
		}
	case 1:
		// fill reply
		reply.taskType = NeverMind

		// change mapTasks status
		delete(c.mTProcess, send.mapNumber)
		c.mTEnd[send.mapNumber] = true

		// reserve for redirect the locations information
		for index, v := range send.reducePartition {
			c.intermediateFiles[index][send.mapNumber] = v
		}
	case 2:
		// fill reply
		for i := 0; i < c.mMap; i++ {
			file, err := os.Open(c.intermediateFiles[send.reduceNumber][i])
			if err != nil {
				log.Fatalf("cannot open %v", reply.inputFile)
			}
			defer file.Close()
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.inputFile)
			}

			// append []byte
			reply.bufferedData = append(reply.bufferedData, content...)
		}
	case 3:
		// fill reply
		reply.taskType = NeverMind

		// change reduceTask status
		delete(c.rTProcess, send.reduceNumber)
		c.rTEnd[send.reduceNumber] = true
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
	c.mMap = len(files)
	c.nReduce = nReduce

	// initialize mapTask status
	for i := 0; i < len(files); i++ {
		c.mTReady[i] = true
	}

	// initialize intermediateFiles array
	c.intermediateFiles = make([][]string, nReduce)
	for i := 0; i < len(files); i++ {
		c.intermediateFiles[i] = make([]string, len(files))
	}

	// initialize reduceTask status
	for i := 0; i < nReduce; i++ {
		c.rTReady[i] = true
	}

	c.server()
	return &c
}
