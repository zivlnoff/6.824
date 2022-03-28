package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// shakehand message
	shakehand := Send{}
	shakehand.MessageType = 0

	reply := mapReduceCall(&shakehand)

	switch reply.ReplyType {
	case runMap:
		mapTask(reply, mapf)
	case runReduce:
		reduceTask(reply, reducef)
	}
}

func mapTask(reply *Reply, mapf func(string, string) []KeyValue) {
	// open file and read data
	file, err := os.Open(reply.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.InputFile)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.InputFile)
	}

	// concrete map
	kva := mapf(reply.InputFile, string(content))

	intermediateFileNamePrefix := "mr-" + strconv.Itoa(reply.MtNumber)

	intermediateFileNameFile := make([]*os.File, reply.NReduce)

	// create intermediate files
	for i := 0; i < int(reply.NReduce); i++ {
		intermediateFileNameFile[i], _ = os.Create(intermediateFileNamePrefix + "-" + strconv.Itoa(i))
	}

	for _, kv := range kva {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(intermediateFileNameFile[ihash(kv.Key)], "%v %v\n", kv.Key, kv.Value)
	}

	// close file
	for i := 0; i < int(reply.NReduce); i++ {
		intermediateFileNameFile[i].Close()
	}

	var mapDone *Send
	mapDone.MessageType = mapCompleted
	mapDone.MtNumber = reply.MtNumber
	for i := 0; i < reply.NReduce; i++ {
		mapDone.ReducePartitions[i] = intermediateFileNameFile[i].Name()
	}

	mapReduceCall(mapDone)
}

func reduceTask(reply *Reply, reducef func(string, []string) string) {
	// when a reduce worker is notified by the master
	// about these locations, it uses remote procedure
	// calls to read the buffered data from the local
	// disks of the map workers.

	// RPC read
	var rpcReadReq *Send
	rpcReadReq.MessageType = rpcRead
	rpcReadReq.RtNumber = reply.RtNumber

	response := mapReduceCall(rpcReadReq)

	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(string(response.BufferedData), ff)
	intermediate := []KeyValue{}

	for i := 0; i < len(words); i += 2 {
		intermediate = append(intermediate, KeyValue{words[i], words[i+1]})
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.RtNumber)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	var reduceDone *Send
	reduceDone.MessageType = reduceCompleted
	reduceDone.RtNumber = reply.RtNumber

	mapReduceCall(reduceDone)
}

func mapReduceCall(sendMessage *Send) *Reply {

	// make send
	send := sendMessage

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MapReduce", &send, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.MessageType %v\n", reply.ReplyType)
	} else {
		fmt.Printf("call failed!\n")
	}

	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
