package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"errors"
	"io/ioutil"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// TODO Your worker implementation here.
	mapCompleted := false
	var previousFileResult []KeyValue
	for (!mapCompleted) {
		filename, err := GetJob(previousFileResult)
		if err == nil {
			previousFileResult = ParseFile(filename, mapf)
		} else {
			fmt.Println(err)
			mapCompleted = true
		}
	}
}

// GetJob retrieve a filename from the master and parse it
func GetJob(previousFileResult []KeyValue) (string, error) {
	// declare an argument structure.
	args := MRRequest{X:previousFileResult}
	
	// declare a reply structure.
	reply := MRReply{}
	
	// send the RPC request, wait for the reply.
	if call("Master.Example", &args, &reply) {
		return reply.Y, nil
	} else {
		return "", errors.New("NO MORE JOB")
	}
}

func ParseFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	fmt.Println("filename: ", filename)
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return []KeyValue{}
	}
	return mapf(filename, string(content))
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// \c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
