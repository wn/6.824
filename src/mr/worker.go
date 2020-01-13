package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"io/ioutil"
	"encoding/json"
	"os"
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
	prevJob := -1
	for {
		job, mapCompleted, nReduce := GetJob(prevJob)
		fmt.Println("LLLLLLLLLLLLLLLLLL",job, mapCompleted, nReduce)
		if mapCompleted {
			break
		}
		prevJob = ParseFile(job, nReduce, mapf)
	}
}

// GetJob retrieve a filename from the master.
func GetJob(previousJob int) (Job, bool, int) {
	args := MRRequest{PrevCompletedJob: previousJob}
	reply := MRReply{}
	
	// send the RPC request, wait for the reply.
	call("Master.RequestMapJob", &args, &reply)
	return reply.Job, reply.MapStageCompleted, reply.NReduce
}

// ParseFile returns the id of the last successful job. If unsuccessful, job will return -1.
func ParseFile(job Job, nReduce int, mapf func(string, string) []KeyValue) int {
	filename, jobId := job.Filename, job.JobId
	fmt.Println(" parsing filename: ", filename)
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("File not found error!: ", filename)
		return -1
	}

	table := make(map[int][]KeyValue)
	
	kvs := mapf(filename, string(content))
	for _, kv := range kvs {
		reduceId := ihash(kv.Key) % nReduce
		table[reduceId] = append(table[reduceId], kv)
	}

	for id, kvs := range table {
		reduceFile := generateReduceFileName(jobId, id)
		b, err := json.Marshal(kvs)
		if err != nil {
			return -1
		}
		if ioutil.WriteFile(reduceFile, b, 0644) != nil {
			return -1
		}
	}

	return job.JobId
}

func generateReduceFileName(jobId int, reduceId int) string {
	filename := fmt.Sprintf("mr-%d-%d", jobId, reduceId)
	os.Create(filename)
	return filename
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
