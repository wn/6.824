package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"io/ioutil"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		workerMapStage(mapf)
		// TODO reduce stage.

		fmt.Println("Starting reduce stage!")
		workerReduceStage(reducef)
}

func workerReduceStage(reducef func(string, []string) string) {
	prevJob := -1
	for {
		reduceID, reduceCompleted := getReduceJob(prevJob)
		if reduceCompleted {
			break
		}
		prevJob = parseReduceFile(reduceID, reducef)
	}
}

func getReduceJob(prevJob int) (int, bool) {
	req := MRRequest{PrevCompletedJob: prevJob}
	reply := RReply{}
	
	// send the RPC request, wait for the reply.
	call("Master.RequestReduceJob", &req, &reply)

	return reply.ReduceJob, reply.ReduceStageCompleted
}
func parseReduceFile(reduceID int, reducef func(string, []string) string) int {
	intermediateFileFormat := fmt.Sprintf("*-%d", reduceID)
	files, err := filepath.Glob(intermediateFileFormat)
	if err != nil {
        log.Fatal(err)
	}
	intermediate := []KeyValue{}
	for _, file := range files {
		content, err := ioutil.ReadFile(file)
		result := []KeyValue{}
		if err != nil || json.Unmarshal(content, &result) != nil {
			return -1
		}
		intermediate = append(intermediate, result...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	return reduceID
}

func workerMapStage(mapf func(string, string) []KeyValue) {
	prevJob := -1
	for {
		job, mapCompleted, nReduce := getMapJob(prevJob)
		if mapCompleted {
			break
		}
		prevJob = parseMapFile(job, nReduce, mapf)
	}
}

// GetJob retrieve a filename from the master.
func getMapJob(previousJob int) (Job, bool, int) {
	req := MRRequest{PrevCompletedJob: previousJob}
	reply := MRReply{}
	
	// send the RPC request, wait for the reply.
	call("Master.RequestMapJob", &req, &reply)

	return reply.Job, reply.MapStageCompleted, reply.NReduce
}

// given job and map function, create an auxilary file and write intermediate 
// result to it.
func parseMapFile(job Job, nReduce int, mapf func(string, string) []KeyValue) int {
	filename, jobID := job.Filename, job.JobId
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return -1
	}

	table := make(map[int][]KeyValue)
	
	kvs := mapf(filename, string(content))
	for _, kv := range kvs {
		reduceID := ihash(kv.Key) % nReduce
		table[reduceID] = append(table[reduceID], kv)
	}

	for id, kvs := range table {
		reduceFilename := generateIntermediateFileName(jobID, id)
		b, err := json.Marshal(kvs)
		if err != nil || ioutil.WriteFile(reduceFilename, b, 0644) != nil {
			return -1
		}
	}

	return jobID
}

func generateIntermediateFileName(jobID int, reduceID int) string {
	filename := fmt.Sprintf("mr-%d-%d", jobID, reduceID)
	os.Create(filename)
	return filename
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
