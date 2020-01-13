package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "errors"

type Master struct {
	// Your definitions here.
	
	// list of available workers
	// list of work
	works []string
	jobCounter int
	jobCompleted int

	resultTable map[string][]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *MRRequest, reply *MRReply) error {
	if m.jobCounter == len(m.works) {
		return errors.New("No more job")
	}
	reply.Y = m.works[m.jobCounter]
	m.jobCounter++
	return nil
}

func (m *Master) GetWorkerResult(args *MRRequest, reply *MRReply) {
	m.jobCompleted++
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.jobCompleted == len(m.works)
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.works = files
	m.server()

	fmt.Println(nReduce)

	return &m
}
