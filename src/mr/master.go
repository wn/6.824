package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"

var timeout int = 5

type Job struct {
	Filename string
	JobId int
}

type Master struct {
	nReduce int
	c chan Job
	mapCompletedCount chan int
	
	mapJobStatus map[int]bool
	mapJobCompleted int
	jobCount int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) RequestMapJob(req *MRRequest, reply *MRReply) error {
	if req.PrevCompletedJob != -1 {
		m.mapJobStatus[req.PrevCompletedJob] = true
	}
	for {
		// https://stackoverflow.com/questions/3398490/checking-if-a-channel-has-a-ready-to-read-value-using-go
		select { 
		case job, ok := <-m.c:
			
			if ok {
				fmt.Printf("Value %v was read.\n", job)
				reply.Job = job
				reply.NReduce = m.nReduce
				// if after timeout, resend work
				go func(job Job, m *Master) {
					time.Sleep(2 * time.Second)
					fmt.Println("LL", len(m.mapJobStatus))
					if m.mapJobStatus[job.JobId] {
						// TODO race
						m.mapCompletedCount <- 1
						fmt.Println("LL", len(m.mapCompletedCount))
						if len(m.mapCompletedCount) >= m.jobCount {
							 close(m.c)
						 }
					 } else {
						m.c <- job
					 }
				}(job, m)
				return nil
			} else {
				fmt.Println("Channel closed!")
				reply.MapStageCompleted = true
				return nil
			}
		default:
			fmt.Println("No value ready, moving on.")
			time.Sleep(3 * time.Second)
		}
	}
	return nil
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
	return len(m.mapCompletedCount) >= m.jobCount
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println(nReduce)
	m := Master{
		c: make(chan Job, len(files)),
		nReduce: nReduce,
		mapJobStatus: make(map[int]bool),
		jobCount: len(files),
		mapCompletedCount: make(chan int, len(files)),
	}
	for id, file := range files {
		m.c <- Job{Filename: file, JobId: id}
		fmt.Println(id, file)
	}

	m.server()

	return &m
}
