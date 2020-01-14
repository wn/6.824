package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"

var timeout time.Duration = 5

type Job struct {
	Filename string
	JobId int
}

type Master struct {
	nReduce 			int

	// Map stage
	c 					chan Job
	mapCompletedCount 	chan int // TODO use a better way to use atomic counter
	mapJobStatus 		map[int]bool
	jobCount 			int

	// Reduce stage
	reduceJobs			chan int
	reduceCompletedCount chan int // TODO use a better way to use atomic counter
	reduceJobStatus 		map[int]bool
}

//
// Map RPC handler.
//
func (m *Master) RequestReduceJob(req *MRRequest, reply *RReply) error {
	if prevJob := req.PrevCompletedJob; prevJob != -1 {
		m.reduceJobStatus[prevJob] = true
	}
	for {
		select { 
		case reduceJob, ok := <-m.reduceJobs:
			if ok {
				fmt.Printf("Value %v was read.\n", reduceJob)
				reply.ReduceJob= reduceJob
				// if after timeout, resend work
				go func(reduceJob int, m *Master) {
					time.Sleep(timeout * time.Second)
					if m.reduceJobStatus[reduceJob] {
						m.reduceCompletedCount <- 1
						if len(m.reduceCompletedCount) >= m.jobCount { // scary part as we didnt mutex this, but reducecount can never go above jobcount.
							fmt.Println("Closing channel!")
							// TODO we can consider clearing master resources for use in reduce.
							close(m.reduceJobs)
						 }
					 } else {
						m.reduceJobs <- reduceJob
					 }
				}(reduceJob, m)
				return nil
			} else {
				fmt.Println("Channel closed!")
				// Signal to slaves to end Map Stage.
				reply.ReduceStageCompleted = true
				return nil
			}
		default:
			fmt.Println("No value ready, moving on.")
			time.Sleep(3 * time.Second) // Sleep so that we don't waste compute power.
		}
	}
	return nil
}

//
// Reduce RPC handler.
//
func (m *Master) RequestMapJob(req *MRRequest, reply *MRReply) error {
	// TODO write reduce job. Very similar to above.

	if req.PrevCompletedJob != -1 {
		m.mapJobStatus[req.PrevCompletedJob] = true
	}
	for {
		// Code below copied from: 
		// https://stackoverflow.com/questions/3398490/checking-if-a-channel-has-a-ready-to-read-value-using-go
		select { 
		case job, ok := <-m.c:
			if ok {
				fmt.Printf("Value %v was read.\n", job)
				reply.Job, reply.NReduce = job, m.nReduce
				// if after timeout, resend work
				go func(job Job, m *Master) {
					time.Sleep(timeout * time.Second)
					if m.mapJobStatus[job.JobId] {
						m.mapCompletedCount <- 1
						if len(m.mapCompletedCount) >= m.jobCount {
							fmt.Println("Closing channel!")
							// TODO we can consider clearing master resources for use in reduce.
							close(m.c)
						 }
					 } else {
						m.c <- job
					 }
				}(job, m)
				return nil
			} else {
				fmt.Println("Channel closed!")
				// Signal to slaves to end Map Stage.
				reply.MapStageCompleted = true
				return nil
			}
		default:
			fmt.Println("No value ready, moving on.")
			time.Sleep(3 * time.Second) // Sleep so that we don't waste compute power.
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
	// TODO
	return len(m.mapCompletedCount) >= m.jobCount
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		c: make(chan Job, len(files)),
		nReduce: nReduce,
		mapJobStatus: make(map[int]bool),
		jobCount: len(files),
		mapCompletedCount: make(chan int, len(files)),
	}

	// Set up work for map stage.
	for id, file := range files {
		m.c <- Job{Filename: file, JobId: id}
	}

	// Let up work for reduce stage
	for i := 0; i < nReduce; i++ {
		m.reduceJobs <- i
	}

	m.server()

	return &m
}
