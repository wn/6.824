package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"
import "sync"

var timeout time.Duration = 5

var debug = true
func print(str string) {
	if debug {
		fmt.Println(str)
	}
}

type Job struct {
	Filename string
	JobId int
}

type Master struct {
	nReduce 			int

	// Map stage
	c 					chan Job
	mapCompletedCount 	chan int // TODO use a better way to use atomic counter
	mapJobStatus 		sync.Map
	jobCount 			int

	// Reduce stage
	reduceJobs			chan int
	reduceCompletedCount chan int // TODO use a better way to use atomic counter
	reduceJobStatus 		sync.Map
}

//
// Reduce RPC handler.
//
func (m *Master) RequestReduceJob(req *MRRequest, reply *RReply) error {
		// TODO write reduce job. Very similar to above.
		if prevJob := req.PrevCompletedJob; prevJob != -1 {
			fmt.Printf("reduce %d completed", prevJob)
			m.reduceJobStatus.Store(prevJob, true)
		}
		for {
			select { 
			case reduceJob, ok := <-m.reduceJobs:
				if ok {
					fmt.Printf("Value %v was read.\n", reduceJob)
					reply.ReduceJob = reduceJob
					// if after timeout, resend work
					go func(reduceJob int, m *Master) {
						time.Sleep(timeout * time.Second)
						if _, ok := m.reduceJobStatus.Load(reduceJob); ok {
							m.reduceCompletedCount <- 1
							if len(m.reduceCompletedCount) == m.nReduce { // scary part as we didnt mutex this, but reducecount can never go above jobcount.
								print("Closing channel!")
								// TODO we can consider clearing master resources for use in reduce.
								close(m.reduceJobs)
							}
						} else {
							m.reduceJobs <- reduceJob
						}
					}(reduceJob, m)
					return nil
				} else {
					print("Channel closed!")
					// Signal to slaves to end Map Stage.
					reply.ReduceStageCompleted = true
					return nil
				}
			default:
				print("No value ready, moving on.")
				time.Sleep(3 * time.Second) // Sleep so that we don't waste compute power.
			}
		}

	return nil
}

//
// Map RPC handler.
//
func (m *Master) RequestMapJob(req *MRRequest, reply *MRReply) error {
		if prevJob := req.PrevCompletedJob; prevJob != -1 {
			fmt.Printf("map %d completed", prevJob)
			m.mapJobStatus.Store(prevJob, true)
		}
		for {
			// Code below copied from: 
			// https://stackoverflow.com/questions/3398490/checking-if-a-channel-has-a-ready-to-read-value-using-go
			select { 
			case job, ok := <-m.c:
				if ok {
					fmt.Printf("[Map Stage] Sending job: %v.\n", job)
					reply.Job, reply.NReduce = job, m.nReduce
					// if after timeout, resend work
					go func(job Job, m *Master) {
						time.Sleep(timeout * time.Second)
						if _, ok := m.mapJobStatus.Load(job.JobId); ok {
							m.mapCompletedCount <- 1
							if len(m.mapCompletedCount) == m.jobCount {
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
					print("Map channel closed!")
					// Signal to slaves to end Map Stage.
					reply.MapStageCompleted = true
					return nil
				}
			default:
				print("Master has no job currently")
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
	return len(m.reduceCompletedCount) == m.nReduce
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
		c: make(chan Job, len(files)),
		mapJobStatus: sync.Map{},
		jobCount: len(files),
		mapCompletedCount: make(chan int, len(files)),

		reduceJobs: make(chan int, nReduce),
		reduceJobStatus: sync.Map{},
		reduceCompletedCount: make(chan int, nReduce),
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
