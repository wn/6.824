package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"

var timeout time.Duration = 5

type Job struct {
	Filename string
	JobId int
}

type Master struct {
	nReduce 			int

	// Map stage
	mapJobs 					chan Job
	mapCompletedCount 	chan int // TODO use a better way to use atomic counter
	mapJobStatus 		sync.Map
	jobCount 			int

	// Reduce stage
	reduceJobs			chan int
	reduceCompletedCount chan int // TODO use a better way to use atomic counter
	reduceJobStatus 		sync.Map
}

func (m *Master) GetWorkerEnv(req *SetupWorkerReq, reply *SetupWorkerReply) error {
	reply.NReduce = m.nReduce
	reply.MapCount = m.jobCount
	return nil
}

//
// Reduce RPC handler.
//
func (m *Master) RequestReduceJob(req *MRRequest, reply *RReply) error {
		if prevJob := req.PrevCompletedJob; prevJob != -1 {
			m.reduceJobStatus.Store(prevJob, true)
		}
		for {
			select { 
			case reduceJob, ok := <-m.reduceJobs:
				fmt.Println("stuck here")
				if ok {
					reply.ReduceJob = reduceJob
					fmt.Println("issued job", reduceJob)
					// if after timeout, resend work
					go func(reduceJob int, m *Master) {
						time.Sleep(timeout * time.Second)
						if _, ok := m.reduceJobStatus.Load(reduceJob); ok {
							m.reduceCompletedCount <- 1 // Just a dummy number.
							if len(m.reduceCompletedCount) == m.nReduce {
								fmt.Println("aoeuaoeua")
								close(m.reduceJobs)
							}
						} else {
							m.reduceJobs <- reduceJob
						}
					}(reduceJob, m)
					return nil
				}
				if len(m.reduceCompletedCount) == m.nReduce {
					// Signal to slaves to end Map Stage.
					fmt.Println("aqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqoeuaoeua")
					reply.ReduceStageCompleted = true
					return nil
				}
			default:
				time.Sleep(time.Second) // Sleep so that we don't waste compute power.
			}
		}

	return nil
}

//
// Map RPC handler.
//
func (m *Master) RequestMapJob(req *MRRequest, reply *MRReply) error {
	if prevJob := req.PrevCompletedJob; prevJob != -1 {
		m.mapJobStatus.Store(prevJob, true)
	}
	for {
		// Code below copied from: 
		// https://stackoverflow.com/questions/3398490/checking-if-a-channel-has-a-ready-to-read-value-using-go
		select { 
		case job, ok := <-m.mapJobs:
			if ok {
				reply.Job = job
				// if after timeout, resend work
				go func(job Job, m *Master) {
					time.Sleep(timeout * time.Second)
					if _, ok := m.mapJobStatus.Load(job.JobId); ok {
						m.mapCompletedCount <- 1
						if len(m.mapCompletedCount) == m.jobCount {
							close(m.mapJobs)
						}
					} else {
						m.mapJobs <- job
					}
				}(job, m)
			}
			if len(m.mapCompletedCount) == m.jobCount {
				// Signal to slaves to end Map Stage.
				reply.MapStageCompleted = true
			}
			return nil
		default:
			time.Sleep(time.Second) // Sleep so that we don't waste compute power.
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

		mapJobs: make(chan Job, len(files)),
		mapJobStatus: sync.Map{},
		jobCount: len(files),
		mapCompletedCount: make(chan int, len(files)),

		reduceJobs: make(chan int, nReduce),
		reduceJobStatus: sync.Map{},
		reduceCompletedCount: make(chan int, nReduce),
	}
	
	// Set up work for map stage.
	for id, file := range files {
		m.mapJobs <- Job{Filename: file, JobId: id}
	}

	// Let up work for reduce stage
	for i := 0; i < nReduce; i++ {
		m.reduceJobs <- i
	}

	m.server()

	return &m
}
