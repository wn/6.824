package mr

//
// RPC definitions.
//

type MRRequest struct {
	PrevCompletedJob int
}

// master assign filename and jobId to slave.
type MRReply struct {
	Job Job
	MapStageCompleted bool
	NReduce int
}

// Add your RPC definitions here.

