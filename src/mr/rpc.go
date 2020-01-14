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

type RReply struct {
	ReduceJob int
	ReduceStageCompleted bool
}

