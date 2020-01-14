package mr

//
// RPC definitions.
//

type SetupWorkerReq struct {
	
}

type SetupWorkerReply struct {
	NReduce int
}


type MRRequest struct {
	PrevCompletedJob int
}

// master assign filename and jobId to slave.
type MRReply struct {
	Job Job
	MapStageCompleted bool
}

type RReply struct {
	ReduceJob int
	ReduceStageCompleted bool
}

