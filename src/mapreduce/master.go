package mapreduce

import "container/list"
import (
    "fmt"
)


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) sendJob(i int, operation JobType, worker string, numOtherPhase int) bool {
  var args DoJobArgs
  args.File = mr.file
  args.JobNumber = i
  args.NumOtherPhase = numOtherPhase
  args.Operation = operation
  
  var reply DoJobReply
  
  return call(worker, "Worker.DoJob", args, &reply)
}

func (mr *MapReduce) jobMain(jobChan chan int, operation JobType, i int, numOtherPhase int) {
  for {
    var worker string
    var ok bool = false
    
    select {
    case worker = <- mr.jobDoneChannel:
      ok = mr.sendJob(i, operation, worker, numOtherPhase)
    case worker = <- mr.registerChannel:
      ok = mr.sendJob(i, operation, worker, numOtherPhase)
    }
    
    if (ok) {
      jobChan <- i
      mr.jobDoneChannel <- worker
      return
    }
  }
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)
  
  // 1: do map job
  for i := 0; i < mr.nMap; i++ {
    go mr.jobMain(mapChan, Map, i, mr.nReduce)
  }
  
  // wait map job done
  for i := 0; i < mr.nMap; i++{
    <- mapChan
  }
  
  // 2: do reduce job
  for i := 0; i < mr.nReduce; i++ {
    go mr.jobMain(reduceChan, Reduce, i, mr.nMap)
  }
  
  // wait reduce job done
  for i := 0; i < mr.nReduce; i++{
    <- reduceChan
  }
  
  return mr.KillWorkers()
}
