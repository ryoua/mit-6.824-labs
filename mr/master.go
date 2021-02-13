package mr

import (
	"log"
	"sync"
	"time"
)

const (
	StatusIdel = 0
	StatusProgess = 1
	StatusComplete = 2
)

type Master struct {
	NReduce int

	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask

	MapTaskChan    chan *MapTask
	ReduceTaskChan chan *ReduceTask

	done bool
	mu sync.Mutex
}

func initMapTask() {

}

func initReduceTask() {

}

func (master *Master) AssignTask(args *PullTaskArgs, reply *PullTaskReply) error {
	select {
	case mapTask := <-master.MapTaskChan:
		if Debug {
			log.Fatalf("Assign a map task: %v\n", *mapTask)
		}
		mapTask.Status = StatusProgess
		reply.Task = *mapTask
		go master.MonitorMapTask(mapTask)
	}
	return nil
}

func (master *Master) MonitorMapTask(mapTask *MapTask)  {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <- t.C:
			master.mu.Lock()
			mapTask.Status = StatusIdel
			master.MapTaskChan <- mapTask
			master.mu.Unlock()
		default:
			if mapTask.Status == StatusComplete {
				return
			}
		}
	}
}

func (master *Master) GenerateMapTask(files []string) {
	for i, file := range files {
		mapTask := MapTask{
			Id:       i,
			FileName: file,
			NReduce:  master.NReduce,
			Status:   StatusIdel,
		}
		master.MapTasks = append(master.MapTasks, &mapTask)
		master.MapTaskChan <- &mapTask
	}
}

func MakeMaster(files []string, nReduce int) *Master {
	master := Master{}
	return &master
}

func (master *Master) Done() bool {
	return master.done
}

func (master *Master) CheckAlive(workers []*Worker) {
	for {
		for _, worker := range workers {
			go CheckServer(worker)
		}
		time.Sleep(10 * time.Second)
	}
}
