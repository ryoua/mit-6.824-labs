package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


var workerSeq = 0

type Master struct {
	NMap    int
	NReduce int

	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask

	Phase int

	MapTaskChan    chan *MapTask
	ReduceTaskChan chan *ReduceTask

	IntermediateFiles [][]string

	NCompleteMap    int
	NCompleteReduce int

	mu sync.Mutex
}

// 启动一个协程监听来自worker的rpc请求
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 返回 Master 是否完成
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ReduceTasks) == m.NCompleteReduce && m.Phase == Finish
}

// 创建一个 Master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NReduce = nReduce

	m.initMapTasks(files)
	m.server()
	return &m
}

// 初始化 Map 任务
func (m *Master) initMapTasks(files []string) {
	for i, file := range files {
		mapTask := MapTask{
			Id:       i,
			FileName: file,
			Status:   Ready,
			NReduce:  m.NReduce,
		}
		if Debug {
			log.Printf("maptask %+v have been created", mapTask)
		}
		m.MapTasks = append(m.MapTasks, &mapTask)
		m.MapTaskChan <- &mapTask
	}
}

func (m *Master) initReduceTasks() {
	for i := 0; i < m.NReduce; i++ {
		reduceTask := ReduceTask{
			Id:        i,
			FileNames: m.IntermediateFiles[i],
			Status:    Ready,
		}
		if Debug {
			log.Printf("reducetask %+v have been created", reduceTask)
		}
		m.ReduceTasks = append(m.ReduceTasks, &reduceTask)
		m.ReduceTaskChan <- &reduceTask
	}
}

// 获取一个 Task
func (m *Master) GetTask(args *PullTaskArgs, reply *PullTaskReply) error {
	select {
	case mapTask := <-m.MapTaskChan:
		if Debug {
			log.Printf("get a map task: %+v", mapTask)
		}
		mapTask.Status = Exec
		reply.Task = *mapTask
		go m.MonitorMapTask(mapTask)
	case reduceTask := <-m.ReduceTaskChan:
		if Debug {
			log.Printf("get a reduce task: %+v", reduceTask)
		}
		reduceTask.Status = Exec
		reply.Task = *reduceTask
		go m.MonitorReduceTask(reduceTask)
	}
	return nil
}

// 监控 MapTask, 如果 Work 挂掉,就把 MapTask 重新放入 Channel
func (m *Master) MonitorMapTask(mapTask *MapTask) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.mu.Lock()
			mapTask.Status = Ready
			m.MapTaskChan <- mapTask
			m.mu.Unlock()
		default:
			if mapTask.Status == Finish {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// 监控 ReduceTask, 如果 Work 挂掉,就把 ReduceTask 重新放入 Channel
func (m *Master) MonitorReduceTask(reduceTask *ReduceTask) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.mu.Lock()
			reduceTask.Status = Ready
			m.ReduceTaskChan <- reduceTask
			m.mu.Unlock()
		default:
			if reduceTask.Status == Finish {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// worker 上报任务完成情况
func (m *Master) ReportFinishTask(args *ReportFinishTaskArgs, reply *ReportFinishTaskReply) error {
	switch args.Phase {
	case MapPhase:
		log.Printf("complete map task %+v\n", args.TaskId)
		m.mu.Lock()
		m.MapTasks[args.TaskId].Status = Finish
		m.NCompleteMap++

		for i := 0; i < m.NReduce; i++ {
			m.IntermediateFiles[i] = append(m.IntermediateFiles[i], args.IntermediateFiles[i])
		}

		if m.NCompleteMap == m.NMap {
			if Debug {
				log.Printf("map tasks all finish")
			}
		}
		m.Phase = ReducePhase
		go m.initReduceTasks()
		m.mu.Unlock()
	case ReducePhase:
		log.Printf("complete reduce task %+v\n", args.TaskId)
		m.mu.Lock()
		m.ReduceTasks[args.TaskId].Status = Finish
		m.NCompleteReduce++

		if m.NCompleteReduce == m.NReduce {
			if Debug {
				log.Printf("reduce tasks all finish")
			}
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	workerSeq++
	reply.WorkerId = workerSeq
	return nil
}