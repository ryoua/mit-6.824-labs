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


type Master struct {
	NMap int
	NReduce int

	MapTasks []*MapTask
	ReduceTasks []*ReduceTask

	Phase int

	MapTaskChan chan *MapTask
	ReduceTaskChan chan *ReduceTask

	IntermediateFile [][]string

	NCompleteMap int
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

func (m *Master) initMapTasks(files []string) {
	for i, file := range files {
		mapTask := MapTask{
			Id:       i,
			FileName: file,
			Status:   Ready,
			NReduce:  m.NReduce,
		}
		m.MapTasks = append(m.MapTasks, &mapTask)
		m.MapTaskChan <- &mapTask
	}
}

// 获取一个 Task
func (m *Master) GetTask(args *PullTaskArgs, reply *PullTaskReply) error {
	select {
	case mapTask := <- m.MapTaskChan:
		if Debug {
			log.Printf("get a map task: %+v", mapTask)
		}
		mapTask.Status = Exec
		reply.Task = *mapTask
		go m.MonitorMapTask(mapTask)
	case reduceTask := <- m.ReduceTaskChan:
		if Debug {
			log.Printf("get a reduce task: %+v", reduceTask)
		}
		reduceTask.Status = Exec
		reply.Task = *reduceTask
	}
	return nil
}

// 监控 MapTask, 如果 Work 挂掉,就把 MapTask 重新放入 Channel
func (m *Master) MonitorMapTask(mapTask *MapTask)  {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <- t.C:
			m.mu.Lock()
			mapTask.Status = Ready
			m.MapTaskChan <- mapTask
			m.mu.Unlock()
		default:
			if mapTask.Status == Finish {
				return
			}
		}
	}
}

func (m *Master) ReportFinishTask()  {
	
}