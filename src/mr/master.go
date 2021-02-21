package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


const (
	Ready = 0
	Queue = 1
	Running = 2
	Finish = 3
	Error = 4
)

const (
	TaskMaxRunTime = 5 * time.Second
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status int
	WorkerId int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	filenames []string
	nReduce int
	taskPhase TaskPhase
	taskStat []TaskStat
	taskChan chan Task

	done bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// 开启协程监听worker的rpc请求
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

// 返回主任务是否完成
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	res := m.done
	m.mu.Unlock()
	return res
}

// 创建一个master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.filenames = files
	m.nReduce = nReduce

	if nReduce > len(files) {
		m.taskChan = make(chan Task, nReduce)
	} else {
		m.taskChan = make(chan Task, len(m.filenames))
	}
	m.initMapTask()
	go m.tickSchedule()
	m.server()
	fmt.Println("master init")
	return &m
}

// 定时调度, 每隔 ScheduleInterval 秒进行一次调度
func (m *Master) tickSchedule()  {
	for !m.done {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// 初始化 Map 任务
func (m *Master) initMapTask()  {
	m.taskPhase = MapPhase
	m.taskStat = make([]TaskStat, len(m.filenames))
}

// 初始化 Reduce 任务
func (m *Master) initReduceTask() {
	fmt.Println("init  ReduceTask")
	m.taskPhase = ReducePhase
	m.taskStat = make([]TaskStat, m.nReduce)
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	allFinish := false
	for index, t := range m.taskStat {
		switch t.Status {
		case Ready:
			m.taskChan <- m.getTask(index)
			m.taskStat[index].Status = Queue
		case Queue:
		case Running:
			if time.Now().Sub(t.StartTime) > TaskMaxRunTime {
				m.taskStat[index].Status = Queue
				m.taskChan <- m.getTask(index)
			}
		case Finish:
			allFinish = true
			m.taskStat[index].Status = Error
			m.taskChan <- m.getTask(index)
		case Error:
		default:
			
		}
	}
	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		filename: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.filenames),
		Seq:    taskSeq,
		Phase:    0,
		Alive:    false,
	}
	if task.Phase == MapPhase {
		task.filename = m.filenames[taskSeq]
	}
	return task
}