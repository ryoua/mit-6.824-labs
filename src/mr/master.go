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

type TaskStat struct {
	Status int
	WorkerId int
	StartTime time.Time
}

type Master struct {
	files []string
	nReduce int
	taskPhase TaskPhase
	taskStats []TaskStat
	mu sync.Mutex
	done bool
	workerSeq int
	taskChan chan Task
}

const (
	MaxTaskRunTime = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

const (
	Ready = 0
	Queue = 1
	Running = 2
	Finish = 3
	Err = 4
)

// 返回 Master 是否完成
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.done
	return r
}

// 创建一个 Master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskChan = make(chan Task, nReduce)
	} else {
		m.taskChan = make(chan Task, len(m.files))
	}

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	DPrintf("master init")
	return &m
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTask() {
	DPrintf("init reduce tasks")
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		Seq:      taskSeq,
		Phase:    m.taskPhase,
		Alive:    true,
	}
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == MapPhase {
		task.FileName = m.files[taskSeq]
	}
	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case Ready:
			allFinish = false
			m.taskChan <- m.getTask(index)
			m.taskStats[index].Status = Queue
		case Queue:
			allFinish = false
		case Running:
			allFinish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = Queue
				m.taskChan <- m.getTask(index)
			}
		case Finish:
		case Err:
			allFinish = false
			m.taskStats[index].Status = Queue
			m.taskChan <- m.getTask(index)
		default:
			panic("t.status err")
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

func (m *Master) regTask(args *TaskArgs, task *Task)  {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task.Phase != m.taskPhase {
		panic("req task phase neq")
	}
	m.taskStats[task.Seq].Status = Running
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <- m.taskChan
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	DPrintf("get report task: %+v, taskPhase: %+v", args, m.taskPhase)
	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}
	if args.Done {
		m.taskStats[args.Seq].Status = Finish
	} else {
		m.taskStats[args.Seq].Status = Err
	}
	go m.schedule()
	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq++
	reply.WorkerId = m.workerSeq
	return nil
}