package mr



type TaskPhase int

const Debug = false

const (
	Ready = 1
	Exec = 3
	Finish = 5
	Error = 6
)

type WorkerMachine struct {
	Id int
	Status int
}

type MapTask struct {
	Id int
	FileName string
	Status int
	NReduce int
	WorkId int
}

type ReduceTask struct {
	Id int
	FileNames []string
	Status int
	WorkerId int
}
