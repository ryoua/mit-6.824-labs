package mr



const Debug = false

const (
	MapPhase int = 1
	ReducePhase int = 2
)

const (
	Ready = 1
	Exec = 2
	Finish = 3
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
