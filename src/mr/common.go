package mr

const (
	MapPhase = TaskPhase(1)
	ReducePhase = TaskPhase(2)
)

type TaskPhase int

type Task struct {
	filename string
	NReduce int
	NMaps int
	Seq int
	Phase TaskPhase
	Alive bool
}