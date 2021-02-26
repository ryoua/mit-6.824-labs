package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = false

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format + "\n", v...)
	}
}

type Task struct {
	FileName string
	NReduce int
	NMap int
	Seq int
	Phase TaskPhase
	Alive bool
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}