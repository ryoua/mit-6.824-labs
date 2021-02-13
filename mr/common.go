package mr

import (
	"fmt"
	"net"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

const Debug = false

type MapTask struct {
	Id       int
	FileName string
	NReduce  int
	Status   int
}

type ReduceTask struct {
	Id        int
	FileNames []string
	Status    int
}

func CheckServer(worker *Worker) {
	timeout := time.Duration(5 * time.Second)
	t1 := time.Now()
	_, err := net.DialTimeout("tcp", worker.address, timeout)
	fmt.Println("waist time :", time.Now().Sub(t1))
	if err != nil {
		fmt.Println("Site unreachable, error: ", err)
		worker.Alive = false
		return
	}
	worker.Alive = true
	fmt.Println("tcp server is ok")
}
