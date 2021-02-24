package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	worker.register()
	worker.run()
}

func (w *worker) register() *RegisterWorkerReply {
	args := &RegisterWorkerArgs{}
	reply := &RegisterWorkerReply{}
	if call("Master.RegisterWorker", args, reply) {
		return reply
	} else {
		return nil
	}
}

func (w *worker) run() {
	for {
		handleTask()
	}
}

func (w *worker) doMapTask(mapTask *MapTask) {

}

func (w *worker) doReduceTask(reduceTask *ReduceTask) {

}

func (w *worker) requestTask() interface{} {

}

type worker struct {
	id int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

// 从 Master 中获取一个 Task
func (w *worker) PullTask() *PullTaskReply {
	 args := &PullTaskArgs{}
	 reply := &PullTaskReply{}
	 if call("Master.GetTask", args, reply) {
	 	return reply
	 } else {
	 	return nil
	 }
}

// 完成 MapTask
func (w *worker) FinishMapTask(mapTask *MapTask) *ReportFinishTaskReply {
	args := &ReportFinishTaskArgs{
		TaskId:            mapTask.Id,
		Phase:             MapPhase,
		IntermediateFiles: nil,
	}
	reply := &ReportFinishTaskReply{}
	if call("Master.ReportFinishTask", args, reply) {
		return reply
	} else {
		return nil
	}
}

// 完成 ReduceTask
func (w *worker) FinishReduceTask(reduceTask *ReduceTask) *ReportFinishTaskReply {
	args := &ReportFinishTaskArgs{
		TaskId:            reduceTask.Id,
		Phase:             ReducePhase,
		IntermediateFiles: nil,
	}
	reply := &ReportFinishTaskReply{}
	if call("Master.ReportFinishTask", args, reply) {
		return reply
	} else {
		return nil
	}
}




//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
