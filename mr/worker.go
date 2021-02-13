package mr

import (
	"log"
	"net/rpc"
)

type Worker struct {
	address string
	Alive bool
}

func (worker *Worker) PullTask() *PullTaskReply {
	args := PullTaskArgs{}
	reply := PullTaskReply{}
	if call("Master.AssignTask", &args, &reply) {
		return &reply
	} else {
		return nil
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing", err)
		return false
	}

	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Fatalf("%+v", err)
	return false
}