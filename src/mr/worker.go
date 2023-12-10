package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"time"
)

type WorkerStatus int

const (
	Idle WorkerStatus = iota
	InProgress
	Completed
)

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	ShuffleTaskType
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Worker is the interface for the worker
type myworker struct {
	assignedId string
	status     WorkerStatus
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string

	pendingTasks SafeQueue
	finishedTask SafeQueue
}

// Task is the unit of work for the worker
type Task struct {
	Id     string
	Input  string
	Output string
	Type   TaskType
}

// Register worker on the corrdinator side and get the assigned id
func (w *myworker) register() string {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)

	if !ok {
		slog.Error("Register error")
		os.Exit(1)
	}
	if reply.Code != 0 {
		slog.Error("Register error: ", reply.Message)
		os.Exit(1)
	}

	return reply.AssgnedId
}

// Report the status of the worker and task to the coordinator
func (w *myworker) AskForTask() {
	args := AskForTaskArgs{}
	reply := AskForTaskReply{}

	ok := call("Coordinator.Report", &args, &reply)
	if !ok {
		slog.Error("Report error")
		return
	}

	if reply.Code != 0 {
		slog.Error("Report error: ", reply.Message)
		return
	}

	w.pendingTasks.Enqueue(reply.Task)
}

func (w *myworker) DoTask() {
	for {
		task := w.pendingTasks.Dequeue()
		if task == nil {
			time.Sleep(10 * time.Second)
			continue
		}

		switch task.Type {
		case MapTaskType:

			content, err := ReadFile(task.Input)
			if err != nil {
				slog.Error("Read file error: ", err)
				time.Sleep(10 * time.Second)
				continue
			}
			w.mapFunc(task.Input, content)

		case ReduceTaskType:
			// w.reduceFunc(task.Input, content)
		case ShuffleTaskType:
			// w.doShuffleTask(task)
		}
	}
}

// Start the worker and keep reporting the status to the coordinator
func (w *myworker) Start() {

	w.assignedId = w.register()

	timer := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-timer.C:
				w.AskForTask()
			}
		}
	}()

	// Block forever
	select {}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	worker := myworker{
		status:     Idle,
		mapFunc:    mapf,
		reduceFunc: reducef,
	}
	worker.Start()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
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
