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

type TaskState int
type TaskType int

const (
	NotStarted TaskState = iota
	Executing
	Finished
)

const (
	MapType TaskType = iota
	ReduceType
	WaitType
	ExitType
)

type mapTask struct {
	filename  string
	startTime time.Time
	state     TaskState
}

type reduceTask struct {
	id        int
	startTime time.Time
	state     TaskState
}

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	mapTasks       []mapTask
	nMapNotDone    int
	reduceTasks    []reduceTask
	nReduceNotDone int
	workerID       uint32
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	DPrintf("nMapNotDone: %d, ReduceNotDone: %d\n", c.nMapNotDone, c.nReduceNotDone)
	return c.nMapNotDone == 0 && c.nReduceNotDone == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMapNotDone = len(files)
	c.nReduceNotDone = nReduce
	c.mapTasks = make([]mapTask, len(files))
	c.reduceTasks = make([]reduceTask, nReduce)
	for i := 0; i < len(files); i++ {
		c.mapTasks[i].filename = files[i]
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i].id = i
	}

	c.server()
	return &c
}

func (c *Coordinator) SendTask(args *SendTaskArgs, reply *SendTaskReplys) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nMapNotDone > 0 {
		n := len(c.mapTasks)
		for i := 0; i < n; i++ {
			if c.mapTasks[i].state == NotStarted || (c.mapTasks[i].state == Executing && time.Now().Sub(c.mapTasks[i].startTime) > 10*time.Second) {
				DPrintf("Send map_task[%d] to worker %d\n", i, c.workerID)
				c.mapTasks[i].state = Executing
				c.mapTasks[i].startTime = time.Now()
				reply.NReduce = len(c.reduceTasks)
				reply.Filename = c.mapTasks[i].filename
				reply.TaskType = MapType
				reply.TaskID = i
				reply.WorkerID = c.workerID
				c.workerID++
				return nil
			}
		}
		reply.TaskType = WaitType
	} else if c.nReduceNotDone > 0 {
		n := len(c.reduceTasks)
		for i := 0; i < n; i++ {
			if c.reduceTasks[i].state == NotStarted || (c.reduceTasks[i].state == Executing && time.Now().Sub(c.reduceTasks[i].startTime) > 10*time.Second) {
				DPrintf("Send reduce_task[%d] to worker %d\n", i, c.workerID)
				c.reduceTasks[i].state = Executing
				c.reduceTasks[i].startTime = time.Now()
				reply.NMap = len(c.mapTasks)
				reply.TaskType = ReduceType
				reply.TaskID = i
				reply.WorkerID = c.workerID
				c.workerID++
				return nil
			}
		}
		reply.TaskType = WaitType
	} else {
		reply.TaskType = ExitType
	}

	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReplys) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapType {
		if c.mapTasks[args.TaskID].state != Finished {
			c.nMapNotDone--
			DPrintf("map_task[%d] done!\n", args.TaskID)
		}
		c.mapTasks[args.TaskID].state = Finished
	} else if args.TaskType == ReduceType {
		if c.reduceTasks[args.TaskID].state != Finished {
			c.nReduceNotDone--
			DPrintf("reduce_task[%d] done!\n", args.TaskID)
			for i := 0; i < len(c.mapTasks); i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, args.TaskID)
				os.Remove(filename)
			}
		}
		c.reduceTasks[args.TaskID].state = Finished
	}

	return nil
}
