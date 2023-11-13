package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	UnAssigned TaskStatus = iota
	MapAssigned
	MapDone
	MapFailed
	ReduceAssigned
	ReduceDone
	ReduceFailed
	Wait
	Done
)

type Task struct {
	Index      int
	FileName   string
	AssignedAt time.Time
	Status     TaskStatus
}
type Coordinator struct {
	// Your definitions here.
	NReduce     int
	MapTask     []Task
	ReduceTask  []TaskStatus
	Mapassigned int
	MapDone     int
	ReduceDone  int
	mutex       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for index, task := range c.MapTask {
		if task.Status == UnAssigned {
			task.AssignedAt = time.Now()
			task.Status = MapAssigned
			c.MapTask[index] = task
			reply.Index = task.Index
			reply.FileName = task.FileName
			reply.NReduce = c.NReduce
			reply.Status = MapAssigned
			c.Mapassigned++
			break
		}
		if task.Status == MapFailed || (task.Status == MapAssigned && time.Since(task.AssignedAt) >= 10*time.Second) {
			task.Status = UnAssigned
			c.MapTask[index] = task
			break
		}
		if c.Mapassigned == len(c.MapTask) && len(c.MapTask) != c.MapDone {
			reply.Status = Wait
			break
		}

	}

	//Start Reduce
	if len(c.MapTask) == c.MapDone {
		for index, task := range c.ReduceTask {
			if task != ReduceAssigned && task != ReduceDone {
				task = ReduceAssigned
				reply.Index = index
				reply.Status = ReduceAssigned
				reply.NReduce = c.NReduce
				reply.TaskLength = len(c.MapTask)
				break
			}
			if c.NReduce == c.ReduceDone {
				reply.Status = Done
				break
			}
		}
	}

	return nil

}
func (c *Coordinator) TaskDone(taskdone *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i := range c.MapTask {
		if c.MapTask[i].FileName == taskdone.FileName {
			if taskdone.Status == MapDone {
				c.MapTask[i].Status = taskdone.Status
				c.MapDone++
				break
			}
		}
	}
	if taskdone.Status == ReduceDone {
		c.ReduceTask[taskdone.Index] = ReduceDone
	}
	return nil
}
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
	ret := false

	// Your code here.
	// if c.ReduceDone == c.NReduce {
	// 	ret = true
	// }
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	filemap := make([]Task, 0)

	for index, file := range files {
		a := Task{
			Index:    index,
			FileName: file,
			Status:   UnAssigned,
		}
		filemap = append(filemap, a)
	}
	c.MapTask = filemap
	c.NReduce = nReduce
	c.ReduceTask = make([]TaskStatus, nReduce)
	c.server()

	return &c
}
