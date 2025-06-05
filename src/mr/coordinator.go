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

const (
	TaskReady = iota
	TaskInProgress
	TaskFinished
)

const (
	MapTask    = "Map"
	ReduceTask = "Reduce"
	WaitTask   = "Wait"
	ExitTask   = "Exit"
)

type Task struct {
	Id       int
	Type     string // "Map" or "Reduce"
	FileName string // used for map task
	State    int
	// WorkerId  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []*Task
	reduceTasks []*Task
	doneChan    chan bool
}

// Your code here -- RPC handlers for the worker to call.
// rpc mothod for worker to request task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. assign map task first
	for i, task := range c.mapTasks {
		if task.State == TaskReady {
			task.State = TaskInProgress
			task.StartTime = time.Now()
			// task.WorkerId = args.WorkerId
			reply.TaskType = MapTask
			reply.TaskId = i
			reply.FileName = task.FileName
			reply.NReduce = c.nReduce
			return nil
		}
	}

	// if all map tasks finish, assgin reduce task
	if c.allMapTasksFinished() {
		for i, task := range c.reduceTasks {
			if task.State == TaskReady {
				task.State = TaskInProgress
				task.StartTime = time.Now()
				// task.WorkerId = args.WorkerId
				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}

		// 3. if all task finish, let worker exit
		if c.allReduceTasksFinished() {
			reply.TaskType = ExitTask
			return nil
		}
	}

	// 4. else, let worker wait
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) allMapTasksFinished() bool {
	for _, task := range c.mapTasks {
		if task.State != TaskFinished {
			return false
		}
	}

	return true
}

func (c *Coordinator) allReduceTasksFinished() bool {
	for _, task := range c.reduceTasks {
		if task.State != TaskFinished {
			return false
		}
	}

	return true
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask && args.TaskId < len(c.mapTasks) {
		c.mapTasks[args.TaskId].State = TaskFinished
		log.Printf("Map Task %d Finished", args.TaskId)
	} else if args.TaskType == ReduceTask && args.TaskId < len(c.reduceTasks) {
		c.reduceTasks[args.TaskId].State = TaskFinished
		log.Printf("Reduce Task %d Finished", args.TaskId)
	}

	return nil
}

func (c *Coordinator) monitorTasks() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for range ticker.C {
			c.mu.Lock()

			// check timeouted map task
			for _, task := range c.mapTasks {
				if task.State == TaskInProgress && time.Since(task.StartTime) > 10*time.Second {
					task.State = TaskReady
					log.Printf("Map task %d Time out, reassign.", task.Id)
				}
			}

			for _, task := range c.reduceTasks {
				if task.State == TaskInProgress && time.Since(task.StartTime) > 10*time.Second {
					task.State = TaskReady
					log.Printf("Reduce task %d Time out, reassign.", task.Id)
				}
			}

			c.mu.Unlock()
		}
	}()
}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	return c.allMapTasksFinished() && c.allReduceTasksFinished()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create new coordinator
	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		doneChan: make(chan bool),
	}

	// initialize Map Task
	c.mapTasks = make([]*Task, len(files))
	for i, file := range files {
		c.mapTasks[i] = &Task{
			Id:       i,
			Type:     MapTask,
			FileName: file,
			State:    TaskReady,
		}
	}

	// initialize Reduce Task
	c.reduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			Id:    i,
			Type:  ReduceTask,
			State: TaskReady,
		}
	}

	// Your code here.
	c.server()
	c.monitorTasks()
	return &c
}
