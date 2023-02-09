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

type Coordinator struct {
	// Your definitions here.
	NReduce    int
	MapTask    map[int]task
	ReduceTask map[int]task
	MapDone    int      `default:0`
	ReduceDone int      `default:0`
	Complete   bool     `default:false`
	OutputFile []string `default:nil`
	mu         sync.Mutex
}

type task struct {
	files     []string `default:nil`
	workerId  []int    `default:nil`
	done      bool     `default:false`
	timeStamp time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	done := c.Complete
	c.mu.Unlock()
	// Your code here.
	if done {
		// c.delIntermediateFiles()
		ret = true
	}

	return ret
}

func (c *Coordinator) Intialise(files []string, nReduce int) {
	c.NReduce = nReduce
	c.MapTask = make(map[int]task)
	c.ReduceTask = make(map[int]task)
	for i := 0; i < len(files); i++ {
		var f []string
		f = append(f, files[i])
		c.MapTask[i] = task{files: f, timeStamp: time.Now().Add(time.Duration(-10) * time.Second)}
	}
}

func (c *Coordinator) TaskReport(sendReport *SendTaskReport, replyReport *ReplyTaskReport) error {
	if sendReport.Status {
		if sendReport.Type == "Map" {
			c.mapReport(sendReport.TaskId, sendReport.Files)
		} else if sendReport.Type == "Reduce" {
			c.reduceReport(sendReport.TaskId, sendReport.Files)
		}
		replyReport.Acknowledgement = true
	}
	return nil
}

func (c *Coordinator) mapReport(taskId int, files []string) {
	c.mu.Lock()
	mtask := c.MapTask[taskId]
	if !mtask.done {
		mtask.done = true
		c.MapDone++
		for i := range files {
			tfile := c.ReduceTask[i].files
			tfile = append(tfile, files[i])
			c.ReduceTask[i] = task{files: tfile}
		}
		c.MapTask[taskId] = mtask
	}
	c.mu.Unlock()

}

func (c *Coordinator) delIntermediateFiles() {
	for _, task := range c.ReduceTask {
		for i := 0; i < len(task.files); i++ {
			deletefile(task.files[i])
		}
	}
}
func deletefile(file string) {
	err := os.Remove(file)
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Coordinator) reduceReport(taskId int, files []string) {
	c.mu.Lock()
	rtask := c.ReduceTask[taskId]
	if !rtask.done {
		rtask.done = true
		c.ReduceDone++
		c.OutputFile = append(c.OutputFile, files...)
		c.ReduceTask[taskId] = rtask
		if c.ReduceDone == len(c.ReduceTask) {
			c.Complete = true
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) GetTask(getTask *GetTask, replyTask *ReplyTask) error {
	replyTask.NReduce = c.NReduce
	c.mu.Lock()
	if c.MapDone < len(c.MapTask) {
		replyTask.TaskId = getPendingTask(c.MapTask)
		if replyTask.TaskId != -1 {
			replyTask.Type = "Map"
			mtask := c.MapTask[replyTask.TaskId]
			replyTask.File = mtask.files
			mtask.workerId = append(mtask.workerId, getTask.WorkerId)
			mtask.timeStamp = time.Now()
			c.MapTask[replyTask.TaskId] = mtask
		}
	} else if c.ReduceDone < len(c.ReduceTask) {
		replyTask.TaskId = getPendingTask(c.ReduceTask)
		if replyTask.TaskId != -1 {
			replyTask.Type = "Reduce"
			rtask := c.ReduceTask[replyTask.TaskId]
			replyTask.File = rtask.files
			rtask.workerId = append(rtask.workerId, getTask.WorkerId)
			rtask.timeStamp = time.Now()
			c.ReduceTask[replyTask.TaskId] = rtask
		}
	}
	c.mu.Unlock()
	return nil
}

func getPendingTask(tasks map[int]task) int {
	for taskId, task := range tasks {
		if !task.done && (task.workerId == nil || task.timeStamp.Add(time.Duration(10)*time.Second).Before(time.Now())) {
			return taskId
		}
	}
	return -1
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.Intialise(files, nReduce)
	c.server()
	return &c
}
