package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	arg struct {
		files   []string
		nReduce int
	}
	tasks    []sTask
	mu       sync.Mutex
	phase    int
	finished bool
}

func (m *Master) getTaskByID(t Task) *sTask {
	// map and reduce share ID ,so we have to check here
	if t.Tp != m.phase {
		return nil
	}
	return &(m.tasks[t.TaskID])
}

func (m *Master) watchdog(t Task) {
	time.Sleep(5 * time.Second)
	tt := m.getTaskByID(t)
	if tt == nil || tt.status != Pending {
		return
	}
	tt.restart()
}

//
func (m *Master) changePhaseTo(to int) {
	nMap := len(m.arg.files)
	nReduce := m.arg.nReduce
	switch to {
	case sMap:
		{
			tasks := make([]sTask, 0, nMap)
			for index, fname := range m.arg.files {
				tasks = append(tasks, sTask{Task{sMap, fname, nMap, nReduce,
					index}, Notstart})
			}
			m.tasks = tasks
			m.phase = sMap
		}
	case sReduce:
		{
			nReduce := m.arg.nReduce
			tasks := make([]sTask, 0, nReduce)
			for i := 0; i < nReduce; i++ {
				tasks = append(tasks, sTask{Task{sReduce, "", nMap, nReduce,
					i}, Notstart})
			}
			m.tasks = tasks
			m.phase = sReduce
		}
	case sDone:
		{
			m.finished = true

		}
	default:
		panic("insane phase change detected")
	}
}

func (m *Master) Sche(args *ScheArgs, r *ScheReply) error {
	*r = ScheReply{Wait, "", 0, 0, -1}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, task := range m.tasks {
		if task.status != Notstart {
			continue
		} else {
			*r = newScheReply(task.t)
			log.Println("new task:", task.t.TaskID, "filename is:", task.t.Filename)
			m.tasks[i].status = Pending
			go m.watchdog(task.t)
			return nil
		}
	}
	return nil

}

func (m *Master) ReportDone(args *ReportDoneArgs, r *ReportDoneReply) error {
	*r = ReportDoneReply{}
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Tp != m.phase {
		return nil
	}
	var t *Task = (*Task)(args)
	m.getTaskByID(*t).status = Done

	finish := true
	for _, t := range m.tasks {
		if t.status != Done {
			finish = false
		}
	}

	if finish && m.phase != sDone {
		m.changePhaseTo(m.phase + 1)
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.arg.files = files
	m.arg.nReduce = nReduce
	m.mu = sync.Mutex{}

	// Your code here.
	m.changePhaseTo(sMap)
	log.Println("task number is:", len(m.tasks))
	for _, t := range m.tasks {
		log.Println(t.t.Filename)
	}
	m.server()
	return &m
}
