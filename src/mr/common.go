package mr
type taskStatus int

// task type
const(
	Wait int=iota
	Map
	Reduce
	Exit
)

//task status
const(
	Notstart taskStatus =iota
	Pending
	Done
)

//master status
const(
	sNotStart int =iota
	sMap
	sReduce
	sDone

)


type Task struct {
	Tp       int
	Filename string
	NMap     int
	NReduce  int
	TaskID   int
}

// task in master's view
type sTask struct {
	t      Task
	status taskStatus
}

func (t *sTask)restart() {
	t.status=Notstart
}

type ScheArgs struct {
	Dummy bool
}

type ScheReply Task
func newScheReply(t Task) ScheReply {
	return (ScheReply)(t)
}

type ReportDoneArgs Task

type ReportDoneReply struct {
	Dummy bool
}

