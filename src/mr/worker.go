package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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

func DoMap(mapf func(string, string) []KeyValue,filename string,index,nReduce int){
	intermediate := make([][]KeyValue,nReduce,nReduce)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _,kv :=range kva{
		nr:=ihash(kv.Key)%nReduce
		intermediate[nr]=append(intermediate[nr],kv)
	}
	for i,_:=range intermediate{
		sort.Sort(ByKey(intermediate[i]))
		oName:=fmt.Sprintf("mr-mid-%d-%d",index,i)
		f,_:=os.Create(oName)
		for _,kv:=range intermediate[i]{
			fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
		}
	}
}

func DoReduce(reducef func(string, []string) string,index int){
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	var targets []string
	for _,name:=range names {
		var nMap,nReduce int
		n, _ := fmt.Sscanf(name, "mr-mid-%d-%d", &nMap,&nReduce)
		if n!=2{
			continue
		}
		if nReduce==index{
			targets =append(targets,name)
		}
	}

	intermediate:=make(map[string][]string,1000)
	for _, target :=range targets {
		f,err:=os.Open(target)
		if err!=nil{
			panic(err.Error())
		}
		scanner:=bufio.NewScanner(bufio.NewReader(f))
		scanner.Split(bufio.ScanLines)
		for scanner.Scan(){
			kv:=KeyValue{}
			n,err:=fmt.Sscanf(scanner.Text(),"%s %s\n",&kv.Key,&kv.Value)
			if err!=nil||n!=2{
				panic(fmt.Sprintf("format wrong:%s\nerr is %s\n n is %d",scanner.Text(),err.Error(),n))
			}
			intermediate[kv.Key]=append(intermediate[kv.Key],kv.Value)
		}
		f.Close()
	}

	oName:=fmt.Sprintf("mr-out-%d",index)
	ofile,_:=os.Create(oName)
	for k,v:=range intermediate{
		vv:=reducef(k,v)
		fmt.Fprintf(ofile,"%v %v\n",k,vv)
	}
	ofile.Close()
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for{

		// step 1, ask for a job
		var task ScheReply
		ok:=call("Master.Sche",&ScheArgs{},&task)
		if !ok{
			log.Print("rpc error...")
			break
		}
		log.Print(fmt.Sprintf("task tp:%d,fn:%s,index:%d\n",task.Tp,task.Filename,task.TaskID))
		// step 2, run job (might fail)
		switch task.Tp {
		case Wait:
			time.Sleep(3*time.Second)
		case Exit:
			break
		case sMap:
			DoMap(mapf,task.Filename,task.TaskID,task.NReduce)
		case sReduce:
			DoReduce(reducef,task.TaskID)
		default:
			panic(fmt.Sprintf("unknown task:%d",task.Tp))
		}

		// step 3, check if down,continue if so
		// step 4, submit task

		call("Master.ReportDone",&task,&ReportDoneReply{})
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
