package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	log.Println("Worker start to process task")

	for {
		task := requestTask()

		switch task.TaskType {
		case MapTask:
			processMapTask(task, mapf)
		case ReduceTask:
			processReduceTask(task, reducef)
		case WaitTask:
			log.Println("No task ready, waiting...")
			time.Sleep(2 * time.Second)
		case ExitTask:
			log.Println("all task completed, worker exit")
			return
		default:
			log.Fatalf("none type for task: %s", task.TaskType)
		}
	}
}

func requestTask() *RequestTaskReply {
	for {
		args := &RequestTaskArgs{}
		reply := &RequestTaskReply{}
		if call("Coordinator.RequestTask", args, reply) {
			return reply
		}
		log.Printf("request task failed, retry after 5 secs")
		time.Sleep(5 * time.Second)
	}
}

func processMapTask(task *RequestTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("start process task %d, file: %s", task.TaskId, task.FileName)

	// read input file
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("failed to open file %s: %v", task.FileName, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %s: %v", task.FileName, err)
	}

	// invoke Map function to process content
	kva := mapf(task.FileName, string(content))

	// create intermediate files by NReduce
	intermediateFiles := make([]string, task.NReduce)

	for i := 0; i < task.NReduce; i++ {
		tempName := fmt.Sprintf("mr-%d-%d.tmp", task.TaskId, i)
		intermediateFiles[i] = fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		tempFile, err := os.Create(tempName)
		if err != nil {
			log.Fatalf("cannot create temp file %s: %v", tempName, err)
		}
		defer tempFile.Close()

		encoder := json.NewEncoder(tempFile)
		for _, kv := range kva {
			reduceTask := ihash(kv.Key) % task.NReduce
			if reduceTask == i {
				if err := encoder.Encode(&kv); err != nil {
					log.Fatalf("failed to write into intermediate file: %v", err)
				}
			}
		}

		if err := os.Rename(tempName, intermediateFiles[i]); err != nil {
			log.Fatalf("failed to rename temp file %s to %s: %v", tempName, intermediateFiles[i], err)
		}
	}

	// report task finished
	if !reportTask(MapTask, task.TaskId) {
		log.Fatalf("report task finished failed")
	}

	log.Printf("Map task %d finished", task.TaskId)
}

func reportTask(taskType string, taskId int) bool {
	args := &ReportTaskArgs{
		TaskType: taskType,
		TaskId:   taskId,
	}
	reply := &ReportTaskReply{}

	return call("Coordinator.ReportTask", args, reply)
}

func processReduceTask(task *RequestTaskReply, reducef func(string, []string) string) {
	log.Printf("start reduce task %d", task.TaskId)

	// gather all intermediate files
	intermediate := []KeyValue{}
	for mapTaskID := 0; mapTaskID < task.NMap; mapTaskID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskID, task.TaskId)

		if _, err := os.Stat(filename); os.IsNotExist(err) {
			log.Printf("warning: intermediate file %s not found", filename)
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("err: cannot open intermediate file %s: %v", filename, err)
			continue
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	outputTempName := fmt.Sprintf("mr-out-%d.tmp", task.TaskId)
	outputFinalName := fmt.Sprintf("mr-out-%d", task.TaskId)
	tempFile, err := os.Create(outputTempName)
	if err != nil {
		log.Fatalf("cannot create temp output file %s: %v", outputTempName, err)
	}
	defer tempFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%s %s\n", intermediate[i].Key, output)

		i = j
	}

	if err := os.Rename(outputTempName, outputFinalName); err != nil {
		log.Fatalf("failed to rename output file %s to %s: %v", outputTempName, outputFinalName, err)
	}

	if !reportTask(ReduceTask, task.TaskId) {
		log.Fatalf("report Reduce task failed")
	}

	log.Printf("Reduce task %d finished", task.TaskId)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
