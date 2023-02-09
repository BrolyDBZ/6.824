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
	"strconv"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	Id int
}

//
// main/mrworker.go calls this function.
//

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var wg sync.WaitGroup
	// Your worker implementation here.
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			getTask := GetTask{workerId}
			replyTask := ReplyTask{}
			for !replyTask.Done {
				success := call("Coordinator.GetTask", &getTask, &replyTask)
				if success {
					if replyTask.Type == "Map" {
						maptask(mapf, replyTask.File, getTask.WorkerId, replyTask.TaskId, replyTask.NReduce)

					} else if replyTask.Type == "Reduce" {
						reducetask(reducef, replyTask.File, getTask.WorkerId, replyTask.TaskId)
					} else {
						time.Sleep(10 * time.Second)
					}
				}
				replyTask = ReplyTask{}
			}

		}(i)
	}
	wg.Wait()
}

func maptask(mapf func(string, string) []KeyValue, files []string, workerId int, taskId int, nReduce int) {
	kva, err := mapHelper(mapf, files[0])
	if err != nil {
		log.Fatal(err)
	}
	intermediatefiles, err := intermediateFileCreator(kva, workerId, taskId, nReduce)
	if err != nil {
		log.Fatal(err)
	}
	reportTask("Coordinator.TaskReport", intermediatefiles, "Map", workerId, taskId)
}

func reducetask(reducef func(string, []string) string, files []string, workerId int, taskId int) {
	OutputFile, err := reduceHelper(reducef, files, taskId)
	if err != nil {
		log.Fatal(err)
	}
	var ofile []string
	ofile = append(ofile, OutputFile)
	reportTask("Coordinator.TaskReport", ofile, "Reduce", workerId, taskId)
}

func reportTask(rpcName string, files []string, Type string, workerId int, taskId int) {
	sendReport := SendTaskReport{true, files, workerId, taskId, Type}
	replyReport := ReplyTaskReport{}
	success := call(rpcName, &sendReport, &replyReport)
	if !success {
		log.Fatal("Something went wrong")
	}

}

// uncomment to send the Example RPC to the coordinator.
// CallExample()

func mapHelper(mapf func(string, string) []KeyValue, fileName string) ([]KeyValue, error) {
	f, err := os.Open(fileName)
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	if err != nil {
		log.Fatal(err)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	kva := mapf(fileName, string(content))
	return kva, nil
}

func reduceHelper(reducef func(string, []string) string, fileNames []string, taskID int) (string, error) {
	kva := ReadAllfile(fileNames)
	fname := "mr-out-" + strconv.Itoa(taskID)
	sort.Sort(ByKey(kva))
	tempfile, err := os.CreateTemp("", "out*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := tempfile.Close()
		if err != nil {
			log.Fatal(err)
		}
		err = os.Rename(tempfile.Name(), fname)
		if err != nil {
			log.Fatal(err)
		}
	}()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	return fname, nil

}

func intermediateFileCreator(kva []KeyValue, workerId int, taskId int, nReduce int) ([]string, error) {
	var intermediatefiles []string
	fileContent := make(map[string][]KeyValue)
	prefix := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(taskId) + "-"
	for i := 0; i < nReduce; i++ {
		intermediatefiles = append(intermediatefiles, prefix+strconv.Itoa(i))
	}
	for idx := range kva {
		kv := kva[idx]
		hash := ihash(kv.Key) % nReduce
		fileContent[intermediatefiles[hash]] = append(fileContent[intermediatefiles[hash]], kv)
	}
	var wg sync.WaitGroup
	for k, v := range fileContent {
		wg.Add(1)
		go func(fileName string, data []KeyValue) {
			defer wg.Done()
			createFile(fileName, data)
		}(k, v)
	}
	wg.Wait()
	return intermediatefiles, nil

}

func createFile(fileName string, data []KeyValue) {
	tempfile, err := os.CreateTemp("", "temp*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := tempfile.Close()
		if err != nil {
			log.Fatal(err)
		}
		err = os.Rename(tempfile.Name(), fileName)
		if err != nil {
			log.Fatal(err)
		}
	}()
	writefile(tempfile, data)
}

func writefile(file *os.File, data []KeyValue) {
	enc := json.NewEncoder(file)
	for _, kv := range data {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func decodefile(fileName string) []KeyValue {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	var kva []KeyValue
	dec := json.NewDecoder(f)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func ReadAllfile(files []string) []KeyValue {
	var kvas []KeyValue
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()
			kva := decodefile(fileName)
			mu.Lock()
			kvas = append(kvas, kva...)
			mu.Unlock()
		}(files[i])
	}
	wg.Wait()
	return kvas
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	return false
}
