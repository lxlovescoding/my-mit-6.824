package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

// for sorting by key.
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

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Printf("worker接受任务成功！task为%v...\n", reply)
	} else {
		fmt.Println("worker接受任务失败！")
	}
	return reply
}

func callDone(task *Task) Task {
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &task, &reply)
	if ok {
		fmt.Printf("%v worker任务完成！\n", reply)
	} else {
		fmt.Println("worker完成任务失败！")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	bookName := response.FileSlice[0]
	// 读取文件 下一步准备输出键值对
	file, err := os.Open(bookName)
	if err != nil {
		log.Fatalf("DoMapTask:cannot open %v...", bookName)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("DoMapTask:cannot read %v...", bookName)
		return
	}
	file.Close()

	// 从map函数中得到键值对数组 [(k, 1), (k, 1), ...]
	intermediate := mapf(bookName, string(content))

	// 把上面的每 个key 根据 ihash(kv.Key)%response.ReducerNum 分组
	HaskedKV := make([][]KeyValue, response.ReducerNum)
	for _, kv := range intermediate {
		HaskedKV[ihash(kv.Key)%response.ReducerNum] = append(HaskedKV[ihash(kv.Key)%response.ReducerNum], kv)
	}
	// 把分好的组分别写入以json的形式写入临时文件
	for i := 0; i < response.ReducerNum; i++ {
		oname := "mr-tmp" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		coder := json.NewEncoder(ofile)
		for _, kv := range HaskedKV[i] {
			coder.Encode(kv)
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(response.TaskId))
}

func shuffle(files []string) []KeyValue {
	var ans []KeyValue
	for _, name := range files {
		file, _ := os.Open(name)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			ans = append(ans, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(ans))
	return ans
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaittingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("Task about :[", task.TaskId, "] is terminated...")
				keepFlag = false
			}
		}
	}
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

	fmt.Println(err)
	return false
}
