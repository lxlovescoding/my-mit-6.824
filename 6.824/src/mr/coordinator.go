package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task     // 使用chan保证并发安全
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
	mu                sync.Mutex     // 互斥锁
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间 判断这个任务是否超时
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// Your code here -- RPC handlers for the worker to call.

// Example
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
	fmt.Printf("coordinator开始监听来自worker的rpc请求， sockname为%v...\n", l.Addr())
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i), // worker生成的临时文件后缀存了哈希值，这里完成分组
		}
		taskMetaInfo := TaskMetaInfo{TaskAdr: &task, state: Waiting}
		c.taskMetaHolder.addMetaInfo(&taskMetaInfo)
		fmt.Println("makeReduceTasks: ", task)
		c.TaskChannelReduce <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var ans []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			ans = append(ans, file.Name())
		}
	}
	return ans
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   MapTask,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}
		taskMetaInfo := TaskMetaInfo{TaskAdr: &task, state: Waiting}
		c.taskMetaHolder.addMetaInfo(&taskMetaInfo)
		fmt.Println("makeMapTasks: ", task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	ans := c.TaskId
	c.TaskId++
	return ans
}

func (t *TaskMetaHolder) addMetaInfo(info *TaskMetaInfo) bool {
	taskId := info.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = info
	}
	return true
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 拉取任务要上锁，防止多个worker竞争
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				// 这里是把MetaInfoHolder里面保存的task的state变为running 主要是为了 c.taskMetaHolder.checkTaskDone() 服务
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Map - taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				// 如果此时map任务管道里已经取不出来任务，那么说明map任务要么还在进行中，要么已完成
				reply.TaskType = WaittingTask
				// 如果map任务都完成，那么就该进入下一阶段
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Reduce - taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	default:
		{
			reply.TaskType = ExitTask
		}
	}

	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成,下同
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}

	fmt.Printf("doneMap:%d, undoneMap:%d, doneReduce:%d, undoneReduce:%d\n", mapDoneNum, mapUnDoneNum, reduceDoneNum, reduceUnDoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
			} else {
				fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
		}
	case ReduceTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
			} else {
				fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}

func (c *Coordinator) crashDetector() {
	for {
		time.Sleep(time.Second * 2)
		c.mu.Lock()
		if c.DistPhase == AllDone {
			c.mu.Unlock()
			break
		}
		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.TaskChannelMap <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.TaskChannelReduce <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		c.mu.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder:    TaskMetaHolder{MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce)},
		mu:                sync.Mutex{},
	}
	c.makeMapTasks(files)
	go c.crashDetector()
	c.server()
	return &c
}
