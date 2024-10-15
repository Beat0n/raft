package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const MaxReplyDoneTries = 4
const WaitDuration = 1

// Map functions return a slice of KeyValue.
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
	for {
		args := SendTaskArgs{}
		reply := SendTaskReplys{}
		if ok := call("Coordinator.SendTask", &args, &reply); !ok {
			fmt.Printf("call failed!\n")
			continue
		}
		if reply.TaskType == MapType {
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				fmt.Printf("cannot open %v\n", filename)
				continue
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Printf("cannot read %v\n", filename)
				continue
			}
			err = file.Close()
			if err != nil {
				fmt.Printf("cannot close %v\n", filename)
				continue
			}
			//writers := make([]*bufio.Writer, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			fds := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				oFilename := fmt.Sprintf("tmp-%d-%d", reply.WorkerID, i)
				fds[i], _ = os.OpenFile(oFilename, os.O_CREATE|os.O_WRONLY, 0766)
				encoders[i] = json.NewEncoder(fds[i])
			}
			kva := mapf(filename, string(content))
			for _, kv := range kva {
				id := ihash(kv.Key) % reply.NReduce
				encoders[id].Encode(&kv)
			}
			for i := 0; i < reply.NReduce; i++ {
				oldName := fmt.Sprintf("tmp-%d-%d", reply.WorkerID, i)
				NewName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				os.Rename(oldName, NewName)
				fds[i].Close()
			}
			go replyDone(reply.TaskID, MapType, 0)
		} else if reply.TaskType == ReduceType {
			var kv KeyValue
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				iFilename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
				fd, err := os.Open(iFilename)
				if err != nil {
					fmt.Printf("cannot open %v\n", iFilename)
					continue
				}
				decoder := json.NewDecoder(fd)
				for {
					if err = decoder.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			ofile, _ := os.Create(oname)

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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
			go replyDone(reply.TaskID, ReduceType, reply.NMap)
		} else if reply.TaskType == WaitType {
			DPrintf("wait.../n")
			time.Sleep(WaitDuration * time.Second)
		} else if reply.TaskType == ExitType {
			break
		}
	}

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

func replyDone(taskID int, taskType TaskType, nMap int) {
	args := DoneTaskArgs{
		TaskID:   taskID,
		TaskType: taskType,
	}
	reply := DoneTaskReplys{}
	for i := 0; i < MaxReplyDoneTries; i++ {
		if call("Coordinator.DoneTask", &args, &reply) {
			break
		}
	}
}
