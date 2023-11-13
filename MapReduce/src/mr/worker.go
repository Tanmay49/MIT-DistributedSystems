package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	GetTask(mapf, reducef)

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := Args{}
		reply := Reply{}

		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			fmt.Println("Error from Coordinator")
		}
		if reply.Status == MapAssigned {
			Mapper(&reply, mapf)
		}
		if reply.Status == Wait {
			time.Sleep(1 * time.Second)
		}
		if reply.Status == ReduceAssigned {
			Reduce(reducef, &reply)
		}
		if reply.Status == Done {
			os.Exit(0)
			break
		}
	}
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Mapper(reply *Reply, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, reply.NReduce)
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("Error %v", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error %v", err)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	sort.Sort(ByKey(kva))

	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	for r, kva := range intermediate {

		oname := fmt.Sprintf("mr-%d-%d", reply.Index, r)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("Error creating temporary file: %v", err)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	call("Coordinator.TaskDone", &Args{
		FileName: reply.FileName,
		Status:   MapDone,
	}, &reply)
}

func Reduce(reducef func(string, []string) string, reply *Reply) {

	intermediate := []KeyValue{}
	for i := 0; i < reply.TaskLength; i++ {
		{
			filename := fmt.Sprintf("mr-%d-%d", i, reply.Index)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Error %v", err)
			}

			defer file.Close()

			dec := json.NewDecoder(file)

			for {
				var kv KeyValue

				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", reply.Index)
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

	call("Coordinator.TaskDone", &Args{
		Index:  reply.Index,
		Status: ReduceDone,
	}, &reply)

}

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
