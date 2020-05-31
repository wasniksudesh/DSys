package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "time"
import "sort"
import "strings"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		w_init := ExampleArgs{}
		w_init.Sleep=false
		w_init.Mapfcomp=false
		w_init.Prime=false
		w_init.Overwrite=false
		filename:= ExampleReply{}
		finalcount:=0;
		filename.Prime=false
		w_init.Nreduce=0;
		for(!w_init.Mapfcomp){
			call("Master.Takefiles", &filename, &w_init)
			if(w_init.Mapfcomp){
				break;
			}
			// fmt.Printf("Pura w_init %+v \n", w_init)
			if(w_init.Sleep){
				time.Sleep(3 * time.Second)
			}else if w_init.Prime{
				filename.Prime=true
				time.Sleep(10 * time.Second)
				// fmt.Printf("Prime ne idhar aana chiye \n")
				// Sleep prime worker for 10 secs
			}else {
				if(w_init.Overwrite){
					// delete the files created by w_init.Fa_index
				}
				file, err := os.Open(w_init.File)
				// fmt.Printf("Loop hua poora? %v\n",w_init.File)
				if err != nil {
					log.Fatalf("cannot open bhai %v", w_init.File)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", w_init.File)
				}
				kva := mapf(w_init.File, string(content))
				nn:=w_init.Nreduce
				file.Close()
				for i:=0;i< len(kva); i++{
					hashval:= ihash(kva[i].Key)%nn
					if(kva[i].Key=="A"){
						finalcount++;
					}
					oname := strconv.Itoa(hashval)+strconv.Itoa(w_init.Fa_index);
				    f, err := os.OpenFile(oname,os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				    if err != nil {
				        fmt.Printf("error opening %s: %s", oname, err)
				        return
				    }
					if _, err := f.WriteString(kva[i].Key+"\n"); err != nil {
						log.Println(err)
					}
				    f.Close()
				}

				call("Master.Completefiles", &w_init,&filename);

			}
		}

		// fmt.Printf("Idhar Mapf khtm ho chuka hai??? %v\n", finalcount)

		// Start lifting reduce tasks from here.
		w_init.Sleep=false
		w_init.Mapfcomp=false
		w_init.Prime=false
		w_init.Overwrite=false

		// oname:= "mr-out-1"

		// filhaal assuming niche wala part crash nai hoyega... isme loop b daalna padega btw
		for(!w_init.Mapfcomp){
			c:=Reducedata{}
			call("Master.Reducefiles", &w_init,&c);
			if(c.Filenumber==-1){
				break;
			}

			// Filenumber hi nReduce hai
			intermediate := []string{}
			for i:=0;i<c.Total;i++{
				oname := strconv.Itoa(c.Filenumber)+strconv.Itoa(i);
				// fmt.Printf("Idhar ai???%v\n",oname)
				// file, err := os.Open(oname)
				// if err != nil {
				// 	log.Fatalf("cannot open hain?? %v")
				// }   
				content, err := ioutil.ReadFile(oname)
				if err != nil {
					log.Fatalf("cannot read %v")
				}
				lines := strings.Split(string(content), "\n")
				final:=[]string{}
				final=append(final,lines...)	
				// fmt.Printf("KYuuuuuuuu???%v\n",len(final))
				intermediate = append(intermediate, final...)
				// iske baad delete files xy
			}

			sort.Strings(intermediate)

			oname := "mr-out-"+ strconv.Itoa(c.Filenumber)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j] == intermediate[i] {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, "1")
				}
				if(intermediate[i]!=""){
					output := reducef(intermediate[i], values)
					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i], output)
				}
				i = j
			}
			ofile.Close()
		}
		// fmt.Printf("Baahar aaya re")
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.

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
