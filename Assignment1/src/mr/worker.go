package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "strings"
import "sort"
import "time"


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



var mapfcomp bool =false;

var dummy string ="ac"
var reducefcomp bool =false;
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		taker:= Taker{}
		giver:= Giver{}
		taker.Prime=false;
		taker.Mapfcomp=false;
		taker.Sleep=false;
		giver.Prime=false
		for !mapfcomp{
			call("Master.Takefiles",&giver,&taker);
			fmt.Printf("%+v \n " taker)
			if(taker.Mapfcomp){
				// when fc==ac in master.go
				mapfcomp=true;
				break;
			}else if taker.Prime{
				// when first thread (Prime) gets ac==len(filesarr) 
				// so you wait for all other workers to complete and then check if fc==ac
				giver.Prime=true;  // did this because I want to tell master this thread is prime when I go back
				time.Sleep(10 * time.Second)
			}else if taker.Sleep{
				// when non prime workers complete their work but ac!==fc, then you sleep till all workers are done
				time.Sleep(2* time.Second);
			}else{
				file, err := os.Open(taker.File)
				if err != nil {
					log.Fatalf("cannot open %v", taker.File)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", taker.File)
				}
				file.Close()
				kva := mapf(taker.File, string(content))
				for i:=0;i<len(kva);i++{
					hashval:= ihash(kva[i].Key) % taker.Nreduce;
					oname :=strconv.Itoa(hashval)+ strconv.Itoa(taker.Fileindex);
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
				// now here we go back and do fc++ in master
				call("Master.Completemapf",&taker,&dummy)
			}
		}
		// taker.Mapfcomp = true is done, now move to reduce files;
		get:=Get{};
		put:=Put{};
		get.Prime=false;
		get.Sleep=false;
		get.Reducefcomp=false;
		put.Prime=false
		for !reducefcomp{
			call("Master.TakeNumber",&put,&get);
			if(get.Reducefcomp){
				reducefcomp=true;
				break;
			}else if get.Prime{
				put.Prime=true;
				time.Sleep(2 * time.Second)
			}else if get.Sleep{
				time.Sleep(1* time.Second);
			}else{
				intermediate:=[]string{}
				for i:=0;i<get.Total;i++{
					oname:= strconv.Itoa(get.Index)+ strconv.Itoa(i);
					file, err := os.Open(oname)
					if err != nil {
						log.Fatalf("cannot open %v", oname)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", oname)
					}
					lines := strings.Split(string(content), "\n")
					final:=[]string{}
					final=append(final,lines...)	
					intermediate = append(intermediate, final...)
					file.Close()
				}
				sort.Strings(intermediate)

				oname := "mr-out-"+ strconv.Itoa(get.Index)
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

			call("Master.Completereducef",&get,&dummy)
		}
}





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
