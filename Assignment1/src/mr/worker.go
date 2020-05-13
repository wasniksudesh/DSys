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
			// fmt.Printf("%+v \n " ,taker)
			if(giver.Prime){
				giver.Prime=false;
				taker.Prime=false;
			}else if(taker.Mapfcomp){
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
				kva := mapf(taker.File, string(content))
				file.Close()
				for i:=0;i<len(kva);i++{
					hashval:= ihash(kva[i].Key) % taker.Nreduce;
					oname :=strconv.Itoa(hashval)+ strconv.Itoa(taker.Fileindex);
				    f, err := os.OpenFile(oname,os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				    if err != nil {
				        fmt.Printf("error opening %s: %s", oname, err)
				        return
				    }
					if _, err := f.WriteString(kva[i].Key+"+"+kva[i].Value+"\n"); err != nil {
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
				time.Sleep(10 * time.Second)
			}else if get.Sleep{
				time.Sleep(2* time.Second);
			}else{
				inter:=[]string{}
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
					inter = append(inter, final...)
					file.Close()
				}
				intermediate := []KeyValue{}
				for i:=0;i<len(inter);i++{
					pp:=strings.Split(string(inter[i]), "+")	
					if(len(pp)==2){
						g:=KeyValue{}	
						g.Key= pp[0]
						g.Value= pp[1]
						intermediate = append(intermediate, g)
					}
				}
				sort.Sort(ByKey(intermediate))
				oname := "mr-out-"+ strconv.Itoa(get.Index)
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

	// fmt.Println(err)
	return false
}
