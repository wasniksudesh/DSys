package mr

import "log"
import "net"
import "net/rpc"
import "net/http"
// import "fmt"
import "sync"
import "os"
import "strconv"

var filesarr []string
var counter int =0
type Master struct {
	// Your definitions here.
	Y int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.

// the RPC argument and reply types are defined in rpc.go.

var mu sync.Mutex
var prime bool =true
var ow bool=false
func (m *Master) Takefiles(file_name *ExampleReply,w_init *ExampleArgs) error {
	// in sab values ko lock karna hai
	mu.Lock()
	defer mu.Unlock()
	if(prime){
		w_init.Nreduce=nr
		// fmt.Printf("ac ?? %v %v \n",ac,n)
		if(ac!=n){
			i:=0
			for i<n{
				if(aa[i]){
					i++
				}else{
					break;
				}
			}
			ac++
			aa[i]=true
			w_init.File=filesarr[i]
			// fmt.Printf("aaya master me? purana %v \n",w_init.File)
			w_init.Fa_index=i
			if(ow){
				w_init.Overwrite=true
			}
			return nil
		}else{
			if(ac==fc){
				w_init.Mapfcomp=true
				// for workers change the main while loop variable after returning to loop out of main while loop of mapf
				// then call for reduce tasks in another function 
				return nil
			}else{
				prime=false
				// prime tha kyuki ab koi worker andaar na ghus paaye, sab bas sote rahe jab tak prime thread 10 sec ke baad return nai kar leta
				// har thread after completion goes to dusra function to change fc and fa
				w_init.Prime=true
				return nil
				//here prime worker goes back and sleeps for 10 secs
			}
		}
	}else{
		// everyone except prime goes to sleep. when coming here
		if(!file_name.Prime){
			w_init.Sleep=true
		}
		if(ac==fc){
			w_init.Mapfcomp=true
			prime=true
		}else{
			ac=fc
			aa=fa
			prime=true
			ow=true
		}
	}
	return nil
}




func (m *Master) Completefiles(w_init *ExampleArgs,file_name *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	// fmt.Printf("Completefileske andar %+v \n",w_init)
	fc++
	fa[w_init.Fa_index]=true
	return nil
}

var c int =-1
func (m *Master) Reducefiles(w_init *ExampleArgs ,data *Reducedata) error {
	mu.Lock()
	defer mu.Unlock()
	// fmt.Printf("Completefileske edhxrhf %+v \n",w_init)
	data.Total=n
	if c==nr-1{
		m.Done()
		data.Filenumber=-1
		return nil
	}
	c++
	data.Filenumber=c
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if(c==nr-1){
		ret=true
	}
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

var fa,aa []bool
var fc int =0
var ac int =0
var nr int
var n int 
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	nr=nReduce
	for j:=0;j< len(files);j++{
		for i:=0 ; i< nReduce ;i++{
			oname := strconv.Itoa(i)+strconv.Itoa(j)
			ofile, _ := os.Create(oname)	
			ofile.Close()
		}
	}
	for _, filename := range files {
		filesarr=append(filesarr,filename)
		// fmt.Printf("ready ek ek file %v\n",filename)
		n=len(filesarr)
		for i:=0; i<n; i++{
			fa=append(fa,false)
			aa=append(aa,false)
		}
	}
	// fmt.Printf("%v\n",filesarr)
	// fmt.Printf("ready %v%v\n",fa,n)

	m.server()
	return &m
}
