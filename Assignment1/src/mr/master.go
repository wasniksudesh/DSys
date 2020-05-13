package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
// import "fmt"
import "sync"
import "strconv"
type Master struct {

}

var nreduce int =0;
var filesarr []string;

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

var mu sync.Mutex
var prime bool =false
func (m *Master) Takefiles(giver *Giver, taker *Taker) error {
	mu.Lock();
	n:= len(filesarr)
	taker.Nreduce=nreduce;
	if(!prime){
		if(ac== n){
			//fmt.Printf("1\n")
			if(fc==n){
				taker.Mapfcomp=true;
			}else{
				// fmt.Printf("heere are %v%v%v%+v\n",ac,fc,prime,n)
				taker.Prime=true;
				prime=true;
			}
		}else {
			// fmt.Printf("2\n")
			for i:=0;i<n;i++{
				if(!aa[i]){
					taker.File=filesarr[i];
					taker.Fileindex=i;
					aa[i]=true;
					ac++;
					break;
				}
			}
		}
	}else{
		if(ac==fc){
			//fmt.Printf("3\n")
			taker.Mapfcomp=true;
		}else if(giver.Prime){
			// fmt.Printf("taker.Prime must become false;!!!! %v%v%v%+v\n",ac,fc,prime,n)
			//fmt.Printf("4\n")
			ac=fc;
			aa=fa;
			taker.Prime=false   //Issue here, changes made here are not getting to worker's TAKER variable, just it is still true in worker.
			// fmt.Printf("print this ",taker.Prime)
			prime=false;
		}else{
			//fmt.Printf("5\n")
			taker.Sleep=true;
		}
	}
	mu.Unlock()
	return nil
}


func (m *Master) Completemapf(args *Taker, reply *string) error {
	mu.Lock();
	fc++;
	fa[args.Fileindex]=true;
	// fmt.Printf("compelte hua %v\n",args.File)
	mu.Unlock()
	return nil
}

var rfa []bool;
var rfc int =0;
var rac int =0;
var raa []bool
var prime2 bool =false
func (m *Master) TakeNumber(put *Put,get *Get) error {
	mu.Lock();
	// defer mu.Unlock();
	get.Total=len(filesarr);
	if(!prime2){
		if(rfc== nreduce){
		//fmt.Printf("11\n")
			if(rfc==nreduce){
				get.Reducefcomp=true;
			}else{
				get.Prime=true;
				prime2=true;
			}
		}else {
		//fmt.Printf("12\n")

			for i:=0;i<nreduce;i++{
				if(!raa[i]){
					get.Index=i;
					raa[i]=true;
					rac++;
					break
				}
			}
		}
	}else{
		if(rac==rfc){
		//fmt.Printf("13\n")

			get.Reducefcomp=true;
		}else if(put.Prime){
		//fmt.Printf("14\n")

			rac=rfc;
			raa=rfa;
			prime2=false;
		}else{
		//fmt.Printf("15\n")

			get.Sleep=true;
		}
	}
	mu.Unlock();
	return nil
}


func (m *Master) Completereducef(args *Get, reply *string) error {
	mu.Lock();
	rfc++;
	rfa[args.Index]=true;
	mu.Unlock();
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
var m2 sync.Mutex

func (m *Master) Done() bool {
	m2.Lock();

	ret := false
	if(rfc==nreduce){
		ret=true;
	}
	m2.Unlock()
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
var fa []bool;
var aa []bool;
var ac int =0;
var fc int =0;
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	nreduce=nReduce;
	filesarr=files;
	for j:=0;j< len(files);j++{
		for i:=0 ; i< nReduce ;i++{
			oname := strconv.Itoa(i)+strconv.Itoa(j)
			ofile, _ := os.Create(oname)	
			ofile.Close()
		}
	}
	for i:=0;i<len(files);i++{
		fa=append(fa,false);
		aa=append(aa,false);
	}
	for i:=0;i<nReduce;i++{
		raa=append(raa,false);
		rfa=append(rfa,false);
	}
	m.server()
	return &m
}
