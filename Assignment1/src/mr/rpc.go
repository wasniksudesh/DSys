package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Giver struct {
	Prime bool
}


type Get struct {
	Prime bool
	Reducefcomp bool
	Index int
	Sleep bool
	Total int
}
type Put struct {
	Prime bool
}

type Taker struct {
	File string
	Prime bool
	Mapfcomp bool
	Sleep bool
	Fileindex int
	Nreduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
