package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data   map[string]string
	record sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}

	// duplicate detection
	if res, ok := kv.record.Load(args.MessageID); ok {
		reply.Value = res.(string)
		return
	}

	old := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = old
	kv.record.Store(args.MessageID, old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}

	if res, ok := kv.record.Load(args.MessageID); ok {
		reply.Value = res.(string)
		return
	}

	oldValue := kv.data[args.Key]
	kv.data[args.Key] = oldValue + args.Value
	reply.Value = oldValue
	kv.record.Store(args.MessageID, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.data = make(map[string]string)
	return kv
}
