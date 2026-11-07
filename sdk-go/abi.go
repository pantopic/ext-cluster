package cluster

import (
	"unsafe"

	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

var (
	bufCap uint32 = 10 << 10 // 10 KiB
	bufLen uint32
	buf    []byte
	errCap uint32 = 1 << 10 // 1 KiB
	errLen uint32
	err    = make([]byte, int(errCap))
	meta   = make([]uint32, 6)

	component = new(pb.Component)
)

//export __cluster
func __cluster() (res uint32) {
	for i, p := range []unsafe.Pointer{
		unsafe.Pointer(&bufCap),
		unsafe.Pointer(&bufLen),
		unsafe.Pointer(&buf[0]),
		unsafe.Pointer(&errCap),
		unsafe.Pointer(&errLen),
		unsafe.Pointer(&err[0]),
	} {
		meta[i] = uint32(uintptr(p))
	}
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

//export __cluster_reconcile
func _reconcile() uint32 {
	component.UnmarshalVT(buf[:bufLen])
	return reconcile(component)
}

func getErr() (e error) {
	if errLen > 0 {
		e = strErr(string(err[:errLen]))
	}
	return
}

//go:wasm-module pantopic/ext-cluster
//export __cluster_shard_next
func _shard_next()

//go:wasm-module pantopic/ext-cluster
//export __cluster_shard_iter
func _shard_iter()

//go:wasm-module pantopic/ext-cluster
//export __cluster_shard_create
func _shard_create()

//go:wasm-module pantopic/ext-cluster
//export __cluster_shard_update
func _shard_update()

// Fix for lint rule `unusedfunc`
var _ = __cluster
var _ = _reconcile
var _ = _shard_next
var _ = _shard_iter
var _ = _shard_create
var _ = _shard_update
