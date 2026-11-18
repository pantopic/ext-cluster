package cluster

import (
	"iter"

	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

var (
	reconcile func(*pb.Component) uint32
	shard     = new(pb.Shard)
)

func Init(opts ...Option) {
	for _, opt := range opts {
		opt()
	}
}

func Shard() *pb.Shard {
	return shard
}

func ShardIter() iter.Seq[*pb.Shard] {
	_shard_iter()
	return func(yield func(*pb.Shard) bool) {
		for {
			if bufLen == 0 {
				return
			}
			shard.Reset()
			if err := shard.UnmarshalVT(buf[:bufLen]); err != nil {
				return
			}
			if !yield(shard) {
				return
			}
			_shard_next()
		}
	}
}

func ShardCreate(name, module string, opts ...ShardOption) (*pb.Shard, error) {
	shard.Reset()
	shard.Name = name
	shard.Type = "pantopic/ext-raft"
	shard.Tags = map[string]string{
		`pantopic:module`: module,
	}
	for _, opt := range opts {
		opt(shard)
	}
	n, err := shard.MarshalToVT(buf)
	if err != nil {
		panic(err)
	}
	bufLen = uint32(n)
	_shard_create()
	if bufLen == 0 {
		return nil, getErr()
	}
	shard.Reset()
	if err := shard.UnmarshalVT(buf[:bufLen]); err != nil {
		panic(err)
	}
	return shard, nil
}

func ShardUpdate(s *pb.Shard, opts ...ShardOption) (*pb.Shard, error) {
	for _, opt := range opts {
		opt(s)
	}
	n, err := s.MarshalToVT(buf)
	if err != nil {
		panic(err)
	}
	bufLen = uint32(n)
	_shard_update()
	if bufLen == 0 {
		return nil, getErr()
	}
	shard.Reset()
	if err := shard.UnmarshalVT(buf[:bufLen]); err != nil {
		panic(err)
	}
	return shard, nil
}

func ResourceTagged(key string, val ...string) bool {
	str := key
	if len(val) > 0 {
		str += `=` + val[0]
	}
	setBuf([]byte(str))
	_resource_tagged()
	return bufLen > 0
}

func ComponentTagged(key string, val ...string) bool {
	str := key
	if len(val) > 0 {
		str += `=` + val[0]
	}
	setBuf([]byte(str))
	_component_tagged()
	return bufLen > 0
}
