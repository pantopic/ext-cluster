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
	buf = make([]byte, int(bufCap))
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

func ShardCreate(s *pb.Shard) (*pb.Shard, error) {
	n, err := s.MarshalToVT(buf)
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

func ShardUpdate(s *pb.Shard) (*pb.Shard, error) {
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
