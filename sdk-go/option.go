package cluster

import (
	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

type Option func()

func WithComponentReconciler(fn func(*pb.Component) uint32) Option {
	return func() {
		reconcile = fn
	}
}

type ShardOption func(s *pb.Shard)

func WithTag(key, value string) ShardOption {
	return func(s *pb.Shard) {
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}
		s.Tags[key] = value
	}
}
