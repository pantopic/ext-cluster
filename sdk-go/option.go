package cluster

import (
	"maps"

	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

type Option func()

func WithBankReconciler(fn func(*pb.Bank) uint32) Option {
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

func WithTags(m map[string]string) ShardOption {
	return func(s *pb.Shard) {
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}
		maps.Copy(s.Tags, m)
	}
}

func Persistent() ShardOption {
	return func(s *pb.Shard) {
		s.Type = "pantopic/ext-raft/persistent"
	}
}
