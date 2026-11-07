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

func WithBufferCap(n uint32) Option {
	return func() {
		bufCap = n
	}
}
