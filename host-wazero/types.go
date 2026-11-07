package wazero_cluster

import (
	"context"

	"github.com/pantopic/wazero-pool"

	"github.com/pantopic/ext-cluster/host-wazero/pb"
)

const (
	StatusPending uint32 = iota
	StatusActive
)

type Reconciler interface {
	Reconcile(ctx context.Context, pool wazeropool.Instance, item *pb.Component) (status uint32, err error)
}
