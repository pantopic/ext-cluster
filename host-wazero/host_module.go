package wazero_cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/logbn/zongzi"
	"github.com/pantopic/wazero-pool"

	"github.com/pantopic/ext-cluster/host-wazero/pb"
)

var (
	ctxKeyMeta  = Name + `/meta`
	ctxKeyLocal = Name + `/local`
)

type ctxLocal struct {
	nextShard chan *pb.Shard
}

type meta struct {
	ptrBufCap uint32
	ptrBufLen uint32
	ptrBuf    uint32
	ptrErrCap uint32
	ptrErrLen uint32
	ptrErr    uint32
}

type hostModule struct {
	sync.RWMutex

	module api.Module
	agent  *zongzi.Agent
}

func New(agent *zongzi.Agent, opts ...Option) *hostModule {
	h := &hostModule{
		agent: agent,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *hostModule) Name() string {
	return Name
}

// Register instantiates the host module, making it available to all module instances in this runtime
func (h *hostModule) Register(ctx context.Context, r wazero.Runtime) (err error) {
	builder := r.NewHostModuleBuilder(Name)
	register := func(name string, fn func(ctx context.Context, m api.Module, stack []uint64)) {
		builder = builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(fn), []api.ValueType{}, nil).Export(name)
	}
	for name, fn := range map[string]any{
		"__cluster_shard_iter": func(ctx context.Context, shard *pb.Shard) (res *pb.Shard, err error) {
			c := get[*pb.Component](ctx, ctxKeyComponent)
			name := fmt.Sprintf(`%s.%s.%s`, c.GetNamespace(), c.GetResource(), c.GetName())
			next := make(chan *pb.Shard)
			get[*ctxLocal](ctx, ctxKeyLocal).nextShard = next
			go func() {
				defer close(next)
				t := time.NewTicker(5 * time.Second)
				defer t.Stop()
				err = h.agent.State(ctx, func(state *zongzi.State) {
					state.ShardIterateByTag(`pantopic:component`, name, func(s zongzi.Shard) bool {
						t.Reset(5 * time.Second)
						select {
						case <-t.C:
							return false
						case next <- shardToPb(s):
						}
						return true
					})
				})
			}()
			res, _ = <-next
			return
		},
		"__cluster_shard_next": func(ctx context.Context) (res *pb.Shard) {
			next := get[*ctxLocal](ctx, ctxKeyLocal).nextShard
			if next == nil {
				return
			}
			res, _ = <-next
			return
		},
		"__cluster_shard_create": func(ctx context.Context, shard *pb.Shard) (res *pb.Shard, err error) {
			c := get[*pb.Component](ctx, ctxKeyComponent)
			name := fmt.Sprintf(`%s.%s.%s`, c.GetNamespace(), c.GetResource(), c.GetName())
			shard.Tags[`pantopic:component`] = name
			s, _, err := h.agent.ShardCreate(ctx, shard.Type,
				zongzi.WithName(name+"."+shard.Name),
				zongzi.WithTags(shard.Tags))
			if err == nil {
				res = shardToPb(s)
			}
			fmt.Printf("Created shard: %#v\n", s)
			return
		},
		"__cluster_shard_update": func(ctx context.Context, shard *pb.Shard) (res *pb.Shard, err error) {
			c := get[*pb.Component](ctx, ctxKeyComponent)
			name := fmt.Sprintf(`%s.%s.%s`, c.GetNamespace(), c.GetResource(), c.GetName())
			shard.Tags[`pantopic:component`] = name
			s, err := h.agent.ShardUpdate(ctx, shard.Id, zongzi.WithTags(shard.Tags))
			if err == nil {
				res = shardToPb(s)
			}
			return
		},
	} {
		switch fn := fn.(type) {
		case func(context.Context, *pb.Shard) (res *pb.Shard, err error):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				shard := &pb.Shard{}
				shard.UnmarshalVT(getBuf(mod, meta))
				res, err := fn(ctx, shard)
				if err != nil {
					setErr(mod, meta, err)
				}
				if res != nil {
					n, err := res.MarshalToVT(buf(mod, meta))
					if err != nil {
						log.Fatal(err)
					}
					writeUint32(mod, meta.ptrBufLen, uint32(n))
				} else {
					writeUint32(mod, meta.ptrBufLen, 0)
				}
			})
		case func(context.Context) (res *pb.Shard):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				res := fn(ctx)
				if res != nil {
					n, err := res.MarshalToVT(buf(mod, meta))
					if err != nil {
						log.Fatal(err)
					}
					writeUint32(mod, meta.ptrBufLen, uint32(n))
				} else {
					writeUint32(mod, meta.ptrBufLen, 0)
				}
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
	h.module, err = builder.Instantiate(ctx)
	return
}

// InitContext retrieves the meta page from the wasm module
func (h *hostModule) InitContext(ctx context.Context, m api.Module) (context.Context, error) {
	fn := m.ExportedFunction(`__cluster`)
	if fn == nil {
		return ctx, nil
	}
	stack, err := fn.Call(ctx)
	if err != nil {
		return ctx, err
	}
	meta := &meta{}
	ptr := uint32(stack[0])
	for i, v := range []*uint32{
		&meta.ptrBufCap,
		&meta.ptrBufLen,
		&meta.ptrBuf,
		&meta.ptrErrCap,
		&meta.ptrErrLen,
		&meta.ptrErr,
	} {
		*v = readUint32(m, ptr+uint32(4*i))
	}
	return context.WithValue(ctx, ctxKeyMeta, meta), nil
}

// ContextCopier populates dst context with the meta page from src context.
func (h *hostModule) ContextCopy(dst, src context.Context) context.Context {
	if v := src.Value(ctxKeyMeta); v != nil {
		dst = context.WithValue(dst, ctxKeyMeta, v.(*meta))
		dst = context.WithValue(dst, ctxKeyLocal, &ctxLocal{})
	}
	return dst
}

func (r *hostModule) Reconcile(ctx context.Context, pool wazeropool.Instance, item *pb.Component) (status uint32, err error) {
	ctx = context.WithValue(ctx, ctxKeyComponent, item)
	pool.Run(func(mod api.Module) {
		fn := mod.ExportedFunction("__cluster_reconcile")
		if fn == nil {
			err = fmt.Errorf("no reconcile function found for component %s", item.Type)
			return
		}
		meta := get[*meta](ctx, ctxKeyMeta)
		n, err := item.MarshalToVT(buf(mod, meta))
		if err != nil {
			log.Fatal(err)
		}
		writeUint32(mod, meta.ptrBufLen, uint32(n))
		var stack []uint64
		stack, err = fn.Call(ctx)
		if err != nil {
			return
		}
		if len(stack) > 0 {
			status = uint32(stack[0])
		}
	})
	return
}

func get[T any](ctx context.Context, key string) T {
	v := ctx.Value(key)
	if v == nil {
		log.Panicf("Context item missing %s", key)
	}
	return v.(T)
}

func read(m api.Module, ptr, ptrLen, ptrCap uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptr, readUint32(m, ptrCap))
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptr, ptrCap)
	}
	return buf[:readUint32(m, ptrLen)]
}

func getBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrBuf, meta.ptrBufLen, meta.ptrBufCap)
}

func getBufCopy(m api.Module, meta *meta) []byte {
	return append([]byte(nil), getBuf(m, meta)...)
}

func buf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrBuf, 0, meta.ptrBufCap)
}

func readUint32(m api.Module, ptr uint32) (val uint32) {
	val, ok := m.Memory().ReadUint32Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func writeUint32(m api.Module, ptr uint32, val uint32) {
	if ok := m.Memory().WriteUint32Le(ptr, val); !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
}

func errBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrErr, 0, meta.ptrErrCap)
}

func getErr(m api.Module, meta *meta) (err error) {
	if b := read(m, meta.ptrErr, meta.ptrErrLen, meta.ptrErrCap); len(b) > 0 {
		err = errors.New(string(b))
	}
	return
}

func setErr(m api.Module, meta *meta, err error) {
	var msg string
	if err != nil {
		msg = err.Error()
		copy(errBuf(m, meta)[:len(msg)], msg)
	}
	writeUint32(m, meta.ptrErrLen, uint32(len(msg)))
}

func shardToPb(s zongzi.Shard) *pb.Shard {
	return &pb.Shard{
		Id:      s.ID,
		Created: s.Created,
		Updated: s.Updated,
		Leader:  s.Leader,
		Term:    s.Term,
		Type:    s.Type,
		Status:  string(s.Status),
		Name:    s.Name,
		Tags:    s.Tags,
	}
}

func pbToShard(item *pb.Shard) zongzi.Shard {
	return zongzi.Shard{
		ID:      item.Id,
		Created: item.Created,
		Updated: item.Updated,
		Leader:  item.Leader,
		Term:    item.Term,
		Type:    item.Type,
		Status:  zongzi.ShardStatus(item.Status),
		Name:    item.Name,
		Tags:    item.Tags,
	}
}
