package wazero_cluster

import (
	"context"
)

// Name is the name of this host module.
const Name = "pantopic/wazero-cluster"

var (
	ctxKeyNamespace = Name + `/namespace`
	ctxKeyResource  = Name + `/resource`
)

type ContextCopy = func(dst, src context.Context) context.Context

func NewResolver(namespace, resource string) ContextCopy {
	return func(dst, src context.Context) context.Context {
		dst = context.WithValue(dst, ctxKeyNamespace, namespace)
		dst = context.WithValue(dst, ctxKeyResource, resource)
		return dst
	}
}

func NamespaceFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyNamespace).(string); ok {
		return v
	}
	panic("Namespace missing from context")
}

func ResourceFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyResource).(string); ok {
		return v
	}
	panic("Resource missing from context")
}

func ResolveFrom(ctx context.Context) (string, string) {
	return NamespaceFrom(ctx), ResourceFrom(ctx)
}
