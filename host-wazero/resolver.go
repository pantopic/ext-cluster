package wazero_cluster

import (
	"context"
	"log"
)

const Name = "pantopic/ext-cluster"

var (
	ctxKeyNamespace = Name + `/namespace`
	ctxKeyResource  = Name + `/resource`
	ctxKeyComponent = Name + "/component"
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
	log.Fatal("Namespace missing from context")
	return ""
}

func ResourceFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyResource).(string); ok {
		return v
	}
	log.Fatal("Resource missing from context")
	return ""
}

func ResolveFrom(ctx context.Context) (string, string) {
	return NamespaceFrom(ctx), ResourceFrom(ctx)
}
