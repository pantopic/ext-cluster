package wazero_cluster

import (
	"context"
	"log"

	"github.com/pantopic/ext-cluster/host-wazero/pb"
)

const Name = "pantopic/ext-cluster"

var (
	ctxKeyNamespace     = Name + `/namespace`
	ctxKeyNamespaceName = Name + `/namespace-name`
	ctxKeyResource      = Name + `/resource`
	ctxKeyResourceName  = Name + `/resource-name`
	ctxKeyBank          = Name + "/bank"
	ctxKeyBankName      = Name + "/bank-name"
)

type ContextCopy = func(dst, src context.Context) context.Context

func NewResolver(namespace, resource string) ContextCopy {
	return func(dst, src context.Context) context.Context {
		dst = context.WithValue(dst, ctxKeyNamespaceName, namespace)
		dst = context.WithValue(dst, ctxKeyResourceName, resource)
		return dst
	}
}

func NamespaceFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyNamespaceName).(string); ok {
		return v
	}
	log.Fatal("Namespace missing from context")
	return ""
}

func ResourceFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyResourceName).(string); ok {
		return v
	}
	log.Fatal("Resource missing from context")
	return ""
}

func ResolveFrom(ctx context.Context) (string, string) {
	return NamespaceFrom(ctx), ResourceFrom(ctx)
}

func ContextSetResource(ctx context.Context, resource *pb.Resource) context.Context {
	return context.WithValue(ctx, ctxKeyResource, resource)
}

func ContextSetBank(ctx context.Context, bank *pb.Bank) context.Context {
	return context.WithValue(ctx, ctxKeyBank, bank)
}
