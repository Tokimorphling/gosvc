package kitexexample

import (
	"context"
	"errors"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// StaticResolver is a minimal discovery.Resolver over a fixed instance list.
//
// It exists to show the extension point without pulling in a registry. In
// production replace it with kitex-contrib/registry-etcd, registry-nacos or
// registry-polaris, which implement the same interface.
type StaticResolver struct {
	instances []discovery.Instance
}

// NewStaticResolver builds a resolver from explicit instances.
func NewStaticResolver(instances ...discovery.Instance) *StaticResolver {
	return &StaticResolver{instances: instances}
}

// Target implements discovery.Resolver.
func (r *StaticResolver) Target(context.Context, rpcinfo.EndpointInfo) string { return r.Name() }

// Resolve implements discovery.Resolver.
func (r *StaticResolver) Resolve(_ context.Context, desc string) (discovery.Result, error) {
	if len(r.instances) == 0 {
		return discovery.Result{}, errors.New("static resolver has no instances")
	}
	return discovery.Result{CacheKey: desc, Instances: r.instances}, nil
}

// Diff implements discovery.Resolver. The instance list never changes, so any
// new result is authoritative.
func (r *StaticResolver) Diff(_ string, _, next discovery.Result) (discovery.Change, bool) {
	return discovery.Change{Result: next}, true
}

// Name implements discovery.Resolver.
func (r *StaticResolver) Name() string { return "static" }
