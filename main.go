/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"github.com/containerd/containerd"
	diffapi "github.com/containerd/containerd/api/services/diff/v1"
	"github.com/containerd/containerd/contrib/diffservice"
	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/diff/apply"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/sys"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
)

func main() {
	serverOpts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			streamNamespaceInterceptor,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			unaryNamespaceInterceptor,
		)),
	}

	s := grpc.NewServer(serverOpts...)

	diffapi.RegisterDiffServer(s, diffservice.FromApplierAndComparer(&applierService{address: defaults.DefaultAddress}, nil))

	path := "proxy.sock"
	if len(os.Args) == 2 {
		path = os.Args[1]
	}

	l, err := sys.GetLocalListener(path, os.Getuid(), os.Getgid())
	if err != nil {
		log.L.WithError(err).Fatalf("unable to listen on %s", path)
	}

	defer l.Close()
	if err := s.Serve(l); err != nil {
		log.L.WithError(err).Fatal("serve GRPC")
	}
}

func unaryNamespaceInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if ns, ok := namespaces.Namespace(ctx); ok {
		// The above call checks the *incoming* metadata, this makes sure the outgoing metadata is also set
		ctx = namespaces.WithNamespace(ctx, ns)
	}
	return handler(ctx, req)
}

func streamNamespaceInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	if ns, ok := namespaces.Namespace(ctx); ok {
		// The above call checks the *incoming* metadata, this makes sure the outgoing metadata is also set
		ctx = namespaces.WithNamespace(ctx, ns)
		ss = &wrappedSSWithContext{ctx: ctx, ServerStream: ss}
	}

	return handler(srv, ss)
}

type wrappedSSWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedSSWithContext) Context() context.Context {
	return w.ctx
}

type applierService struct {
	address string

	applier diff.Applier
	loaded  uint32
	loadM   sync.Mutex

	diffapi.UnimplementedDiffServer
}

func (a *applierService) getApplier() (diff.Applier, error) {
	if atomic.LoadUint32(&a.loaded) == 1 {
		return a.applier, nil
	}
	a.loadM.Lock()
	defer a.loadM.Unlock()
	if a.loaded == 1 {
		return a.applier, nil
	}

	client, err := containerd.New(a.address)
	if err != nil {
		return nil, nil
	}

	defer atomic.StoreUint32(&a.loaded, 1)
	a.applier = apply.NewFileSystemApplier(client.ContentStore())

	return a.applier, nil
}

func (a *applierService) Apply(ctx context.Context, desc ocispec.Descriptor, mounts []mount.Mount, opts ...diff.ApplyOpt) (d ocispec.Descriptor, err error) {
	applier, err := a.getApplier()
	if err != nil {
		return d, err
	}

	log.G(ctx).WithField("digest", desc.Digest).Info("applying diff")

	return applier.Apply(ctx, desc, mounts, opts...)
}