package content_addressable_storage_server_proxy

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func requestCountingUnaryInterceptor(count *atomic.Int32) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		count.Add(1)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func requestCountingStreamInterceptor(count *atomic.Int32) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		count.Add(1)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func runRemoteCASServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) (repb.ContentAddressableStorageClient, *atomic.Int32, *atomic.Int32) {
	remoteCASServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	grpcServver, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterContentAddressableStorageServer(grpcServver, remoteCASServer)
	go runFunc()
	unaryRequestCounter := atomic.Int32{}
	streamRequestCounter := atomic.Int32{}
	conn, err := testenv.LocalGRPCConn(ctx, env,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)),
		grpc.WithUnaryInterceptor(requestCountingUnaryInterceptor(&unaryRequestCounter)),
		grpc.WithStreamInterceptor(requestCountingStreamInterceptor(&streamRequestCounter)))
	require.NoError(t, err)
	return repb.NewContentAddressableStorageClient(conn), &unaryRequestCounter, &streamRequestCounter
}

func runCASProxy(ctx context.Context, remoteClient repb.ContentAddressableStorageClient, localEnv *testenv.TestEnv, t *testing.T) repb.ContentAddressableStorageClient {
	localEnv.SetContentAddressableStorageClient(remoteClient)
	casServer, err := NewCASServerProxy(localEnv)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(localEnv)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, localEnv, grpc.WithDefaultCallOptions())
	if err != nil {
		t.Error(err)
	}

	return repb.NewContentAddressableStorageClient(clientConn)
}

func TestFindMissingBlobs(t *testing.T) {
	ctx := context.Background()
	remoteEnv := testenv.GetTestEnv(t)
	localEnv := testenv.GetTestEnv(t)
	remoteClient, unaryRequestCount, streamRequestCount := runRemoteCASServer(ctx, remoteEnv, t)
	localClient := runCASProxy(ctx, remoteClient, localEnv, t)

	digestA := repb.Digest{
		Hash:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 3,
	}
	digestB := repb.Digest{
		Hash:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		SizeBytes: 3,
	}
	digestC := repb.Digest{
		Hash:      "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		SizeBytes: 3,
	}

	_, err := remoteClient.BatchUpdateBlobs(ctx,
		&repb.BatchUpdateBlobsRequest{
			Requests: []*repb.BatchUpdateBlobsRequest_Request{
				&repb.BatchUpdateBlobsRequest_Request{
					Digest: &digestA,
					Data:   []byte("foo"),
				},
				&repb.BatchUpdateBlobsRequest_Request{
					Digest: &digestB,
					Data:   []byte("bar"),
				},
			},
			DigestFunction: repb.DigestFunction_SHA256,
		})
	require.NoError(t, err)
	unaryRequestCount.Store(0)

	abcReq := repb.FindMissingBlobsRequest{
		BlobDigests:    []*repb.Digest{&digestA, &digestB, &digestC},
		DigestFunction: repb.DigestFunction_SHA256,
	}
	resp, err := localClient.FindMissingBlobs(ctx, &abcReq)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
	require.Equal(t, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", resp.MissingBlobDigests[0])
	require.Equal(t, int32(1), unaryRequestCount.Load())
	require.Equal(t, int32(0), streamRequestCount.Load())

	resp, err = localClient.FindMissingBlobs(ctx, &abcReq)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MissingBlobDigests))
	require.Equal(t, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", resp.MissingBlobDigests[0])
	require.Equal(t, int32(2), unaryRequestCount.Load())
	require.Equal(t, int32(0), streamRequestCount.Load())

	abReq := repb.FindMissingBlobsRequest{
		BlobDigests:    []*repb.Digest{&digestA, &digestB},
		DigestFunction: repb.DigestFunction_SHA256,
	}
	resp, err = localClient.FindMissingBlobs(ctx, &abReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(resp.MissingBlobDigests))
	require.Equal(t, int32(2), unaryRequestCount.Load())
	require.Equal(t, int32(0), streamRequestCount.Load())
}
