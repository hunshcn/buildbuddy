package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	guuid "github.com/google/uuid"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

type remoteReadExpectation int

const (
	never remoteReadExpectation = iota
	once
	always
)

func requestCountingStreamInterceptor(count *atomic.Int32) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		count.Add(1)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func runRemoteByteStreamServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) (bspb.ByteStreamClient, *atomic.Int32) {
	remoteByteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServver, runFunc := testenv.RegisterLocalGRPCServer(env)
	bspb.RegisterByteStreamServer(grpcServver, remoteByteStreamServer)
	go runFunc()
	streamRequestCounter := atomic.Int32{}
	conn, err := testenv.LocalGRPCConn(ctx, env,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)),
		// byte_stream APIs are only stremaing, so unary interceptor is unnecessary.
		grpc.WithStreamInterceptor(requestCountingStreamInterceptor(&streamRequestCounter)))
	require.NoError(t, err)
	return bspb.NewByteStreamClient(conn), &streamRequestCounter
}

func runByteStreamServerProxy(ctx context.Context, remoteEnv *testenv.TestEnv, localEnv *testenv.TestEnv, t *testing.T) (*grpc.ClientConn, *atomic.Int32) {
	bsClient, requestCount := runRemoteByteStreamServer(ctx, remoteEnv, t)
	localEnv.SetByteStreamClient(bsClient)
	byteStreamServer, err := NewByteStreamServerProxy(localEnv)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(localEnv)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	// TODO(vadim): can we remove the MsgSize override from the default options?
	clientConn, err := testenv.LocalGRPCConn(ctx, localEnv, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	if err != nil {
		t.Error(err)
	}

	return clientConn, requestCount
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	remoteEnv := testenv.GetTestEnv(t)
	localEnv := testenv.GetTestEnv(t)
	clientConn, requestCount := runByteStreamServerProxy(ctx, remoteEnv, localEnv, t)
	bsClient := bspb.NewByteStreamClient(clientConn)

	// randStr := func(i int) string {
	// 	rstr, err := random.RandomString(i)
	// 	if err != nil {
	// 		t.Error(err)
	// 	}
	// 	return rstr
	// }
	// simpleRn, simpleBuf := testdigest.NewRandomResourceAndBuf(t, 1234, rspb.CacheType_CAS, "")
	cases := []struct {
		wantError error
		cacheType rspb.CacheType
		size      int64
		offset    int64
		remotes   remoteReadExpectation
	}{
		{ // Simple Read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    0,
			remotes:   once,
		},
		{ // Large Read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1000 * 1000 * 100,
			offset:    0,
			remotes:   once,
		},
		{ // 0 length read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      0,
			offset:    0,
			remotes:   never,
		},
		{ // Offset
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    1,
			remotes:   always,
		},
		{ // Max offset
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    1234,
			remotes:   always,
		},
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, localEnv)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	for _, tc := range cases {
		rn, data := testdigest.NewRandomResourceAndBuf(t, tc.size, tc.cacheType, "")
		// Set the value in the remote cache.
		if err := remoteEnv.GetCache().Set(ctx, rn, data); err != nil {
			t.Fatal(err)
		}

		// Read it through the proxied bytestream API twice, ensuring that it's
		// only read from the remote byestream server on the first request.
		for i := 1; i < 4; i++ {
			var buf bytes.Buffer
			gotErr := byte_stream.ReadBlob(ctx, bsClient, digest.ResourceNameFromProto(rn), &buf, tc.offset)
			if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
				t.Errorf("got %v; want %v", gotErr, tc.wantError)
				//			continue
			}
			got := buf.String()
			if got != string(data[tc.offset:]) {
				t.Errorf("got %.100s; want %.100s", got, data)
			}

			switch tc.remotes {
			case never:
				require.Equal(t, int32(0), requestCount.Load())
			case once:
				require.Equal(t, int32(1), requestCount.Load())
			case always:
				require.Equal(t, int32(i), requestCount.Load())
			}
		}
		requestCount.Store(0)
	}
}

func TestWrite(t *testing.T) {
	// Make blob big enough to require multiple chunks to upload
	rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, 5e6, "" /*instanceName*/)
	compressedBlob := compression.CompressZstd(nil, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d := rn.GetDigest()

	testCases := []struct {
		name                        string
		uploadResourceName          string
		uploadBlob                  []byte
		downloadResourceName        string
		expectedDownloadCompression repb.Compressor_Value
		bazelVersion                string
	}{
		{
			name:                        "Write compressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
		{
			name:                        "Write compressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", newUUID(t), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
	}
	for _, tc := range testCases {
		run := func(t *testing.T) {
			remoteEnv := testenv.GetTestEnv(t)
			te := testenv.GetTestEnv(t)
			ctx := byte_stream.WithBazelVersion(t, context.Background(), tc.bazelVersion)

			// Enable compression
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			te.SetCache(&testcompression.CompressionCache{Cache: te.GetCache()})

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(t, err)
			clientConn, requestCount := runByteStreamServerProxy(ctx, remoteEnv, te, t)
			bsClient := bspb.NewByteStreamClient(clientConn)

			// Upload the blob
			byte_stream.MustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, true)

			// Read back the blob we just uploaded
			downloadBuf := []byte{}
			downloadStream, err := bsClient.Read(ctx, &bspb.ReadRequest{
				ResourceName: tc.downloadResourceName,
			})
			require.NoError(t, err, tc.name)
			for {
				res, err := downloadStream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, tc.name)
				downloadBuf = append(downloadBuf, res.Data...)
			}

			if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
				require.Equal(t, blob, downloadBuf, tc.name)
			} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
				decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
				require.NoError(t, err, tc.name)
				require.Equal(t, blob, decompressedDownloadBuf, tc.name)
			}

			// There should've been 1 request from the proxy to the backing cache.
			require.Equal(t, int32(1), requestCount.Load())

			// Now try uploading a duplicate. The duplicate upload should not fail,
			// and we should still be able to read the blob.
			byte_stream.MustUploadChunked(t, ctx, bsClient, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)

			downloadBuf = []byte{}
			downloadStream, err = bsClient.Read(ctx, &bspb.ReadRequest{
				ResourceName: tc.downloadResourceName,
			})
			require.NoError(t, err, tc.name)
			for {
				res, err := downloadStream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, tc.name)
				downloadBuf = append(downloadBuf, res.Data...)
			}

			if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
				require.Equal(t, blob, downloadBuf, tc.name)
			} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
				decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
				require.NoError(t, err, tc.name)
				require.Equal(t, blob, decompressedDownloadBuf, tc.name)
			}

			// There should've been 1 more request from the proxy to the backing cache.
			require.Equal(t, int32(2), requestCount.Load())
		}

		// Run all tests for both bazel 5.0.0 (which introduced compression) and
		// 5.1.0 (which added support for short-circuiting duplicate compressed
		// uploads)
		tc.bazelVersion = "5.0.0"
		t.Run(tc.name+", bazel "+tc.bazelVersion, run)

		// tc.bazelVersion = "5.1.0"
		// t.Run(tc.name+", bazel "+tc.bazelVersion, run)
	}
}

func newUUID(t *testing.T) string {
	uuid, err := guuid.NewRandom()
	require.NoError(t, err)
	return uuid.String()
}
