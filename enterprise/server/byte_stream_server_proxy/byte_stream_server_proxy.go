package byte_stream_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Channel size to use for tee-ing writes to the local bytestream server.
	// This value should be somewhat large to prevent blocking remote writes if
	// the local server is slow for some reason.
	localWriteChannelSize = 256
)

type ByteStreamServerProxy struct {
	env          environment.Env
	localCache   interfaces.Cache
	localServer  byte_stream_server.ByteStreamServer
	localClient  bspb.ByteStreamClient
	remoteClient bspb.ByteStreamClient
}

func Register(env *real_environment.RealEnv) error {
	byteStreamServer, err := NewByteStreamServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(byteStreamServer)
	return nil
}

func NewByteStreamServerProxy(env environment.Env) (*ByteStreamServerProxy, error) {
	localCache := env.GetCache()
	if localCache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServerProxy")
	}
	localServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		return nil, err
	}
	remoteCache := env.GetByteStreamClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("A ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	return &ByteStreamServerProxy{
		env:          env,
		localCache:   localCache,
		localServer:  *localServer,
		remoteClient: remoteCache,
	}, nil
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	err := s.localServer.Read(req, stream)
	if status.IsNotFoundError(err) {
		return s.readRemote(req, stream)
	}
	return err
}

type localWriteStream struct {
	ctx         context.Context
	initialized bool
	offset      int64
	channel     chan *bspb.WriteRequest
}

func (s localWriteStream) Context() context.Context {
	return s.ctx
}
func (s localWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	return nil
}
func (s localWriteStream) Recv() (*bspb.WriteRequest, error) {
	req := <-s.channel
	return req, nil
}

func (s *ByteStreamServerProxy) readRemote(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteStream, err := s.remoteClient.Read(stream.Context(), req)
	if err != nil {
		return err
	}

	var localStream *localWriteStream = nil
	if req.ReadOffset == 0 {
		localStream = &localWriteStream{
			ctx:         stream.Context(),
			channel:     make(chan *bspb.WriteRequest, localWriteChannelSize),
			initialized: false,
			offset:      req.ReadOffset,
		}
		go func() {
			if err := s.localServer.WriteDirect(localStream); err != nil {
				log.Warningf("Error writing to local cache: %s", err)
			}
		}()
	}

	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			if localStream != nil {
				localStream.channel <- &bspb.WriteRequest{WriteOffset: localStream.offset, FinishWrite: true}
			}
			break
		}
		if err != nil {
			return err
		}
		if localStream != nil {
			localReq := &bspb.WriteRequest{WriteOffset: localStream.offset, Data: rsp.Data}
			if !localStream.initialized {
				// TODO(iain): fix
				localReq.ResourceName = req.ResourceName
				localStream.initialized = true
			}
			localStream.offset += int64(len(rsp.Data))
			localStream.channel <- localReq
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	localStream := localWriteStream{
		ctx:     stream.Context(),
		channel: make(chan *bspb.WriteRequest, localWriteChannelSize),
	}
	go s.localServer.WriteDirect(localStream)

	remoteStream, err := s.remoteClient.Write(stream.Context())
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		localStream.channel <- req
		writeDone := req.GetFinishWrite()
		if err := remoteStream.Send(req); err != nil {
			if err == io.EOF {
				writeDone = true
			} else {
				return err
			}
		}
		if writeDone {
			lastRsp, err := remoteStream.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(lastRsp)
		}
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return s.remoteClient.QueryWriteStatus(ctx, req)
}
