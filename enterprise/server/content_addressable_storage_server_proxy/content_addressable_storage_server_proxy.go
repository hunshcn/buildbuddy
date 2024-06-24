package content_addressable_storage_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type CASServerProxy struct {
	env          environment.Env
	localServer  content_addressable_storage_server.ContentAddressableStorageServer
	remoteClient repb.ContentAddressableStorageClient
}

func Register(env *real_environment.RealEnv) error {
	casServer, err := NewCASServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServerProxy: %s", err)
	}
	env.SetCASServer(casServer)
	return nil
}

func NewCASServerProxy(env environment.Env) (*CASServerProxy, error) {
	localServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		return nil, err
	}
	remoteClient := env.GetContentAddressableStorageClient()
	if remoteClient == nil {
		return nil, fmt.Errorf("A ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	return &CASServerProxy{
		env:          env,
		localServer:  *localServer,
		remoteClient: remoteClient,
	}, nil
}

func (s *CASServerProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	resp, err := s.localServer.FindMissingBlobs(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.MissingBlobDigests) == 0 {
		return resp, nil
	}
	remoteReq := repb.FindMissingBlobsRequest{
		InstanceName:   req.InstanceName,
		BlobDigests:    resp.MissingBlobDigests,
		DigestFunction: req.DigestFunction,
	}
	return s.remoteClient.FindMissingBlobs(ctx, &remoteReq)
}

func (s *CASServerProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	go s.localServer.BatchUpdateBlobs(context.Background(), req)
	return s.remoteClient.BatchUpdateBlobs(ctx, req)
}

func (s *CASServerProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	localResp, err := s.localServer.BatchReadBlobs(ctx, req)
	if err != nil {
		return s.batchReadBlobsRemote(ctx, req)
	}
	if len(localResp.Responses) == len(req.Digests) {
		return localResp, nil
	}
	localDigests := make([]*repb.Digest, len(localResp.Responses))
	for _, response := range localResp.Responses {
		if response.Status.Code == int32(codes.OK) {
			localDigests = append(localDigests, response.Digest)
		}
	}
	missing, _ := digest.Diff(req.Digests, localDigests)
	remoteReq := repb.BatchReadBlobsRequest{
		InstanceName:          req.InstanceName,
		Digests:               missing,
		AcceptableCompressors: req.AcceptableCompressors,
		DigestFunction:        req.DigestFunction,
	}
	remoteResp, err := s.batchReadBlobsRemote(ctx, &remoteReq)
	if err != nil {
		return nil, err
	}
	localResp.Responses = append(localResp.Responses, remoteResp.Responses...)
	return localResp, nil
}

func (s *CASServerProxy) batchReadBlobsRemote(ctx context.Context, readReq *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	readResp, err := s.remoteClient.BatchReadBlobs(ctx, readReq)
	if err != nil {
		return nil, err
	}
	updateReq := repb.BatchUpdateBlobsRequest{
		InstanceName:   readReq.InstanceName,
		DigestFunction: readReq.DigestFunction,
	}
	for _, response := range readResp.Responses {
		if response.Status.Code != int32(codes.OK) {
			// ????
			continue
		}
		updateReq.Requests = append(updateReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     response.Digest,
			Data:       response.Data,
			Compressor: response.Compressor,
		})
	}
	go s.localServer.BatchUpdateBlobs(context.Background(), &updateReq)
	return readResp, nil
}

// ooof
func (s *CASServerProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	remoteStream, err := s.remoteClient.GetTree(context.Background(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}
