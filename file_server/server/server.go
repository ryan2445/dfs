package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"dfs/file_server/chunk_store"
	"dfs/file_server/metadata_store"
	"dfs/file_server/proto"

	"google.golang.org/grpc"
)

type FileServer struct {
	proto.UnimplementedFileServiceServer
	metadataStore metadata_store.MetadataStoreInterface
	chunkStore    chunk_store.ChunkStoreInterface
}

func StartServer() error {
	metadataStore, err := metadata_store.NewMetadataStore()
	if err != nil {
		return fmt.Errorf("failed to create metadata store: %v", err)
	}
	defer metadataStore.Close()

	chunkStore, err := chunk_store.NewChunkStore()
	if err != nil {
		return fmt.Errorf("failed to create chunk store: %v", err)
	}

	server := grpc.NewServer()
	fileServer := &FileServer{metadataStore: metadataStore, chunkStore: chunkStore}
	proto.RegisterFileServiceServer(server, fileServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("Shutting down gRPC server...")
		server.GracefulStop()
	}()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	fmt.Println("gRPC file server starting on :50051")
	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

func (s *FileServer) UploadChunk(ctx context.Context, req *proto.UploadChunkRequest) (*proto.UploadChunkResponse, error) {
	log.Printf("received UploadChunkRequest: filepath=%s offset=%d", req.FilePath, req.Offset)

	chunkID, nodeID, err := s.chunkStore.Write(req)
	if err != nil {
		return &proto.UploadChunkResponse{
			Success: false,
			Message: fmt.Sprintf("failed to write chunk: %v", err),
		}, nil
	}

	log.Printf("chunkID: %s, nodeID: %s", chunkID, nodeID)

	if err := s.metadataStore.PutChunk(chunkID, &metadata_store.Chunk{
		Nodes:    map[string]struct{}{nodeID: {}},
		Offset:   req.Offset,
		Size:     int64(len(req.Content)),
		FilePath: req.FilePath,
	}); err != nil {
		return &proto.UploadChunkResponse{
			Success: false,
			Message: fmt.Sprintf("failed to store chunk mapping: %v", err),
		}, nil
	}

	return &proto.UploadChunkResponse{
		Success: true,
		Message: fmt.Sprintf("chunk uploaded successfully: %s", chunkID),
	}, nil
}

func (s *FileServer) ListChunks(ctx context.Context, req *proto.ListChunksRequest) (*proto.ListChunksResponse, error) {
	log.Printf("received ListChunksRequest for file: %s", req.FilePath)

	chunks, err := s.metadataStore.GetChunksForFile(req.FilePath)
	if err != nil {
		return &proto.ListChunksResponse{
			Success: false,
			Message: fmt.Sprintf("failed to get chunks for file: %v", err),
		}, nil
	}

	var chunkInfos []*proto.ChunkInfo
	for chunkID, chunk := range chunks {
		chunkInfos = append(chunkInfos, &proto.ChunkInfo{
			ChunkId: chunkID,
			Offset:  chunk.Offset,
			Size:    chunk.Size,
		})
	}

	sort.Slice(chunkInfos, func(i, j int) bool {
		return chunkInfos[i].Offset < chunkInfos[j].Offset
	})

	return &proto.ListChunksResponse{
		Chunks:  chunkInfos,
		Success: true,
		Message: fmt.Sprintf("found %d chunks for file", len(chunkInfos)),
	}, nil
}

func (s *FileServer) DownloadChunk(ctx context.Context, req *proto.DownloadChunkRequest) (*proto.DownloadChunkResponse, error) {
	log.Printf("received DownloadChunkRequest for chunk: %s", req.ChunkId)

	content, err := s.chunkStore.Read(req.ChunkId)
	if err != nil {
		return &proto.DownloadChunkResponse{
			Success: false,
			Message: fmt.Sprintf("failed to read chunk: %v", err),
		}, nil
	}

	return &proto.DownloadChunkResponse{
		Success: true,
		Message: fmt.Sprintf("chunk downloaded successfully: %s", req.ChunkId),
		Content: content,
	}, nil
}
