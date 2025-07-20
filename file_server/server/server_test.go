package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"dfs/file_server/chunk_store"
	"dfs/file_server/metadata_store"
	"dfs/file_server/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024
const testDataDir = "./test_data"

var lis *bufconn.Listener
var metadataStore metadata_store.MetadataStoreInterface
var chunkStore chunk_store.ChunkStoreInterface

func init() {
	var err error

	os.RemoveAll(testDataDir)
	os.MkdirAll(testDataDir, 0744)

	metadataStore, err = metadata_store.NewBoltMetadataStore(testDataDir)
	if err != nil {
		panic(fmt.Sprintf("Failed to create metadata store: %v", err))
	}

	chunkStore, err = chunk_store.NewOSChunkStore(testDataDir)
	if err != nil {
		panic(fmt.Sprintf("Failed to create chunk store: %v", err))
	}

	lis = bufconn.Listen(bufSize)
	server := grpc.NewServer()
	fileServer := &FileServer{metadataStore: metadataStore, chunkStore: chunkStore}
	proto.RegisterFileServiceServer(server, fileServer)

	go func() {
		if err := server.Serve(lis); err != nil {
			panic(fmt.Sprintf("Server exited with error: %v", err))
		}
	}()
}

func getTestClient(t *testing.T) proto.FileServiceClient {
	conn, err := grpc.NewClient(
		"passthrough:target",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	t.Cleanup(func() { conn.Close() })

	return proto.NewFileServiceClient(conn)
}

func TestUploadChunk(t *testing.T) {
	client := getTestClient(t)

	req := &proto.UploadChunkRequest{
		FilePath: "/test/file.txt",
		Offset:   0,
		Content:  []byte("hello world"),
	}
	resp, err := client.UploadChunk(context.Background(), req)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected success=true, got success=%v", resp.Success)
	}

	hash := sha256.Sum256(req.Content)
	chunkID := fmt.Sprintf("%x", hash)

	file, err := metadataStore.GetFile(req.FilePath)
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}
	if file == nil {
		t.Errorf("Expected file metadata to exist for file %s", req.FilePath)
	} else {
		if file.Size != int64(len(req.Content)) {
			t.Errorf("Expected Size=%d, got Size=%d", len(req.Content), file.Size)
		}
		if _, ok := file.Chunks[chunkID]; !ok {
			t.Errorf("Expected chunk ID %s to be in file metadata", chunkID)
		}
	}

	chunk, err := metadataStore.GetChunk(chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}
	if chunk == nil {
		t.Errorf("Expected chunk metadata to exist for chunk ID %s", chunkID)
	} else {
		if chunk.FilePath != req.FilePath {
			t.Errorf("Expected FilePath=%s, got FilePath=%s", req.FilePath, chunk.FilePath)
		}
		if chunk.Offset != req.Offset {
			t.Errorf("Expected Offset=%d, got Offset=%d", req.Offset, chunk.Offset)
		}
		if chunk.Size != int64(len(req.Content)) {
			t.Errorf("Expected Size=%d, got Size=%d", len(req.Content), chunk.Size)
		}
	}

	chunkPath := filepath.Join(testDataDir, chunkID)
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		t.Errorf("Expected chunk file to exist at %s", chunkPath)
	} else if err != nil {
		t.Errorf("Error checking chunk file: %v", err)
	}
}
