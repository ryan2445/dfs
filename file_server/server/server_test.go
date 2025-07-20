package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
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

	chunkHash := fmt.Sprintf("%x", sha256.Sum256(req.Content))
	chunkID := fmt.Sprintf("%s:%d", chunkHash, req.Offset)

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
		if !slices.Contains(file.Chunks, chunkID) {
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

	chunkPath := filepath.Join(testDataDir, chunkHash)
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		t.Errorf("Expected chunk file to exist at %s", chunkPath)
	} else if err != nil {
		t.Errorf("Error checking chunk file: %v", err)
	}
}

func TestListChunks(t *testing.T) {
	client := getTestClient(t)

	filePath := "/test/multipart_file.txt"
	chunks := []struct {
		offset  int64
		content string
	}{
		{0, "chunk1"},
		{6, "chunk2"},
		{12, "chunk3"},
	}

	var expectedChunkIDs []string
	for _, chunk := range chunks {
		req := &proto.UploadChunkRequest{
			FilePath: filePath,
			Offset:   chunk.offset,
			Content:  []byte(chunk.content),
		}
		resp, err := client.UploadChunk(context.Background(), req)
		if err != nil {
			t.Fatalf("UploadChunk failed: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Expected upload success, got: %s", resp.Message)
		}

		chunkHash := fmt.Sprintf("%x", sha256.Sum256(req.Content))
		chunkID := fmt.Sprintf("%s:%d", chunkHash, chunk.offset)
		expectedChunkIDs = append(expectedChunkIDs, chunkID)
	}

	listReq := &proto.ListChunksRequest{
		FilePath: filePath,
	}
	listResp, err := client.ListChunks(context.Background(), listReq)
	if err != nil {
		t.Fatalf("ListChunks failed: %v", err)
	}
	if !listResp.Success {
		t.Fatalf("Expected ListChunks success, got: %s", listResp.Message)
	}

	if len(listResp.Chunks) != len(chunks) {
		t.Errorf("Expected %d chunks, got %d", len(chunks), len(listResp.Chunks))
	}

	for i := 0; i < len(listResp.Chunks)-1; i++ {
		if listResp.Chunks[i].Offset >= listResp.Chunks[i+1].Offset {
			t.Errorf("Chunks not sorted by offset: chunk %d offset=%d, chunk %d offset=%d",
				i, listResp.Chunks[i].Offset, i+1, listResp.Chunks[i+1].Offset)
		}
	}

	expectedOffsets := []int64{0, 6, 12}
	expectedSizes := []int64{6, 6, 6}
	for i, chunkInfo := range listResp.Chunks {
		if chunkInfo.Offset != expectedOffsets[i] {
			t.Errorf("Chunk %d: expected offset=%d, got offset=%d", i, expectedOffsets[i], chunkInfo.Offset)
		}
		if chunkInfo.Size != expectedSizes[i] {
			t.Errorf("Chunk %d: expected size=%d, got size=%d", i, expectedSizes[i], chunkInfo.Size)
		}
		found := slices.Contains(expectedChunkIDs, chunkInfo.ChunkId)
		if !found {
			t.Errorf("Chunk %d: unexpected chunk ID %s", i, chunkInfo.ChunkId)
		}
	}
}

func TestDownloadChunk(t *testing.T) {
	client := getTestClient(t)

	testContent := []byte("test chunk content for download")
	uploadReq := &proto.UploadChunkRequest{
		FilePath: "/test/download_test.txt",
		Offset:   0,
		Content:  testContent,
	}
	uploadResp, err := client.UploadChunk(context.Background(), uploadReq)
	if err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}
	if !uploadResp.Success {
		t.Fatalf("Expected upload success, got: %s", uploadResp.Message)
	}

	chunkHash := fmt.Sprintf("%x", sha256.Sum256(testContent))
	chunkID := fmt.Sprintf("%s:%d", chunkHash, 0)

	downloadReq := &proto.DownloadChunkRequest{
		ChunkId: chunkID,
	}
	downloadResp, err := client.DownloadChunk(context.Background(), downloadReq)
	if err != nil {
		t.Fatalf("DownloadChunk failed: %v", err)
	}
	if !downloadResp.Success {
		t.Fatalf("Expected DownloadChunk success, got: %s", downloadResp.Message)
	}

	if string(downloadResp.Content) != string(testContent) {
		t.Errorf("Downloaded content doesn't match. Expected: %s, Got: %s",
			string(testContent), string(downloadResp.Content))
	}
}
