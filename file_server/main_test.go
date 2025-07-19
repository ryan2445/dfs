package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"dfs/file_server/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func init() {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	proto.RegisterFileServiceServer(s, &fileServer{})
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(fmt.Sprintf("Server exited with error: %v", err))
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func getTestClient(t *testing.T) proto.FileServiceClient {
	conn, err := grpc.NewClient("passthrough:target",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return proto.NewFileServiceClient(conn)
}

func TestUploadFile(t *testing.T) {
	client := getTestClient(t)

	stream, err := client.UploadFile(context.Background())
	if err != nil {
		t.Fatalf("UploadFile stream creation failed: %v", err)
	}

	err = stream.Send(&proto.UploadFileRequest{
		Data: &proto.UploadFileRequest_Metadata{
			Metadata: &proto.FileMetadata{
				Filename:    "test.txt",
				Size:        11,
				ContentType: "text/plain",
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to send metadata: %v", err)
	}

	err = stream.Send(&proto.UploadFileRequest{
		Data: &proto.UploadFileRequest_Chunk{
			Chunk: []byte("hello world"),
		},
	})
	if err != nil {
		t.Fatalf("Failed to send chunk: %v", err)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
}

func TestDownloadFile(t *testing.T) {
	client := getTestClient(t)

	stream, err := client.DownloadFile(context.Background(), &proto.DownloadFileRequest{
		FileId: "test-file-id",
	})
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Try to receive at least one message (even if empty)
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to receive from DownloadFile stream: %v", err)
	}
}

func TestListFiles(t *testing.T) {
	client := getTestClient(t)

	resp, err := client.ListFiles(context.Background(), &proto.ListFilesRequest{
		PageSize:  10,
		PageToken: "",
	})
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}

	if resp == nil {
		t.Fatal("ListFiles returned nil response")
	}
}

func TestDeleteFile(t *testing.T) {
	client := getTestClient(t)

	resp, err := client.DeleteFile(context.Background(), &proto.DeleteFileRequest{
		FileId: "test-file-id",
	})
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	if resp == nil {
		t.Fatal("DeleteFile returned nil response")
	}

	if resp.Success {
		t.Error("Expected DeleteFile to return success=false (not implemented)")
	}

	if resp.Message != "File deletion not implemented yet" {
		t.Errorf("Expected message 'File deletion not implemented yet', got: %s", resp.Message)
	}
}

func TestServerStartup(t *testing.T) {
	// Test that we can create a client and it connects successfully
	client := getTestClient(t)

	// Make a simple call to verify the server is working
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ListFiles(ctx, &proto.ListFilesRequest{})
	if err != nil {
		t.Fatalf("Server connectivity test failed: %v", err)
	}
}
