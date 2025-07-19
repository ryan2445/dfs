package server

import (
	"context"
	"fmt"
	"net"
	"testing"

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
	proto.RegisterFileServiceServer(s, &FileServer{})
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
