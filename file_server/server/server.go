package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"dfs/file_server/proto"

	"google.golang.org/grpc"
)

type FileServer struct {
	proto.UnimplementedFileServiceServer
}

func StartServer() error {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	proto.RegisterFileServiceServer(s, &FileServer{})

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("Shutting down gRPC server...")
		s.GracefulStop()
	}()

	fmt.Println("gRPC file server starting on :50051")
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

func (s *FileServer) UploadFile(stream proto.FileService_UploadFileServer) error {
	log.Println("UploadFile called")

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		switch data := req.Data.(type) {
		case *proto.UploadFileRequest_Metadata:
			log.Printf("Received metadata: filename=%s, size=%d", data.Metadata.Filename, data.Metadata.Size)
		case *proto.UploadFileRequest_Chunk:
			log.Printf("Received chunk: %d bytes", len(data.Chunk))
		}
	}

	return stream.SendAndClose(&proto.UploadFileResponse{
		Message: "File uploaded successfully",
	})
}
