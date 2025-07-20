package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"

	"dfs/file_server/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	chunkSize  = 1024 * 1024 * 2 // 2MiB
	serverAddr = "localhost:50051"
)

func main() {
	command := os.Args[1]

	switch command {
	case "download-file":
		if len(os.Args) != 4 {
			printUsage(os.Args[0])
			os.Exit(1)
		}
		filePath := os.Args[2]
		outputPath := os.Args[3]
		if err := downloadFile(filePath, outputPath); err != nil {
			log.Fatalf("Failed to download file: %v", err)
		}
		fmt.Printf("Successfully downloaded file: %s to %s\n", filePath, outputPath)
	case "upload-file":
		if len(os.Args) != 3 {
			printUsage(os.Args[0])
			os.Exit(1)
		}
		filePath := os.Args[2]
		if err := uploadFile(filePath); err != nil {
			log.Fatalf("Failed to upload file: %v", err)
		}
		fmt.Printf("Successfully uploaded file: %s\n", filePath)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage(os.Args[0])
		os.Exit(1)
	}
}

func printUsage(progName string) {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> <args>\n", progName)
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  download-file <remote_path> <local_output_path>\n")
	fmt.Fprintf(os.Stderr, "  upload-file <local_file_path>\n")
}

func createClient() (proto.FileServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	return proto.NewFileServiceClient(conn), conn, nil
}

func startUploadWorkers(client proto.FileServiceClient, ctx context.Context, uploadQueue chan *proto.UploadChunkRequest, deleteQueue chan *proto.DeleteChunkRequest, resultChan chan error) {
	workers := runtime.NumCPU()
	log.Printf("using %d workers", workers)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for uploadRequest := range uploadQueue {
				_, err := client.UploadChunk(ctx, uploadRequest)
				if err != nil {
					resultChan <- fmt.Errorf("failed to upload chunk %d: %v", uploadRequest.Offset, err)
					continue
				}
				resultChan <- nil
			}
			for deleteRequest := range deleteQueue {
				_, err := client.DeleteChunk(ctx, deleteRequest)
				if err != nil {
					resultChan <- fmt.Errorf("failed to delete chunk %s: %v", deleteRequest, err)
					continue
				}
				resultChan <- nil
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()
}

func startDownloadWorkers(client proto.FileServiceClient, ctx context.Context, outputFile *os.File, workChan chan *proto.ChunkInfo, resultChan chan error) {
	maxWorkers := runtime.NumCPU()
	log.Printf("using %d workers", maxWorkers)
	var fileMutex sync.Mutex
	var wg sync.WaitGroup

	for range maxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range workChan {
				resp, err := client.DownloadChunk(ctx, &proto.DownloadChunkRequest{
					ChunkId: chunk.ChunkId,
				})
				if err != nil {
					resultChan <- fmt.Errorf("failed to download chunk %s: %v", chunk.ChunkId, err)
					continue
				}

				fileMutex.Lock()
				_, writeErr := outputFile.WriteAt(resp.Content, chunk.Offset)
				fileMutex.Unlock()

				if writeErr != nil {
					resultChan <- fmt.Errorf("failed to write chunk %s to file: %v", chunk.ChunkId, writeErr)
					continue
				}

				log.Printf("downloaded and wrote chunk (offset: %d, size: %d)", chunk.Offset, chunk.Size)
				resultChan <- nil
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()
}

func processFileChunks(filePath string, existingChunks map[string]struct{}, uploadQueue chan *proto.UploadChunkRequest, deleteQueue chan *proto.DeleteChunkRequest) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("failed to open file: %v", err)
		return
	}
	defer file.Close()

	currentChunkIDs := make(map[string]struct{})
	offset := int64(0)

	for {
		buffer := make([]byte, chunkSize)
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			log.Printf("failed to read file: %v", err)
			return
		}
		if n == 0 {
			break
		}

		chunkData := buffer[:n]
		chunkID := generateChunkID(chunkData, offset)
		currentChunkIDs[chunkID] = struct{}{}

		if _, exists := existingChunks[chunkID]; !exists {
			log.Printf("uploading chunk %s", chunkID)
			uploadQueue <- &proto.UploadChunkRequest{
				FilePath: filePath,
				Offset:   offset,
				Content:  chunkData,
			}
		}

		offset += int64(n)
		if err == io.EOF {
			break
		}
	}
	close(uploadQueue)

	for existing := range existingChunks {
		if _, exists := currentChunkIDs[existing]; !exists {
			log.Printf("deleting chunk %s", existing)
			deleteQueue <- &proto.DeleteChunkRequest{
				ChunkId: existing,
			}
		}
	}
	close(deleteQueue)
}

func generateChunkID(chunkData []byte, offset int64) string {
	return fmt.Sprintf("%x:%d", sha256.Sum256(chunkData), offset)
}

func waitForResults(resultChan chan error) error {
	completedChunks := 0
	for err := range resultChan {
		if err != nil {
			return err
		}
		completedChunks++
	}
	log.Printf("successfully processed %d chunks", completedChunks)
	return nil
}

func uploadFile(filePath string) error {
	client, conn, err := createClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := context.Background()

	existingChunks := make(map[string]struct{})
	listResp, err := client.ListChunks(ctx, &proto.ListChunksRequest{
		FilePath: filePath,
	})
	if err == nil {
		for _, chunk := range listResp.Chunks {
			existingChunks[chunk.ChunkId] = struct{}{}
		}
	}
	log.Printf("found %d existing chunks", len(existingChunks))

	uploadQueue := make(chan *proto.UploadChunkRequest)
	deleteQueue := make(chan *proto.DeleteChunkRequest)
	resultChan := make(chan error)

	startUploadWorkers(client, ctx, uploadQueue, deleteQueue, resultChan)

	go processFileChunks(filePath, existingChunks, uploadQueue, deleteQueue)

	return waitForResults(resultChan)
}

func downloadFile(filePath, outputPath string) error {
	client, conn, err := createClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := context.Background()

	log.Printf("Getting chunk list for file: %s", filePath)
	listResp, err := client.ListChunks(ctx, &proto.ListChunksRequest{
		FilePath: filePath,
	})
	if err != nil {
		return fmt.Errorf("failed to list chunks: %v", err)
	}
	if len(listResp.Chunks) == 0 {
		return fmt.Errorf("no chunks found for file: %s", filePath)
	}

	log.Printf("Found %d chunks to download", len(listResp.Chunks))

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	workChan := make(chan *proto.ChunkInfo, len(listResp.Chunks))
	resultChan := make(chan error, len(listResp.Chunks))

	startDownloadWorkers(client, ctx, outputFile, workChan, resultChan)

	go func() {
		for _, chunk := range listResp.Chunks {
			workChan <- chunk
		}
		close(workChan)
	}()

	return waitForResults(resultChan)
}
