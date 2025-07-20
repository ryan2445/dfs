package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"dfs/file_server/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const chunkSize = 1024 * 1024 * 2 // 2MiB

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> <file_path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands: download-file, upload-file\n")
		os.Exit(1)
	}

	command := os.Args[1]
	filePath := os.Args[2]

	switch command {
	case "download-file":
		if err := downloadFile(filePath); err != nil {
			log.Fatalf("Failed to download file: %v", err)
		}
		fmt.Printf("Successfully downloaded file: %s\n", filePath)

	case "upload-file":
		if err := uploadFile(filePath); err != nil {
			log.Fatalf("Failed to upload file: %v", err)
		}
		fmt.Printf("Successfully uploaded file: %s\n", filePath)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		fmt.Fprintf(os.Stderr, "Commands: download-file, upload-file\n")
		os.Exit(1)
	}
}

type uploadChunk struct {
	data   []byte
	offset int64
	index  int
}

func uploadFile(filePath string) error {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := proto.NewFileServiceClient(conn)
	ctx := context.Background()

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}

	log.Printf("Uploading file: %s (size: %d bytes)", filePath, fileInfo.Size())

	maxWorkers := runtime.NumCPU()
	log.Printf("using %d workers", maxWorkers)
	workChan := make(chan uploadChunk, maxWorkers)
	resultChan := make(chan error, maxWorkers)
	var wg sync.WaitGroup

	for range maxWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range workChan {
				resp, err := client.UploadChunk(ctx, &proto.UploadChunkRequest{
					FilePath: filePath,
					Offset:   chunk.offset,
					Content:  chunk.data,
				})
				if err != nil {
					resultChan <- fmt.Errorf("failed to upload chunk %d: %v", chunk.index, err)
					continue
				}
				if !resp.Success {
					resultChan <- fmt.Errorf("server error uploading chunk %d: %s", chunk.index, resp.Message)
					continue
				}
				log.Printf("uploaded chunk %d (offset: %d, size: %d)", chunk.index, chunk.offset, len(chunk.data))
				resultChan <- nil
			}
		}()
	}

	go func() {
		defer close(workChan)
		offset := int64(0)
		chunkIndex := 1

		for {
			buffer := make([]byte, chunkSize)
			n, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				resultChan <- fmt.Errorf("failed to read file: %v", err)
				return
			}
			if n == 0 {
				break
			}

			workChan <- uploadChunk{
				data:   buffer[:n],
				offset: offset,
				index:  chunkIndex,
			}

			offset += int64(n)
			chunkIndex++

			if err == io.EOF {
				break
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	chunkCount := 0
	for err := range resultChan {
		if err != nil {
			return err
		}
		chunkCount++
	}

	log.Printf("successfully uploaded %d chunks", chunkCount)
	return nil
}

func downloadFile(filePath string) error {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := proto.NewFileServiceClient(conn)
	ctx := context.Background()

	log.Printf("Getting chunk list for file: %s", filePath)
	listResp, err := client.ListChunks(ctx, &proto.ListChunksRequest{
		FilePath: filePath,
	})
	if err != nil {
		return fmt.Errorf("failed to list chunks: %v", err)
	}
	if !listResp.Success {
		return fmt.Errorf("server error: %s", listResp.Message)
	}
	if len(listResp.Chunks) == 0 {
		return fmt.Errorf("no chunks found for file: %s", filePath)
	}

	log.Printf("Found %d chunks to download", len(listResp.Chunks))

	outputPath := filepath.Base(filePath)
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	maxWorkers := runtime.NumCPU()
	log.Printf("using %d workers", maxWorkers)
	workChan := make(chan *proto.ChunkInfo, len(listResp.Chunks))
	resultChan := make(chan error, len(listResp.Chunks))
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
				if !resp.Success {
					resultChan <- fmt.Errorf("failed to download chunk %s: %s", chunk.ChunkId, resp.Message)
					continue
				}

				fileMutex.Lock()
				_, writeErr := outputFile.WriteAt(resp.Content, chunk.Offset)
				fileMutex.Unlock()

				if writeErr != nil {
					resultChan <- fmt.Errorf("failed to write chunk %s to file: %v", chunk.ChunkId, writeErr)
					continue
				}

				log.Printf("Downloaded and wrote chunk (offset: %d, size: %d)", chunk.Offset, chunk.Size)
				resultChan <- nil
			}
		}()
	}

	go func() {
		for _, chunk := range listResp.Chunks {
			workChan <- chunk
		}
		close(workChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	completedChunks := 0
	for err := range resultChan {
		if err != nil {
			return err
		}
		completedChunks++
	}

	log.Printf("successfully downloaded %d chunks", completedChunks)
	return nil
}
