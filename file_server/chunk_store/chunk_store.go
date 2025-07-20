package chunk_store

import (
	"dfs/file_server/proto"
	"fmt"
)

const (
	MaxChunkSize = 1024 * 1024 * 3 // 3MiB max chunk size
)

type ChunkStoreInterface interface {
	Write(chunk *proto.UploadChunkRequest) (string, string, error)
	Read(chunkID string) ([]byte, error)
	Delete(chunkID string) error
}

func NewChunkStore() (ChunkStoreInterface, error) {
	return NewOSChunkStore("./data")
}

func ValidateChunk(data []byte) error {
	if len(data) > MaxChunkSize {
		return fmt.Errorf("chunk size %d exceeds maximum %d", len(data), MaxChunkSize)
	}
	if len(data) == 0 {
		return fmt.Errorf("chunk cannot be empty")
	}
	return nil
}
