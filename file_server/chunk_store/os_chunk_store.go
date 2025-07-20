package chunk_store

import (
	"crypto/sha256"
	"dfs/file_server/proto"
	"fmt"
	"os"
	"path/filepath"
)

type OSChunkStore struct {
	directory string
}

func NewOSChunkStore(directory string) (ChunkStoreInterface, error) {
	if err := os.MkdirAll(directory, 0744); err != nil {
		return nil, fmt.Errorf("failed to create chunk directory: %v", err)
	}
	return &OSChunkStore{directory: directory}, nil
}

func (c *OSChunkStore) Write(chunk *proto.UploadChunkRequest) (string, string, error) {
	if err := ValidateChunk(chunk.Content); err != nil {
		return "", "", err
	}

	node, err := os.Hostname()
	if err != nil {
		return "", "", fmt.Errorf("failed to get hostname: %v", err)
	}

	hash := sha256.Sum256(chunk.Content)
	hashStr := fmt.Sprintf("%x", hash)

	chunkPath := filepath.Join(c.directory, hashStr)

	if _, err := os.Stat(chunkPath); err == nil {
		return hashStr, node, nil
	}

	if err := os.WriteFile(chunkPath, chunk.Content, 0744); err != nil {
		return "", "", fmt.Errorf("failed to write chunk file: %v", err)
	}

	return hashStr, node, nil
}

func (c *OSChunkStore) Read(chunkID string) ([]byte, error) {
	chunkPath := filepath.Join(c.directory, chunkID)

	content, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file: %v", err)
	}

	return content, nil
}
