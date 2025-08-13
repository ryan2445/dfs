package metadata_store

import (
	"google.golang.org/protobuf/proto"
	pb "dfs/file_server/proto"
)


func serializeFile(file *pb.File) ([]byte, error) {
	return proto.Marshal(file)
}

func deserializeFile(data []byte) (*pb.File, error) {
	file := &pb.File{}
	if err := proto.Unmarshal(data, file); err != nil {
		return nil, err
	}
	return file, nil
}

func serializeChunk(chunk *pb.Chunk) ([]byte, error) {
	return proto.Marshal(chunk)
}

func deserializeChunk(data []byte) (*pb.Chunk, error) {
	chunk := &pb.Chunk{}
	if err := proto.Unmarshal(data, chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

type MetadataStoreInterface interface {
	GetFile(key string) (*pb.File, error)
	GetChunk(key string) (*pb.Chunk, error)
	PutChunk(chunk *pb.Chunk) error
	DeleteChunk(chunkID string, filePath string) error
	GetChunksForFile(filePath string) (map[string]*pb.Chunk, error)
	Close() error
}

func NewMetadataStore() (MetadataStoreInterface, error) {
	return NewBoltMetadataStore("./data")
}
