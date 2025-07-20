package metadata_store

import (
	"bytes"
	"encoding/gob"
)

type File struct {
	Size   int64    `json:"size"`
	Chunks []string `json:"chunks"`
}

type Chunk struct {
	Nodes    map[string]struct{} `json:"nodes"`
	Offset   int64               `json:"offset"`
	Size     int64               `json:"size"`
	FilePath string              `json:"file_path"`
	Hash     string              `json:"hash"`
}

func serialize[T any](value *T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deserialize[T any](data []byte) (*T, error) {
	var value T
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return &value, nil
}

type MetadataStoreInterface interface {
	GetFile(key string) (*File, error)
	GetChunk(key string) (*Chunk, error)
	PutChunk(chunk *Chunk) error
	GetChunksForFile(filePath string) (map[string]*Chunk, error)
	Close() error
}

func NewMetadataStore() (MetadataStoreInterface, error) {
	return NewBoltMetadataStore("./data")
}
