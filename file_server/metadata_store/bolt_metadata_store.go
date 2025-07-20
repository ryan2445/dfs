package metadata_store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var (
	FileMetadataBucket = []byte("file_metadata")
	ChunksBucket       = []byte("chunks")
	NodesBucket        = []byte("nodes")
)

type BoltMetadataStore struct {
	db *bolt.DB
}

func NewBoltMetadataStore(directory string) (*BoltMetadataStore, error) {
	if err := os.MkdirAll(directory, 0744); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	db, err := bolt.Open(filepath.Join(directory, "metadata.db"), 0744, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata database: %v", err)
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(FileMetadataBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(ChunksBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create buckets: %v", err)
	}

	return &BoltMetadataStore{db: db}, nil
}

func (metadata *BoltMetadataStore) Close() error {
	return metadata.db.Close()
}

func (metadata *BoltMetadataStore) GetFile(key string) (*File, error) {
	var fileMetadata *File

	if err := metadata.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(FileMetadataBucket)
		value := bucket.Get([]byte(key))
		if value == nil {
			return nil
		}
		return json.Unmarshal(value, &fileMetadata)
	}); err != nil {
		return nil, err
	}

	return fileMetadata, nil
}

func (metadata *BoltMetadataStore) PutFile(key string, value *File) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %v", err)
	}

	return metadata.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(FileMetadataBucket)
		return bucket.Put([]byte(key), data)
	})
}

func (metadata *BoltMetadataStore) GetChunk(key string) (*Chunk, error) {
	var chunkMetadata *Chunk

	if err := metadata.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ChunksBucket)
		value := bucket.Get([]byte(key))
		if value == nil {
			return nil
		}
		return json.Unmarshal(value, &chunkMetadata)
	}); err != nil {
		return nil, err
	}

	return chunkMetadata, nil
}

func (metadata *BoltMetadataStore) PutChunk(key string, value *Chunk) error {
	file, err := metadata.GetFile(value.FilePath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %v", err)
	}
	if file == nil {
		file = &File{
			Size:   0,
			Chunks: make(map[string]struct{}),
		}
	}
	if _, ok := file.Chunks[key]; !ok {
		file.Chunks[key] = struct{}{}
		file.Size += value.Size
		if err := metadata.PutFile(value.FilePath, file); err != nil {
			return fmt.Errorf("failed to update file metadata: %v", err)
		}
	}

	valueData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk mapping: %v", err)
	}

	return metadata.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ChunksBucket)
		return bucket.Put([]byte(key), valueData)
	})
}
