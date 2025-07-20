package metadata_store

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var (
	FileMetadataBucket = []byte("file_metadata")
	ChunksBucket       = []byte("chunks")
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

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(FileMetadataBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(ChunksBucket); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
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

	err := metadata.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(FileMetadataBucket)
		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("file not found: %s", key)
		}
		var err error
		fileMetadata, err = deserialize[File](value)
		return err
	})

	if err != nil {
		return nil, err
	}

	return fileMetadata, nil
}

func (metadata *BoltMetadataStore) GetChunk(key string) (*Chunk, error) {
	var chunkMetadata *Chunk

	err := metadata.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ChunksBucket)
		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("chunk not found: %s", key)
		}
		var err error
		chunkMetadata, err = deserialize[Chunk](value)
		return err
	})

	if err != nil {
		return nil, err
	}

	return chunkMetadata, nil
}

func (metadata *BoltMetadataStore) PutChunk(chunk *Chunk) error {
	chunkID := fmt.Sprintf("%s:%d", chunk.Hash, chunk.Offset)

	value, err := serialize(chunk)
	if err != nil {
		return fmt.Errorf("failed to serialize chunk mapping: %v", err)
	}

	return metadata.db.Update(func(tx *bolt.Tx) error {
		fileMetadataBucket := tx.Bucket(FileMetadataBucket)
		fileData := fileMetadataBucket.Get([]byte(chunk.FilePath))

		var file *File
		if fileData == nil {
			file = &File{
				Chunks: make(map[string]struct{}),
			}
		} else {
			file, err = deserialize[File](fileData)
			if err != nil {
				return fmt.Errorf("failed to deserialize file metadata: %v", err)
			}
		}

		file.Chunks[chunkID] = struct{}{}
		updatedFileData, err := serialize(file)
		if err != nil {
			return fmt.Errorf("failed to serialize file metadata: %v", err)
		}

		if err := fileMetadataBucket.Put([]byte(chunk.FilePath), updatedFileData); err != nil {
			return fmt.Errorf("failed to update file metadata: %v", err)
		}

		chunksBucket := tx.Bucket(ChunksBucket)
		return chunksBucket.Put([]byte(chunkID), value)
	})
}

func (metadata *BoltMetadataStore) GetChunksForFile(filePath string) (map[string]*Chunk, error) {
	file, err := metadata.GetFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file metadata: %v", err)
	}

	chunks := make(map[string]*Chunk, len(file.Chunks))

	err = metadata.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(ChunksBucket)
		for chunkID := range file.Chunks {
			value := bucket.Get([]byte(chunkID))
			if value == nil {
				return fmt.Errorf("chunk not found: %s", chunkID)
			}

			chunk, err := deserialize[Chunk](value)
			if err != nil {
				return fmt.Errorf("failed to deserialize chunk %s: %v", chunkID, err)
			}
			chunks[chunkID] = chunk
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return chunks, nil
}

func (metadata *BoltMetadataStore) DeleteChunk(chunkID string, filePath string) error {
	return metadata.db.Update(func(tx *bolt.Tx) error {
		fileMetadataBucket := tx.Bucket(FileMetadataBucket)
		fileData := fileMetadataBucket.Get([]byte(filePath))
		if fileData != nil {
			file, err := deserialize[File](fileData)
			if err != nil {
				return fmt.Errorf("failed to deserialize file metadata: %v", err)
			}

			delete(file.Chunks, chunkID)

			updatedFileData, err := serialize(file)
			if err != nil {
				return fmt.Errorf("failed to serialize file metadata: %v", err)
			}

			if err := fileMetadataBucket.Put([]byte(filePath), updatedFileData); err != nil {
				return fmt.Errorf("failed to update file metadata: %v", err)
			}
		}

		chunksBucket := tx.Bucket(ChunksBucket)
		return chunksBucket.Delete([]byte(chunkID))
	})
}
