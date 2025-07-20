package metadata_store

type File struct {
	Size   int64               `json:"size"`
	Chunks map[string]struct{} `json:"chunks"`
}

type Chunk struct {
	Nodes    map[string]struct{} `json:"nodes"`
	Offset   int64               `json:"offset"`
	Size     int64               `json:"size"`
	FilePath string              `json:"file_path"`
}

type MetadataStoreInterface interface {
	GetFile(key string) (*File, error)
	PutFile(key string, value *File) error
	GetChunk(key string) (*Chunk, error)
	PutChunk(key string, value *Chunk) error
	Close() error
}

func NewMetadataStore() (MetadataStoreInterface, error) {
	return NewBoltMetadataStore("./data")
}
