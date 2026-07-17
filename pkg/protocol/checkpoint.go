package protocol

type BeginCheckpointRequest struct {
	Generation int64  `json:"generation"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
}

type BeginCheckpointResponse struct {
	ProjectID     string `json:"project_id"`
	ShardID       string `json:"shard_id"`
	Generation    int64  `json:"generation"`
	UploadID      string `json:"upload_id"`
	PartSizeBytes int64  `json:"part_size_bytes"`
	CreatedAt     int64  `json:"created_at"`
}

type CheckpointPartURLRequest struct {
	Generation int64  `json:"generation"`
	PartNumber int32  `json:"part_number"`
	SizeBytes  int64  `json:"size_bytes"`
	ContentMD5 string `json:"content_md5"`
}

type CheckpointPartURLResponse struct {
	UploadID   string            `json:"upload_id"`
	PartNumber int32             `json:"part_number"`
	URL        string            `json:"url"`
	Headers    map[string]string `json:"headers"`
	ExpiresAt  int64             `json:"expires_at"`
}

type CheckpointPart struct {
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

type CompleteCheckpointRequest struct {
	Generation int64            `json:"generation"`
	Parts      []CheckpointPart `json:"parts"`
}

type AbortCheckpointRequest struct {
	Generation int64 `json:"generation"`
}

type CheckpointResponse struct {
	ProjectID  string `json:"project_id"`
	ShardID    string `json:"shard_id"`
	Generation int64  `json:"generation"`
	Sequence   int64  `json:"sequence"`
	URI        string `json:"uri"`
	Format     string `json:"format"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes"`
	CreatedAt  int64  `json:"created_at"`
}

type AbortCheckpointResponse struct {
	UploadID string `json:"upload_id"`
	Status   string `json:"status"`
}
