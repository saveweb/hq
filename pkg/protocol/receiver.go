package protocol

type ReceiverBatchRequest struct {
	SessionID string      `json:"session_id"`
	Jobs      []JobSpecV1 `json:"jobs"`
}

type ReceiverBatchResponse struct {
	ProjectID  string `json:"project_id"`
	ReceiverID string `json:"receiver_id"`
	ObjectURI  string `json:"object_uri"`
	Format     string `json:"format"`
	JobsCount  int64  `json:"jobs_count"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	CreatedAt  int64  `json:"created_at"`
}
