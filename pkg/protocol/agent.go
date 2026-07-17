package protocol

type AgentUpsertRequest struct {
	Kind            string  `json:"kind"`
	Name            string  `json:"name"`
	Version         string  `json:"version"`
	Attrs           Attrs   `json:"attrs"`
	Endpoint        *string `json:"endpoint"`
	EndpointVersion *int64  `json:"endpoint_version"`
	TLSSPKISHA256   *string `json:"tls_spki_sha256"`
}

type Agent struct {
	ID              string  `json:"id"`
	Kind            string  `json:"kind"`
	Name            string  `json:"name"`
	Status          string  `json:"status"`
	Endpoint        *string `json:"endpoint"`
	EndpointVersion *int64  `json:"endpoint_version"`
	TLSSPKISHA256   *string `json:"tls_spki_sha256"`
	EndpointStatus  string  `json:"endpoint_status"`
	LastHeartbeatAt *int64  `json:"last_heartbeat_at"`
}

type AgentResponse struct {
	Agent                 Agent `json:"agent"`
	HeartbeatAfterSeconds int64 `json:"heartbeat_after_seconds"`
	ServerTime            int64 `json:"server_time"`
}

type AgentHeartbeatRequest struct {
	Version string `json:"version"`
	Attrs   Attrs  `json:"attrs"`
}

type SigningKey struct {
	KeyID            string `json:"kid"`
	Algorithm        string `json:"alg"`
	PublicKeyEd25519 string `json:"public_key_ed25519"`
	NotBefore        int64  `json:"not_before"`
	NotAfter         int64  `json:"not_after"`
}

type OwnerAssignment struct {
	Route
	Status              string             `json:"status"`
	OwnerLeaseExpiresAt int64              `json:"owner_lease_expires_at"`
	SourceURI           *string            `json:"source_uri"`
	SourceFormat        *string            `json:"source_format"`
	SourceETag          *string            `json:"source_etag"`
	SourceDownloadURL   *string            `json:"source_download_url"`
	SourceURLExpiresAt  *int64             `json:"source_url_expires_at"`
	Checkpoint          *CheckpointRestore `json:"checkpoint"`
}

// CheckpointRestore is the immutable, read-only checkpoint selected by the
// tracker for one recovering owner assignment. URI and object credentials stay
// inside the tracker; the shard only receives an exact-object download URL.
type CheckpointRestore struct {
	URI          string `json:"-"`
	Generation   int64  `json:"generation"`
	Sequence     int64  `json:"sequence"`
	Format       string `json:"format"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
	CreatedAt    int64  `json:"created_at"`
	DownloadURL  string `json:"download_url"`
	URLExpiresAt int64  `json:"url_expires_at"`
}

type AgentHeartbeatResponse struct {
	ServerTime            int64             `json:"server_time"`
	HeartbeatAfterSeconds int64             `json:"heartbeat_after_seconds"`
	OwnerAssignments      []OwnerAssignment `json:"owner_assignments"`
	SigningKeys           []SigningKey      `json:"signing_keys"`
}

type ShardLoadResultRequest struct {
	Generation int64  `json:"generation"`
	Success    bool   `json:"success"`
	ErrorCode  string `json:"error_code"`
}

type ShardLoadResultResponse struct {
	ProjectID  string `json:"project_id"`
	ShardID    string `json:"shard_id"`
	Generation int64  `json:"generation"`
	Status     string `json:"status"`
}

type ShardRecoveryResultRequest struct {
	Generation int64  `json:"generation"`
	Success    bool   `json:"success"`
	ErrorCode  string `json:"error_code"`
}

type ShardRecoveryResultResponse struct {
	ProjectID  string `json:"project_id"`
	ShardID    string `json:"shard_id"`
	Generation int64  `json:"generation"`
	Status     string `json:"status"`
}
