package tracker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/objectstorage"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Config struct {
	AgentHeartbeatSeconds   int64
	OwnerLeaseSeconds       int64
	SessionHeartbeatSeconds int64
	SessionLeaseSeconds     int64
	AccessTokenTTLSeconds   int64
	SigningKeyNotBefore     int64
	SigningKeyNotAfter      int64
	SourceURLTTLSeconds     int64
	SourceURLSigner         SourceURLSigner
	CheckpointStore         objectstorage.CheckpointStore
	CheckpointPrefixURI     string
	CheckpointURLSigner     SourceURLSigner
	CheckpointURLTTLSeconds int64
	CheckpointPartURLTTL    int64
	CheckpointPartSizeBytes int64
	CheckpointMaxBytes      int64
}

type SourceURLSigner interface {
	PresignGet(context.Context, string, int64, time.Duration) (string, int64, error)
}

func DefaultConfig() Config {
	return Config{
		AgentHeartbeatSeconds: 30, OwnerLeaseSeconds: 120,
		SessionHeartbeatSeconds: 30, SessionLeaseSeconds: 120,
		AccessTokenTTLSeconds:   600,
		SourceURLTTLSeconds:     900,
		CheckpointURLTTLSeconds: 3600,
		CheckpointPartURLTTL:    3600, CheckpointPartSizeBytes: 8 << 20,
		CheckpointMaxBytes: 64 << 30,
	}
}

type Service struct {
	store           Store
	endpointChecker EndpointChecker
	signer          *access.Signer
	now             func() int64
	config          Config
	signingKey      protocol.SigningKey
}

func NewService(store Store, endpointChecker EndpointChecker, signer *access.Signer, now func() int64, config Config) (*Service, error) {
	if store == nil || endpointChecker == nil || signer == nil || now == nil {
		return nil, fmt.Errorf("tracker: missing service dependency")
	}
	if config.AgentHeartbeatSeconds < 1 || config.OwnerLeaseSeconds <= config.AgentHeartbeatSeconds ||
		config.SessionHeartbeatSeconds < 1 || config.SessionLeaseSeconds <= config.SessionHeartbeatSeconds ||
		config.AccessTokenTTLSeconds < 1 || config.AccessTokenTTLSeconds > access.MaxTTLSeconds ||
		(config.SourceURLSigner != nil && (config.SourceURLTTLSeconds < 60 || config.SourceURLTTLSeconds > 86400)) ||
		((config.CheckpointStore == nil) != (config.CheckpointPrefixURI == "") ||
			(config.CheckpointStore == nil) != (config.CheckpointURLSigner == nil)) ||
		(config.CheckpointStore != nil && (config.CheckpointPartURLTTL < 60 || config.CheckpointPartURLTTL > 86400 ||
			config.CheckpointURLTTLSeconds < 60 || config.CheckpointURLTTLSeconds > 86400 ||
			config.CheckpointPartSizeBytes < 5<<20 || config.CheckpointPartSizeBytes > 64<<20 ||
			config.CheckpointMaxBytes < 1 || config.CheckpointMaxBytes > 5<<40 ||
			config.CheckpointMaxBytes > config.CheckpointPartSizeBytes*10_000 ||
			!validS3Prefix(config.CheckpointPrefixURI))) {
		return nil, fmt.Errorf("tracker: invalid service configuration")
	}
	if config.SigningKeyNotBefore == 0 {
		config.SigningKeyNotBefore = now()
	}
	if config.SigningKeyNotAfter == 0 {
		config.SigningKeyNotAfter = config.SigningKeyNotBefore + 86400*365
	}
	return &Service{
		store: store, endpointChecker: endpointChecker, signer: signer, now: now, config: config,
		signingKey: protocol.SigningKey{
			KeyID: signer.KeyID(), Algorithm: "EdDSA",
			PublicKeyEd25519: base64.RawURLEncoding.EncodeToString(signer.PublicKey()),
			NotBefore:        config.SigningKeyNotBefore, NotAfter: config.SigningKeyNotAfter,
		},
	}, nil
}

func (s *Service) BeginCheckpoint(
	ctx context.Context,
	machineToken, agentID, projectID, shardID string,
	request protocol.BeginCheckpointRequest,
) (protocol.BeginCheckpointResponse, error) {
	user, err := s.authenticateCheckpointRequest(machineToken, ctx, agentID, projectID, shardID, request.Generation)
	if err != nil {
		return protocol.BeginCheckpointResponse{}, err
	}
	if s.config.CheckpointStore == nil || request.SizeBytes < 1 || request.SizeBytes > s.config.CheckpointMaxBytes ||
		!validSHA256(request.SHA256) {
		return protocol.BeginCheckpointResponse{}, invalidRequest("invalid or unsupported checkpoint request")
	}
	now := s.now()
	if _, err := s.store.GetCheckpointTarget(ctx, user.ID, agentID, projectID, shardID, request.Generation, now); err != nil {
		return protocol.BeginCheckpointResponse{}, err
	}
	current, err := s.store.GetCurrentCheckpointUpload(ctx, user.ID, agentID, projectID, shardID, request.Generation, now)
	if err != nil {
		return protocol.BeginCheckpointResponse{}, err
	}
	if current != nil && current.SizeBytes == request.SizeBytes && current.SHA256 == request.SHA256 {
		return s.checkpointUploadResponse(*current), nil
	}
	if current != nil {
		if err := s.config.CheckpointStore.AbortMultipart(ctx, current.URI, current.S3UploadID); err != nil {
			return protocol.BeginCheckpointResponse{}, fmt.Errorf("tracker: abort superseded checkpoint: %w", err)
		}
		if err := s.store.AbortCheckpoint(ctx, user.ID, agentID, projectID, shardID, current.ID, request.Generation, s.now()); err != nil {
			return protocol.BeginCheckpointResponse{}, err
		}
	}
	uploadID, err := randomControlID("cp_")
	if err != nil {
		return protocol.BeginCheckpointResponse{}, err
	}
	uri := checkpointObjectURI(s.config.CheckpointPrefixURI, projectID, shardID, request.Generation, uploadID)
	s3UploadID, err := s.config.CheckpointStore.CreateMultipart(ctx, uri)
	if err != nil {
		return protocol.BeginCheckpointResponse{}, fmt.Errorf("tracker: create checkpoint multipart: %w", err)
	}
	upload, err := s.store.ReserveCheckpoint(ctx, user.ID, agentID, CheckpointUpload{
		ProjectID: projectID, ShardID: shardID, Generation: request.Generation,
		ID: uploadID, S3UploadID: s3UploadID, URI: uri,
		SizeBytes: request.SizeBytes, SHA256: request.SHA256,
	}, s.now())
	if err != nil {
		abortContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = s.config.CheckpointStore.AbortMultipart(abortContext, uri, s3UploadID)
		cancel()
		return protocol.BeginCheckpointResponse{}, err
	}
	return s.checkpointUploadResponse(upload), nil
}

func (s *Service) PresignCheckpointPart(
	ctx context.Context,
	machineToken, agentID, projectID, shardID, uploadID string,
	request protocol.CheckpointPartURLRequest,
) (protocol.CheckpointPartURLResponse, error) {
	user, err := s.authenticateCheckpointRequest(machineToken, ctx, agentID, projectID, shardID, request.Generation)
	if err != nil {
		return protocol.CheckpointPartURLResponse{}, err
	}
	if s.config.CheckpointStore == nil || !queue.ValidateIdentifier(uploadID) || request.PartNumber < 1 ||
		request.PartNumber > 10_000 || request.SizeBytes < 1 || request.SizeBytes > 5<<30 ||
		!validMD5(request.ContentMD5) {
		return protocol.CheckpointPartURLResponse{}, invalidRequest("invalid checkpoint part request")
	}
	now := s.now()
	upload, err := s.store.GetCheckpointUpload(ctx, user.ID, agentID, projectID, shardID, uploadID, request.Generation, now)
	if err != nil {
		return protocol.CheckpointPartURLResponse{}, err
	}
	partCount := (upload.SizeBytes + s.config.CheckpointPartSizeBytes - 1) / s.config.CheckpointPartSizeBytes
	expectedSize := s.config.CheckpointPartSizeBytes
	if int64(request.PartNumber) == partCount {
		expectedSize = upload.SizeBytes - (partCount-1)*s.config.CheckpointPartSizeBytes
	}
	if int64(request.PartNumber) > partCount || request.SizeBytes != expectedSize {
		return protocol.CheckpointPartURLResponse{}, invalidRequest("checkpoint part does not match declared object size")
	}
	part, err := s.config.CheckpointStore.PresignUploadPart(
		ctx, upload.URI, upload.S3UploadID, request.PartNumber, request.SizeBytes,
		request.ContentMD5, now, time.Duration(s.config.CheckpointPartURLTTL)*time.Second,
	)
	if err != nil {
		return protocol.CheckpointPartURLResponse{}, fmt.Errorf("tracker: presign checkpoint part: %w", err)
	}
	return protocol.CheckpointPartURLResponse{
		UploadID: upload.ID, PartNumber: request.PartNumber,
		URL: part.URL, Headers: part.Headers, ExpiresAt: part.ExpiresAt,
	}, nil
}

func (s *Service) CompleteCheckpoint(
	ctx context.Context,
	machineToken, agentID, projectID, shardID, uploadID string,
	request protocol.CompleteCheckpointRequest,
) (protocol.CheckpointResponse, error) {
	user, err := s.authenticateCheckpointRequest(machineToken, ctx, agentID, projectID, shardID, request.Generation)
	if err != nil {
		return protocol.CheckpointResponse{}, err
	}
	parts, err := validateCheckpointParts(request.Parts)
	if s.config.CheckpointStore == nil || !queue.ValidateIdentifier(uploadID) || err != nil {
		return protocol.CheckpointResponse{}, invalidRequest("invalid checkpoint completion")
	}
	upload, err := s.store.GetCheckpointUpload(ctx, user.ID, agentID, projectID, shardID, uploadID, request.Generation, s.now())
	if err != nil {
		return protocol.CheckpointResponse{}, err
	}
	expectedParts := (upload.SizeBytes + s.config.CheckpointPartSizeBytes - 1) / s.config.CheckpointPartSizeBytes
	if int64(len(parts)) != expectedParts {
		return protocol.CheckpointResponse{}, invalidRequest("checkpoint part count does not match declared object size")
	}
	completeError := s.config.CheckpointStore.CompleteMultipart(ctx, upload.URI, upload.S3UploadID, parts)
	size, _, headError := s.config.CheckpointStore.Head(ctx, upload.URI)
	if completeError != nil && headError != nil {
		return protocol.CheckpointResponse{}, fmt.Errorf("tracker: complete checkpoint multipart: %w", completeError)
	}
	if headError != nil {
		return protocol.CheckpointResponse{}, fmt.Errorf("tracker: verify checkpoint object: %w", headError)
	}
	if size != upload.SizeBytes {
		return protocol.CheckpointResponse{}, fmt.Errorf("tracker: checkpoint object size mismatch")
	}
	checkpoint, err := s.store.PublishCheckpoint(
		ctx, user.ID, agentID, projectID, shardID, uploadID, request.Generation, s.now(),
	)
	if err != nil {
		return protocol.CheckpointResponse{}, err
	}
	return toCheckpointResponse(checkpoint), nil
}

func (s *Service) AbortCheckpoint(
	ctx context.Context,
	machineToken, agentID, projectID, shardID, uploadID string,
	request protocol.AbortCheckpointRequest,
) (protocol.AbortCheckpointResponse, error) {
	user, err := s.authenticateCheckpointRequest(machineToken, ctx, agentID, projectID, shardID, request.Generation)
	if err != nil {
		return protocol.AbortCheckpointResponse{}, err
	}
	if s.config.CheckpointStore == nil || !queue.ValidateIdentifier(uploadID) {
		return protocol.AbortCheckpointResponse{}, invalidRequest("invalid checkpoint abort")
	}
	upload, err := s.store.GetCheckpointUpload(ctx, user.ID, agentID, projectID, shardID, uploadID, request.Generation, s.now())
	if err != nil {
		return protocol.AbortCheckpointResponse{}, err
	}
	if err := s.config.CheckpointStore.AbortMultipart(ctx, upload.URI, upload.S3UploadID); err != nil {
		return protocol.AbortCheckpointResponse{}, fmt.Errorf("tracker: abort checkpoint multipart: %w", err)
	}
	if err := s.store.AbortCheckpoint(ctx, user.ID, agentID, projectID, shardID, uploadID, request.Generation, s.now()); err != nil {
		return protocol.AbortCheckpointResponse{}, err
	}
	return protocol.AbortCheckpointResponse{UploadID: uploadID, Status: "aborted"}, nil
}

func (s *Service) authenticateCheckpointRequest(
	machineToken string,
	ctx context.Context,
	agentID, projectID, shardID string,
	generation int64,
) (User, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return User{}, err
	}
	if !user.HasRole(RoleShardOwner) {
		return User{}, permissionDenied("shard_owner role required")
	}
	if !queue.ValidateIdentifier(agentID) || !queue.ValidateIdentifier(projectID) ||
		!queue.ValidateIdentifier(shardID) || generation < 1 {
		return User{}, invalidRequest("invalid checkpoint route")
	}
	return user, nil
}

func (s *Service) checkpointUploadResponse(upload CheckpointUpload) protocol.BeginCheckpointResponse {
	return protocol.BeginCheckpointResponse{
		ProjectID: upload.ProjectID, ShardID: upload.ShardID, Generation: upload.Generation,
		UploadID: upload.ID, PartSizeBytes: s.config.CheckpointPartSizeBytes, CreatedAt: upload.CreatedAt,
	}
}

func validateCheckpointParts(values []protocol.CheckpointPart) ([]objectstorage.CompletedPart, error) {
	if len(values) < 1 || len(values) > 10_000 {
		return nil, fmt.Errorf("invalid part count")
	}
	result := make([]objectstorage.CompletedPart, len(values))
	for index, value := range values {
		if value.PartNumber != int32(index+1) || value.ETag == "" || len(value.ETag) > 512 ||
			strings.ContainsAny(value.ETag, "\r\n") {
			return nil, fmt.Errorf("invalid part")
		}
		result[index] = objectstorage.CompletedPart{PartNumber: value.PartNumber, ETag: value.ETag}
	}
	return result, nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}

func validMD5(value string) bool {
	decoded, err := base64.StdEncoding.DecodeString(value)
	return err == nil && len(decoded) == 16
}

func validS3Prefix(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "s3" && parsed.Host != "" && parsed.User == nil &&
		parsed.RawQuery == "" && parsed.Fragment == "" && !strings.HasSuffix(parsed.Path, "/")
}

func checkpointObjectURI(prefix, projectID, shardID string, generation int64, uploadID string) string {
	project := base64.RawURLEncoding.EncodeToString([]byte(projectID))
	shard := base64.RawURLEncoding.EncodeToString([]byte(shardID))
	return prefix + "/" + project + "/" + shard + "/" + strconv.FormatInt(generation, 10) + "/" + uploadID + ".sqlite.zst"
}

func randomControlID(prefix string) (string, error) {
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("tracker: random control ID: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func toCheckpointResponse(value Checkpoint) protocol.CheckpointResponse {
	return protocol.CheckpointResponse{
		ProjectID: value.ProjectID, ShardID: value.ShardID, Generation: value.Generation,
		Sequence: value.Sequence, URI: value.URI, Format: value.Format,
		SHA256: value.SHA256, SizeBytes: value.SizeBytes, CreatedAt: value.CreatedAt,
	}
}

func (s *Service) UpsertAgent(ctx context.Context, machineToken, agentID string, request protocol.AgentUpsertRequest) (protocol.AgentResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.AgentResponse{}, err
	}
	if !queue.ValidateIdentifier(agentID) || request.Name == "" || len(request.Name) > 128 ||
		request.Version == "" || len(request.Version) > 64 || request.Attrs == nil {
		return protocol.AgentResponse{}, invalidRequest("invalid agent identity or fields")
	}
	if request.Kind != protocol.AgentKindShard && request.Kind != protocol.AgentKindWorker {
		return protocol.AgentResponse{}, invalidRequest("agent kind must be shard or worker")
	}
	if request.Kind == protocol.AgentKindShard && !user.HasRole(RoleShardOwner) {
		return protocol.AgentResponse{}, permissionDenied("shard_owner role required")
	}
	if request.Kind == protocol.AgentKindWorker && !user.HasRole(RoleWorker) {
		return protocol.AgentResponse{}, permissionDenied("worker role required")
	}

	endpointStatus := EndpointNotApplicable
	if request.Kind == protocol.AgentKindWorker {
		if request.Endpoint != nil || request.EndpointVersion != nil || request.TLSSPKISHA256 != nil {
			return protocol.AgentResponse{}, invalidRequest("worker agents cannot register an endpoint")
		}
	} else {
		if request.Endpoint == nil || request.EndpointVersion == nil || *request.EndpointVersion < 1 {
			return protocol.AgentResponse{}, invalidRequest("shard endpoint and positive endpoint version are required")
		}
		parsed, err := url.Parse(*request.Endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
			parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery {
			return protocol.AgentResponse{}, invalidRequest("invalid shard endpoint URI")
		}
		if parsed.Scheme == "http" && request.TLSSPKISHA256 != nil {
			return protocol.AgentResponse{}, invalidRequest("HTTP endpoint cannot have a TLS pin")
		}
		endpointStatus, err = s.endpointChecker.Check(ctx, agentID, *request.Endpoint, request.TLSSPKISHA256)
		if err != nil {
			return protocol.AgentResponse{}, err
		}
	}
	now := s.now()
	agent, err := s.store.UpsertAgent(ctx, AgentUpsert{
		ID: agentID, UserID: user.ID, Kind: request.Kind, Name: request.Name,
		Version: request.Version, Endpoint: request.Endpoint, EndpointVersion: request.EndpointVersion,
		TLSSPKISHA256: request.TLSSPKISHA256, EndpointStatus: endpointStatus,
		Attrs: cloneMap(request.Attrs), Now: now,
	})
	if err != nil {
		return protocol.AgentResponse{}, err
	}
	return protocol.AgentResponse{
		Agent: toProtocolAgent(agent), HeartbeatAfterSeconds: s.config.AgentHeartbeatSeconds,
		ServerTime: now,
	}, nil
}

func (s *Service) HeartbeatAgent(ctx context.Context, machineToken, agentID string, request protocol.AgentHeartbeatRequest) (protocol.AgentHeartbeatResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.AgentHeartbeatResponse{}, err
	}
	if !queue.ValidateIdentifier(agentID) || request.Version == "" || len(request.Version) > 64 || request.Attrs == nil {
		return protocol.AgentHeartbeatResponse{}, invalidRequest("invalid agent heartbeat")
	}
	agent, err := s.store.GetAgent(ctx, user.ID, agentID)
	if err != nil {
		return protocol.AgentHeartbeatResponse{}, err
	}
	if agent.Status == "revoked" {
		return protocol.AgentHeartbeatResponse{}, &Error{Code: protocol.ErrorAgentDisabled, Message: "agent is revoked"}
	}
	if agent.Kind == protocol.AgentKindShard && !user.HasRole(RoleShardOwner) ||
		agent.Kind == protocol.AgentKindWorker && !user.HasRole(RoleWorker) {
		return protocol.AgentHeartbeatResponse{}, permissionDenied("agent role is no longer granted")
	}
	endpointStatus := agent.EndpointStatus
	if agent.Kind == protocol.AgentKindShard {
		if agent.Endpoint == nil {
			return protocol.AgentHeartbeatResponse{}, fmt.Errorf("tracker: shard agent invariant has no endpoint")
		}
		endpointStatus, err = s.endpointChecker.Check(ctx, agent.ID, *agent.Endpoint, agent.TLSSPKISHA256)
		if err != nil {
			return protocol.AgentHeartbeatResponse{}, err
		}
	}
	now := s.now()
	heartbeat, err := s.store.HeartbeatAgent(ctx, user.ID, agentID, request.Version, cloneMap(request.Attrs),
		endpointStatus, user.HasRole(RoleShardOwner), user.HasRole(RoleWorker), now, now+s.config.OwnerLeaseSeconds)
	if err != nil {
		return protocol.AgentHeartbeatResponse{}, err
	}
	for index := range heartbeat.OwnerAssignments {
		assignment := &heartbeat.OwnerAssignments[index]
		switch assignment.Status {
		case ShardStatusLoading:
			if assignment.SourceURI == nil || assignment.SourceFormat == nil || *assignment.SourceFormat != "jobs-jsonl-zstd-v1" ||
				assignment.SourceETag == nil || *assignment.SourceETag == "" || assignment.Checkpoint != nil {
				return protocol.AgentHeartbeatResponse{}, fmt.Errorf("tracker: incomplete source assignment")
			}
			if s.config.SourceURLSigner == nil || s.config.SourceURLTTLSeconds < 60 || s.config.SourceURLTTLSeconds > 86400 {
				return protocol.AgentHeartbeatResponse{}, &Error{
					Code: protocol.ErrorUnsupportedOperation, Message: "source object storage is not configured",
				}
			}
			downloadURL, expiresAt, err := s.config.SourceURLSigner.PresignGet(
				ctx, *assignment.SourceURI, now, time.Duration(s.config.SourceURLTTLSeconds)*time.Second,
			)
			if err != nil {
				return protocol.AgentHeartbeatResponse{}, fmt.Errorf("tracker: presign source download: %w", err)
			}
			assignment.SourceDownloadURL = &downloadURL
			assignment.SourceURLExpiresAt = &expiresAt
		case ShardStatusRecovering:
			checkpoint := assignment.Checkpoint
			if assignment.SourceURI != nil || assignment.SourceFormat != nil || assignment.SourceETag != nil ||
				checkpoint == nil || checkpoint.URI == "" || checkpoint.Format != "sqlite-zstd-v1" ||
				checkpoint.Generation < 1 || checkpoint.Generation > assignment.Generation || checkpoint.Sequence < 1 ||
				checkpoint.SizeBytes < 1 || !validSHA256(checkpoint.SHA256) {
				return protocol.AgentHeartbeatResponse{}, fmt.Errorf("tracker: incomplete checkpoint recovery assignment")
			}
			if s.config.CheckpointURLSigner == nil {
				return protocol.AgentHeartbeatResponse{}, &Error{
					Code: protocol.ErrorUnsupportedOperation, Message: "checkpoint object storage is not configured",
				}
			}
			downloadURL, expiresAt, err := s.config.CheckpointURLSigner.PresignGet(
				ctx, checkpoint.URI, now, time.Duration(s.config.CheckpointURLTTLSeconds)*time.Second,
			)
			if err != nil {
				return protocol.AgentHeartbeatResponse{}, fmt.Errorf("tracker: presign checkpoint download: %w", err)
			}
			checkpoint.DownloadURL, checkpoint.URLExpiresAt, checkpoint.URI = downloadURL, expiresAt, ""
		}
	}
	keys := []protocol.SigningKey{}
	if heartbeat.Agent.Kind == protocol.AgentKindShard {
		keys = append(keys, s.signingKey)
	}
	return protocol.AgentHeartbeatResponse{
		ServerTime: now, HeartbeatAfterSeconds: s.config.AgentHeartbeatSeconds,
		OwnerAssignments: heartbeat.OwnerAssignments, SigningKeys: keys,
	}, nil
}

func (s *Service) CreateSession(ctx context.Context, machineToken, agentID string, request protocol.CreateSessionRequest) (protocol.SessionResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.SessionResponse{}, err
	}
	if !queue.ValidateIdentifier(agentID) || !queue.ValidateIdentifier(request.ProjectID) || request.Attrs == nil {
		return protocol.SessionResponse{}, invalidRequest("invalid session request")
	}
	if !user.HasRole(RoleWorker) {
		return protocol.SessionResponse{}, permissionDenied("worker role required")
	}
	now := s.now()
	session, err := s.store.CreateSession(ctx, user.ID, agentID, request.ProjectID, cloneMap(request.Attrs),
		now, now+s.config.SessionLeaseSeconds)
	if err != nil {
		return protocol.SessionResponse{}, err
	}
	return toSessionResponse(session, s.config.SessionHeartbeatSeconds), nil
}

func (s *Service) ReportShardLoad(
	ctx context.Context,
	machineToken, agentID, projectID, shardID string,
	request protocol.ShardLoadResultRequest,
) (protocol.ShardLoadResultResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.ShardLoadResultResponse{}, err
	}
	if !user.HasRole(RoleShardOwner) {
		return protocol.ShardLoadResultResponse{}, permissionDenied("shard_owner role required")
	}
	if !queue.ValidateIdentifier(agentID) ||
		!queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(shardID) || request.Generation < 1 ||
		(request.Success && request.ErrorCode != "") ||
		(!request.Success && (!queue.ValidateIdentifier(request.ErrorCode) || len(request.ErrorCode) > 64)) {
		return protocol.ShardLoadResultResponse{}, invalidRequest("invalid shard load result")
	}
	value, err := s.store.FinishShardLoad(
		ctx, user.ID, agentID, projectID, shardID, request.Generation,
		request.Success, request.ErrorCode, s.now(),
	)
	if err != nil {
		return protocol.ShardLoadResultResponse{}, err
	}
	return protocol.ShardLoadResultResponse{
		ProjectID: value.ProjectID, ShardID: value.ID,
		Generation: value.Generation, Status: value.Status,
	}, nil
}

func (s *Service) ReportShardRecovery(
	ctx context.Context,
	machineToken, agentID, projectID, shardID string,
	request protocol.ShardRecoveryResultRequest,
) (protocol.ShardRecoveryResultResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.ShardRecoveryResultResponse{}, err
	}
	if !user.HasRole(RoleShardOwner) {
		return protocol.ShardRecoveryResultResponse{}, permissionDenied("shard_owner role required")
	}
	if !queue.ValidateIdentifier(agentID) || !queue.ValidateIdentifier(projectID) ||
		!queue.ValidateIdentifier(shardID) || request.Generation < 1 ||
		(request.Success && request.ErrorCode != "") ||
		(!request.Success && (!queue.ValidateIdentifier(request.ErrorCode) || len(request.ErrorCode) > 64)) {
		return protocol.ShardRecoveryResultResponse{}, invalidRequest("invalid shard recovery result")
	}
	value, err := s.store.FinishShardRecovery(
		ctx, user.ID, agentID, projectID, shardID, request.Generation,
		request.Success, request.ErrorCode, s.now(),
	)
	if err != nil {
		return protocol.ShardRecoveryResultResponse{}, err
	}
	return protocol.ShardRecoveryResultResponse{
		ProjectID: value.ProjectID, ShardID: value.ID,
		Generation: value.Generation, Status: value.Status,
	}, nil
}

func (s *Service) HeartbeatSession(ctx context.Context, machineToken, agentID, sessionID string) (protocol.SessionResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.SessionResponse{}, err
	}
	if !queue.ValidateIdentifier(agentID) || !queue.ValidateIdentifier(sessionID) {
		return protocol.SessionResponse{}, invalidRequest("invalid session heartbeat")
	}
	if !user.HasRole(RoleWorker) {
		return protocol.SessionResponse{}, permissionDenied("worker role required")
	}
	now := s.now()
	session, err := s.store.HeartbeatSession(ctx, user.ID, agentID, sessionID,
		now, now+s.config.SessionLeaseSeconds)
	if err != nil {
		return protocol.SessionResponse{}, err
	}
	return toSessionResponse(session, s.config.SessionHeartbeatSeconds), nil
}

func (s *Service) GetAssignment(ctx context.Context, machineToken, agentID string, request protocol.GetAssignmentRequest) (protocol.GetAssignmentResponse, error) {
	user, err := s.authenticate(ctx, machineToken)
	if err != nil {
		return protocol.GetAssignmentResponse{}, err
	}
	if !queue.ValidateIdentifier(agentID) || !queue.ValidateIdentifier(request.SessionID) {
		return protocol.GetAssignmentResponse{}, invalidRequest("invalid assignment request")
	}
	if !user.HasRole(RoleWorker) {
		return protocol.GetAssignmentResponse{}, permissionDenied("worker role required")
	}
	if _, err := normalizeAcceptTypes(request.AcceptTypes); err != nil {
		return protocol.GetAssignmentResponse{}, err
	}
	now := s.now()
	candidate, err := s.store.FindAssignment(ctx, user.ID, agentID, request.SessionID, now)
	if err != nil {
		return protocol.GetAssignmentResponse{}, err
	}
	if candidate == nil {
		return protocol.GetAssignmentResponse{Assignment: nil, RetryAfterMS: 1000}, nil
	}
	if candidate.Agent.Endpoint == nil || candidate.Agent.EndpointVersion == nil {
		return protocol.GetAssignmentResponse{}, fmt.Errorf("tracker: assignment invariant has no endpoint")
	}
	scope := access.Scope{
		WorkerAgentID: agentID, SessionID: candidate.Session.ID,
		ProjectID: candidate.Shard.ProjectID, ShardID: candidate.Shard.ID,
		Generation: candidate.Shard.Generation, OwnerAgentID: candidate.Agent.ID,
	}
	token, claims, err := s.signer.Sign(scope, candidate.Session.LeaseExpiresAt, s.config.AccessTokenTTLSeconds)
	if err != nil {
		return protocol.GetAssignmentResponse{}, fmt.Errorf("tracker: sign assignment: %w", err)
	}
	return protocol.GetAssignmentResponse{Assignment: &protocol.Assignment{
		Route: protocol.Route{
			ProjectID: candidate.Shard.ProjectID, ShardID: candidate.Shard.ID,
			Generation: candidate.Shard.Generation,
		},
		OwnerAgentID: candidate.Agent.ID, Endpoint: *candidate.Agent.Endpoint,
		EndpointVersion: *candidate.Agent.EndpointVersion,
		TLSSPKISHA256:   candidate.Agent.TLSSPKISHA256,
		AccessToken:     token, AccessTokenExpires: claims.ExpiresAt,
	}, RetryAfterMS: 0}, nil
}

func (s *Service) authenticate(ctx context.Context, token string) (User, error) {
	if token == "" || len(token) > 1024 {
		return User{}, &Error{Code: protocol.ErrorInvalidMachineToken, Message: "invalid machine token"}
	}
	user, err := s.store.AuthenticateMachineToken(ctx, token)
	if err != nil {
		return User{}, err
	}
	if user.Status != UserStatusActive {
		return User{}, &Error{Code: protocol.ErrorAgentDisabled, Message: "user is not active"}
	}
	return user, nil
}

func toProtocolAgent(agent Agent) protocol.Agent {
	return protocol.Agent{
		ID: agent.ID, Kind: agent.Kind, Name: agent.Name, Status: agent.Status,
		Endpoint: agent.Endpoint, EndpointVersion: agent.EndpointVersion,
		TLSSPKISHA256: agent.TLSSPKISHA256, EndpointStatus: agent.EndpointStatus,
		LastHeartbeatAt: agent.LastHeartbeatAt,
	}
}

func toSessionResponse(session Session, heartbeatSeconds int64) protocol.SessionResponse {
	return protocol.SessionResponse{
		SessionID: session.ID, LeaseExpiresAt: session.LeaseExpiresAt,
		HeartbeatAfterSeconds: heartbeatSeconds,
	}
}

func normalizeAcceptTypes(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != protocol.JobTypeSeed && value != protocol.JobTypeAsset {
			return nil, invalidRequest("unsupported accepted job type")
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result, nil
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func PublicKeyFromProtocol(value protocol.SigningKey) (ed25519.PublicKey, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value.PublicKeyEd25519)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("tracker: invalid Ed25519 public key")
	}
	return ed25519.PublicKey(decoded), nil
}
