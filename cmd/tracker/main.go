package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/endpointcheck"
	"git.saveweb.org/saveweb/hq/internal/githuboauth"
	"git.saveweb.org/saveweb/hq/internal/objectstore"
	"git.saveweb.org/saveweb/hq/internal/receiveringest"
	"git.saveweb.org/saveweb/hq/internal/signingkey"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/internal/trackerhttp"
	"git.saveweb.org/saveweb/hq/internal/trackerweb"
)

const keyValiditySeconds = int64(365 * 24 * 60 * 60)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("tracker stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "keygen":
		return runKeygen(args[1:])
	case "web-keygen":
		return runWebKeygen(args[1:])
	case "migrate":
		return runMigrate(args[1:])
	case "bootstrap-user":
		return runBootstrapUser(args[1:])
	case "put-project":
		return runPutProject(args[1:])
	case "put-shard":
		return runPutShard(args[1:])
	case "transition-shard":
		return runTransitionShard(args[1:])
	case "put-receiver":
		return runPutReceiver(args[1:])
	case "serve":
		return runServe(args[1:], logger)
	default:
		return usageError()
	}
}

func usageError() error {
	return fmt.Errorf("usage: tracker {keygen|web-keygen|migrate|bootstrap-user|put-project|put-shard|transition-shard|put-receiver|serve} [flags]")
}

func runKeygen(args []string) error {
	flags := flag.NewFlagSet("keygen", flag.ContinueOnError)
	out := flags.String("out", "", "new key file path")
	keyID := flags.String("key-id", "", "stable public key identifier")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *out == "" || *keyID == "" {
		return fmt.Errorf("keygen: --out and --key-id are required")
	}
	_, err := signingkey.Create(*out, *keyID, time.Now().Unix())
	return err
}

func runWebKeygen(args []string) error {
	flags := flag.NewFlagSet("web-keygen", flag.ContinueOnError)
	out := flags.String("out", "", "new 0600 web session secret file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *out == "" {
		return fmt.Errorf("web-keygen: --out is required")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("web-keygen: random: %w", err)
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("web-keygen: create: %w", err)
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(*out)
		}
	}()
	if _, err := fmt.Fprintln(file, base64.RawURLEncoding.EncodeToString(random[:])); err != nil {
		return fmt.Errorf("web-keygen: write: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("web-keygen: sync: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("web-keygen: close: %w", err)
	}
	remove = false
	return nil
}

func runMigrate(args []string) error {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" {
		return fmt.Errorf("migrate: --database-url or HQ_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Migrate(ctx)
}

func runBootstrapUser(args []string) error {
	flags := flag.NewFlagSet("bootstrap-user", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	userID := flags.String("user-id", "", "stable user identifier")
	rolesText := flags.String("roles", "", "comma-separated admin,shard_owner,worker roles")
	tokenFile := flags.String("machine-token-file", "", "0600 file containing the reusable machine token")
	githubUserID := flags.Int64("github-user-id", 0, "optional stable numeric GitHub user ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *userID == "" || *rolesText == "" || *tokenFile == "" {
		return fmt.Errorf("bootstrap-user: database URL, --user-id, --roles, and --machine-token-file are required")
	}
	roles, err := parseRoles(*rolesText)
	if err != nil {
		return err
	}
	token, err := readSecretFile(*tokenFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	user := tracker.User{
		ID: *userID, Status: tracker.UserStatusActive, Roles: roles,
	}
	if *githubUserID < 0 {
		return fmt.Errorf("bootstrap-user: GitHub user ID must be positive")
	}
	if *githubUserID > 0 {
		user.GitHubUserID = githubUserID
	}
	return store.PutUserAndToken(ctx, user, token, time.Now().Unix())
}

func runPutProject(args []string) error {
	flags := flag.NewFlagSet("put-project", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	projectID := flags.String("project-id", "", "explicit project identifier")
	status := flags.String("status", tracker.ProjectStatusActive, "active, draining, or archived")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" {
		return fmt.Errorf("put-project: database URL and --project-id are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.PutProject(ctx, tracker.Project{ID: *projectID, Status: *status}, time.Now().Unix())
}

func runPutShard(args []string) error {
	flags := flag.NewFlagSet("put-shard", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	projectID := flags.String("project-id", "", "explicit project identifier")
	shardID := flags.String("shard-id", "", "explicit shard identifier")
	ownerAgentID := flags.String("owner-agent-id", "", "registered shard agent identifier")
	status := flags.String("status", "", "explicit active, loading, or recovering status")
	generation := flags.Int64("generation", 1, "positive owner generation")
	sourceURI := flags.String("source-uri", "", "immutable s3:// source object")
	sourceFormat := flags.String("source-format", "", "source format (jobs-jsonl-zstd-v1)")
	sourceETag := flags.String("source-etag", "", "immutable source object ETag")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" || *shardID == "" ||
		*ownerAgentID == "" || *generation < 1 || *status == "" {
		return fmt.Errorf("put-shard: database URL, project, shard, owner agent, positive generation, and explicit status are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	var sourceURIPointer, sourceFormatPointer, sourceETagPointer *string
	anySource := *sourceURI != "" || *sourceFormat != "" || *sourceETag != ""
	if anySource {
		if *sourceURI == "" || *sourceFormat != "jobs-jsonl-zstd-v1" || *sourceETag == "" ||
			*status != tracker.ShardStatusLoading {
			return fmt.Errorf("put-shard: source URI, jobs-jsonl-zstd-v1 format, ETag, and loading status are required together")
		}
		if _, err := objectstore.ParseURI(*sourceURI); err != nil {
			return err
		}
		sourceURIPointer, sourceFormatPointer, sourceETagPointer = sourceURI, sourceFormat, sourceETag
	} else if *status == tracker.ShardStatusLoading {
		return fmt.Errorf("put-shard: loading status requires an immutable source")
	}
	return store.PutShard(ctx, tracker.Shard{
		ProjectID: *projectID, ID: *shardID, Status: *status,
		OwnerAgentID: *ownerAgentID, Generation: *generation,
		SourceURI: sourceURIPointer, SourceFormat: sourceFormatPointer, SourceETag: sourceETagPointer,
	}, time.Now().Unix())
}

func runTransitionShard(args []string) error {
	flags := flag.NewFlagSet("transition-shard", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	actorUserID := flags.String("actor-user-id", "", "active administrator user ID")
	projectID := flags.String("project-id", "", "explicit project identifier")
	shardID := flags.String("shard-id", "", "explicit shard identifier")
	expectedGeneration := flags.Int64("expected-generation", 0, "current positive shard generation")
	targetStatus := flags.String("target-status", "", "active, draining, or paused")
	reason := flags.String("reason", "", "non-empty audit reason")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *actorUserID == "" || *projectID == "" ||
		*shardID == "" || *expectedGeneration < 1 || *targetStatus == "" || strings.TrimSpace(*reason) == "" {
		return fmt.Errorf("transition-shard: database URL, actor, project, shard, positive generation, target status, and reason are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.AdminTransitionShard(
		ctx, *actorUserID, *projectID, *shardID, *expectedGeneration,
		*targetStatus, *reason, time.Now().Unix(),
	)
}

func runPutReceiver(args []string) error {
	flags := flag.NewFlagSet("put-receiver", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	projectID := flags.String("project-id", "", "explicit project identifier")
	receiverID := flags.String("receiver-id", "", "explicit receiver identifier")
	status := flags.String("status", tracker.ReceiverStatusActive, "active or removed")
	sinkURI := flags.String("sink-uri", "", "immutable receiver object s3:// prefix")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" || *receiverID == "" ||
		*sinkURI == "" || strings.HasSuffix(*sinkURI, "/") ||
		(*status != tracker.ReceiverStatusActive && *status != tracker.ReceiverStatusRemoved) {
		return fmt.Errorf("put-receiver: database URL, project, receiver, active/removed status, and sink URI are required")
	}
	if _, err := objectstore.ParseURI(*sinkURI + "/probe"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.PutReceiver(ctx, tracker.Receiver{
		ProjectID: *projectID, ID: *receiverID, Status: *status,
		SinkURI: *sinkURI, Format: "jobs-jsonl-zstd-v1",
	}, time.Now().Unix())
}

func runServe(args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := flags.String("listen", envOr("HQ_LISTEN", ":8080"), "HTTP listen address")
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	publicURL := flags.String("public-url", os.Getenv("HQ_PUBLIC_URL"), "public tracker URL used as token issuer")
	keyFile := flags.String("signing-key-file", os.Getenv("HQ_SIGNING_KEY_FILE"), "0600 Ed25519 key file")
	allowInsecurePublicURL := flags.Bool("allow-insecure-public-url", false, "allow an HTTP issuer for local testing")
	allowPrivateShardEndpoints := flags.Bool("allow-private-shard-endpoints", false, "allow private shard endpoints for local E2E testing")
	agentHeartbeatSeconds := flags.Int64("agent-heartbeat-seconds", 30, "agent heartbeat interval")
	ownerLeaseSeconds := flags.Int64("owner-lease-seconds", 120, "shard owner lease")
	sessionHeartbeatSeconds := flags.Int64("session-heartbeat-seconds", 30, "worker session heartbeat interval")
	sessionLeaseSeconds := flags.Int64("session-lease-seconds", 120, "worker session lease")
	accessTokenTTLSeconds := flags.Int64("access-token-ttl-seconds", 600, "shard access token TTL")
	githubClientID := flags.String("github-client-id", os.Getenv("HQ_GITHUB_CLIENT_ID"), "GitHub OAuth app client ID")
	githubClientSecretFile := flags.String("github-client-secret-file", os.Getenv("HQ_GITHUB_CLIENT_SECRET_FILE"), "0600 GitHub OAuth client secret file")
	webSessionSecretFile := flags.String("web-session-secret-file", os.Getenv("HQ_WEB_SESSION_SECRET_FILE"), "0600 tracker web session secret file")
	oauthAutoGrantWorker := flags.Bool("oauth-auto-grant-worker", false, "activate new GitHub users with worker role")
	s3Endpoint := flags.String("s3-endpoint", os.Getenv("HQ_S3_ENDPOINT"), "trusted S3-compatible endpoint")
	s3Region := flags.String("s3-region", envOr("HQ_S3_REGION", "auto"), "S3-compatible signing region")
	s3AccessKeyFile := flags.String("s3-access-key-id-file", os.Getenv("HQ_S3_ACCESS_KEY_ID_FILE"), "0600 S3 access key ID file")
	s3SecretKeyFile := flags.String("s3-secret-access-key-file", os.Getenv("HQ_S3_SECRET_ACCESS_KEY_FILE"), "0600 S3 secret access key file")
	s3PathStyle := flags.Bool("s3-path-style", false, "use path-style S3 object URLs")
	allowHTTPS3 := flags.Bool("allow-http-s3", false, "allow an HTTP S3 endpoint for local testing")
	sourceURLTTLSeconds := flags.Int64("source-url-ttl-seconds", 900, "exact-object source download URL lifetime")
	checkpointPrefixURI := flags.String("checkpoint-prefix-uri", os.Getenv("HQ_CHECKPOINT_PREFIX_URI"), "trusted s3:// bucket/prefix for checkpoints")
	checkpointPartURLTTL := flags.Int64("checkpoint-part-url-ttl-seconds", 3600, "per-part upload URL lifetime")
	checkpointURLTTL := flags.Int64("checkpoint-download-url-ttl-seconds", 3600, "checkpoint recovery URL lifetime")
	checkpointPartSize := flags.Int64("checkpoint-part-size-bytes", 8<<20, "recommended multipart checkpoint part size")
	checkpointMaxBytes := flags.Int64("checkpoint-max-bytes", 64<<30, "maximum compressed checkpoint size")
	receiverMaxJobs := flags.Int("receiver-max-jobs", 1000, "maximum jobs in one receiver batch")
	receiverMaxObjectBytes := flags.Int64("receiver-max-object-bytes", 16<<20, "maximum compressed receiver object size")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *publicURL == "" || *keyFile == "" {
		return fmt.Errorf("serve: database URL, public URL, and signing key file are required")
	}
	if err := validatePublicURL(*publicURL, *allowInsecurePublicURL); err != nil {
		return err
	}
	key, err := signingkey.Load(*keyFile)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	if now >= key.CreatedAt+keyValiditySeconds {
		return fmt.Errorf("serve: signing key is outside its advertised validity period")
	}
	store, err := postgres.Open(context.Background(), *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	signer, err := access.NewSigner(*publicURL, key.KeyID, key.PrivateKey, func() int64 { return time.Now().Unix() })
	if err != nil {
		return err
	}
	config := tracker.DefaultConfig()
	config.AgentHeartbeatSeconds = *agentHeartbeatSeconds
	config.OwnerLeaseSeconds = *ownerLeaseSeconds
	config.SessionHeartbeatSeconds = *sessionHeartbeatSeconds
	config.SessionLeaseSeconds = *sessionLeaseSeconds
	config.AccessTokenTTLSeconds = *accessTokenTTLSeconds
	config.SigningKeyNotBefore = key.CreatedAt
	config.SigningKeyNotAfter = key.CreatedAt + keyValiditySeconds
	config.SourceURLTTLSeconds = *sourceURLTTLSeconds
	objectClient, err := configureObjectStore(
		*s3Endpoint, *s3Region, *s3AccessKeyFile, *s3SecretKeyFile, *s3PathStyle, *allowHTTPS3,
	)
	if err != nil {
		return err
	}
	config.SourceURLSigner = objectClient
	config.CheckpointPrefixURI = strings.TrimSuffix(*checkpointPrefixURI, "/")
	if config.CheckpointPrefixURI != "" {
		config.CheckpointStore = objectClient
		config.CheckpointURLSigner = objectClient
	}
	config.CheckpointURLTTLSeconds = *checkpointURLTTL
	config.CheckpointPartURLTTL = *checkpointPartURLTTL
	config.CheckpointPartSizeBytes = *checkpointPartSize
	config.CheckpointMaxBytes = *checkpointMaxBytes
	config.ReceiverMaxJobs = *receiverMaxJobs
	if objectClient != nil {
		receiverWriter, err := receiveringest.New(receiveringest.Config{
			Store: objectClient, MaxObjectBytes: *receiverMaxObjectBytes,
		})
		if err != nil {
			return err
		}
		config.ReceiverSink = receiverWriter
	}
	checker := endpointcheck.NewWithOptions(endpointcheck.Options{AllowPrivate: *allowPrivateShardEndpoints})
	if *allowPrivateShardEndpoints {
		logger.Warn("private shard endpoints are enabled; do not use this setting in production")
	}
	service, err := tracker.NewService(store, checker, signer, func() int64 { return time.Now().Unix() }, config)
	if err != nil {
		return err
	}
	handler := trackerhttp.New(service, logger)
	webSecret, oauthClient, err := configureWeb(
		*publicURL, *githubClientID, *githubClientSecretFile, *webSessionSecretFile,
	)
	if err != nil {
		return err
	}
	web, err := trackerweb.New(store, oauthClient, trackerweb.Config{
		PublicURL: *publicURL, Secret: webSecret,
		SecureCookies:   strings.HasPrefix(*publicURL, "https://"),
		AutoGrantWorker: *oauthAutoGrantWorker,
	}, logger)
	if err != nil {
		return err
	}
	web.Register(handler)
	server := &http.Server{
		Addr: *listen, Handler: handler,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() {
		logger.Info("tracker listening", "address", *listen, "public_url", *publicURL)
		result <- server.ListenAndServe()
	}()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("serve: graceful shutdown: %w", err)
		}
		return nil
	}
}

func configureObjectStore(
	endpoint, region, accessKeyFile, secretKeyFile string,
	pathStyle, allowHTTP bool,
) (*objectstore.Client, error) {
	configured := endpoint != "" || accessKeyFile != "" || secretKeyFile != ""
	if !configured {
		return nil, nil
	}
	if endpoint == "" || region == "" || accessKeyFile == "" || secretKeyFile == "" {
		return nil, fmt.Errorf("serve: S3 endpoint, region, access key file, and secret key file are required together")
	}
	accessKey, err := readSecretFile(accessKeyFile)
	if err != nil {
		return nil, err
	}
	secretKey, err := readSecretFile(secretKeyFile)
	if err != nil {
		return nil, err
	}
	return objectstore.New(objectstore.Config{
		Endpoint: endpoint, Region: region, AccessKeyID: accessKey,
		SecretAccessKey: secretKey, UsePathStyle: pathStyle, AllowHTTP: allowHTTP,
	})
}

func validatePublicURL(value string, allowInsecure bool) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		(parsed.Scheme != "https" && !(allowInsecure && parsed.Scheme == "http")) {
		return fmt.Errorf("serve: public URL must be a root HTTPS URL without credentials, query, or fragment")
	}
	return nil
}

func configureWeb(
	publicURL, clientID, clientSecretFile, webSecretFile string,
) ([]byte, trackerweb.OAuth, error) {
	configured := clientID != "" || clientSecretFile != "" || webSecretFile != ""
	if !configured {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, nil, err
		}
		return secret, nil, nil
	}
	if clientID == "" || clientSecretFile == "" || webSecretFile == "" {
		return nil, nil, fmt.Errorf("serve: GitHub client ID, client secret file, and web session secret file must be configured together")
	}
	clientSecret, err := readSecretFile(clientSecretFile)
	if err != nil {
		return nil, nil, err
	}
	encodedSecret, err := readSecretFile(webSecretFile)
	if err != nil {
		return nil, nil, err
	}
	webSecret, err := base64.RawURLEncoding.DecodeString(encodedSecret)
	if err != nil || len(webSecret) < 32 {
		return nil, nil, fmt.Errorf("serve: invalid web session secret")
	}
	oauthClient, err := githuboauth.New(githuboauth.Config{
		ClientID: clientID, ClientSecret: clientSecret,
		RedirectURL: strings.TrimSuffix(publicURL, "/") + "/auth/github/callback",
	})
	if err != nil {
		return nil, nil, err
	}
	return webSecret, oauthClient, nil
}

func parseRoles(value string) (map[string]bool, error) {
	result := make(map[string]bool)
	for _, role := range strings.Split(value, ",") {
		role = strings.TrimSpace(role)
		switch role {
		case tracker.RoleAdmin, tracker.RoleShardOwner, tracker.RoleWorker:
			result[role] = true
		default:
			return nil, fmt.Errorf("bootstrap-user: unknown role %q", role)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("bootstrap-user: at least one role is required")
	}
	return result, nil
}

func readSecretFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("bootstrap-user: stat token file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("bootstrap-user: token file permissions must not allow group or other access")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("bootstrap-user: read token file: %w", err)
	}
	value := strings.TrimSpace(string(encoded))
	if value == "" || len(value) > 1024 {
		return "", fmt.Errorf("bootstrap-user: invalid machine token")
	}
	return value, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
