package main

import (
	"context"
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
	"git.saveweb.org/saveweb/hq/internal/signingkey"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/internal/trackerhttp"
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
	case "migrate":
		return runMigrate(args[1:])
	case "bootstrap-user":
		return runBootstrapUser(args[1:])
	case "put-project":
		return runPutProject(args[1:])
	case "put-shard":
		return runPutShard(args[1:])
	case "serve":
		return runServe(args[1:], logger)
	default:
		return usageError()
	}
}

func usageError() error {
	return fmt.Errorf("usage: tracker {keygen|migrate|bootstrap-user|put-project|put-shard|serve} [flags]")
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
	return store.PutUserAndToken(ctx, tracker.User{
		ID: *userID, Status: tracker.UserStatusActive, Roles: roles,
	}, token, time.Now().Unix())
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
	status := flags.String("status", tracker.ShardStatusActive, "shard lifecycle status")
	generation := flags.Int64("generation", 1, "positive owner generation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" || *shardID == "" ||
		*ownerAgentID == "" || *generation < 1 {
		return fmt.Errorf("put-shard: database URL, project, shard, owner agent, and positive generation are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.PutShard(ctx, tracker.Shard{
		ProjectID: *projectID, ID: *shardID, Status: *status,
		OwnerAgentID: *ownerAgentID, Generation: *generation,
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
	checker := endpointcheck.NewWithOptions(endpointcheck.Options{AllowPrivate: *allowPrivateShardEndpoints})
	if *allowPrivateShardEndpoints {
		logger.Warn("private shard endpoints are enabled; do not use this setting in production")
	}
	service, err := tracker.NewService(store, checker, signer, func() int64 { return time.Now().Unix() }, config)
	if err != nil {
		return err
	}
	handler := trackerhttp.New(service, logger)
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

func validatePublicURL(value string, allowInsecure bool) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(allowInsecure && parsed.Scheme == "http")) {
		return fmt.Errorf("serve: public URL must be an HTTPS URL without credentials, query, or fragment")
	}
	return nil
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
