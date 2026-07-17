package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"git.saveweb.org/saveweb/hq/internal/agentidentity"
	"git.saveweb.org/saveweb/hq/internal/checkpointrestore"
	"git.saveweb.org/saveweb/hq/internal/checkpointupload"
	"git.saveweb.org/saveweb/hq/internal/localadmin"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/internal/shardhttp"
	"git.saveweb.org/saveweb/hq/internal/sourceloader"
	"git.saveweb.org/saveweb/hq/internal/tlsidentity"
	"git.saveweb.org/saveweb/hq/internal/trackerclient"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("shard stopped", "error", err)
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "init":
		return runInit(args[1:])
	case "tls-init":
		return runTLSInit(args[1:])
	case "serve":
		return runServe(args[1:], logger)
	default:
		return usageError()
	}
}

func usageError() error { return fmt.Errorf("usage: shard {init|tls-init|serve} [flags]") }

func runInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	out := flags.String("out", "", "new shard identity file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *out == "" {
		return fmt.Errorf("init: --out is required")
	}
	identity, err := agentidentity.Create(*out, protocol.AgentKindShard, time.Now().Unix())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, identity.AgentID)
	return err
}

func runTLSInit(args []string) error {
	flags := flag.NewFlagSet("tls-init", flag.ContinueOnError)
	keyOut := flags.String("key-out", "", "new P-256 private key file")
	certificateOut := flags.String("cert-out", "", "new self-signed certificate file")
	serverName := flags.String("server-name", "", "endpoint DNS name or IP address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *keyOut == "" || *certificateOut == "" || *serverName == "" {
		return fmt.Errorf("tls-init: --key-out, --cert-out, and --server-name are required")
	}
	pin, err := tlsidentity.Create(*keyOut, *certificateOut, *serverName, time.Now(), tlsidentity.DefaultValidity)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, pin)
	return err
}

type serveConfig struct {
	listen                string
	trackerURL            string
	trackerIssuer         string
	allowHTTPTracker      bool
	allowHTTPSource       bool
	machineTokenFile      string
	identityFile          string
	dataDir               string
	name                  string
	endpoint              string
	endpointVersion       int64
	tlsSPKISHA256         string
	tlsCertificateFile    string
	tlsKeyFile            string
	tlsTerminatedByProxy  bool
	allowInsecureEndpoint bool
	maxConcurrent         int
	maxResets             int
	adminListen           string
	localAdminTokenFile   string
	localAdminToken       string
	checkpointInterval    int64
	checkpointWorkDir     string
}

func runServe(args []string, logger *slog.Logger) error {
	config, err := parseServeConfig(args)
	if err != nil {
		return err
	}
	identity, err := agentidentity.Load(config.identityFile, protocol.AgentKindShard)
	if err != nil {
		return err
	}
	machineToken, err := readSecretFile(config.machineTokenFile)
	if err != nil {
		return err
	}
	parsedEndpoint, err := validateEndpoint(config.endpoint, config.allowInsecureEndpoint)
	if err != nil {
		return err
	}
	if err := validateTLSMode(config, parsedEndpoint); err != nil {
		return err
	}
	if err := validateAdminListen(config.adminListen); err != nil {
		return err
	}
	adminToken, err := localadmin.ResolveToken(config.localAdminToken, config.localAdminTokenFile)
	if err != nil {
		return err
	}
	if adminToken.FromEnv {
		logger.Info("local admin token loaded from environment")
	} else {
		logger.Info("local admin token rotated", "file", adminToken.FilePath)
	}
	basePath := strings.TrimSuffix(parsedEndpoint.Path, "/")
	loaderConfig := sourceloader.DefaultConfig()
	loaderConfig.AllowHTTP = config.allowHTTPSource
	sourceLoader, err := sourceloader.New(loaderConfig)
	if err != nil {
		return err
	}
	restoreConfig := checkpointrestore.DefaultConfig()
	restoreConfig.AllowHTTP = config.allowHTTPSource
	checkpointRestorer, err := checkpointrestore.New(restoreConfig)
	if err != nil {
		return err
	}
	var trackerControl *trackerclient.Client
	manager, err := shard.NewManager(shard.ManagerConfig{
		AgentID: identity.AgentID, Issuer: config.trackerIssuer, DataDir: config.dataDir,
		LoadSource: func(ctx context.Context, assignment protocol.OwnerAssignment, store queue.Store) error {
			_, err := sourceLoader.Load(ctx, assignment, store)
			return err
		},
		ReportSource: func(ctx context.Context, assignment protocol.OwnerAssignment, loadError error) error {
			request := protocol.ShardLoadResultRequest{
				Generation: assignment.Generation,
				Success:    loadError == nil,
			}
			if loadError != nil {
				request.ErrorCode = "source_load_failed"
			}
			_, err := trackerControl.ReportShardLoad(ctx, assignment.ProjectID, assignment.ShardID, request)
			return err
		},
		RestoreCheckpoint: checkpointRestorer.Restore,
		ReportRecovery: func(ctx context.Context, assignment protocol.OwnerAssignment, recoveryError error) error {
			request := protocol.ShardRecoveryResultRequest{
				Generation: assignment.Generation,
				Success:    recoveryError == nil,
			}
			if recoveryError != nil {
				request.ErrorCode = "checkpoint_restore_failed"
			}
			_, err := trackerControl.ReportShardRecovery(ctx, assignment.ProjectID, assignment.ShardID, request)
			return err
		},
	})
	if err != nil {
		return err
	}
	defer manager.Close()
	httpConfig := shardhttp.DefaultConfig(identity.AgentID)
	httpConfig.BasePath = basePath
	httpConfig.MaxConcurrent = config.maxConcurrent
	httpConfig.MaxResets = config.maxResets
	handler, err := shardhttp.New(manager, httpConfig, logger)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.listen)
	if err != nil {
		return fmt.Errorf("shard: listen: %w", err)
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	adminListener, err := net.Listen("tcp", config.adminListen)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("shard: local admin listen: %w", err)
	}
	adminOrigin := "http://" + adminListener.Addr().String()
	adminHandler, err := localadmin.NewServer(manager, adminToken.Token, adminOrigin, nil)
	if err != nil {
		_ = listener.Close()
		_ = adminListener.Close()
		return err
	}
	adminServer := &http.Server{
		Handler: adminHandler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	type serverExit struct {
		name string
		err  error
	}
	serverResult := make(chan serverExit, 2)
	go func() {
		logger.Info("shard listening", "address", listener.Addr().String(), "endpoint", config.endpoint, "agent_id", identity.AgentID)
		if config.tlsCertificateFile != "" {
			serverResult <- serverExit{name: "queue", err: server.ServeTLS(listener, config.tlsCertificateFile, config.tlsKeyFile)}
		} else {
			serverResult <- serverExit{name: "queue", err: server.Serve(listener)}
		}
	}()
	go func() {
		logger.Info("local admin listening", "address", adminListener.Addr().String())
		serverResult <- serverExit{name: "local admin", err: adminServer.Serve(adminListener)}
	}()

	tracker, err := trackerclient.New(trackerclient.Config{
		BaseURL: config.trackerURL, MachineToken: machineToken, AgentID: identity.AgentID,
		AllowHTTP: config.allowHTTPTracker,
	})
	if err != nil {
		_ = server.Close()
		_ = adminServer.Close()
		return err
	}
	trackerControl = tracker
	var pin *string
	if config.tlsSPKISHA256 != "" {
		pin = &config.tlsSPKISHA256
	}
	registerContext, cancelRegister := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = tracker.UpsertAgent(registerContext, protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: config.name, Version: version,
		Attrs:    protocol.Attrs{"queue_backend": "sqlite", "max_concurrent": config.maxConcurrent},
		Endpoint: &config.endpoint, EndpointVersion: &config.endpointVersion, TLSSPKISHA256: pin,
	})
	cancelRegister()
	if err != nil {
		_ = server.Close()
		_ = adminServer.Close()
		return fmt.Errorf("shard: register agent: %w", err)
	}
	heartbeatContext, cancelHeartbeat := context.WithTimeout(context.Background(), 30*time.Second)
	heartbeat, err := tracker.HeartbeatAgent(heartbeatContext, heartbeatRequest(config))
	cancelHeartbeat()
	if err != nil {
		_ = server.Close()
		_ = adminServer.Close()
		return fmt.Errorf("shard: initial heartbeat: %w", err)
	}
	if err := manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		_ = server.Close()
		_ = adminServer.Close()
		return fmt.Errorf("shard: apply initial heartbeat: %w", err)
	}
	var checkpointUploader *checkpointupload.Uploader
	if config.checkpointInterval > 0 {
		checkpointUploader, err = checkpointupload.New(manager, tracker, checkpointupload.Config{
			WorkDir: config.checkpointWorkDir, AllowHTTP: config.allowHTTPSource,
		})
		if err != nil {
			_ = server.Close()
			_ = adminServer.Close()
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	loopContext, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		heartbeatLoop(loopContext, tracker, manager, config, heartbeat.HeartbeatAfterSeconds, logger)
	}()
	if checkpointUploader != nil {
		loops.Add(1)
		go func() {
			defer loops.Done()
			checkpointLoop(loopContext, checkpointUploader, config.checkpointInterval, logger)
		}()
	}
	go func() {
		defer loops.Done()
		maintenanceLoop(loopContext, manager, config.maxResets, logger)
	}()
	select {
	case exit := <-serverResult:
		cancelLoops()
		_ = server.Close()
		_ = adminServer.Close()
		loops.Wait()
		if !errors.Is(exit.err, http.ErrServerClosed) {
			return fmt.Errorf("shard: %s server: %w", exit.name, exit.err)
		}
		return nil
	case <-ctx.Done():
		cancelLoops()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		shutdownError := errors.Join(server.Shutdown(shutdownContext), adminServer.Shutdown(shutdownContext))
		loops.Wait()
		if shutdownError != nil {
			return fmt.Errorf("shard: graceful shutdown: %w", shutdownError)
		}
		return nil
	}
}

func parseServeConfig(args []string) (serveConfig, error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	var config serveConfig
	flags.StringVar(&config.listen, "listen", envOr("HQ_SHARD_LISTEN", ":8081"), "public Queue API listen address")
	flags.StringVar(&config.trackerURL, "tracker-url", os.Getenv("HQ_TRACKER_URL"), "tracker API base URL")
	flags.StringVar(&config.trackerIssuer, "tracker-issuer", os.Getenv("HQ_TRACKER_ISSUER"), "expected access-token issuer")
	flags.BoolVar(&config.allowHTTPTracker, "allow-http-tracker", false, "allow an HTTP tracker for local testing")
	flags.BoolVar(&config.allowHTTPSource, "allow-http-object-download", false, "allow HTTP object transfers for local testing")
	flags.StringVar(&config.machineTokenFile, "machine-token-file", os.Getenv("HQ_MACHINE_TOKEN_FILE"), "0600 machine token file")
	flags.StringVar(&config.identityFile, "identity-file", os.Getenv("HQ_SHARD_IDENTITY_FILE"), "0600 shard identity file")
	flags.StringVar(&config.dataDir, "data-dir", envOr("HQ_SHARD_DATA_DIR", "./shard-data"), "local SQLite state directory")
	flags.StringVar(&config.name, "name", envOr("HQ_SHARD_NAME", "Saveweb shard"), "agent display name")
	flags.StringVar(&config.endpoint, "endpoint", os.Getenv("HQ_SHARD_ENDPOINT"), "public HTTP(S) base endpoint")
	flags.Int64Var(&config.endpointVersion, "endpoint-version", 1, "monotonic endpoint identity version")
	flags.StringVar(&config.tlsSPKISHA256, "tls-spki-sha256", os.Getenv("HQ_SHARD_TLS_SPKI_SHA256"), "optional base64url SPKI digest")
	flags.StringVar(&config.tlsCertificateFile, "tls-cert-file", os.Getenv("HQ_SHARD_TLS_CERT_FILE"), "direct HTTPS certificate PEM")
	flags.StringVar(&config.tlsKeyFile, "tls-key-file", os.Getenv("HQ_SHARD_TLS_KEY_FILE"), "direct HTTPS private key PEM")
	flags.BoolVar(&config.tlsTerminatedByProxy, "tls-terminated-by-proxy", false, "serve local HTTP behind an HTTPS reverse proxy")
	flags.BoolVar(&config.allowInsecureEndpoint, "allow-insecure-public-endpoint", false, "allow a public HTTP endpoint")
	flags.IntVar(&config.maxConcurrent, "max-concurrent", 128, "maximum concurrent Queue API requests")
	flags.IntVar(&config.maxResets, "max-resets", 3, "maximum retry/reset count")
	flags.StringVar(&config.adminListen, "admin-listen", "127.0.0.1:9081", "localhost-only admin listen address")
	flags.StringVar(&config.localAdminTokenFile, "local-admin-token-file", "", "rotating 0600 local admin token file")
	flags.Int64Var(&config.checkpointInterval, "checkpoint-interval-seconds", 0, "periodic checkpoint interval; 0 disables publication")
	flags.StringVar(&config.checkpointWorkDir, "checkpoint-work-dir", "", "bounded local checkpoint work directory")
	config.localAdminToken = os.Getenv("SAVEWEB_LOCAL_ADMIN_TOKEN")
	if err := flags.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if flags.NArg() != 0 || config.trackerURL == "" || config.machineTokenFile == "" ||
		config.identityFile == "" || config.endpoint == "" || config.endpointVersion < 1 {
		return serveConfig{}, fmt.Errorf("serve: tracker URL, machine token file, identity file, endpoint, and positive endpoint version are required")
	}
	if config.trackerIssuer == "" {
		config.trackerIssuer = config.trackerURL
	}
	if config.localAdminTokenFile == "" {
		config.localAdminTokenFile = filepath.Join(config.dataDir, "runtime", "local-admin.token")
	}
	if config.checkpointInterval < 0 {
		return serveConfig{}, fmt.Errorf("serve: checkpoint interval cannot be negative")
	}
	if config.checkpointWorkDir == "" {
		config.checkpointWorkDir = filepath.Join(config.dataDir, "runtime", "checkpoints")
	}
	return config, nil
}

func validateAdminListen(value string) error {
	host, port, err := net.SplitHostPort(value)
	if err != nil || host != "127.0.0.1" || port == "" {
		return fmt.Errorf("shard: local admin must listen on 127.0.0.1 with an explicit port")
	}
	return nil
}

func heartbeatRequest(config serveConfig) protocol.AgentHeartbeatRequest {
	return protocol.AgentHeartbeatRequest{
		Version: version,
		Attrs:   protocol.Attrs{"queue_backend": "sqlite", "max_concurrent": config.maxConcurrent},
	}
}

func heartbeatLoop(
	ctx context.Context,
	tracker *trackerclient.Client,
	manager *shard.Manager,
	config serveConfig,
	intervalSeconds int64,
	logger *slog.Logger,
) {
	if intervalSeconds < 1 {
		intervalSeconds = 30
	}
	timer := time.NewTimer(time.Duration(intervalSeconds) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		heartbeatContext, cancel := context.WithTimeout(ctx, 30*time.Second)
		heartbeat, err := tracker.HeartbeatAgent(heartbeatContext, heartbeatRequest(config))
		cancel()
		next := int64(5)
		if err != nil {
			logger.Warn("shard heartbeat failed", "error", err)
		} else if err := manager.ApplyHeartbeat(ctx, heartbeat); err != nil {
			logger.Error("shard rejected tracker heartbeat", "error", err)
		} else {
			next = heartbeat.HeartbeatAfterSeconds
			if next < 1 {
				next = 30
			}
		}
		timer.Reset(time.Duration(next) * time.Second)
	}
}

func maintenanceLoop(ctx context.Context, manager *shard.Manager, maxResets int, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := manager.Maintain(ctx, maxResets, 1000)
			if err != nil {
				logger.Warn("queue maintenance failed", "error", err)
			} else if result.Requeued > 0 || result.ResetExhausted > 0 {
				logger.Info("queue leases reaped", "requeued", result.Requeued, "reset_exhausted", result.ResetExhausted)
			}
		}
	}
}

func checkpointLoop(ctx context.Context, uploader *checkpointupload.Uploader, intervalSeconds int64, logger *slog.Logger) {
	ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, result := range uploader.RunOnce(ctx) {
			if result.Err != nil {
				logger.Warn("checkpoint publication failed", "project_id", result.Target.ProjectID,
					"shard_id", result.Target.ShardID, "generation", result.Target.Generation, "error", result.Err)
				continue
			}
			logger.Info("checkpoint published", "project_id", result.Target.ProjectID,
				"shard_id", result.Target.ShardID, "generation", result.Target.Generation,
				"sequence", result.Checkpoint.Sequence, "size_bytes", result.Checkpoint.SizeBytes)
		}
	}
}

func validateEndpoint(value string, allowHTTP bool) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http")) {
		return nil, fmt.Errorf("serve: endpoint must be HTTPS, or HTTP with explicit opt-in")
	}
	return parsed, nil
}

func validateTLSMode(config serveConfig, endpoint *url.URL) error {
	hasCertificate := config.tlsCertificateFile != "" || config.tlsKeyFile != ""
	if hasCertificate && (config.tlsCertificateFile == "" || config.tlsKeyFile == "") {
		return fmt.Errorf("serve: direct TLS requires both certificate and key files")
	}
	if endpoint.Scheme == "http" {
		if hasCertificate || config.tlsTerminatedByProxy || config.tlsSPKISHA256 != "" {
			return fmt.Errorf("serve: HTTP endpoint cannot use TLS files, proxy TLS mode, or an SPKI pin")
		}
		return nil
	}
	if hasCertificate == config.tlsTerminatedByProxy {
		return fmt.Errorf("serve: HTTPS endpoint requires exactly one of direct TLS files or --tls-terminated-by-proxy")
	}
	if hasCertificate {
		pin, err := tlsidentity.PinFromCertificateFile(config.tlsCertificateFile)
		if err != nil {
			return err
		}
		if config.tlsSPKISHA256 == "" || config.tlsSPKISHA256 != pin {
			return fmt.Errorf("serve: direct TLS requires --tls-spki-sha256 matching the certificate")
		}
	}
	return nil
}

func readSecretFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("shard: stat machine token file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("shard: machine token file permissions must not allow group or other access")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("shard: open machine token file: %w", err)
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, 1025))
	if err != nil {
		return "", fmt.Errorf("shard: read machine token file: %w", err)
	}
	value := strings.TrimSpace(string(encoded))
	if value == "" || len(value) > 1024 {
		return "", fmt.Errorf("shard: invalid machine token")
	}
	return value, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
