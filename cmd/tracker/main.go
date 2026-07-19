package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"git.saveweb.org/saveweb/hq/internal/projectqueuehttp"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

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
	case "migrate":
		return runMigrate(args[1:])
	case "bootstrap-user":
		return runBootstrapUser(args[1:])
	case "put-project":
		return runPutProject(args[1:])
	case "enqueue-source":
		return runEnqueueSource(args[1:])
	case "serve":
		return runServe(args[1:], logger)
	default:
		return usageError()
	}
}

func usageError() error {
	return fmt.Errorf("usage: tracker {migrate|bootstrap-user|put-project|enqueue-source|serve} [flags]")
}

func runMigrate(args []string) error {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" {
		return fmt.Errorf("migrate: database URL is required")
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
	rolesText := flags.String("roles", "worker", "comma-separated admin or worker roles")
	tokenFile := flags.String("machine-token-file", "", "0600 machine-token file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *userID == "" || *tokenFile == "" {
		return fmt.Errorf("bootstrap-user: database URL, user ID, and machine-token file are required")
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
	return store.PutUserAndToken(ctx, tracker.User{ID: *userID, Status: tracker.UserStatusActive, Roles: roles}, token, time.Now().Unix())
}

func runPutProject(args []string) error {
	flags := flag.NewFlagSet("put-project", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	projectID := flags.String("project-id", "", "project identifier")
	status := flags.String("status", tracker.ProjectStatusActive, "active, draining, or archived")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" {
		return fmt.Errorf("put-project: database URL and project ID are required")
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

func runEnqueueSource(args []string) error {
	flags := flag.NewFlagSet("enqueue-source", flag.ContinueOnError)
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	projectID := flags.String("project-id", "", "existing project identifier")
	inputPath := flags.String("input", "", "jobs-jsonl-zstd-v1 source file")
	maxJobs := flags.Int64("max-jobs", 100_000_000, "maximum source jobs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" || *projectID == "" || *inputPath == "" || *maxJobs < 1 {
		return fmt.Errorf("enqueue-source: database URL, project, input, and positive max-jobs are required")
	}
	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("enqueue-source: open input: %w", err)
	}
	defer input.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := postgres.Open(ctx, *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	var inserted int64
	stats, err := sourceformat.Decode(ctx, input, sourceformat.Limits{MaxUncompressedBytes: 1 << 40, MaxJobs: *maxJobs}, func(batch []queue.JobSpec) error {
		jobs := make([]protocol.JobSpecV1, 0, len(batch))
		for _, job := range batch {
			jobs = append(jobs, protocol.JobSpecV1{ID: job.ID, URL: job.URL, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs})
		}
		count, err := store.EnqueueProjectJobs(ctx, *projectID, jobs, time.Now().Unix())
		inserted += count
		return err
	})
	if err != nil {
		return err
	}
	slog.Info("source enqueued", "project_id", *projectID, "jobs", stats.Jobs, "inserted", inserted)
	return nil
}

func runServe(args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := flags.String("listen", envOr("HQ_LISTEN", ":8080"), "HTTP listen address")
	databaseURL := flags.String("database-url", os.Getenv("HQ_DATABASE_URL"), "PostgreSQL connection URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *databaseURL == "" {
		return fmt.Errorf("serve: database URL is required")
	}
	store, err := postgres.Open(context.Background(), *databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	handler := projectqueuehttp.New(store, func() int64 { return time.Now().Unix() }, logger)
	server := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() { logger.Info("tracker listening", "address", *listen); result <- server.ListenAndServe() }()
	select {
	case err := <-result:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func parseRoles(value string) (map[string]bool, error) {
	result := map[string]bool{}
	for _, role := range strings.Split(value, ",") {
		role = strings.TrimSpace(role)
		switch role {
		case tracker.RoleAdmin, tracker.RoleWorker:
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
		return "", err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("secret file permissions must not allow group or other access")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(raw))
	if value == "" || len(value) > 1024 {
		return "", fmt.Errorf("invalid machine token")
	}
	return value, nil
}
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
