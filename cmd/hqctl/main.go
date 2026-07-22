package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/internal/trackerclient"
	"github.com/saveweb/hq/pkg/protocol"
)

const (
	defaultHQURL     = "https://hq.saveweb.org"
	defaultMaxJobs   = int64(10_000_000)
	defaultBatchSize = 1000
	maxInputLine     = 64 << 10
)

type adminAPI interface {
	AdminProject(context.Context, string) (protocol.AdminProjectSummary, error)
	EnqueueAdminProjectJobs(context.Context, string, []protocol.AdminEnqueueJob) (protocol.AdminEnqueueJobsResponse, error)
	EnqueueAdminProjectSource(context.Context, string, io.Reader) (protocol.AdminEnqueueSourceResponse, error)
}

type commonConfig struct {
	url, projectID, tokenFile string
	timeout                   time.Duration
	allowHTTP                 bool
}

type enqueueResult struct {
	ProjectID    string `json:"project_id"`
	IdentityMode string `json:"identity_mode"`
	Submitted    int64  `json:"submitted"`
	Inserted     int64  `json:"inserted"`
	Batches      int64  `json:"batches"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "enqueue":
		return runEnqueue(args[1:], output)
	case "enqueue-source":
		return runEnqueueSource(args[1:], output)
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage: hqctl {enqueue|enqueue-source} [flags]")
}

func addCommonFlags(flags *flag.FlagSet) *commonConfig {
	config := &commonConfig{}
	flags.StringVar(&config.url, "url", envOr("HQ_URL", defaultHQURL), "HQ root URL")
	flags.StringVar(&config.projectID, "project-id", "", "target project identifier")
	flags.StringVar(&config.tokenFile, "machine-token-file", os.Getenv("HQ_MACHINE_TOKEN_FILE"), "0600 admin machine-token file")
	flags.DurationVar(&config.timeout, "timeout", 10*time.Minute, "HTTP request timeout")
	flags.BoolVar(&config.allowHTTP, "allow-http", false, "allow an HTTP URL for local development")
	return config
}

func runEnqueue(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	common := addCommonFlags(flags)
	inputPath := flags.String("input", "-", "input file, or - for stdin")
	inputFormat := flags.String("format", "values", "values or jsonl")
	batchSize := flags.Int("batch-size", defaultBatchSize, "jobs per request")
	progressEvery := flags.Int64("progress-every", 100, "report progress every N batches, or 0 to disable")
	maxJobs := flags.Int64("max-jobs", defaultMaxJobs, "maximum input jobs")
	jobType := flags.String("type", "", "job type for values input: seed or asset")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || common.projectID == "" || common.tokenFile == "" ||
		(*inputFormat != "values" && *inputFormat != "jsonl") || *batchSize < 1 || *maxJobs < 1 || *progressEvery < 0 ||
		common.timeout <= 0 || (*inputFormat == "jsonl" && *jobType != "") ||
		(*jobType != "" && *jobType != protocol.JobTypeSeed && *jobType != protocol.JobTypeAsset) {
		return fmt.Errorf("enqueue: project, token file, valid format, batch size, and max jobs are required")
	}
	client, err := newClient(*common)
	if err != nil {
		return err
	}
	input, closeInput, err := openInput(*inputPath)
	if err != nil {
		return err
	}
	defer closeInput()
	ctx, stop := commandContext()
	defer stop()
	project, err := client.AdminProject(ctx, common.projectID)
	if err != nil {
		return fmt.Errorf("enqueue: get project: %w", err)
	}
	progress := func(result enqueueResult) {
		if *progressEvery > 0 && result.Batches%*progressEvery == 0 {
			fmt.Fprintf(os.Stderr, "batches=%d submitted=%d inserted=%d\n", result.Batches, result.Submitted, result.Inserted)
		}
	}
	result, err := enqueueBatches(ctx, client, project, input, *inputFormat, *jobType, *batchSize, *maxJobs, progress)
	if err != nil {
		return err
	}
	return writeJSON(output, result)
}

func enqueueBatches(ctx context.Context, client adminAPI, project protocol.AdminProjectSummary, input io.Reader, inputFormat, jobType string, batchSize int, maxJobs int64, progress func(enqueueResult)) (enqueueResult, error) {
	result := enqueueResult{ProjectID: project.ID, IdentityMode: project.IdentityMode}
	initialCapacity := batchSize
	if initialCapacity > defaultBatchSize {
		initialCapacity = defaultBatchSize
	}
	batch := make([]protocol.AdminEnqueueJob, 0, initialCapacity)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		response, err := client.EnqueueAdminProjectJobs(ctx, project.ID, batch)
		if err != nil {
			return fmt.Errorf("enqueue: batch %d failed after %d submitted jobs: %w", result.Batches+1, result.Submitted, err)
		}
		if response.ProjectID != project.ID || response.Submitted != len(batch) || response.Inserted < 0 || response.Inserted > int64(response.Submitted) {
			return fmt.Errorf("enqueue: batch %d returned an invalid response", result.Batches+1)
		}
		result.Submitted += int64(response.Submitted)
		result.Inserted += response.Inserted
		result.Batches++
		if progress != nil {
			progress(result)
		}
		batch = batch[:0]
		return nil
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 8192), maxInputLine)
	var lineNumber, jobs int64
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSuffix(scanner.Bytes(), []byte{'\r'})
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		job, err := decodeInputJob(line, inputFormat, jobType)
		if err != nil {
			return result, fmt.Errorf("enqueue: line %d: %w", lineNumber, err)
		}
		job, err = applyIdentityMode(job, project.IdentityMode)
		if err != nil {
			return result, fmt.Errorf("enqueue: line %d: %w", lineNumber, err)
		}
		if _, validation := queue.NormalizeJob(queue.JobSpec{ID: job.ID, Value: job.Value, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs}); validation != nil {
			return result, fmt.Errorf("enqueue: line %d: %w", lineNumber, validation)
		}
		jobs++
		if jobs > maxJobs {
			return result, fmt.Errorf("enqueue: input exceeds max jobs")
		}
		batch = append(batch, job)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return result, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("enqueue: read input: %w", err)
	}
	if jobs == 0 {
		return result, fmt.Errorf("enqueue: input contains no jobs")
	}
	if err := flush(); err != nil {
		return result, err
	}
	return result, nil
}

func decodeInputJob(line []byte, inputFormat, jobType string) (protocol.AdminEnqueueJob, error) {
	if inputFormat == "values" {
		return protocol.AdminEnqueueJob{Value: string(line), Type: jobType}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var job protocol.AdminEnqueueJob
	if err := decoder.Decode(&job); err != nil {
		return protocol.AdminEnqueueJob{}, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return protocol.AdminEnqueueJob{}, fmt.Errorf("line must contain one JSON object")
	}
	return job, nil
}

func applyIdentityMode(job protocol.AdminEnqueueJob, identityMode string) (protocol.AdminEnqueueJob, error) {
	switch identityMode {
	case tracker.IdentityModeExternalID:
		if job.ID == "" {
			job.ID = protocol.DefaultJobID(job.Type, job.Value)
		}
	case tracker.IdentityModeUniqueValue, tracker.IdentityModeNone:
		if job.ID != "" {
			return protocol.AdminEnqueueJob{}, fmt.Errorf("project identity mode %s rejects job id", identityMode)
		}
	default:
		return protocol.AdminEnqueueJob{}, fmt.Errorf("project has unknown identity mode %q", identityMode)
	}
	return job, nil
}

func runEnqueueSource(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("enqueue-source", flag.ContinueOnError)
	common := addCommonFlags(flags)
	inputPath := flags.String("input", "", "jobs-jsonl-zstd-v1 file, or - for stdin")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || common.projectID == "" || common.tokenFile == "" || *inputPath == "" || common.timeout <= 0 {
		return fmt.Errorf("enqueue-source: project, token file, and input are required")
	}
	client, err := newClient(*common)
	if err != nil {
		return err
	}
	input, closeInput, err := openInput(*inputPath)
	if err != nil {
		return err
	}
	defer closeInput()
	ctx, stop := commandContext()
	defer stop()
	result, err := client.EnqueueAdminProjectSource(ctx, common.projectID, input)
	if err != nil {
		return fmt.Errorf("enqueue-source: %w", err)
	}
	if result.ProjectID != common.projectID || result.Jobs < 0 || result.Inserted < 0 || result.Inserted > result.Jobs || result.UncompressedBytes < 0 {
		return fmt.Errorf("enqueue-source: server returned an invalid response")
	}
	return writeJSON(output, result)
}

func newClient(config commonConfig) (*trackerclient.Client, error) {
	token, err := readSecretFile(config.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read machine token: %w", err)
	}
	return trackerclient.New(trackerclient.Config{
		BaseURL: config.url, MachineToken: token, AllowHTTP: config.allowHTTP, RequestTimeout: config.timeout,
	})
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
		return "", fmt.Errorf("invalid secret")
	}
	return value, nil
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { _ = file.Close() }, nil
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func commandContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
