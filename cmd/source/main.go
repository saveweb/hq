package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return sourceUsage()
	}
	switch args[0] {
	case "pack":
		return runPack(args[1:])
	case "merge":
		return runMerge(args[1:])
	default:
		return sourceUsage()
	}
}

func sourceUsage() error {
	return fmt.Errorf("usage: source {pack|merge} [flags]")
}

func runPack(args []string) error {
	flags := flag.NewFlagSet("pack", flag.ContinueOnError)
	inputPath := flags.String("input", "", "UTF-8 file with one exact value per line, or - for stdin")
	outputPath := flags.String("output", "", "new jobs-jsonl-zstd-v1 file")
	identityMode := flags.String("identity-mode", tracker.IdentityModeExternalID, "external_id, unique_value, or none")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *inputPath == "" || *outputPath == "" || *outputPath == "-" ||
		(*identityMode != tracker.IdentityModeExternalID && *identityMode != tracker.IdentityModeUniqueValue && *identityMode != tracker.IdentityModeNone) {
		return fmt.Errorf("source pack: --input and a file --output are required")
	}
	input, closeInput, err := openInput(*inputPath)
	if err != nil {
		return err
	}
	defer closeInput()
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("source pack: create output: %w", err)
	}
	remove := true
	defer func() {
		_ = output.Close()
		if remove {
			_ = os.Remove(*outputPath)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := make(chan protocol.JobSpecV1, 256)
	producerResult := make(chan error, 1)
	go func() {
		producerResult <- produceJobs(ctx, input, jobs, *identityMode)
		close(jobs)
	}()
	encodeError := sourceformat.Encode(ctx, output, jobs)
	cancel()
	producerError := <-producerResult
	if encodeError != nil {
		return encodeError
	}
	if producerError != nil {
		return producerError
	}
	if err := output.Sync(); err != nil {
		return fmt.Errorf("source pack: sync output: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("source pack: close output: %w", err)
	}
	remove = false
	return nil
}

func produceJobs(ctx context.Context, input io.Reader, output chan<- protocol.JobSpecV1, identityMode string) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 8192), 8193)
	for scanner.Scan() {
		value := strings.TrimSuffix(scanner.Text(), "\r")
		if value == "" {
			continue
		}
		job := protocol.JobSpecV1{Value: value}
		if identityMode == tracker.IdentityModeExternalID {
			job.ID = protocol.DefaultJobID(protocol.JobTypeSeed, value)
		}
		select {
		case output <- job:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("source pack: read jobs.txt: %w", err)
	}
	return nil
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("source pack: open input: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}
