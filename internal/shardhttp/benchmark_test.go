package shardhttp_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/internal/shardhttp"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type httpBenchmarkShard struct {
	manager *shard.Manager
	server  *httptest.Server
	client  *http.Client
	token   string
	route   protocol.SessionRoute
}

func BenchmarkShardHTTPLoopbackClaimComplete(b *testing.B) {
	for _, batchSize := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.StopTimer()
			value := newHTTPBenchmarkShard(b, 0, b.N*batchSize)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for range b.N {
				if err := value.cycle(b.Context(), batchSize); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N*batchSize)/elapsed, "jobs/s")
			b.ReportMetric(float64(b.N*2)/elapsed, "requests/s")
		})
	}
}

func BenchmarkShardHTTPConcurrentClaimComplete(b *testing.B) {
	for _, batchSize := range []int{1, 64} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.StopTimer()
			value := newHTTPBenchmarkShard(b, 0, b.N*batchSize)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			b.RunParallel(func(parallel *testing.PB) {
				for parallel.Next() {
					if err := value.cycle(b.Context(), batchSize); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N*batchSize)/elapsed, "jobs/s")
			b.ReportMetric(float64(b.N*2)/elapsed, "requests/s")
		})
	}
}

func BenchmarkShardHTTPFourShardAggregate(b *testing.B) {
	const shardCount = 4
	for _, batchSize := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.StopTimer()
			values := make([]*httpBenchmarkShard, shardCount)
			iterations := make([]int, shardCount)
			for index := range shardCount {
				iterations[index] = b.N / shardCount
				if index < b.N%shardCount {
					iterations[index]++
				}
				values[index] = newHTTPBenchmarkShard(b, index, iterations[index]*batchSize)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			var workers sync.WaitGroup
			errors := make(chan error, shardCount)
			for index := range shardCount {
				workers.Add(1)
				go func(index int) {
					defer workers.Done()
					for range iterations[index] {
						if err := values[index].cycle(b.Context(), batchSize); err != nil {
							errors <- err
							return
						}
					}
				}(index)
			}
			workers.Wait()
			b.StopTimer()
			close(errors)
			for err := range errors {
				b.Fatal(err)
			}
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(b.N*batchSize)/elapsed, "jobs/s")
			b.ReportMetric(float64(b.N*2)/elapsed, "requests/s")
		})
	}
}

func newHTTPBenchmarkShard(b *testing.B, index, jobsCount int) *httpBenchmarkShard {
	b.Helper()
	projectID := "benchmark"
	shardID := fmt.Sprintf("shard-%d", index)
	ownerID := fmt.Sprintf("owner-%d", index)
	sessionID := fmt.Sprintf("session-%d", index)
	manager, err := shard.NewManager(shard.ManagerConfig{
		AgentID: ownerID, Issuer: "https://benchmark.invalid", DataDir: b.TempDir(),
		Clock: func() int64 { return httpNow },
	})
	if err != nil {
		b.Fatal(err)
	}
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	signer, err := access.NewSigner(
		"https://benchmark.invalid", "benchmark-key", privateKey, func() int64 { return httpNow },
	)
	if err != nil {
		b.Fatal(err)
	}
	heartbeat := protocol.AgentHeartbeatResponse{
		ServerTime: httpNow, HeartbeatAfterSeconds: 30,
		SigningKeys: []protocol.SigningKey{{
			KeyID: "benchmark-key", Algorithm: "EdDSA",
			PublicKeyEd25519: base64.RawURLEncoding.EncodeToString(ed25519.PublicKey(publicKey)),
			NotBefore:        httpNow - 60, NotAfter: httpNow + 7200,
		}},
		OwnerAssignments: []protocol.OwnerAssignment{{
			Route:  protocol.Route{ProjectID: projectID, ShardID: shardID, Generation: 1},
			Status: trackerActiveStatus, OwnerLeaseExpiresAt: httpNow + 3600,
		}},
	}
	if err := manager.ApplyHeartbeat(b.Context(), heartbeat); err != nil {
		b.Fatal(err)
	}
	token, _, err := signer.Sign(access.Scope{
		WorkerAgentID: fmt.Sprintf("worker-%d", index), SessionID: sessionID,
		ProjectID: projectID, ShardID: shardID, Generation: 1, OwnerAgentID: ownerID,
	}, httpNow+3600, 600)
	if err != nil {
		b.Fatal(err)
	}
	authorization, err := manager.Authorize(token)
	if err != nil {
		b.Fatal(err)
	}
	prepareHTTPBenchmarkJobs(b, authorization.Store, index, jobsCount)
	config := shardhttp.DefaultConfig(ownerID)
	serverHandler, err := shardhttp.New(
		manager, config, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(serverHandler)
	transport := &http.Transport{
		MaxIdleConns: 16, MaxIdleConnsPerHost: 16, IdleConnTimeout: time.Minute,
	}
	value := &httpBenchmarkShard{
		manager: manager, server: server,
		client: &http.Client{Transport: transport, Timeout: 30 * time.Second}, token: token,
		route: protocol.SessionRoute{
			Route:     protocol.Route{ProjectID: projectID, ShardID: shardID, Generation: 1},
			SessionID: sessionID,
		},
	}
	b.Cleanup(func() {
		server.Close()
		transport.CloseIdleConnections()
		if err := manager.Close(); err != nil {
			b.Error(err)
		}
	})
	return value
}

const trackerActiveStatus = "active"

func prepareHTTPBenchmarkJobs(b *testing.B, store queue.Store, shardIndex, count int) {
	b.Helper()
	const loadBatch = 256
	for offset := 0; offset < count; offset += loadBatch {
		end := min(offset+loadBatch, count)
		jobs := make([]queue.JobSpec, end-offset)
		for index := offset; index < end; index++ {
			jobs[index-offset] = queue.JobSpec{
				ID:    fmt.Sprintf("s%d-job-%09d", shardIndex, index),
				URL:   fmt.Sprintf("https://benchmark.invalid/archive/%d/%09d", shardIndex, index),
				Type:  protocol.JobTypeSeed,
				Attrs: map[string]any{"profile": "default", "depth": 0},
			}
		}
		if _, err := store.Enqueue(b.Context(), 1, httpNow, jobs); err != nil {
			b.Fatal(err)
		}
	}
}

func (s *httpBenchmarkShard) cycle(ctx context.Context, batchSize int) error {
	claimRequest := protocol.ClaimRequest{
		SessionRoute: s.route, MaxJobs: batchSize, LeaseSeconds: 300,
	}
	var claim protocol.ClaimResponse
	if err := s.post(ctx, "/api/v1/queue/claim", claimRequest, &claim); err != nil {
		return err
	}
	if len(claim.Jobs) != batchSize {
		return fmt.Errorf("claim returned %d jobs, expected %d", len(claim.Jobs), batchSize)
	}
	items := make([]protocol.CompleteItem, len(claim.Jobs))
	for index, job := range claim.Jobs {
		items[index] = protocol.CompleteItem{
			JobID: job.ID, AttemptID: job.AttemptID,
			Outcome: protocol.Outcome{
				Kind: "success", Meta: protocol.Attrs{"warc_filename": "benchmark.warc.gz"},
			},
			DiscoveredJobs: []protocol.JobSpecV1{},
		}
	}
	var completed protocol.BatchResultResponse
	if err := s.post(ctx, "/api/v1/queue/complete", protocol.CompleteRequest{
		SessionRoute: s.route, Items: items,
	}, &completed); err != nil {
		return err
	}
	if len(completed.Results) != batchSize {
		return fmt.Errorf("complete returned %d results, expected %d", len(completed.Results), batchSize)
	}
	return nil
}

func (s *httpBenchmarkShard) post(ctx context.Context, path string, requestValue, responseValue any) error {
	body, err := json.Marshal(requestValue)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, s.server.URL+path, bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	request.Header.Set("Pragma", "no-cache")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	responseBody, readError := io.ReadAll(response.Body)
	closeError := response.Body.Close()
	if readError != nil {
		return readError
	}
	if closeError != nil {
		return closeError
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, responseBody)
	}
	if err := json.Unmarshal(responseBody, responseValue); err != nil {
		return err
	}
	return nil
}
