package sqlitequeue_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	benchmarkNow        = int64(1_780_000_000)
	benchmarkGeneration = int64(1)
)

func BenchmarkSQLiteClaimComplete(b *testing.B) {
	for _, batchSize := range []int{64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.StopTimer()
			store := openBenchmarkStore(b, "shard-1")
			prepareBenchmarkJobs(b, store, "job", b.N*batchSize)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for range b.N {
				claimAndComplete(b.Context(), b, store, "session-1", batchSize)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "jobs/s")
		})
	}
}

func BenchmarkSQLiteFourShardAggregate(b *testing.B) {
	const shardCount = 4
	for _, batchSize := range []int{64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.StopTimer()
			stores := make([]*sqlitequeue.Store, shardCount)
			iterations := make([]int, shardCount)
			for index := range shardCount {
				iterations[index] = b.N / shardCount
				if index < b.N%shardCount {
					iterations[index]++
				}
				stores[index] = openBenchmarkStore(b, fmt.Sprintf("shard-%d", index))
				prepareBenchmarkJobs(
					b, stores[index], fmt.Sprintf("s%d-job", index), iterations[index]*batchSize,
				)
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
						if err := claimAndCompleteError(
							b.Context(), stores[index], fmt.Sprintf("session-%d", index), batchSize,
						); err != nil {
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
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "jobs/s")
		})
	}
}

func openBenchmarkStore(b *testing.B, shardID string) *sqlitequeue.Store {
	b.Helper()
	store, err := sqlitequeue.Open(
		b.Context(), filepath.Join(b.TempDir(), shardID+".sqlite"),
		queue.Identity{ProjectID: "benchmark", ShardID: shardID, Generation: benchmarkGeneration},
		sqlitequeue.WithClock(func() int64 { return benchmarkNow }),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := store.SetFence(
		b.Context(), benchmarkGeneration, benchmarkNow, benchmarkNow+3600,
	); err != nil {
		b.Fatal(err)
	}
	return store
}

func prepareBenchmarkJobs(b *testing.B, store *sqlitequeue.Store, prefix string, count int) {
	b.Helper()
	const loadBatch = 256
	for offset := 0; offset < count; offset += loadBatch {
		end := min(offset+loadBatch, count)
		jobs := make([]queue.JobSpec, end-offset)
		for index := offset; index < end; index++ {
			jobs[index-offset] = queue.JobSpec{
				ID:    fmt.Sprintf("%s-%09d", prefix, index),
				URL:   fmt.Sprintf("https://benchmark.invalid/archive/%09d", index),
				Type:  protocol.JobTypeSeed,
				Attrs: map[string]any{"profile": "default", "depth": 0},
			}
		}
		if _, err := store.Enqueue(
			b.Context(), benchmarkGeneration, benchmarkNow, jobs,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func claimAndComplete(
	ctx context.Context,
	b *testing.B,
	store *sqlitequeue.Store,
	sessionID string,
	batchSize int,
) {
	b.Helper()
	if err := claimAndCompleteError(ctx, store, sessionID, batchSize); err != nil {
		b.Fatal(err)
	}
}

func claimAndCompleteError(
	ctx context.Context,
	store *sqlitequeue.Store,
	sessionID string,
	batchSize int,
) error {
	jobs, err := store.ClaimBatch(
		ctx, benchmarkGeneration, benchmarkNow, sessionID, nil, batchSize, 300,
	)
	if err != nil {
		return err
	}
	if len(jobs) != batchSize {
		return fmt.Errorf("claim returned %d jobs, expected %d", len(jobs), batchSize)
	}
	items := make([]queue.CompleteItem, len(jobs))
	for index, job := range jobs {
		items[index] = queue.CompleteItem{
			JobID: job.ID, AttemptID: job.AttemptID,
			Outcome: queue.Outcome{
				Kind: "success", Meta: map[string]any{"warc_filename": "benchmark.warc.gz"},
			},
			DiscoveredJobs: []queue.JobSpec{},
		}
	}
	results, err := store.CompleteBatch(
		ctx, benchmarkGeneration, benchmarkNow, sessionID, items,
	)
	if err != nil {
		return err
	}
	if len(results) != batchSize {
		return fmt.Errorf("complete returned %d results, expected %d", len(results), batchSize)
	}
	return nil
}
