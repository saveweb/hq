package shardhttp

import (
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func toQueueJob(value protocol.JobSpecV1) queue.JobSpec {
	return queue.JobSpec{
		ID: value.ID, URL: value.URL, Type: value.Type, Via: value.Via,
		Hops: value.Hops, Attrs: value.Attrs,
	}
}

func toProtocolJob(value queue.ClaimedJob) protocol.ClaimedJob {
	return protocol.ClaimedJob{
		JobSpecV1: protocol.JobSpecV1{
			ID: value.ID, URL: value.URL, Type: value.Type, Via: value.Via,
			Hops: value.Hops, Attrs: value.Attrs,
		},
		AttemptID: value.AttemptID, LeaseExpiresAt: value.LeaseExpiresAt,
	}
}

func toQueueComplete(values []protocol.CompleteItem) []queue.CompleteItem {
	result := make([]queue.CompleteItem, len(values))
	for index, value := range values {
		discovered := make([]queue.JobSpec, len(value.DiscoveredJobs))
		for jobIndex, job := range value.DiscoveredJobs {
			discovered[jobIndex] = toQueueJob(job)
		}
		result[index] = queue.CompleteItem{
			JobID: value.JobID, AttemptID: value.AttemptID,
			Outcome: queue.Outcome{
				Kind: value.Outcome.Kind, Code: value.Outcome.Code,
				URI: value.Outcome.URI, Meta: value.Outcome.Meta,
			},
			DiscoveredJobs: discovered,
		}
	}
	return result
}

func toQueueFail(values []protocol.FailItem) []queue.FailItem {
	result := make([]queue.FailItem, len(values))
	for index, value := range values {
		result[index] = queue.FailItem{
			JobID: value.JobID, AttemptID: value.AttemptID, Retryable: value.Retryable,
			Error: queue.ExecutionError{
				Code: value.Error.Code, Message: value.Error.Message, Details: value.Error.Details,
			},
		}
	}
	return result
}

func toQueueAttempts(values []protocol.AttemptRef) []queue.AttemptRef {
	result := make([]queue.AttemptRef, len(values))
	for index, value := range values {
		result[index] = queue.AttemptRef{JobID: value.JobID, AttemptID: value.AttemptID}
	}
	return result
}

func toProtocolResults(values []queue.ItemResult) []protocol.ItemResult {
	result := make([]protocol.ItemResult, len(values))
	for index, value := range values {
		var jobStatus *string
		if value.JobStatus != "" {
			status := value.JobStatus
			jobStatus = &status
		}
		var itemError *protocol.APIError
		if value.Error != nil {
			details := protocol.Attrs(value.Error.Details)
			if details == nil {
				details = protocol.Attrs{}
			}
			itemError = &protocol.APIError{
				Code: value.Error.Code, Message: value.Error.Message,
				Retryable: value.Error.Retryable, Details: details,
			}
		}
		result[index] = protocol.ItemResult{
			JobID: value.JobID, AttemptID: value.AttemptID, Status: value.Status,
			JobStatus: jobStatus, LeaseExpiresAt: value.LeaseExpiresAt, Error: itemError,
		}
	}
	return result
}
