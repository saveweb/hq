package queue

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"unicode/utf8"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	maxIDBytes        = 128
	maxURLBytes       = 8192
	maxAttrsBytes     = 8 << 10
	maxJobSpecBytes   = 32 << 10
	maxMessageBytes   = 2048
	maxErrorCodeBytes = 64
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

func ValidateIdentifier(value string) bool {
	return len(value) > 0 && len(value) <= maxIDBytes && identifierPattern.MatchString(value)
}

type NormalizedJob struct {
	JobSpec
	AttrsJSON string
}

func NormalizeJob(job JobSpec) (NormalizedJob, *Error) {
	if !ValidateIdentifier(job.ID) {
		return NormalizedJob{}, invalidJob("id must be 1-128 ASCII bytes matching [A-Za-z0-9._:-]+")
	}
	if len(job.URL) == 0 || len(job.URL) > maxURLBytes || !utf8.ValidString(job.URL) {
		return NormalizedJob{}, invalidJob("url must be valid UTF-8 and at most 8192 bytes")
	}
	if job.Type == "" {
		job.Type = protocol.JobTypeSeed
	}
	if job.Type != protocol.JobTypeSeed && job.Type != protocol.JobTypeAsset {
		return NormalizedJob{}, invalidJob("type must be seed or asset")
	}
	if job.Via != nil && (len(*job.Via) > maxURLBytes || !utf8.ValidString(*job.Via)) {
		return NormalizedJob{}, invalidJob("via must be valid UTF-8 and at most 8192 bytes")
	}
	if job.Hops < 0 {
		return NormalizedJob{}, invalidJob("hops must be non-negative")
	}
	if job.Attrs == nil {
		job.Attrs = map[string]any{}
	}
	attrs, err := canonicalObject(job.Attrs, maxAttrsBytes)
	if err != nil {
		return NormalizedJob{}, invalidJob("invalid attr: " + err.Error())
	}
	encoded, err := json.Marshal(struct {
		ID    string          `json:"id"`
		URL   string          `json:"url"`
		Type  string          `json:"type"`
		Via   *string         `json:"via"`
		Hops  int             `json:"hops"`
		Attrs json.RawMessage `json:"attr"`
	}{job.ID, job.URL, job.Type, job.Via, job.Hops, attrs})
	if err != nil || len(encoded) > maxJobSpecBytes {
		return NormalizedJob{}, invalidJob("normalized JobSpec exceeds 32 KiB")
	}
	return NormalizedJob{JobSpec: job, AttrsJSON: string(attrs)}, nil
}

func NormalizeOutcome(outcome Outcome) (Outcome, string, *Error) {
	if outcome.Kind != "success" && outcome.Kind != "http_error" && outcome.Kind != "skipped" {
		return Outcome{}, "", invalidRequest("outcome kind must be success, http_error, or skipped")
	}
	if outcome.Code != nil && (*outcome.Code < 0 || *outcome.Code > 999) {
		return Outcome{}, "", invalidRequest("outcome code must be between 0 and 999")
	}
	if outcome.URI != nil && len(*outcome.URI) > 4096 {
		return Outcome{}, "", invalidRequest("outcome URI exceeds 4096 bytes")
	}
	if outcome.Meta == nil {
		outcome.Meta = map[string]any{}
	}
	meta, err := canonicalObject(outcome.Meta, maxAttrsBytes)
	if err != nil {
		return Outcome{}, "", invalidRequest("invalid outcome meta: " + err.Error())
	}
	return outcome, string(meta), nil
}

func NormalizeExecutionError(value ExecutionError) (ExecutionError, string, *Error) {
	if len(value.Code) == 0 || len(value.Code) > maxErrorCodeBytes || !identifierPattern.MatchString(value.Code) {
		return ExecutionError{}, "", invalidRequest("execution error code is invalid")
	}
	if len(value.Message) > maxMessageBytes || !utf8.ValidString(value.Message) {
		return ExecutionError{}, "", invalidRequest("execution error message is invalid")
	}
	if value.Details == nil {
		value.Details = map[string]any{}
	}
	details, err := canonicalObject(value.Details, maxAttrsBytes)
	if err != nil {
		return ExecutionError{}, "", invalidRequest("invalid execution error details: " + err.Error())
	}
	encoded, err := json.Marshal(struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details"`
	}{value.Code, value.Message, details})
	if err != nil {
		return ExecutionError{}, "", invalidRequest("execution error is not JSON encodable")
	}
	return value, string(encoded), nil
}

func canonicalObject(value map[string]any, maxBytes int) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxBytes {
		return nil, fmt.Errorf("object exceeds %d bytes", maxBytes)
	}
	return encoded, nil
}

func PayloadHash(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(encoded)
	return sum[:], nil
}

func invalidJob(message string) *Error {
	return &Error{Code: protocol.ErrorInvalidJob, Message: message}
}

func invalidRequest(message string) *Error {
	return &Error{Code: protocol.ErrorInvalidRequest, Message: message}
}
