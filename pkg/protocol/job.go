// Package protocol contains the transport types shared by SavewebHQ servers
// and Go clients. It deliberately contains no storage or networking code.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
)

const (
	JobTypeSeed  = "seed"
	JobTypeAsset = "asset"
)

// JobSpecV1 is the stable queue identity and work description exchanged by
// source loaders, shards, and workers.
type JobSpecV1 struct {
	ID    string         `json:"id"`
	URL   string         `json:"url"`
	Type  string         `json:"type,omitempty"`
	Via   *string        `json:"via"`
	Hops  int            `json:"hops,omitempty"`
	Attrs map[string]any `json:"attr,omitempty"`
}

// DefaultJobID returns the v1 content-derived job ID. No URL normalization is
// performed. An empty job type is treated as "seed".
func DefaultJobID(jobType, url string) string {
	if jobType == "" {
		jobType = JobTypeSeed
	}
	h := sha256.New()
	_, _ = h.Write([]byte(jobType))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(url))
	return "j1_" + hex.EncodeToString(h.Sum(nil))
}
