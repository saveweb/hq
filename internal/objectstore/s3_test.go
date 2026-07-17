package objectstore

import "testing"

func TestS3URIIsStrictAndRoundTrips(t *testing.T) {
	object, err := ParseURI("s3://archive-bucket/sources/project/jobs-01.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	if object.Bucket != "archive-bucket" || object.Key != "sources/project/jobs-01.jsonl.zst" ||
		URI(object) != "s3://archive-bucket/sources/project/jobs-01.jsonl.zst" {
		t.Fatalf("object = %+v URI=%q", object, URI(object))
	}
	for _, value := range []string{
		"https://bucket/key", "s3://bucket", "s3://user@bucket/key",
		"s3://bucket/key?version=x", "s3://bucket/key#fragment", "s3:///key",
	} {
		if _, err := ParseURI(value); err == nil {
			t.Fatalf("invalid URI accepted: %q", value)
		}
	}
	if NormalizeETag(` "abc-1" `) != "abc-1" {
		t.Fatal("ETag normalization failed")
	}
}

func TestS3ConfigurationRequiresExplicitSecureEndpoint(t *testing.T) {
	base := Config{
		Endpoint: "https://account.r2.cloudflarestorage.com", Region: "auto",
		AccessKeyID: "access", SecretAccessKey: "secret",
	}
	if _, err := New(base); err != nil {
		t.Fatal(err)
	}
	base.Endpoint = "http://127.0.0.1:9000"
	if _, err := New(base); err == nil {
		t.Fatal("HTTP endpoint accepted without local-test opt-in")
	}
	base.AllowHTTP = true
	if _, err := New(base); err != nil {
		t.Fatal(err)
	}
}
