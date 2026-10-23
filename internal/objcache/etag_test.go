package objcache

import (
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

func TestTextVersion(t *testing.T) {
	if got := textVersion("abc123"); got != "abc123" {
		t.Fatalf("ascii version = %q", got)
	}
	if got := textVersion("\x94\x01"); got != "9401" {
		t.Fatalf("binary version = %q, want 9401", got)
	}
	if got := textVersion(""); got != "" {
		t.Fatalf("empty version = %q", got)
	}
}

func TestEtagForAttrs(t *testing.T) {
	when := time.Unix(0, 42)
	if got := etagForAttrs(originAttrs{ETag: "etag", Size: 5, LastModified: when}); got != "etag" {
		t.Fatalf("real etag = %q", got)
	}
	if got := etagForAttrs(originAttrs{Size: 5, LastModified: when}); got != "5-42" {
		t.Fatalf("synthetic etag = %q", got)
	}
}

func TestOriginETag(t *testing.T) {
	m := &store.Meta{ETag: "content-md5", OriginETag: "origin-etag"}
	if got := originETag(m); got != "origin-etag" {
		t.Fatalf("origin etag = %q", got)
	}
	m.OriginETag = ""
	if got := originETag(m); got != "content-md5" {
		t.Fatalf("fallback etag = %q", got)
	}
}
