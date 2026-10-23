package objcache

import (
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

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
