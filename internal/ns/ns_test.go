package ns

import "testing"

func TestWriteBackNamespaceIdentity(t *testing.T) {
	if !WriteBack.Valid() {
		t.Fatal("write-back kind is not valid")
	}
	if got, want := WriteBack.Prefix(), "s3w_"; got != want {
		t.Fatalf("prefix = %q, want %q", got, want)
	}
	if got, want := WriteBack.InlineMax(), 65536; got != want {
		t.Fatalf("inline max = %d, want %d", got, want)
	}
	if !WriteBack.Logged() {
		t.Fatal("write-back tables must be WAL-logged")
	}
	if !Cache.Logged() {
		t.Fatal("cache tables must be WAL-logged")
	}
	schema, err := WriteBack.Schema("origin")
	if err != nil || schema != "s3w_origin" {
		t.Fatalf("schema = %q, err=%v", schema, err)
	}
	kind, name, ok := ParseSchema(schema)
	if !ok || kind != WriteBack || name != "origin" {
		t.Fatalf("ParseSchema(%q) = %q/%q/%v", schema, kind, name, ok)
	}
	if got := WriteBackRef("origin").Key(); got != "writeback\x00origin" {
		t.Fatalf("ref key = %q", got)
	}
	if _, err := WriteBack.Schema("Bad Name"); err == nil {
		t.Fatal("invalid write-back name accepted")
	}
	if got, want := WriteBack.MaxKeyLen(), 2048; got != want {
		t.Fatalf("max key = %d, want %d", got, want)
	}
	if Bucket.MaxKeyLen() != 1024 {
		t.Fatalf("bucket max key = %d", Bucket.MaxKeyLen())
	}
}
