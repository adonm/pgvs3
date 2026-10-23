package sqs

import (
	"strings"
	"testing"
)

func TestFifoFields(t *testing.T) {
	body := []byte("hello")
	if g, d, err := fifoFields(false, false, body, "g", "d"); err != nil || g != "g" || d != "" {
		t.Fatalf("standard queue: g=%q d=%q err=%v", g, d, err)
	}
	if _, _, err := fifoFields(true, false, body, "", "d"); err == nil {
		t.Fatal("missing group ID should fail")
	}
	if _, _, err := fifoFields(true, false, body, "g", ""); err == nil {
		t.Fatal("missing dedup ID should fail without content dedup")
	}
	if _, d, err := fifoFields(true, true, body, "g", ""); err != nil || d != sha256Hex(body) {
		t.Fatalf("content dedup: d=%q err=%v", d, err)
	}
	g, d, err := fifoFields(true, false, body, "group", "dedup")
	if err != nil || g != "group" || d != "dedup" {
		t.Fatalf("explicit: g=%q d=%q err=%v", g, d, err)
	}
	long := strings.Repeat("x", 129)
	if _, _, err := fifoFields(true, false, body, long, "d"); err == nil {
		t.Fatal("long group ID should fail")
	}
	if _, _, err := fifoFields(true, false, body, "g", long); err == nil {
		t.Fatal("long dedup ID should fail")
	}
}

func TestAttrBool(t *testing.T) {
	for _, v := range []any{true, "true", "TRUE"} {
		if !attrBool(map[string]any{"FifoQueue": v}, "FifoQueue") {
			t.Fatalf("%v should be true", v)
		}
	}
	for _, v := range []any{false, "false", "", 1} {
		if attrBool(map[string]any{"FifoQueue": v}, "FifoQueue") {
			t.Fatalf("%v should not be true", v)
		}
	}
}
