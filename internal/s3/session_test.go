package s3

import (
	"testing"
	"time"
)

func TestSessionIssuerRoundTrip(t *testing.T) {
	iss := NewSessionIssuer([]byte("test-master"), 5*time.Minute)
	s := iss.Create("demo--use1-az1--x-s3", "ReadOnly")
	got, err := iss.Validate(s.Token, "demo--use1-az1--x-s3")
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessKey != s.AccessKey || got.SecretKey != s.SecretKey || got.Mode != "ReadOnly" {
		t.Fatalf("session mismatch: %+v vs %+v", got, s)
	}
	if _, err := iss.Validate(s.Token, "other--use1-az1--x-s3"); err == nil {
		t.Fatal("token accepted for another bucket")
	}
	if _, err := iss.Validate(s.Token+"x", "demo--use1-az1--x-s3"); err == nil {
		t.Fatal("tampered token accepted")
	}
}

func TestSessionExpiry(t *testing.T) {
	iss := NewSessionIssuer([]byte("test-master"), time.Millisecond)
	s := iss.Create("b--use1-az1--x-s3", "ReadWrite")
	time.Sleep(3 * time.Millisecond)
	if _, err := iss.Validate(s.Token, "b--use1-az1--x-s3"); err == nil {
		t.Fatal("expired session accepted")
	}
}

func TestDirectoryBucket(t *testing.T) {
	for _, tc := range []struct {
		name string
		dir  bool
		zone string
	}{
		{"demo--use1-az1--x-s3", true, "use1-az1"},
		{"my-bucket", false, ""},
		{"demo--x-s3", false, ""},
		{"a--b--usw2-az2--x-s3", true, "usw2-az2"},
	} {
		if got := isDirectoryBucket(tc.name); got != tc.dir {
			t.Fatalf("%q dir=%v", tc.name, got)
		}
		if got := bucketZone(tc.name); got != tc.zone {
			t.Fatalf("%q zone=%q", tc.name, got)
		}
	}
}
