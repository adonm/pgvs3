package s3client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func clearAWSCredEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN",
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_ROLE_ARN",
		"AWS_ROLE_SESSION_NAME",
		"AWS_EC2_METADATA_DISABLED",
		"AWS_EC2_METADATA_ENDPOINT",
	} {
		t.Setenv(k, "")
	}
}

func TestChainStaticEnv(t *testing.T) {
	clearAWSCredEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIASTATIC")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-static")
	t.Setenv("AWS_SESSION_TOKEN", "tok-static")

	p := NewChainProvider(ChainConfig{IMDSDisabled: true})
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.AccessKey != "AKIASTATIC" || c.SecretKey != "secret-static" || c.SessionToken != "tok-static" {
		t.Fatalf("got %+v", c)
	}
}

func TestChainPodIdentityAndRefresh(t *testing.T) {
	clearAWSCredEnv(t)
	authFile := filepath.Join(t.TempDir(), "pod-auth-token")
	if err := os.WriteFile(authFile, []byte("eks-pod-token\n"), 0600); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	curNow := now

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "eks-pod-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		n := calls.Add(1)
		exp := curNow.Add(10 * time.Minute).Format(time.RFC3339)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"AccessKeyId":     "AKIAPOD" + string(rune('0'+n)),
			"SecretAccessKey": "sec-pod",
			"Token":           "tok-pod",
			"Expiration":      exp,
		})
	}))
	defer srv.Close()

	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/v1/credentials")
	t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", authFile)

	p := NewChainProvider(ChainConfig{
		IMDSDisabled: true,
		RefreshAhead: 5 * time.Minute,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return curNow
		},
	})

	// Concurrent initial fetches should coalesce to 1 HTTP call.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			c, err := p.Get(context.Background())
			if err != nil || c.AccessKey != "AKIAPOD1" {
				t.Errorf("got %+v err=%v", c, err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}

	// Within refresh buffer (t + 6m, exp is t + 10m, buffer is 5m) -> refreshes!
	mu.Lock()
	curNow = now.Add(6 * time.Minute)
	mu.Unlock()
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c2.AccessKey != "AKIAPOD2" || calls.Load() != 2 {
		t.Fatalf("refresh: got %+v calls=%d", c2, calls.Load())
	}
}

func TestChainIRSAWebIdentity(t *testing.T) {
	clearAWSCredEnv(t)
	tokFile := filepath.Join(t.TempDir(), "oidc-token")
	if err := os.WriteFile(tokFile, []byte("jwt-token-value\n"), 0600); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Form.Get("Action") != "AssumeRoleWithWebIdentity" ||
			r.Form.Get("RoleArn") != "arn:aws:iam::123456789012:role/pgvs3" ||
			r.Form.Get("WebIdentityToken") != "jwt-token-value" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>` +
			`<AccessKeyId>ASIAIRSA</AccessKeyId>` +
			`<SecretAccessKey>sec-irsa</SecretAccessKey>` +
			`<SessionToken>tok-irsa</SessionToken>` +
			`<Expiration>` + exp + `</Expiration>` +
			`</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`))
	}))
	defer srv.Close()

	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokFile)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/pgvs3")

	p := NewChainProvider(ChainConfig{
		IMDSDisabled: true,
		STSEndpoint:  srv.URL,
	})
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.AccessKey != "ASIAIRSA" || c.SecretKey != "sec-irsa" || c.SessionToken != "tok-irsa" {
		t.Fatalf("got %+v", c)
	}
}

func TestChainIMDSv2(t *testing.T) {
	clearAWSCredEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /latest/api/token":
			if r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte("imds-v2-token"))
		case "GET /latest/meta-data/iam/security-credentials/":
			if r.Header.Get("X-aws-ec2-metadata-token") != "imds-v2-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte("pgvs3-node-role\n"))
		case "GET /latest/meta-data/iam/security-credentials/pgvs3-node-role":
			if r.Header.Get("X-aws-ec2-metadata-token") != "imds-v2-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"AccessKeyId":     "ASIAIMDS",
				"SecretAccessKey": "sec-imds",
				"Token":           "tok-imds",
				"Expiration":      exp,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := NewChainProvider(ChainConfig{IMDSEndpoint: srv.URL})
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.AccessKey != "ASIAIMDS" || c.SecretKey != "sec-imds" || c.SessionToken != "tok-imds" {
		t.Fatalf("got %+v", c)
	}
}

func TestChainAnonymousFallback(t *testing.T) {
	clearAWSCredEnv(t)
	p := NewChainProvider(ChainConfig{IMDSDisabled: true})
	c, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.AccessKey != "" {
		t.Fatalf("expected empty anonymous credentials, got %+v", c)
	}
}
