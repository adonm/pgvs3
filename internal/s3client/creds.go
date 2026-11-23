package s3client

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	// ErrIMDS indicates a real IMDSv2 failure (not a missing/disabled IMDS).
	ErrIMDS            = errors.New("imds credential fetch failed")
	errIMDSUnavailable = errors.New("imds unavailable")
)

// Credentials holds temporary or static AWS credentials.
// An empty AccessKey represents anonymous/unsigned access.
type Credentials struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
	Expiration   time.Time
}

func (c Credentials) valid(now time.Time, buffer time.Duration) bool {
	if c.AccessKey == "" {
		return false
	}
	if c.Expiration.IsZero() {
		return true
	}
	return now.Add(buffer).Before(c.Expiration)
}

// CredentialsProvider returns AWS credentials, refreshing them as needed.
type CredentialsProvider interface {
	Get(ctx context.Context) (Credentials, error)
}

// ChainConfig configures the default AWS credential chain.
type ChainConfig struct {
	Region       string
	HTTPClient   *http.Client
	IMDSDisabled bool
	IMDSEndpoint string
	STSEndpoint  string
	RefreshAhead time.Duration
	Now          func() time.Time
}

// ChainProvider resolves credentials from:
//  1. Static environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN)
//  2. EKS Pod Identity (AWS_CONTAINER_CREDENTIALS_FULL_URI + AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE)
//  3. IRSA Web Identity (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN)
//  4. IMDSv2 (unless disabled)
//  5. Anonymous (empty Credentials) if no provider is configured.
type ChainProvider struct {
	cfg    ChainConfig
	client *http.Client
	now    func() time.Time
	buffer time.Duration

	mu            sync.Mutex
	cached        Credentials
	negativeUntil time.Time
	inflight      *credCall
}

type credCall struct {
	done chan struct{}
	cred Credentials
	err  error
}

func NewChainProvider(cfg ChainConfig) *ChainProvider {
	c := cfg.HTTPClient
	if c == nil {
		c = &http.Client{Timeout: 5 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	buf := cfg.RefreshAhead
	if buf <= 0 {
		buf = 5 * time.Minute
	}
	if cfg.Region == "" {
		cfg.Region = firstNonEmpty(os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), "us-east-1")
	}
	if cfg.IMDSEndpoint == "" {
		if ep := os.Getenv("AWS_EC2_METADATA_ENDPOINT"); ep != "" {
			cfg.IMDSEndpoint = strings.TrimRight(ep, "/")
		} else {
			cfg.IMDSEndpoint = "http://169.254.169.254"
		}
	}
	if !cfg.IMDSDisabled && strings.EqualFold(os.Getenv("AWS_EC2_METADATA_DISABLED"), "true") {
		cfg.IMDSDisabled = true
	}
	return &ChainProvider{
		cfg:    cfg,
		client: c,
		now:    now,
		buffer: buf,
	}
}

func (p *ChainProvider) Get(ctx context.Context) (Credentials, error) {
	p.mu.Lock()
	now := p.now()
	if p.cached.valid(now, p.buffer) {
		c := p.cached
		p.mu.Unlock()
		return c, nil
	}
	if !p.negativeUntil.IsZero() && now.Before(p.negativeUntil) {
		p.mu.Unlock()
		return Credentials{}, nil
	}
	if p.inflight != nil {
		call := p.inflight
		p.mu.Unlock()
		select {
		case <-call.done:
			return call.cred, call.err
		case <-ctx.Done():
			return Credentials{}, ctx.Err()
		}
	}
	call := &credCall{done: make(chan struct{})}
	p.inflight = call
	p.mu.Unlock()

	cred, err := p.resolve(ctx)

	p.mu.Lock()
	if err == nil {
		if cred.AccessKey != "" {
			p.cached = cred
			p.negativeUntil = time.Time{}
		} else {
			p.negativeUntil = p.now().Add(time.Minute)
		}
	}
	p.inflight = nil
	call.cred, call.err = cred, err
	close(call.done)
	p.mu.Unlock()
	return cred, err
}

func (p *ChainProvider) resolve(ctx context.Context) (Credentials, error) {
	// 1. Static environment variables.
	if ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"); ak != "" && sk != "" {
		return Credentials{
			AccessKey:    ak,
			SecretKey:    sk,
			SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		}, nil
	}

	// 2. EKS Pod Identity.
	if uri := os.Getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"); uri != "" {
		return p.fetchPodIdentity(ctx, uri)
	}

	// 3. IRSA Web Identity.
	if tokenFile, roleARN := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), os.Getenv("AWS_ROLE_ARN"); tokenFile != "" && roleARN != "" {
		return p.fetchWebIdentity(ctx, tokenFile, roleARN)
	}

	// 4. IMDSv2.
	if !p.cfg.IMDSDisabled {
		cred, err := p.fetchIMDSv2(ctx)
		if err == nil {
			return cred, nil
		}
		if !errors.Is(err, errIMDSUnavailable) {
			return Credentials{}, err
		}
	}

	// 5. Anonymous.
	return Credentials{}, nil
}

type containerCredResponse struct {
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
	Code            string `json:"Code"`
	Message         string `json:"Message"`
}

func (p *ChainProvider) fetchPodIdentity(ctx context.Context, uri string) (Credentials, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("pod identity request: %w", err)
	}
	if authFile := os.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"); authFile != "" {
		tok, err := os.ReadFile(authFile)
		if err != nil {
			return Credentials{}, fmt.Errorf("read pod identity auth token file: %w", err)
		}
		req.Header.Set("Authorization", strings.TrimSpace(string(tok)))
	} else if tok := os.Getenv("AWS_CONTAINER_AUTHORIZATION_TOKEN"); tok != "" {
		req.Header.Set("Authorization", tok)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return Credentials{}, fmt.Errorf("pod identity fetch: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Credentials{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Credentials{}, fmt.Errorf("pod identity HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out containerCredResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return Credentials{}, fmt.Errorf("pod identity decode: %w", err)
	}
	if out.AccessKeyID == "" || out.SecretAccessKey == "" {
		return Credentials{}, fmt.Errorf("pod identity missing credentials: %s %s", out.Code, out.Message)
	}
	exp, err := parseCredTime(out.Expiration)
	if err != nil {
		return Credentials{}, fmt.Errorf("pod identity expiration: %w", err)
	}
	return Credentials{
		AccessKey:    out.AccessKeyID,
		SecretKey:    out.SecretAccessKey,
		SessionToken: out.Token,
		Expiration:   exp,
	}, nil
}

type stsAssumeRoleWithWebIdentityResponse struct {
	Credentials struct {
		AccessKeyID     string `xml:"AccessKeyId"`
		SecretAccessKey string `xml:"SecretAccessKey"`
		SessionToken    string `xml:"SessionToken"`
		Expiration      string `xml:"Expiration"`
	} `xml:"AssumeRoleWithWebIdentityResult>Credentials"`
}

func (p *ChainProvider) fetchWebIdentity(ctx context.Context, tokenFile, roleARN string) (Credentials, error) {
	tok, err := os.ReadFile(tokenFile)
	if err != nil {
		return Credentials{}, fmt.Errorf("read web identity token: %w", err)
	}
	sessionName := os.Getenv("AWS_ROLE_SESSION_NAME")
	if sessionName == "" {
		sessionName = "pgvs3"
	}
	endpoint := p.cfg.STSEndpoint
	if endpoint == "" {
		endpoint = "https://sts." + p.cfg.Region + ".amazonaws.com"
	}
	form := url.Values{
		"Action":           {"AssumeRoleWithWebIdentity"},
		"Version":          {"2011-06-15"},
		"RoleArn":          {roleARN},
		"RoleSessionName":  {sessionName},
		"WebIdentityToken": {strings.TrimSpace(string(tok))},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Credentials{}, fmt.Errorf("sts request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return Credentials{}, fmt.Errorf("sts assume role: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Credentials{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Credentials{}, fmt.Errorf("sts HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out stsAssumeRoleWithWebIdentityResponse
	if err := xml.Unmarshal(body, &out); err != nil {
		return Credentials{}, fmt.Errorf("sts decode: %w", err)
	}
	if out.Credentials.AccessKeyID == "" || out.Credentials.SecretAccessKey == "" {
		return Credentials{}, errors.New("sts response missing credentials")
	}
	exp, err := parseCredTime(out.Credentials.Expiration)
	if err != nil {
		return Credentials{}, fmt.Errorf("sts expiration: %w", err)
	}
	return Credentials{
		AccessKey:    out.Credentials.AccessKeyID,
		SecretKey:    out.Credentials.SecretAccessKey,
		SessionToken: out.Credentials.SessionToken,
		Expiration:   exp,
	}, nil
}

func (p *ChainProvider) fetchIMDSv2(ctx context.Context) (Credentials, error) {
	imdsCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Step 1: IMDSv2 session token.
	tokReq, err := http.NewRequestWithContext(imdsCtx, http.MethodPut, p.cfg.IMDSEndpoint+"/latest/api/token", nil)
	if err != nil {
		return Credentials{}, err
	}
	tokReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tokResp, err := p.client.Do(tokReq)
	if err != nil {
		if ctx.Err() != nil {
			return Credentials{}, fmt.Errorf("%w: %w", ErrIMDS, ctx.Err())
		}
		return Credentials{}, errIMDSUnavailable
	}
	tokBytes, err := io.ReadAll(io.LimitReader(tokResp.Body, 4096))
	_ = tokResp.Body.Close()
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: read token: %w", ErrIMDS, err)
	}
	if tokResp.StatusCode == http.StatusForbidden || tokResp.StatusCode == http.StatusNotFound || tokResp.StatusCode == http.StatusMethodNotAllowed {
		return Credentials{}, errIMDSUnavailable
	}
	if tokResp.StatusCode != http.StatusOK || len(tokBytes) == 0 {
		return Credentials{}, fmt.Errorf("%w: token HTTP %d", ErrIMDS, tokResp.StatusCode)
	}
	imdsToken := strings.TrimSpace(string(tokBytes))

	// Step 2: Role name.
	roleReq, err := http.NewRequestWithContext(imdsCtx, http.MethodGet, p.cfg.IMDSEndpoint+"/latest/meta-data/iam/security-credentials/", nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: %w", ErrIMDS, err)
	}
	roleReq.Header.Set("X-aws-ec2-metadata-token", imdsToken)
	roleResp, err := p.client.Do(roleReq)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: role list: %w", ErrIMDS, err)
	}
	roleBytes, err := io.ReadAll(io.LimitReader(roleResp.Body, 4096))
	_ = roleResp.Body.Close()
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: read role list: %w", ErrIMDS, err)
	}
	if roleResp.StatusCode == http.StatusNotFound {
		return Credentials{}, errIMDSUnavailable
	}
	if roleResp.StatusCode != http.StatusOK {
		return Credentials{}, fmt.Errorf("%w: role list HTTP %d", ErrIMDS, roleResp.StatusCode)
	}
	roleName := strings.TrimSpace(strings.Split(string(roleBytes), "\n")[0])
	if roleName == "" {
		return Credentials{}, fmt.Errorf("%w: empty role name", ErrIMDS)
	}

	// Step 3: Role credentials.
	credReq, err := http.NewRequestWithContext(imdsCtx, http.MethodGet, p.cfg.IMDSEndpoint+"/latest/meta-data/iam/security-credentials/"+url.PathEscape(roleName), nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: %w", ErrIMDS, err)
	}
	credReq.Header.Set("X-aws-ec2-metadata-token", imdsToken)
	credResp, err := p.client.Do(credReq)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: cred fetch: %w", ErrIMDS, err)
	}
	defer credResp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(credResp.Body, 1<<20))
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: read cred: %w", ErrIMDS, err)
	}
	if credResp.StatusCode != http.StatusOK {
		return Credentials{}, fmt.Errorf("%w: cred HTTP %d", ErrIMDS, credResp.StatusCode)
	}
	var out containerCredResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return Credentials{}, fmt.Errorf("%w: decode: %w", ErrIMDS, err)
	}
	if out.AccessKeyID == "" || out.SecretAccessKey == "" {
		return Credentials{}, fmt.Errorf("%w: response missing credentials", ErrIMDS)
	}
	exp, err := parseCredTime(out.Expiration)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: expiration: %w", ErrIMDS, err)
	}
	return Credentials{
		AccessKey:    out.AccessKeyID,
		SecretKey:    out.SecretAccessKey,
		SessionToken: out.Token,
		Expiration:   exp,
	}, nil
}

func parseCredTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
