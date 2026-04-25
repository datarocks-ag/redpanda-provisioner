package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	maxRetries   = 15
	initialDelay = 1 * time.Second
	maxDelay     = 30 * time.Second
	totalTimeout = 5 * time.Minute
)

// AdminClient wraps a franz-go admin client for Kafka operations. It embeds
// *kadm.Client so all kadm methods are promoted directly onto the wrapper,
// and adds [AdminClient.PurgeTopicCache] for invalidating stale topic
// metadata held by the underlying kgo client.
type AdminClient struct {
	*kadm.Client
	kClient *kgo.Client
}

// NewAdminClient wraps an existing kgo.Client as an AdminClient.
func NewAdminClient(kClient *kgo.Client) *AdminClient {
	return &AdminClient{
		Client:  kadm.NewClient(kClient),
		kClient: kClient,
	}
}

// Close closes the underlying Kafka client.
func (c *AdminClient) Close() {
	c.kClient.Close()
}

// PurgeTopicCache evicts the named topics from the underlying kgo client's
// metadata cache so that the next metadata-backed call (e.g. ListTopics) is
// served from a fresh broker query instead of stale cached state.
func (c *AdminClient) PurgeTopicCache(topics ...string) {
	c.kClient.PurgeTopicsFromClient(topics...)
}

// Connect establishes a Kafka admin connection with exponential backoff retry.
func Connect(ctx context.Context, brokers []string, username, password, mechanism string, tlsEnabled bool) (*AdminClient, error) {
	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		client, err := connect(brokers, username, password, mechanism, tlsEnabled)
		if err == nil {
			// Verify connectivity by requesting metadata
			ac := NewAdminClient(client)
			_, err = ac.ListBrokers(ctx)
			if err == nil {
				slog.Info("Connected to Redpanda", "brokers", brokers)
				return ac, nil
			}
			client.Close()
		}

		if ctx.Err() != nil {
			return nil, fmt.Errorf("connection timeout after %s: %w", totalTimeout, ctx.Err())
		}

		delay := time.Duration(float64(initialDelay) * math.Pow(2, float64(attempt)))
		if delay > maxDelay {
			delay = maxDelay
		}

		slog.Warn("Redpanda not ready, retrying",
			"attempt", attempt+1,
			"max_retries", maxRetries,
			"delay", delay,
			"error", err,
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, fmt.Errorf("connection timeout: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("failed to connect after %d retries", maxRetries+1)
}

func connect(brokers []string, username, password, mechanism string, tlsEnabled bool) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
	}

	if username != "" && password != "" {
		var auth scram.Auth
		auth.User = username
		auth.Pass = password

		switch mechanism {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism %q (must be SCRAM-SHA-256 or SCRAM-SHA-512)", mechanism)
		}
	}

	if tlsEnabled {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}

	return kgo.NewClient(opts...)
}

// SchemaRegistryClient is a simple HTTP client for the Confluent Schema Registry API.
//
// HTTP basic auth credentials, when provided, are applied via
// [http.Request.SetBasicAuth] on every request. Embedding credentials in the
// URL is not supported — Go's net/http strips url.User before sending.
type SchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
	username   string
	password   string
}

// ConnectSchemaRegistry establishes a connection to the Schema Registry with
// retry. Pass empty username/password for unauthenticated registries; partial
// credentials (one set, the other empty) are rejected here so misconfiguration
// fails fast at the API boundary instead of producing a confusing 401 on the
// first authenticated request.
func ConnectSchemaRegistry(ctx context.Context, baseURL, username, password string) (*SchemaRegistryClient, error) {
	if (username == "") != (password == "") {
		return nil, fmt.Errorf("schema registry username and password must both be set or both be empty")
	}
	baseURL = strings.TrimRight(baseURL, "/")
	client := &SchemaRegistryClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		username:   username,
		password:   password,
	}

	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := client.ping(ctx)
		if err == nil {
			slog.Info("Connected to Schema Registry", "url", baseURL)
			return client, nil
		}

		if ctx.Err() != nil {
			return nil, fmt.Errorf("connection timeout after %s: %w", totalTimeout, ctx.Err())
		}

		delay := time.Duration(float64(initialDelay) * math.Pow(2, float64(attempt)))
		if delay > maxDelay {
			delay = maxDelay
		}

		slog.Warn("Schema Registry not ready, retrying",
			"attempt", attempt+1,
			"max_retries", maxRetries,
			"delay", delay,
			"error", err,
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, fmt.Errorf("connection timeout: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("failed to connect to schema registry after %d retries", maxRetries+1)
}

// authorize attaches HTTP basic auth to req when credentials are configured.
func (c *SchemaRegistryClient) authorize(req *http.Request) {
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
}

func (c *SchemaRegistryClient) ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/subjects", nil)
	if err != nil {
		return fmt.Errorf("creating ping request: %w", err)
	}
	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("schema registry ping: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("schema registry returned status %d", resp.StatusCode)
	}
	return nil
}

// RegisterSchema registers a schema for a subject. Returns the schema ID.
// This is naturally idempotent — registering the same schema returns the existing ID.
func (c *SchemaRegistryClient) RegisterSchema(ctx context.Context, subject, schemaType, schema string) (int, error) {
	body := map[string]string{
		"schemaType": schemaType,
		"schema":     schema,
	}

	path := "/subjects/" + url.PathEscape(subject) + "/versions"
	resp, err := c.doJSON(ctx, http.MethodPost, path, body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, readError(resp)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decoding register response: %w", err)
	}
	return result.ID, nil
}

// SetCompatibility sets the compatibility level for a subject.
func (c *SchemaRegistryClient) SetCompatibility(ctx context.Context, subject, level string) error {
	body := map[string]string{
		"compatibility": level,
	}

	path := "/config/" + url.PathEscape(subject)
	resp, err := c.doJSON(ctx, http.MethodPut, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readError(resp)
	}
	return nil
}

// GetCompatibility returns the compatibility level for a subject, or empty string if not set.
func (c *SchemaRegistryClient) GetCompatibility(ctx context.Context, subject string) (string, error) {
	path := "/config/" + url.PathEscape(subject)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", readError(resp)
	}

	var result struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decoding compatibility response: %w", err)
	}
	return result.CompatibilityLevel, nil
}

func (c *SchemaRegistryClient) doJSON(ctx context.Context, method, path string, body any) (*http.Response, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	return resp, nil
}

func readError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, err)
	}
	return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
}
