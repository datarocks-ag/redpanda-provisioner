package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(t *testing.T, handler http.Handler) *SchemaRegistryClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &SchemaRegistryClient{
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}
}

func newTestClientWithAuth(t *testing.T, username, password string, handler http.Handler) *SchemaRegistryClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &SchemaRegistryClient{
		baseURL:    srv.URL,
		httpClient: srv.Client(),
		username:   username,
		password:   password,
	}
}

func assertBasicAuth(t *testing.T, r *http.Request, wantUser, wantPass string) {
	t.Helper()
	gotUser, gotPass, ok := r.BasicAuth()
	if !ok {
		t.Errorf("expected basic auth header on %s %s, got none", r.Method, r.URL.Path)
		return
	}
	if gotUser != wantUser || gotPass != wantPass {
		t.Errorf("basic auth mismatch on %s %s: got %q/%q, want %q/%q", r.Method, r.URL.Path, gotUser, gotPass, wantUser, wantPass)
	}
}

func TestRegisterSchema_Success(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/subjects/test-value/versions") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/vnd.schemaregistry.v1+json" {
			t.Errorf("unexpected Content-Type: %s", ct)
		}

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding request body: %v", err)
		}
		if body["schemaType"] != "AVRO" {
			t.Errorf("expected schemaType AVRO, got %q", body["schemaType"])
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]int{"id": 42}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))

	id, err := client.RegisterSchema(context.Background(), "test-value", "AVRO", `{"type":"string"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 {
		t.Errorf("expected id 42, got %d", id)
	}
}

func TestRegisterSchema_Non200(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":409,"message":"schema conflict"}`))
	}))

	_, err := client.RegisterSchema(context.Background(), "test-value", "AVRO", `{"type":"string"}`)
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected error to mention status 409, got: %v", err)
	}
}

func TestRegisterSchema_DecodeError(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))

	_, err := client.RegisterSchema(context.Background(), "test-value", "AVRO", `{"type":"string"}`)
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
}

func TestSetCompatibility_Success(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/config/test-value") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding request body: %v", err)
		}
		if body["compatibility"] != "BACKWARD" {
			t.Errorf("expected compatibility BACKWARD, got %q", body["compatibility"])
		}

		w.WriteHeader(http.StatusOK)
	}))

	err := client.SetCompatibility(context.Background(), "test-value", "BACKWARD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetCompatibility_Non200(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))

	err := client.SetCompatibility(context.Background(), "test-value", "BACKWARD")
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention status 500, got: %v", err)
	}
}

func TestGetCompatibility_Success(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "FULL"}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))

	level, err := client.GetCompatibility(context.Background(), "test-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != "FULL" {
		t.Errorf("expected FULL, got %q", level)
	}
}

func TestGetCompatibility_NotFound(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	level, err := client.GetCompatibility(context.Background(), "test-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if level != "" {
		t.Errorf("expected empty string for not found, got %q", level)
	}
}

func TestGetCompatibility_Non200(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("forbidden"))
	}))

	_, err := client.GetCompatibility(context.Background(), "test-value")
	if err == nil {
		t.Fatal("expected error for non-200/404 response")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected error to mention status 403, got: %v", err)
	}
}

func TestReadError_Success(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader("server error")),
	}
	err := readError(resp)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected status in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "server error") {
		t.Errorf("expected body in error, got: %v", err)
	}
}

func TestReadError_BodyReadError(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(&errorReader{}),
	}
	err := readError(resp)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "failed to read body") {
		t.Errorf("expected body read error, got: %v", err)
	}
}

type errorReader struct{}

func (e *errorReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestPing_Success(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))

	err := client.ping(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPing_Non200(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	err := client.ping(context.Background())
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestGetCompatibility_DecodeError(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))

	_, err := client.GetCompatibility(context.Background(), "test-value")
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
}

func TestBasicAuth_AppliedOnAllRequests(t *testing.T) {
	const user, pass = "alice", "s3cret"

	tests := []struct {
		name string
		call func(*testing.T, *SchemaRegistryClient)
	}{
		{"ping", func(t *testing.T, c *SchemaRegistryClient) {
			if err := c.ping(context.Background()); err != nil {
				t.Fatalf("ping: %v", err)
			}
		}},
		{"RegisterSchema", func(t *testing.T, c *SchemaRegistryClient) {
			if _, err := c.RegisterSchema(context.Background(), "s", "AVRO", "{}"); err != nil {
				t.Fatalf("RegisterSchema: %v", err)
			}
		}},
		{"SetCompatibility", func(t *testing.T, c *SchemaRegistryClient) {
			if err := c.SetCompatibility(context.Background(), "s", "BACKWARD"); err != nil {
				t.Fatalf("SetCompatibility: %v", err)
			}
		}},
		{"GetCompatibility", func(t *testing.T, c *SchemaRegistryClient) {
			if _, err := c.GetCompatibility(context.Background(), "s"); err != nil {
				t.Fatalf("GetCompatibility: %v", err)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClientWithAuth(t, user, pass, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assertBasicAuth(t, r, user, pass)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"id":1,"compatibilityLevel":"FULL"}`))
			}))
			tt.call(t, c)
		})
	}
}

func TestConnectSchemaRegistry_RejectsHalfSetCredentials(t *testing.T) {
	cases := []struct {
		name           string
		user, password string
	}{
		{"username-only", "alice", ""},
		{"password-only", "", "s3cret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ConnectSchemaRegistry(context.Background(), "http://127.0.0.1:1", tc.user, tc.password)
			if err == nil {
				t.Fatal("expected error for half-set credentials, got nil")
			}
			if !strings.Contains(err.Error(), "both be set or both be empty") {
				t.Errorf("unexpected error message: %v", err)
			}
		})
	}
}

func TestNoBasicAuth_WhenCredentialsEmpty(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); ok {
			t.Errorf("did not expect basic auth header, got one on %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	if err := c.ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestRegisterSchema_SubjectEncoding(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]int{"id": 1}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))

	_, err := client.RegisterSchema(context.Background(), "test value", "AVRO", `{}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
