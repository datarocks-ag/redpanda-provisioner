//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	rpmodule "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// End-to-end test that exercises the same broker posture we hit in
// production: SASL/SCRAM-SHA-256 required on the Kafka listener, HTTP basic
// auth on the Schema Registry, and a pre-seeded superuser. The test drives
// the real binary entry point (Run) so a missing-Iterations or missing-SR-auth
// regression would fail here at PR time, exactly like it does in the field.

const (
	e2eAdminUser = "admin"
	e2eAdminPass = "admin-secret-at-least-16-bytes"
	e2eOrdersPwd = "orders-service-pass-at-least-16-bytes"
)

type e2eFixture struct {
	kafkaSeed string
	srURL     string
	cleanup   func()
}

func startProductionLikeRedpanda(t *testing.T) e2eFixture {
	t.Helper()
	ctx := context.Background()

	rp, err := rpmodule.Run(ctx, "redpandadata/redpanda:v26.1.6",
		rpmodule.WithEnableSASL(),
		rpmodule.WithEnableKafkaAuthorization(),
		rpmodule.WithSuperusers(e2eAdminUser),
		rpmodule.WithNewServiceAccount(e2eAdminUser, e2eAdminPass),
		rpmodule.WithEnableSchemaRegistryHTTPBasicAuth(),
	)
	if err != nil {
		t.Fatalf("starting redpanda container: %v", err)
	}

	kafkaSeed, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("kafka seed broker: %v", err)
	}
	srURL, err := rp.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("schema registry address: %v", err)
	}

	cleanup := func() {
		if err := testcontainers.TerminateContainer(rp); err != nil {
			log.Printf("terminating container: %v", err)
		}
	}
	return e2eFixture{kafkaSeed: kafkaSeed, srURL: srURL, cleanup: cleanup}
}

func newSCRAMAdmin(t *testing.T, brokers, user, pass string) *kadm.Client {
	t.Helper()
	auth := scram.Auth{User: user, Pass: pass}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.SASL(auth.AsSha256Mechanism()),
	)
	if err != nil {
		t.Fatalf("kgo client (%s): %v", user, err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

func writeFixtureFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return p
}

func srGet(t *testing.T, url, user, pass string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building SR request: %v", err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("SR request: %v", err)
	}
	return resp
}

// TestE2EProductionLikeProvisioning runs the full provisioner against a
// SASL+http_basic Redpanda fixture with a config that exercises every
// supported resource type, then verifies each one against the broker / SR
// directly. A second run validates idempotency.
func TestE2EProductionLikeProvisioning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fx := startProductionLikeRedpanda(t)
	defer fx.cleanup()

	dir := t.TempDir()
	schemaFile := writeFixtureFile(t, dir, "orders.avsc", `{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}`)

	cfgPath := writeFixtureFile(t, dir, "config.yaml", fmt.Sprintf(`
strategy: update
topics:
  - name: orders
    partitions: 3
    replication_factor: 1
    config:
      retention.ms: "259200000"
schemas:
  - subject: orders-value
    type: avro
    file: %s
    compatibility: BACKWARD
users:
  - username: orders-service
    password: ${ORDERS_PASSWORD}
    mechanism: SCRAM-SHA-256
acls:
  - principal: "User:orders-service"
    operations: [describe, read, write]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
`, schemaFile))

	t.Setenv("ORDERS_PASSWORD", e2eOrdersPwd)
	t.Setenv("REDPANDA_BROKERS", fx.kafkaSeed)
	t.Setenv("REDPANDA_SASL_USERNAME", e2eAdminUser)
	t.Setenv("REDPANDA_SASL_PASSWORD", e2eAdminPass)
	t.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("SCHEMA_REGISTRY_URL", fx.srURL)
	t.Setenv("SCHEMA_REGISTRY_USERNAME", e2eAdminUser)
	t.Setenv("SCHEMA_REGISTRY_PASSWORD", e2eAdminPass)
	t.Setenv("REDPANDA_CONFIG_PATH", cfgPath)

	if err := Run(ctx); err != nil {
		t.Fatalf("first provisioning run failed: %v", err)
	}

	admin := newSCRAMAdmin(t, fx.kafkaSeed, e2eAdminUser, e2eAdminPass)

	// TC1 — topic was created with the right partition count.
	topics, err := admin.ListTopics(ctx, "orders")
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}
	orders, ok := topics["orders"]
	if !ok || orders.Err != nil {
		t.Fatalf("topic 'orders' missing or in error state: %+v", orders)
	}
	if got := len(orders.Partitions); got != 3 {
		t.Errorf("expected 3 partitions for 'orders', got %d", got)
	}

	// TC2a — broker has stored the SCRAM credential. Without the B1 fix this
	// would never be reached (the upsert would have failed earlier).
	descs, err := admin.DescribeUserSCRAMs(ctx, "orders-service")
	if err != nil {
		t.Fatalf("describing SCRAM users: %v", err)
	}
	user, ok := descs["orders-service"]
	if !ok || user.Err != nil || len(user.CredInfos) == 0 {
		t.Fatalf("orders-service credential missing or errored: %+v", user)
	}

	// TC2b — the new user can actually authenticate. This is the strongest
	// regression test: it would catch any future change that silently weakens
	// the credential (wrong iterations, wrong mechanism, password not stored).
	svc := newSCRAMAdmin(t, fx.kafkaSeed, "orders-service", e2eOrdersPwd)
	if _, err := svc.ListTopics(ctx, "orders"); err != nil {
		t.Fatalf("orders-service cannot authenticate against the broker: %v", err)
	}

	// TC3 — schema registration went through the authenticated SR endpoint.
	// Without the B2 fix the registration would have 403'd at startup.
	resp := srGet(t, fx.srURL+"/subjects/orders-value/versions/latest", e2eAdminUser, e2eAdminPass)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("SR returned status %d for orders-value: %s", resp.StatusCode, body)
	}
	var schemaResp struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
		ID      int    `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		t.Fatalf("decoding SR response: %v", err)
	}
	if schemaResp.Subject != "orders-value" || schemaResp.Version < 1 {
		t.Errorf("unexpected schema response: %+v", schemaResp)
	}

	// TC3b — without basic auth the same request is rejected, proving the
	// listener is actually enforcing http_basic (not silently allowing all).
	noAuth, err := http.Get(fx.srURL + "/subjects/orders-value/versions/latest")
	if err != nil {
		t.Fatalf("unauthenticated SR request: %v", err)
	}
	noAuth.Body.Close()
	if noAuth.StatusCode != http.StatusUnauthorized && noAuth.StatusCode != http.StatusForbidden {
		t.Errorf("expected 401/403 from unauthenticated SR request, got %d", noAuth.StatusCode)
	}

	// TC4 — ACL was created and is visible.
	aclsResp, err := admin.DescribeACLs(ctx, kadm.NewACLs().
		Allow("User:orders-service").
		AllowHosts().
		Topics("orders").
		ResourcePatternType(kadm.ACLPatternLiteral).
		Operations(kadm.OpDescribe, kadm.OpRead, kadm.OpWrite),
	)
	if err != nil {
		t.Fatalf("describing ACLs: %v", err)
	}
	if len(aclsResp) == 0 {
		t.Fatal("expected at least one ACL describe result")
	}
	var foundDescribe, foundRead, foundWrite bool
	for _, r := range aclsResp {
		if r.Err != nil {
			t.Fatalf("ACL describe errored: %v", r.Err)
		}
		for _, d := range r.Described {
			if d.Principal != "User:orders-service" || d.Name != "orders" {
				continue
			}
			switch d.Operation {
			case kadm.OpDescribe:
				foundDescribe = true
			case kadm.OpRead:
				foundRead = true
			case kadm.OpWrite:
				foundWrite = true
			}
		}
	}
	if !foundDescribe || !foundRead || !foundWrite {
		t.Errorf("missing ACL operations on 'orders': describe=%v read=%v write=%v", foundDescribe, foundRead, foundWrite)
	}

	// TC5 — second run is clean and idempotent across all four resource types.
	if err := Run(ctx); err != nil {
		t.Fatalf("second (idempotent) provisioning run failed: %v", err)
	}

	// And the schema subject still has exactly one version (no duplicate
	// registration on the second pass).
	versionsResp := srGet(t, fx.srURL+"/subjects/orders-value/versions", e2eAdminUser, e2eAdminPass)
	defer versionsResp.Body.Close()
	if versionsResp.StatusCode != http.StatusOK {
		t.Fatalf("listing schema versions: status %d", versionsResp.StatusCode)
	}
	var versions []int
	if err := json.NewDecoder(versionsResp.Body).Decode(&versions); err != nil {
		t.Fatalf("decoding versions: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected exactly 1 schema version after idempotent re-run, got %d (%v)", len(versions), versions)
	}
}

// TestE2ELazySchemaRegistryConnect (I1) verifies that Run does not dial the
// Schema Registry when no schemas are configured, even if SCHEMA_REGISTRY_URL
// is set to an unreachable address. Before this guard a misconfigured SR URL
// would block the Job for the full 5-minute retry window.
func TestE2ELazySchemaRegistryConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fx := startProductionLikeRedpanda(t)
	defer fx.cleanup()

	dir := t.TempDir()
	cfgPath := writeFixtureFile(t, dir, "config.yaml", `
topics:
  - name: noop-topic
    partitions: 1
    replication_factor: 1
`)

	t.Setenv("REDPANDA_BROKERS", fx.kafkaSeed)
	t.Setenv("REDPANDA_SASL_USERNAME", e2eAdminUser)
	t.Setenv("REDPANDA_SASL_PASSWORD", e2eAdminPass)
	t.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
	// Deliberately point at a black-hole address. If Run dialed it eagerly the
	// retry loop would burn the full timeout; with the lazy gate it must be
	// skipped entirely because the config has no schemas.
	t.Setenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:1")
	t.Setenv("SCHEMA_REGISTRY_USERNAME", "")
	t.Setenv("SCHEMA_REGISTRY_PASSWORD", "")
	t.Setenv("REDPANDA_CONFIG_PATH", cfgPath)

	start := time.Now()
	if err := Run(ctx); err != nil {
		t.Fatalf("Run with no schemas should succeed even with bad SR URL: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("Run took %s — it should have skipped the SR connect entirely", elapsed)
	}
}
