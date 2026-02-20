//go:build integration

package provisioner_test

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	rpmodule "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"redpanda-provisioner/internal/client"
	"redpanda-provisioner/internal/config"
	"redpanda-provisioner/internal/provisioner"
)

type testCluster struct {
	admin    *client.AdminClient
	schema   *client.SchemaRegistryClient
	cleanup  func()
}

func setupRedpanda(t *testing.T) testCluster {
	t.Helper()
	ctx := context.Background()

	rpContainer, err := rpmodule.Run(ctx, "redpandadata/redpanda:v24.3.1")
	if err != nil {
		t.Fatalf("failed to start redpanda container: %v", err)
	}

	broker, err := rpContainer.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka broker address: %v", err)
	}

	srAddr, err := rpContainer.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("failed to get schema registry address: %v", err)
	}

	kClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("failed to create kafka client: %v", err)
	}

	adminClient := client.NewAdminClient(kClient)

	schemaClient, err := client.ConnectSchemaRegistry(ctx, srAddr)
	if err != nil {
		kClient.Close()
		t.Fatalf("failed to connect to schema registry: %v", err)
	}

	cleanup := func() {
		adminClient.Close()
		if err := testcontainers.TerminateContainer(rpContainer); err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}

	return testCluster{admin: adminClient, schema: schemaClient, cleanup: cleanup}
}

func writeTestConfig(t *testing.T, yaml string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTestSchema(t *testing.T, dir, filename, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationFullProvisioning(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	// Create a schema file for the test
	schemaDir := t.TempDir()
	writeTestSchema(t, filepath.Join(schemaDir, "schemas"), "orders.avsc", `{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}`)

	configYAML := `
topics:
  - name: orders
    partitions: 3
    replication_factor: 1
    config:
      retention.ms: "259200000"
      cleanup.policy: delete

  - name: user-events
    partitions: 1
    replication_factor: 1
    config:
      cleanup.policy: compact

schemas:
  - subject: orders-value
    type: avro
    file: ` + filepath.Join(schemaDir, "schemas", "orders.avsc") + `
    compatibility: BACKWARD
`
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx := context.Background()

	// First run
	p := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("first provisioning run failed: %v", err)
	}

	// Verify topics exist
	topics, err := tc.admin.Admin.ListTopics(ctx)
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}

	ordersTopic, ok := topics["orders"]
	if !ok {
		t.Fatal("topic 'orders' not found after provisioning")
	}
	if len(ordersTopic.Partitions) != 3 {
		t.Errorf("expected 3 partitions for 'orders', got %d", len(ordersTopic.Partitions))
	}

	if _, ok := topics["user-events"]; !ok {
		t.Fatal("topic 'user-events' not found after provisioning")
	}

	// Verify topic configs
	cfgs, err := tc.admin.Admin.DescribeTopicConfigs(ctx, "orders")
	if err != nil {
		t.Fatalf("describing topic configs: %v", err)
	}
	for _, rc := range cfgs {
		if rc.Err != nil {
			t.Fatalf("config error for %q: %v", rc.Name, rc.Err)
		}
		for _, entry := range rc.Configs {
			if entry.Key == "retention.ms" && entry.Value != nil {
				if *entry.Value != "259200000" {
					t.Errorf("expected retention.ms=259200000, got %s", *entry.Value)
				}
			}
		}
	}

	// Verify schema was registered
	compat, err := tc.schema.GetCompatibility(ctx, "orders-value")
	if err != nil {
		t.Fatalf("getting compatibility: %v", err)
	}
	if compat != "BACKWARD" {
		t.Errorf("expected compatibility BACKWARD, got %q", compat)
	}
}

func TestIntegrationIdempotency(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	configYAML := `
topics:
  - name: idempotent-test
    partitions: 2
    replication_factor: 1
    config:
      retention.ms: "86400000"
`
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// First run
	p := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second run (idempotent)
	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("second (idempotent) run: %v", err)
	}

	// Verify topic still has correct state
	topics, err := tc.admin.Admin.ListTopics(ctx, "idempotent-test")
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}
	topic, ok := topics["idempotent-test"]
	if !ok {
		t.Fatal("topic not found after idempotent runs")
	}
	if len(topic.Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(topic.Partitions))
	}
}

func TestIntegrationPartitionIncrease(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create topic with 2 partitions
	cfg1YAML := `
topics:
  - name: grow-topic
    partitions: 2
    replication_factor: 1
`
	cfgPath := writeTestConfig(t, cfg1YAML)
	cfg1, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(tc.admin.Admin, tc.schema, cfg1)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Increase to 4 partitions
	cfg2YAML := `
topics:
  - name: grow-topic
    partitions: 4
    replication_factor: 1
`
	cfgPath2 := writeTestConfig(t, cfg2YAML)
	cfg2, err := config.Load(cfgPath2)
	if err != nil {
		t.Fatal(err)
	}

	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg2)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("partition increase run: %v", err)
	}

	// Verify partition count increased
	topics, err := tc.admin.Admin.ListTopics(ctx, "grow-topic")
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}
	topic := topics["grow-topic"]
	if len(topic.Partitions) != 4 {
		t.Errorf("expected 4 partitions after increase, got %d", len(topic.Partitions))
	}
}

func TestIntegrationPartitionDecreaseWarns(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create topic with 4 partitions
	cfg1YAML := `
topics:
  - name: shrink-topic
    partitions: 4
    replication_factor: 1
`
	cfgPath := writeTestConfig(t, cfg1YAML)
	cfg1, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(tc.admin.Admin, tc.schema, cfg1)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Try to "decrease" to 2 — should not fail, just warn
	cfg2YAML := `
topics:
  - name: shrink-topic
    partitions: 2
    replication_factor: 1
`
	cfgPath2 := writeTestConfig(t, cfg2YAML)
	cfg2, err := config.Load(cfgPath2)
	if err != nil {
		t.Fatal(err)
	}

	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg2)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("decrease run should not fail: %v", err)
	}

	// Verify partitions remain at 4
	topics, err := tc.admin.Admin.ListTopics(ctx, "shrink-topic")
	if err != nil {
		t.Fatalf("listing topics: %v", err)
	}
	topic := topics["shrink-topic"]
	if len(topic.Partitions) != 4 {
		t.Errorf("expected partitions to remain at 4, got %d", len(topic.Partitions))
	}
}

func TestIntegrationConfigUpdate(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create topic with initial config
	cfg1YAML := `
topics:
  - name: config-topic
    partitions: 1
    replication_factor: 1
    config:
      retention.ms: "86400000"
`
	cfgPath := writeTestConfig(t, cfg1YAML)
	cfg1, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(tc.admin.Admin, tc.schema, cfg1)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Update retention
	cfg2YAML := `
topics:
  - name: config-topic
    partitions: 1
    replication_factor: 1
    config:
      retention.ms: "172800000"
`
	cfgPath2 := writeTestConfig(t, cfg2YAML)
	cfg2, err := config.Load(cfgPath2)
	if err != nil {
		t.Fatal(err)
	}

	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg2)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("config update run: %v", err)
	}

	// Verify config was updated
	cfgs, err := tc.admin.Admin.DescribeTopicConfigs(ctx, "config-topic")
	if err != nil {
		t.Fatalf("describing topic configs: %v", err)
	}
	for _, rc := range cfgs {
		for _, entry := range rc.Configs {
			if entry.Key == "retention.ms" && entry.Value != nil {
				if *entry.Value != "172800000" {
					t.Errorf("expected retention.ms=172800000, got %s", *entry.Value)
				}
			}
		}
	}
}

func TestIntegrationStrategyCreate(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	ctx := context.Background()

	// Create topic normally
	cfg1YAML := `
topics:
  - name: strategy-topic
    partitions: 2
    replication_factor: 1
    config:
      retention.ms: "86400000"
`
	cfgPath := writeTestConfig(t, cfg1YAML)
	cfg1, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(tc.admin.Admin, tc.schema, cfg1)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Run with strategy=create and different retention — should NOT update
	cfg2YAML := `
topics:
  - name: strategy-topic
    partitions: 2
    replication_factor: 1
    strategy: "create"
    config:
      retention.ms: "999999999"
`
	cfgPath2 := writeTestConfig(t, cfg2YAML)
	cfg2, err := config.Load(cfgPath2)
	if err != nil {
		t.Fatal(err)
	}

	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg2)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("strategy=create run: %v", err)
	}

	// Verify config was NOT updated (still original value)
	cfgs, err := tc.admin.Admin.DescribeTopicConfigs(ctx, "strategy-topic")
	if err != nil {
		t.Fatalf("describing topic configs: %v", err)
	}
	for _, rc := range cfgs {
		for _, entry := range rc.Configs {
			if entry.Key == "retention.ms" && entry.Value != nil {
				if *entry.Value == "999999999" {
					t.Error("retention.ms should not have been updated with strategy=create")
				}
			}
		}
	}
}

func TestIntegrationSchemaRegistration(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	ctx := context.Background()

	schemaDir := t.TempDir()
	writeTestSchema(t, filepath.Join(schemaDir, "schemas"), "test.avsc", `{
  "type": "record",
  "name": "TestEvent",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "value", "type": "int"}
  ]
}`)

	configYAML := `
schemas:
  - subject: test-events-value
    type: avro
    file: ` + filepath.Join(schemaDir, "schemas", "test.avsc") + `
    compatibility: FULL
`
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	// First run
	p := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("schema registration failed: %v", err)
	}

	// Verify compatibility
	compat, err := tc.schema.GetCompatibility(ctx, "test-events-value")
	if err != nil {
		t.Fatalf("getting compatibility: %v", err)
	}
	if compat != "FULL" {
		t.Errorf("expected compatibility FULL, got %q", compat)
	}

	// Second run (idempotent)
	p2 := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("idempotent schema run: %v", err)
	}
}

func TestIntegrationEmptyConfig(t *testing.T) {
	tc := setupRedpanda(t)
	defer tc.cleanup()

	cfgPath := writeTestConfig(t, "{}")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(tc.admin.Admin, tc.schema, cfg)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("empty config should succeed: %v", err)
	}
}
