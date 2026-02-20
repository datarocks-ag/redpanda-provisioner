package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	yaml := `
topics:
  - name: orders
    partitions: 6
    replication_factor: 3
    config:
      retention.ms: "259200000"
      cleanup.policy: delete

schemas:
  - subject: orders-value
    type: avro
    file: schemas/orders.avsc
    compatibility: BACKWARD

users:
  - username: orders-service
    password: secret
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:orders-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if len(cfg.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(cfg.Topics))
	}
	if cfg.Topics[0].Name != "orders" {
		t.Errorf("expected topic name 'orders', got %q", cfg.Topics[0].Name)
	}
	if *cfg.Topics[0].Partitions != 6 {
		t.Errorf("expected 6 partitions, got %d", *cfg.Topics[0].Partitions)
	}

	if len(cfg.Schemas) != 1 {
		t.Fatalf("expected 1 schema, got %d", len(cfg.Schemas))
	}
	if cfg.Schemas[0].Subject != "orders-value" {
		t.Errorf("expected subject 'orders-value', got %q", cfg.Schemas[0].Subject)
	}

	if len(cfg.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(cfg.Users))
	}
	if cfg.Users[0].Username != "orders-service" {
		t.Errorf("expected username 'orders-service', got %q", cfg.Users[0].Username)
	}

	if len(cfg.ACLs) != 1 {
		t.Fatalf("expected 1 ACL, got %d", len(cfg.ACLs))
	}
	if cfg.ACLs[0].Principal != "User:orders-service" {
		t.Errorf("expected principal 'User:orders-service', got %q", cfg.ACLs[0].Principal)
	}
}

func TestLoadEnvVarExpansion(t *testing.T) {
	t.Setenv("TEST_PASSWORD", "supersecret")
	t.Setenv("TEST_TOPIC", "my-topic")

	yaml := `
topics:
  - name: ${TEST_TOPIC}
    partitions: 1

users:
  - username: svc
    password: ${TEST_PASSWORD}
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Topics[0].Name != "my-topic" {
		t.Errorf("expected topic name 'my-topic', got %q", cfg.Topics[0].Name)
	}
	if cfg.Users[0].Password != "supersecret" {
		t.Errorf("expected password 'supersecret', got %q", cfg.Users[0].Password)
	}
}

func TestLoadUnresolvedEnvVar(t *testing.T) {
	os.Unsetenv("NONEXISTENT_VAR")

	yaml := `
users:
  - username: svc
    password: ${NONEXISTENT_VAR}
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Users[0].Password != "${NONEXISTENT_VAR}" {
		t.Errorf("expected unresolved env var to be kept as-is, got %q", cfg.Users[0].Password)
	}
}

func TestValidateTopicNameRequired(t *testing.T) {
	yaml := `
topics:
  - partitions: 1
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing topic name")
	}
}

func TestValidateDuplicateTopicName(t *testing.T) {
	yaml := `
topics:
  - name: orders
    partitions: 1
  - name: orders
    partitions: 3
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for duplicate topic name")
	}
}

func TestValidateInvalidSchemaType(t *testing.T) {
	yaml := `
schemas:
  - subject: test
    type: xml
    file: test.xml
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid schema type")
	}
}

func TestValidateInvalidSASLMechanism(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass
    mechanism: PLAIN
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid SASL mechanism")
	}
}

func TestValidateACLMissingOperations(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: []
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty operations")
	}
}

func TestValidateInvalidStrategy(t *testing.T) {
	yaml := `
strategy: "delete"
topics:
  - name: test
    partitions: 1
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid strategy")
	}
}

func TestValidatePartitionsMinimum(t *testing.T) {
	yaml := `
topics:
  - name: test
    partitions: 0
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for partitions < 1")
	}
}

func TestValidateInvalidACLOperation(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [invalid_op]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid ACL operation")
	}
}

func TestValidateNullByte(t *testing.T) {
	yaml := "topics:\n  - name: \"test\\x00name\"\n    partitions: 1\n"
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in topic name")
	}
}

func TestEffectiveStrategy(t *testing.T) {
	tests := []struct {
		strategies []string
		want       string
	}{
		{[]string{"", ""}, "update"},
		{[]string{"create", ""}, "create"},
		{[]string{"", "create"}, "create"},
		{[]string{"update", "create"}, "update"},
	}

	for _, tt := range tests {
		got := EffectiveStrategy(tt.strategies...)
		if got != tt.want {
			t.Errorf("EffectiveStrategy(%v) = %q, want %q", tt.strategies, got, tt.want)
		}
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}
	return path
}
