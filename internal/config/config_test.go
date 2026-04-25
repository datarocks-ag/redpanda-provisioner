package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// TestLoadUnresolvedEnvVarFails pins the fail-closed behavior introduced in
// M6: an unresolved ${VAR} in a config field is a load error instead of
// being kept as a literal string. The literal-string fallback was a footgun
// — an unset password would silently become the literal "${PASSWORD}" and
// the broker would reject it with an unrelated UNACCEPTABLE_CREDENTIAL error.
func TestLoadUnresolvedEnvVarFails(t *testing.T) {
	os.Unsetenv("NONEXISTENT_VAR")

	yaml := `
users:
  - username: svc
    password: ${NONEXISTENT_VAR}
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unresolved env var, got nil")
	}
	if !strings.Contains(err.Error(), "NONEXISTENT_VAR") {
		t.Errorf("expected error to name the missing var, got: %v", err)
	}
	if !strings.Contains(err.Error(), "users[0].password") {
		t.Errorf("expected error to point at the offending field, got: %v", err)
	}
}

func TestLoadEnvVarDefault(t *testing.T) {
	os.Unsetenv("MAYBE_UNSET")

	yaml := `
topics:
  - name: ${MAYBE_UNSET:-fallback-topic}
    partitions: 1
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Topics[0].Name != "fallback-topic" {
		t.Errorf("expected default 'fallback-topic', got %q", cfg.Topics[0].Name)
	}
}

func TestLoadEnvVarDefaultOverriddenWhenSet(t *testing.T) {
	t.Setenv("THIS_IS_SET", "actual-value")

	yaml := `
topics:
  - name: ${THIS_IS_SET:-fallback}
    partitions: 1
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Topics[0].Name != "actual-value" {
		t.Errorf("set env should win over default, got %q", cfg.Topics[0].Name)
	}
}

func TestLoadEnvVarEmptyDefault(t *testing.T) {
	os.Unsetenv("MISSING")

	// ${VAR:-} is the explicit "may be empty" escape hatch.
	yaml := `
topics:
  - name: prefix${MISSING:-}suffix
    partitions: 1
`
	path := writeTempConfig(t, yaml)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Topics[0].Name != "prefixsuffix" {
		t.Errorf("expected empty default to expand to '', got %q", cfg.Topics[0].Name)
	}
}

func TestLoadEnvVarMultipleMissingReportedTogether(t *testing.T) {
	os.Unsetenv("ONE")
	os.Unsetenv("TWO")

	yaml := `
users:
  - username: ${ONE}
    password: ${TWO}
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unresolved env vars")
	}
	if !strings.Contains(err.Error(), "ONE") || !strings.Contains(err.Error(), "TWO") {
		t.Errorf("expected both missing vars in one error, got: %v", err)
	}
}

// TestLoadEnvVarMalformedNotMatched verifies the tightened regex rejects
// candidates that do not look like POSIX env var names: spaces, leading
// digits, slashes. These are kept verbatim instead of being silently
// expanded to nothing or matching unintended substrings.
func TestLoadEnvVarMalformedNotMatched(t *testing.T) {
	yaml := `
topics:
  - name: "literal-${not a var}-still-here"
    partitions: 1
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Topics[0].Name != "literal-${not a var}-still-here" {
		t.Errorf("malformed reference should be left untouched, got %q", cfg.Topics[0].Name)
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

func TestValidateSchemaMissingSubject(t *testing.T) {
	yaml := `
schemas:
  - type: avro
    file: test.avsc
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing schema subject")
	}
}

func TestValidateSchemaMissingFile(t *testing.T) {
	yaml := `
schemas:
  - subject: test-value
    type: avro
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing schema file")
	}
}

func TestValidateSchemaDuplicateSubject(t *testing.T) {
	yaml := `
schemas:
  - subject: test-value
    type: avro
    file: a.avsc
  - subject: test-value
    type: avro
    file: b.avsc
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for duplicate schema subject")
	}
}

func TestValidateSchemaInvalidCompatibility(t *testing.T) {
	yaml := `
schemas:
  - subject: test-value
    type: avro
    file: test.avsc
    compatibility: INVALID
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid compatibility")
	}
}

func TestValidateSchemaNullByteSubject(t *testing.T) {
	yaml := "schemas:\n  - subject: \"test\\x00value\"\n    type: avro\n    file: test.avsc\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in schema subject")
	}
}

func TestValidateSchemaNullByteFile(t *testing.T) {
	yaml := "schemas:\n  - subject: test-value\n    type: avro\n    file: \"test\\x00.avsc\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in schema file")
	}
}

func TestValidateUserMissingUsername(t *testing.T) {
	yaml := `
users:
  - password: pass
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing username")
	}
}

func TestValidateUserMissingPassword(t *testing.T) {
	yaml := `
users:
  - username: svc
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing password")
	}
}

func TestValidateUserMissingMechanism(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing mechanism")
	}
}

func TestValidateUserDuplicateUsername(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass1
    mechanism: SCRAM-SHA-256
  - username: svc
    password: pass2
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for duplicate username")
	}
}

func TestValidateUserNullByteUsername(t *testing.T) {
	yaml := "users:\n  - username: \"svc\\x00name\"\n    password: pass\n    mechanism: SCRAM-SHA-256\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in username")
	}
}

func TestValidateUserIterationsBelowMinimumRejected(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass
    mechanism: SCRAM-SHA-256
    iterations: 1024
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for iterations < 4096")
	}
}

func TestValidateUserIterationsZeroAccepted(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass
    mechanism: SCRAM-SHA-256
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected zero iterations to be accepted (default applied at provision time), got: %v", err)
	}
	if cfg.Users[0].Iterations != 0 {
		t.Errorf("expected unset iterations to remain 0 in config, got %d", cfg.Users[0].Iterations)
	}
}

func TestValidateUserIterationsAboveInt32Rejected(t *testing.T) {
	// 2^31 = 2147483648, one past math.MaxInt32 (2147483647). Without the
	// upper bound this would silently overflow when cast to int32.
	yaml := `
users:
  - username: svc
    password: pass
    mechanism: SCRAM-SHA-256
    iterations: 2147483648
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for iterations > MaxInt32")
	}
}

func TestValidateUserIterationsAtMinimumAccepted(t *testing.T) {
	yaml := `
users:
  - username: svc
    password: pass
    mechanism: SCRAM-SHA-256
    iterations: 4096
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected iterations=4096 to be accepted, got: %v", err)
	}
	if cfg.Users[0].Iterations != 4096 {
		t.Errorf("expected iterations 4096, got %d", cfg.Users[0].Iterations)
	}
}

func TestValidateACLMissingPrincipal(t *testing.T) {
	yaml := `
acls:
  - operations: [read]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing principal")
	}
}

func TestValidateACLMissingResourceType(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing resource_type")
	}
}

func TestValidateACLInvalidResourceType(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: invalid
    resource_name: orders
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid resource_type")
	}
}

func TestValidateACLMissingResourceName(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: topic
    pattern: literal
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing resource_name")
	}
}

func TestValidateACLMissingPattern(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: topic
    resource_name: orders
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing pattern")
	}
}

func TestValidateACLInvalidPattern(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: topic
    resource_name: orders
    pattern: wildcard
    permission: allow
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid pattern")
	}
}

func TestValidateACLMissingPermission(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: topic
    resource_name: orders
    pattern: literal
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing permission")
	}
}

func TestValidateACLInvalidPermission(t *testing.T) {
	yaml := `
acls:
  - principal: "User:svc"
    operations: [read]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: maybe
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid permission")
	}
}

func TestValidateACLNullBytePrincipal(t *testing.T) {
	yaml := "acls:\n  - principal: \"User\\x00svc\"\n    operations: [read]\n    resource_type: topic\n    resource_name: orders\n    pattern: literal\n    permission: allow\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in principal")
	}
}

func TestValidateACLNullByteResourceName(t *testing.T) {
	yaml := "acls:\n  - principal: \"User:svc\"\n    operations: [read]\n    resource_type: topic\n    resource_name: \"orders\\x00name\"\n    pattern: literal\n    permission: allow\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for null byte in resource_name")
	}
}

func TestValidateTopicInvalidStrategy(t *testing.T) {
	yaml := `
topics:
  - name: test
    partitions: 1
    strategy: delete
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid per-topic strategy")
	}
}

func TestValidateReplicationFactorMinimum(t *testing.T) {
	yaml := `
topics:
  - name: test
    partitions: 1
    replication_factor: 0
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for replication_factor < 1")
	}
}

func TestLoadFileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	yaml := `
topics:
  - name: [invalid yaml
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadEmptyConfig(t *testing.T) {
	path := writeTempConfig(t, "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Topics) != 0 {
		t.Errorf("expected 0 topics, got %d", len(cfg.Topics))
	}
}

func TestValidateSchemaTypeMissing(t *testing.T) {
	yaml := `
schemas:
  - subject: test-value
    file: test.avsc
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing schema type")
	}
}

func TestSchemaPathResolvedRelativeToConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
schemas:
  - subject: orders-value
    type: avro
    file: schemas/orders.avsc
`), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := filepath.Join(dir, "schemas", "orders.avsc")
	if cfg.Schemas[0].File != want {
		t.Errorf("schema file path: got %q, want %q", cfg.Schemas[0].File, want)
	}
}

func TestSchemaAbsolutePathPreserved(t *testing.T) {
	abs := filepath.Join(t.TempDir(), "absolute.avsc")
	cfgPath := writeTempConfig(t, fmt.Sprintf(`
schemas:
  - subject: orders-value
    type: avro
    file: %s
`, abs))

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Schemas[0].File != abs {
		t.Errorf("absolute schema path mutated: got %q, want %q", cfg.Schemas[0].File, abs)
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
