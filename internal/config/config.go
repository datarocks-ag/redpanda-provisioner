package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// validStrategies is the allowlist of update strategy values.
var validStrategies = map[string]bool{
	"":       true, // inherits from parent/default
	"create": true, // only create if missing, skip if exists
	"update": true, // create or update (default behavior)
}

// EffectiveStrategy returns the first non-empty strategy from the given list,
// defaulting to "update" if all are empty.
func EffectiveStrategy(strategies ...string) string {
	for _, s := range strategies {
		if s != "" {
			return s
		}
	}
	return "update"
}

// Config is the top-level YAML configuration.
type Config struct {
	Strategy string   `yaml:"strategy"`
	Topics   []Topic  `yaml:"topics"`
	Schemas  []Schema `yaml:"schemas"`
	Users    []User   `yaml:"users"`
	ACLs     []ACL    `yaml:"acls"`
}

// Topic defines a Kafka topic to provision.
type Topic struct {
	Name              string            `yaml:"name"`
	Partitions        *int32            `yaml:"partitions"`
	ReplicationFactor *int16            `yaml:"replication_factor"`
	Config            map[string]string `yaml:"config"`
	Strategy          string            `yaml:"strategy"`
}

// Schema defines a Schema Registry subject to provision.
type Schema struct {
	Subject       string `yaml:"subject"`
	Type          string `yaml:"type"`
	File          string `yaml:"file"`
	Compatibility string `yaml:"compatibility"`
}

// User defines a SASL/SCRAM user to provision.
//
// Iterations is the SCRAM PBKDF2 iteration count and defaults to 4096
// (the RFC 5802 minimum) when zero or unset. Raise it for FIPS-hardened
// deployments; values below 4096 are rejected at config-load time.
type User struct {
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	Mechanism  string `yaml:"mechanism"`
	Iterations int    `yaml:"iterations,omitempty"`
}

// MinSCRAMIterations is the RFC 5802 minimum PBKDF2 iteration count for
// SCRAM-SHA-* credentials. Values below this are rejected by the broker
// with UNACCEPTABLE_CREDENTIAL.
const MinSCRAMIterations = 4096

// MaxSCRAMIterations is the wire-level ceiling for the iteration count.
// The Kafka AlterUserScramCredentials API encodes this field as int32, so
// any larger value would silently overflow on the wire. The broker may
// also impose a stricter cap of its own; this is just the protocol limit.
const MaxSCRAMIterations = math.MaxInt32

// ACL defines a Kafka ACL entry to provision.
type ACL struct {
	Principal    string   `yaml:"principal"`
	Operations   []string `yaml:"operations"`
	ResourceType string   `yaml:"resource_type"`
	ResourceName string   `yaml:"resource_name"`
	Pattern      string   `yaml:"pattern"`
	Permission   string   `yaml:"permission"`
}

// containsNullByte returns true if s contains a null byte (\x00).
func containsNullByte(s string) bool {
	return strings.ContainsRune(s, '\x00')
}

var envVarPattern = regexp.MustCompile(`\$\{([^}]+)}`)

// expandEnvVars replaces ${VAR} references with their environment variable values.
func expandEnvVars(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		varName := envVarPattern.FindStringSubmatch(match)[1]
		if val, ok := os.LookupEnv(varName); ok {
			return val
		}
		return match
	})
}

// expandConfig walks the config and expands env vars in string fields.
func expandConfig(cfg *Config) {
	cfg.Strategy = expandEnvVars(cfg.Strategy)

	for i := range cfg.Topics {
		t := &cfg.Topics[i]
		t.Name = expandEnvVars(t.Name)
		t.Strategy = expandEnvVars(t.Strategy)
		for k, v := range t.Config {
			t.Config[k] = expandEnvVars(v)
		}
	}

	for i := range cfg.Schemas {
		s := &cfg.Schemas[i]
		s.Subject = expandEnvVars(s.Subject)
		s.Type = expandEnvVars(s.Type)
		s.File = expandEnvVars(s.File)
		s.Compatibility = expandEnvVars(s.Compatibility)
	}

	for i := range cfg.Users {
		u := &cfg.Users[i]
		u.Username = expandEnvVars(u.Username)
		u.Password = expandEnvVars(u.Password)
		u.Mechanism = expandEnvVars(u.Mechanism)
	}

	for i := range cfg.ACLs {
		a := &cfg.ACLs[i]
		a.Principal = expandEnvVars(a.Principal)
		a.ResourceType = expandEnvVars(a.ResourceType)
		a.ResourceName = expandEnvVars(a.ResourceName)
		a.Pattern = expandEnvVars(a.Pattern)
		a.Permission = expandEnvVars(a.Permission)
		for j := range a.Operations {
			a.Operations[j] = expandEnvVars(a.Operations[j])
		}
	}
}

// Load reads and parses a YAML config file, expanding env vars and validating.
//
// Relative schema file paths are resolved against the directory containing
// the config file, not the process working directory — so a config mounted
// at /config/config.yaml referencing schemas/orders.avsc reads from
// /config/schemas/orders.avsc regardless of where the binary was started.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	expandConfig(&cfg)

	configDir, err := configDirFromPath(path)
	if err != nil {
		return nil, err
	}
	resolveSchemaPaths(&cfg, configDir)

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// configDirFromPath returns the absolute directory holding the config file,
// used as the base for resolving relative schema paths.
func configDirFromPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolving config path: %w", err)
	}
	return filepath.Dir(abs), nil
}

// resolveSchemaPaths rewrites each Schema.File to an absolute path. Relative
// entries are resolved against configDir; absolute entries are kept as-is.
// Empty entries are left alone so validation can produce the right error.
func resolveSchemaPaths(cfg *Config, configDir string) {
	for i := range cfg.Schemas {
		f := cfg.Schemas[i].File
		if f == "" || filepath.IsAbs(f) {
			continue
		}
		cfg.Schemas[i].File = filepath.Join(configDir, f)
	}
}

var validSchemaTypes = map[string]bool{
	"avro":     true,
	"protobuf": true,
	"json":     true,
}

var validCompatibility = map[string]bool{
	"":                    true,
	"BACKWARD":            true,
	"BACKWARD_TRANSITIVE": true,
	"FORWARD":             true,
	"FORWARD_TRANSITIVE":  true,
	"FULL":                true,
	"FULL_TRANSITIVE":     true,
	"NONE":                true,
}

var validSASLMechanism = map[string]bool{
	"SCRAM-SHA-256": true,
	"SCRAM-SHA-512": true,
}

var validResourceTypes = map[string]bool{
	"topic":            true,
	"group":            true,
	"cluster":          true,
	"transactional_id": true,
}

var validOperations = map[string]bool{
	"all":              true,
	"read":             true,
	"write":            true,
	"create":           true,
	"delete":           true,
	"alter":            true,
	"describe":         true,
	"cluster_action":   true,
	"describe_configs": true,
	"alter_configs":    true,
	"idempotent_write": true,
}

var validPatterns = map[string]bool{
	"literal":  true,
	"prefixed": true,
}

var validPermissions = map[string]bool{
	"allow": true,
	"deny":  true,
}

// validateStrategy returns an error if the strategy value is invalid.
func validateStrategy(path, value string) error {
	if !validStrategies[value] {
		return fmt.Errorf("%s: invalid strategy %q (must be \"create\" or \"update\")", path, value)
	}
	return nil
}

func validate(cfg *Config) error {
	if err := validateStrategy("strategy", cfg.Strategy); err != nil {
		return err
	}

	if err := validateTopics(cfg.Topics); err != nil {
		return err
	}
	if err := validateSchemas(cfg.Schemas); err != nil {
		return err
	}
	if err := validateUsers(cfg.Users); err != nil {
		return err
	}
	return validateACLs(cfg.ACLs)
}

func validateTopics(topics []Topic) error {
	names := make(map[string]bool)

	for i, t := range topics {
		prefix := fmt.Sprintf("topics[%d]", i)

		if err := validateStrategy(prefix+".strategy", t.Strategy); err != nil {
			return err
		}

		if t.Name == "" {
			return fmt.Errorf("%s.name: is required", prefix)
		}
		if containsNullByte(t.Name) {
			return fmt.Errorf("%s.name: contains null byte", prefix)
		}
		if names[t.Name] {
			return fmt.Errorf("%s.name: duplicate topic name %q", prefix, t.Name)
		}
		names[t.Name] = true

		if t.Partitions != nil && *t.Partitions < 1 {
			return fmt.Errorf("%s.partitions: must be >= 1, got %d", prefix, *t.Partitions)
		}
		if t.ReplicationFactor != nil && *t.ReplicationFactor < 1 {
			return fmt.Errorf("%s.replication_factor: must be >= 1, got %d", prefix, *t.ReplicationFactor)
		}
	}

	return nil
}

func validateSchemas(schemas []Schema) error {
	subjects := make(map[string]bool)

	for i, s := range schemas {
		prefix := fmt.Sprintf("schemas[%d]", i)

		if s.Subject == "" {
			return fmt.Errorf("%s.subject: is required", prefix)
		}
		if containsNullByte(s.Subject) {
			return fmt.Errorf("%s.subject: contains null byte", prefix)
		}
		if subjects[s.Subject] {
			return fmt.Errorf("%s.subject: duplicate subject %q", prefix, s.Subject)
		}
		subjects[s.Subject] = true

		if s.Type == "" {
			return fmt.Errorf("%s.type: is required", prefix)
		}
		if !validSchemaTypes[s.Type] {
			return fmt.Errorf("%s.type: invalid value %q (must be avro, protobuf, or json)", prefix, s.Type)
		}

		if s.File == "" {
			return fmt.Errorf("%s.file: is required", prefix)
		}
		if containsNullByte(s.File) {
			return fmt.Errorf("%s.file: contains null byte", prefix)
		}

		if !validCompatibility[s.Compatibility] {
			return fmt.Errorf("%s.compatibility: invalid value %q (must be BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, or NONE)", prefix, s.Compatibility)
		}
	}

	return nil
}

func validateUsers(users []User) error {
	usernames := make(map[string]bool)

	for i, u := range users {
		prefix := fmt.Sprintf("users[%d]", i)

		if u.Username == "" {
			return fmt.Errorf("%s.username: is required", prefix)
		}
		if containsNullByte(u.Username) {
			return fmt.Errorf("%s.username: contains null byte", prefix)
		}
		if usernames[u.Username] {
			return fmt.Errorf("%s.username: duplicate username %q", prefix, u.Username)
		}
		usernames[u.Username] = true

		if u.Password == "" {
			return fmt.Errorf("%s.password: is required", prefix)
		}

		if u.Mechanism == "" {
			return fmt.Errorf("%s.mechanism: is required", prefix)
		}
		if !validSASLMechanism[u.Mechanism] {
			return fmt.Errorf("%s.mechanism: invalid value %q (must be SCRAM-SHA-256 or SCRAM-SHA-512)", prefix, u.Mechanism)
		}

		if u.Iterations != 0 && u.Iterations < MinSCRAMIterations {
			return fmt.Errorf("%s.iterations: must be >= %d (RFC 5802 minimum), got %d", prefix, MinSCRAMIterations, u.Iterations)
		}
		if u.Iterations > MaxSCRAMIterations {
			return fmt.Errorf("%s.iterations: must be <= %d (Kafka API int32 limit), got %d", prefix, MaxSCRAMIterations, u.Iterations)
		}
	}

	return nil
}

func validateACLs(acls []ACL) error {
	for i, a := range acls {
		prefix := fmt.Sprintf("acls[%d]", i)

		if a.Principal == "" {
			return fmt.Errorf("%s.principal: is required", prefix)
		}
		if containsNullByte(a.Principal) {
			return fmt.Errorf("%s.principal: contains null byte", prefix)
		}

		if len(a.Operations) == 0 {
			return fmt.Errorf("%s.operations: at least one operation is required", prefix)
		}
		for j, op := range a.Operations {
			if !validOperations[op] {
				return fmt.Errorf("%s.operations[%d]: invalid value %q", prefix, j, op)
			}
		}

		if a.ResourceType == "" {
			return fmt.Errorf("%s.resource_type: is required", prefix)
		}
		if !validResourceTypes[a.ResourceType] {
			return fmt.Errorf("%s.resource_type: invalid value %q (must be topic, group, cluster, or transactional_id)", prefix, a.ResourceType)
		}

		if a.ResourceName == "" {
			return fmt.Errorf("%s.resource_name: is required", prefix)
		}
		if containsNullByte(a.ResourceName) {
			return fmt.Errorf("%s.resource_name: contains null byte", prefix)
		}

		if a.Pattern == "" {
			return fmt.Errorf("%s.pattern: is required", prefix)
		}
		if !validPatterns[a.Pattern] {
			return fmt.Errorf("%s.pattern: invalid value %q (must be literal or prefixed)", prefix, a.Pattern)
		}

		if a.Permission == "" {
			return fmt.Errorf("%s.permission: is required", prefix)
		}
		if !validPermissions[a.Permission] {
			return fmt.Errorf("%s.permission: invalid value %q (must be allow or deny)", prefix, a.Permission)
		}
	}

	return nil
}
