package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"redpanda-provisioner/internal/client"
	"redpanda-provisioner/internal/config"
	"redpanda-provisioner/internal/provisioner"
)

var version = "dev"

func main() {
	setupLogging()
	slog.Info("Starting redpanda-provisioner", "version", version)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := Run(ctx); err != nil {
		slog.Error("redpanda-provisioner failed", "error", err)
		os.Exit(1)
	}

	slog.Info("redpanda-provisioner finished successfully")
}

// Run executes a single provisioning pass. Connection details are sourced
// from the YAML file at REDPANDA_CONFIG_PATH; legacy environment variables
// (REDPANDA_BROKERS, REDPANDA_SASL_*, REDPANDA_TLS_ENABLED, SCHEMA_REGISTRY_*)
// fill in any field the YAML leaves empty, so existing env-only deployments
// keep working without changes.
//
// Run is exported so integration tests can drive the same code path as the
// binary without spawning a subprocess.
func Run(ctx context.Context) error {
	configPath := envOrDefault("REDPANDA_CONFIG_PATH", "./config.yaml")

	slog.Info("Loading configuration", "path", configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}
	slog.Info("Configuration loaded",
		"topics", len(cfg.Topics),
		"schemas", len(cfg.Schemas),
		"users", len(cfg.Users),
		"acls", len(cfg.ACLs),
	)

	brokerAddrs := resolveBrokerAddresses(cfg.Broker.Addresses)
	saslUsername := stringWithEnvFallback(cfg.Broker.SASL.Username, "REDPANDA_SASL_USERNAME")
	saslPassword := stringWithEnvFallback(cfg.Broker.SASL.Password, "REDPANDA_SASL_PASSWORD")
	saslMechanism := stringWithEnvFallback(cfg.Broker.SASL.Mechanism, "REDPANDA_SASL_MECHANISM")
	if saslMechanism == "" {
		saslMechanism = "SCRAM-SHA-256"
	}
	tlsEnabled, err := resolveTLSEnabled(cfg.Broker.TLS.Enabled)
	if err != nil {
		return err
	}

	schemaRegistryURL := stringWithEnvFallback(cfg.SchemaRegistry.URL, "SCHEMA_REGISTRY_URL")
	schemaRegistryUsername := stringWithEnvFallback(cfg.SchemaRegistry.Username, "SCHEMA_REGISTRY_USERNAME")
	schemaRegistryPassword := stringWithEnvFallback(cfg.SchemaRegistry.Password, "SCHEMA_REGISTRY_PASSWORD")

	slog.Info("Connecting to Redpanda", "brokers", brokerAddrs)
	adminClient, err := client.Connect(ctx, brokerAddrs, saslUsername, saslPassword, saslMechanism, tlsEnabled)
	if err != nil {
		return fmt.Errorf("connecting to Redpanda: %w", err)
	}
	defer adminClient.Close()

	// Only connect to Schema Registry when schemas are actually configured.
	// Eagerly connecting against a misconfigured (or unreachable) SR would
	// otherwise block the Job for the full 5-minute retry window even when
	// no schema work is needed.
	var schemaClient *client.SchemaRegistryClient
	if len(cfg.Schemas) > 0 {
		if schemaRegistryURL == "" {
			return fmt.Errorf("schema registry URL required when schemas are configured (set schema_registry.url or SCHEMA_REGISTRY_URL)")
		}
		slog.Info("Connecting to Schema Registry", "url", schemaRegistryURL, "auth", schemaRegistryUsername != "")
		schemaClient, err = client.ConnectSchemaRegistry(ctx, schemaRegistryURL, schemaRegistryUsername, schemaRegistryPassword)
		if err != nil {
			return fmt.Errorf("connecting to Schema Registry: %w", err)
		}
	} else if schemaRegistryURL != "" {
		slog.Info("Skipping Schema Registry connection — no schemas configured", "url", schemaRegistryURL)
	}

	p := provisioner.New(adminClient, schemaClient, cfg)
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("provisioning: %w", err)
	}

	return nil
}

// resolveBrokerAddresses prefers YAML config; if empty it falls back to
// REDPANDA_BROKERS (comma-separated), and finally to "localhost:9092".
// Each entry is trimmed so values like "redpanda:9092, broker2:9092" don't
// produce a leading-space hostname.
func resolveBrokerAddresses(yaml []string) []string {
	if len(yaml) > 0 {
		return trimAll(yaml)
	}
	if env := os.Getenv("REDPANDA_BROKERS"); env != "" {
		return trimAll(strings.Split(env, ","))
	}
	return []string{"localhost:9092"}
}

func trimAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if t := strings.TrimSpace(s); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// stringWithEnvFallback returns yaml if non-empty, else the named env var.
func stringWithEnvFallback(yaml, envKey string) string {
	if yaml != "" {
		return yaml
	}
	return os.Getenv(envKey)
}

// resolveTLSEnabled returns the YAML value if it is true, otherwise consults
// REDPANDA_TLS_ENABLED. The env var accepts any value ParseBool understands
// (1, t, T, true, TRUE, ...). An invalid env value is a config error rather
// than a silent default-to-false.
func resolveTLSEnabled(yamlValue bool) (bool, error) {
	if yamlValue {
		return true, nil
	}
	raw := os.Getenv("REDPANDA_TLS_ENABLED")
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("REDPANDA_TLS_ENABLED: invalid bool %q: %w", raw, err)
	}
	return v, nil
}

func setupLogging() {
	level := slog.LevelInfo
	switch envOrDefault("LOG_LEVEL", "info") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
}

func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
