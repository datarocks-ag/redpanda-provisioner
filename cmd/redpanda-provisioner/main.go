package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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

// Run executes a single provisioning pass using configuration sourced from
// environment variables and the YAML file at REDPANDA_CONFIG_PATH. It is
// extracted from main so that integration tests can drive the same code path
// as the binary without spawning a subprocess.
func Run(ctx context.Context) error {
	brokers := envOrDefault("REDPANDA_BROKERS", "localhost:9092")
	saslUsername := os.Getenv("REDPANDA_SASL_USERNAME")
	saslPassword := os.Getenv("REDPANDA_SASL_PASSWORD")
	saslMechanism := envOrDefault("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
	tlsEnabled := envOrDefault("REDPANDA_TLS_ENABLED", "false")
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	schemaRegistryUsername := os.Getenv("SCHEMA_REGISTRY_USERNAME")
	schemaRegistryPassword := os.Getenv("SCHEMA_REGISTRY_PASSWORD")
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

	brokerAddrs := strings.Split(brokers, ",")

	slog.Info("Connecting to Redpanda", "brokers", brokerAddrs)
	adminClient, err := client.Connect(ctx, brokerAddrs, saslUsername, saslPassword, saslMechanism, tlsEnabled == "true")
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
			return fmt.Errorf("schema registry URL required when schemas are configured (set SCHEMA_REGISTRY_URL)")
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
