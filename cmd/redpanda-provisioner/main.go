package main

import (
	"context"
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
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
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
		slog.Error("Failed to connect to Redpanda", "error", err)
		os.Exit(1)
	}
	defer adminClient.Close()

	// Only connect to Schema Registry when schemas are actually configured.
	// Eagerly connecting against a misconfigured (or unreachable) SR would
	// otherwise block the Job for the full 5-minute retry window even when
	// no schema work is needed.
	var schemaClient *client.SchemaRegistryClient
	if len(cfg.Schemas) > 0 {
		if schemaRegistryURL == "" {
			slog.Error("Schema Registry URL required when schemas are configured (set SCHEMA_REGISTRY_URL)")
			os.Exit(1)
		}
		slog.Info("Connecting to Schema Registry", "url", schemaRegistryURL, "auth", schemaRegistryUsername != "")
		schemaClient, err = client.ConnectSchemaRegistry(ctx, schemaRegistryURL, schemaRegistryUsername, schemaRegistryPassword)
		if err != nil {
			slog.Error("Failed to connect to Schema Registry", "error", err)
			os.Exit(1)
		}
	} else if schemaRegistryURL != "" {
		slog.Info("Skipping Schema Registry connection — no schemas configured", "url", schemaRegistryURL)
	}

	p := provisioner.New(adminClient, schemaClient, cfg)
	if err := p.Run(ctx); err != nil {
		slog.Error("Provisioning failed", "error", err)
		os.Exit(1)
	}

	slog.Info("redpanda-provisioner finished successfully")
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
