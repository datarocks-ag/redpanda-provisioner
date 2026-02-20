# CLAUDE.md

## Project Overview

A Go CLI tool that idempotently provisions Redpanda/Kafka resources (topics, schemas, SASL users, ACLs) from a YAML config file. Designed as a Docker Compose init container or Kubernetes Job.

## Build & Run

```bash
go build -o redpanda-provisioner ./cmd/redpanda-provisioner
make build          # same via Makefile
make test           # unit tests
make test-integration  # integration tests (requires Docker)
make lint           # golangci-lint
```

## Optional Environment Variables

- `REDPANDA_BROKERS` (default: `localhost:9092`) — comma-separated broker addresses
- `REDPANDA_SASL_USERNAME` — SASL username (optional, no auth if empty)
- `REDPANDA_SASL_PASSWORD` — SASL password (optional)
- `REDPANDA_SASL_MECHANISM` (default: `SCRAM-SHA-256`) — SCRAM-SHA-256 or SCRAM-SHA-512
- `REDPANDA_TLS_ENABLED` (default: `false`) — enable TLS
- `SCHEMA_REGISTRY_URL` — Schema Registry URL (required if schemas are configured)
- `REDPANDA_CONFIG_PATH` (default: `./config.yaml`)
- `LOG_LEVEL` (default: `info`)

## Architecture

```
cmd/redpanda-provisioner/main.go       # CLI entrypoint, env vars, slog setup
internal/
  config/config.go                    # YAML config structs, loader, env var expansion, validation
  config/config_test.go               # Unit tests for config parsing
  client/client.go                    # Kafka admin client + Schema Registry HTTP client with retry
  provisioner/
    provisioner.go                    # Top-level orchestrator
    topics.go                         # Idempotent topic create/update
    schemas.go                        # Schema Registry registration + compatibility
    users.go                          # SASL/SCRAM user upsert
    acls.go                           # Idempotent ACL creation
```

## Dependencies

Go 1.25 module using:
- `github.com/twmb/franz-go` — Kafka client (pure Go, no CGO)
- `github.com/twmb/franz-go/pkg/kadm` — Kafka admin operations
- `gopkg.in/yaml.v3` — YAML config parsing

No external Schema Registry SDK — all HTTP via `net/http` + `encoding/json`.

## Key Design Decisions

- **Order**: Topics → Schemas → Users → ACLs
- **Idempotency**: ListTopics → not found? CreateTopics : UpdatePartitions + AlterTopicConfigs; RegisterSchema (naturally idempotent); AlterUserSCRAMs (upsert); CreateACLs (idempotent in Kafka protocol)
- **Immutable constraints**: Never delete topics, never decrease partition count — warns if config has fewer partitions than current
- **YAML naming**: snake_case fields; topic config keys use Kafka native dot notation (e.g., `retention.ms`)
- **Structured logging**: `log/slog` with JSON output
- **Strategy**: `update` (default) or `create` (skip existing). Per-topic overrides global.
- **Connection retry**: Exponential backoff (1s–30s, 15 retries, 5min timeout)
- **Schema Registry**: Simple REST client, content type `application/vnd.schemaregistry.v1+json`
- **Environment variable substitution**: `${VAR}` in YAML strings, critical for passwords
