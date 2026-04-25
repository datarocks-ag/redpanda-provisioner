# redpanda-provisioner

[![CI](https://github.com/datarocks-ag/redpanda-provisioner/actions/workflows/ci.yaml/badge.svg)](https://github.com/datarocks-ag/redpanda-provisioner/actions/workflows/ci.yaml)
![coverage](https://raw.githubusercontent.com/datarocks-ag/redpanda-provisioner/badges/.badges/develop/coverage.svg)

A Go CLI tool that idempotently provisions Redpanda/Kafka resources from a YAML config file. Designed as a Docker Compose init container or Kubernetes Job.

## Features

- Idempotent provisioning of topics, schemas, SASL users, and ACLs
- YAML config with `${VAR}` environment variable expansion
- Configurable strategy: `update` (default) or `create` (skip existing)
- Schema Registry support (Avro, Protobuf, JSON Schema)
- SASL/SCRAM authentication (SCRAM-SHA-256, SCRAM-SHA-512)
- ACL management with literal and prefixed patterns
- Exponential backoff retry for broker and schema registry connectivity
- Structured JSON logging via `log/slog`
- Never deletes topics, schemas, users, or ACLs not in config
- Never decreases partition count (warns on mismatch)

## Quick Start

```bash
docker compose up
```

This starts Redpanda and runs the provisioner with the example config.

## Configuration

Connection details can be set in YAML (`broker:` and `schema_registry:`
blocks) or via environment variables. **YAML wins when set; env vars fill in
the rest** — so existing env-only deployments keep working unchanged. You
can also embed env references inside YAML strings using `${VAR}` or
`${VAR:-default}`.

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `REDPANDA_BROKERS` | no | `localhost:9092` | Comma-separated broker addresses |
| `REDPANDA_SASL_USERNAME` | no | — | SASL username |
| `REDPANDA_SASL_PASSWORD` | no | — | SASL password |
| `REDPANDA_SASL_MECHANISM` | no | `SCRAM-SHA-256` | SCRAM-SHA-256 or SCRAM-SHA-512 |
| `REDPANDA_TLS_ENABLED` | no | `false` | Enable TLS (any value `strconv.ParseBool` understands) |
| `SCHEMA_REGISTRY_URL` | no | — | Schema Registry URL (required if schemas are configured) |
| `SCHEMA_REGISTRY_USERNAME` | no | — | HTTP basic-auth user for SR |
| `SCHEMA_REGISTRY_PASSWORD` | no | — | HTTP basic-auth password for SR |
| `REDPANDA_CONFIG_PATH` | no | `./config.yaml` | Path to YAML config |
| `LOG_LEVEL` | no | `info` | Log level (debug/info/warn/error) |

All auth variables are optional (dev environments often have no auth).

### YAML connection block

```yaml
broker:
  addresses:
    - redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}
  tls:
    enabled: false

schema_registry:
  url: http://redpanda:8081
  username: ${SR_USERNAME:-}
  password: ${SR_PASSWORD:-}
```

## Strategy

Control whether existing resources are updated or skipped using the `strategy` field:

- `update` (default) — create resources if missing, update if they already exist
- `create` — create resources if missing, skip if they already exist

Strategy can be set globally or per topic. Per-topic strategy overrides the global setting.

```yaml
strategy: "create"              # global: skip existing resources

topics:
  - name: orders
    strategy: "update"          # override: always reconcile this topic
    partitions: 6
    replication_factor: 3
```

**Note:** Users, ACLs, and schemas are always upserted idempotently — strategy applies to topics only.

## Environment Variable Expansion

String values support `${VAR}` and `${VAR:-default}` syntax. The variable
name must match `[A-Za-z_][A-Za-z0-9_]*` (POSIX env var rules).

- `${VAR}` — replaced with the env var value. If the env var is **unset and
  no default is provided**, config-load fails with an error pointing at the
  offending field. This is a deliberate fail-closed design: silently leaving
  the literal `"${PASSWORD}"` in a credential field used to mask itself as
  an unrelated broker error.
- `${VAR:-fallback}` — replaced with the env var if set, otherwise the
  literal `fallback`.
- `${VAR:-}` — explicit "may be empty" escape hatch.

```yaml
password: ${ORDERS_SERVICE_PASSWORD}              # required: must be set
sr_password: ${SR_PASSWORD:-}                     # optional: empty if unset
log_level: ${LOG_LEVEL:-info}                     # default: info
```

## Provisioning Order

Resources are provisioned in dependency order:

1. **Topics** — looked up by name; created if not found, updated if they exist
2. **Schemas** — registered with Schema Registry (naturally idempotent); compatibility level set if specified
3. **Users** — SASL/SCRAM users upserted via `AlterUserSCRAMs`
4. **ACLs** — created via `CreateACLs` (idempotent in Kafka protocol)

## Config Example

See [config.example.yaml](config.example.yaml) for a full example.

```yaml
topics:
  - name: orders
    partitions: 6
    replication_factor: 3
    config:
      retention.ms: "259200000"
      cleanup.policy: delete
      min.insync.replicas: "2"
      compression.type: zstd

schemas:
  - subject: orders-value
    type: avro                  # avro | protobuf | json
    file: schemas/orders.avsc
    compatibility: BACKWARD

users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256    # SCRAM-SHA-256 | SCRAM-SHA-512

acls:
  - principal: "User:orders-service"
    operations: [write, describe]
    resource_type: topic        # topic | group | cluster | transactional_id
    resource_name: orders
    pattern: literal            # literal | prefixed
    permission: allow           # allow | deny
```

## Connection Retry

On startup, the tool retries connecting to the broker and schema registry with exponential backoff (1s initial, 30s cap, 15 retries, 5min total timeout). This handles Docker Compose startup ordering without requiring `wait-for-it` scripts.

## Development

```bash
make build            # Build binary
make test             # Run unit tests
make test-integration # Run integration tests (requires Docker)
make lint             # Run golangci-lint
make vet              # Run go vet
make docker           # Build Docker image
```

## Docker Compose Usage

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.3.1
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
    ports: ["19092:19092", "18081:18081"]
    volumes: [redpandadata:/var/lib/redpanda/data]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      timeout: 5s
      retries: 20

  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:latest
    depends_on:
      redpanda: { condition: service_healthy }
    environment:
      REDPANDA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      REDPANDA_CONFIG_PATH: /config.yaml
    volumes:
      - ./config.example.yaml:/config.yaml:ro

  app:
    image: your-app
    depends_on:
      redpanda-provisioner:
        condition: service_completed_successfully

volumes:
  redpandadata:
```

## Container Image

```bash
docker pull ghcr.io/datarocks-ag/redpanda-provisioner:latest
```
