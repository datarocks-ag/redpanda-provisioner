# redpanda-provisioner

[![CI](https://github.com/datarocks-ag/redpanda-provisioner/actions/workflows/ci.yaml/badge.svg)](https://github.com/datarocks-ag/redpanda-provisioner/actions/workflows/ci.yaml)
![coverage](https://raw.githubusercontent.com/datarocks-ag/redpanda-provisioner/badges/.badges/develop/coverage.svg)

Declarative, idempotent provisioning for Redpanda and other Kafka-compatible brokers. Describe
the topics, Schema Registry schemas, SASL/SCRAM users and access control lists (ACLs) your
services need in one YAML file; the provisioner creates what is missing, updates what it
can, and then exits.

It is built to run **before your application starts**: as a Docker Compose service your app
`depends_on`, or as a Kubernetes Job.

## Why

Every service needs the same few things from the broker: its topics with the right
partitions and retention, a registered schema, a login, and ACLs that let it produce and
consume what it should and nothing else. Usually that ends up in a shell script full of
`rpk` calls, in manual console sessions, or in the application creating topics on startup
with far more rights than it needs.

redpanda-provisioner replaces that with a file you can review in a pull request:

- **Idempotent** — run it on every deploy. Missing resources are created; existing topics
  get more partitions and changed config values, and schemas, users and ACLs are upserted.
- **Additive and safe** — it never deletes topics, schemas, users or ACLs, and never
  decreases a topic's partition count.
- **Secret-friendly** — `${VAR}` references keep passwords out of the file, and an unset
  variable stops the run instead of becoming a literal password.
- **Patient** — it waits for the broker and Schema Registry with exponential backoff, so it
  needs no `wait-for-it` script.
- **Small** — a single static binary in a `scratch` image.

## Quick start

Two files next to each other. A config:

```yaml
# config.yaml
topics:
  - name: orders
    partitions: 3
    replication_factor: 1

users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:orders-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
```

And a Compose file that runs the provisioner once Redpanda is healthy:

```yaml
# docker-compose.yaml
services:
  redpanda:
    image: redpandadata/redpanda:v26.1.6
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092
      - --advertise-kafka-addr internal://redpanda:9092
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      timeout: 5s
      retries: 20

  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      REDPANDA_BROKERS: redpanda:9092
      REDPANDA_CONFIG_PATH: /config.yaml
      ORDERS_SERVICE_PASSWORD: orders-dev-password
    volumes:
      - ./config.yaml:/config.yaml:ro
```

```console
$ docker compose up redpanda-provisioner
...
{"level":"INFO","msg":"Creating topic","topic":"orders","partitions":3,"replication_factor":1}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["write","describe"],…}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

Run it again and the topic is left as it is; the user and the ACL are upserted unchanged.
The [getting started guide](docs/getting-started.md) takes this further with a registered
schema, a consumer group and checks with `rpk`. The repository's own
[docker-compose.yaml](docker-compose.yaml) runs the fuller
[config.example.yaml](config.example.yaml).

## Documentation

Read in this order if you're new; jump straight to a how-to if you know what you need.

| | |
|---|---|
| **[Getting started](docs/getting-started.md)** | A guided tour: from an empty Redpanda to topics, a schema, a service user and its ACLs. |
| **[How it works](docs/concepts.md)** | The model behind a run: what gets reconciled, what is never touched, the order of steps, `strategy`, retries. |
| **How-to guides** | Task-focused stories: |
| &nbsp;&nbsp;[Create a producer service user](docs/howto/create-a-producer-user.md) | A SCRAM login that can write to its topics and nothing else. |
| &nbsp;&nbsp;[Create a consumer service user with a consumer group](docs/howto/create-a-consumer-user.md) | Read access to a topic plus the consumer group it commits offsets to. |
| &nbsp;&nbsp;[Grant access to all topics with a prefix](docs/howto/grant-access-by-topic-prefix.md) | One prefixed ACL instead of one per topic. |
| &nbsp;&nbsp;[Allow idempotent and transactional producers](docs/howto/allow-transactional-producers.md) | Transactional ID ACLs, and when cluster `idempotent_write` matters. |
| &nbsp;&nbsp;[Revoke an ACL or remove a user](docs/howto/revoke-access.md) | The manual steps, since the provisioner never deletes. |
| &nbsp;&nbsp;[Set up the provisioner's admin account](docs/howto/set-up-the-provisioner-admin-account.md) | The SASL account the provisioner itself connects with. |
| &nbsp;&nbsp;[Keep passwords out of the config file](docs/howto/manage-secrets.md) | `${VAR}` references, Docker and Kubernetes secrets. |
| &nbsp;&nbsp;[Rotate a service user's password](docs/howto/rotate-a-user-password.md) | Change a SCRAM password without surprises for the clients. |
| &nbsp;&nbsp;[Connect to a SASL- and TLS-secured cluster and Schema Registry](docs/howto/connect-to-a-secured-cluster.md) | SCRAM, TLS and Schema Registry basic auth, and what isn't supported. |
| &nbsp;&nbsp;[Run the provisioner before your app in Docker Compose](docs/howto/run-in-docker-compose.md) | `service_completed_successfully`, mounted schemas, secrets. |
| &nbsp;&nbsp;[Run the provisioner as a Kubernetes Job](docs/howto/run-as-a-kubernetes-job.md) | ConfigMap, Secret, security context and re-runs. |
| &nbsp;&nbsp;[Change a topic's settings](docs/howto/change-topic-settings.md) | Retention and other topic config, and what a removed key does. |
| &nbsp;&nbsp;[Add partitions to a topic](docs/howto/add-partitions-to-a-topic.md) | Increase only, and what it means for keyed messages. |
| &nbsp;&nbsp;[Register Avro, Protobuf or JSON schemas](docs/howto/register-a-schema.md) | Schema files, subjects and compatibility levels. |
| &nbsp;&nbsp;[Evolve a schema safely](docs/howto/evolve-a-schema.md) | New versions, compatibility checks and the 409 error. |
| **[Deployment](docs/deployment.md)** | Docker Compose, Kubernetes, the container image, environment variables, logging, exit codes. |
| **[Configuration reference](docs/configuration.md)** | Every field of the YAML file, `${VAR}` expansion, validation. |
| **[FAQ](docs/faq.md)** | Questions and error messages people run into. |
| **[Upgrading to 2.0](docs/upgrading-to-2.0.md)** | Breaking changes and how to migrate from 1.x. |
| **[Changelog](CHANGELOG.md)** | What changed in each release. |

## Development

```bash
make build            # Build binary
make test             # Run unit tests
make test-integration # Run integration tests (requires Docker)
make lint             # Run golangci-lint
make vet              # Run go vet
make fmt              # Format the code
make mod-tidy         # Tidy go.mod and go.sum
make docker           # Build Docker image (tagged redpanda-provisioner)
make clean            # Remove the built binary
```
