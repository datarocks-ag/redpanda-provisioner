# Run the provisioner before your app in Docker Compose

After this, `docker compose up` starts Redpanda, runs the provisioner once, and starts your
application only after topics, schemas, users and ACLs are in place.

## When you need this

Your application expects its topics, its Schema Registry subject and its service user to exist
when it starts. Without ordering, Compose starts everything at once, and the application fails or
creates topics with broker defaults before the provisioner gets to them.

## What you need to know first

**A one-shot container.** The provisioner runs one pass and exits: code 0 on success, 1 on any
error. Compose treats it like any other service that stops. See [Deployment](../deployment.md).

**Start conditions.** `depends_on` with a `condition` controls when a service starts:
`service_healthy` waits for a healthcheck, `service_completed_successfully` waits until a
container has exited with code 0. See the
[Compose `depends_on` reference](https://docs.docker.com/reference/compose-file/services/#depends_on).

**Schema paths are relative to the config file.** A schema entry like
`file: schemas/orders.avsc` is resolved against the directory that holds the config file, not
the working directory. Mount the config and the schema files into the same directory tree.

The services start in this order:

```mermaid
flowchart LR
    R[redpanda] -- service_healthy --> P[redpanda-provisioner]
    P -- service_completed_successfully --> A[your app]
```

## Steps

The repository's [`docker-compose.yaml`](../../docker-compose.yaml) has the first two services
already; this guide adds an application and moves the password into a file.

### 1. Lay out the files

Put the config file and the schemas side by side:

```text
.
├── docker-compose.yaml
├── config.yaml              # e.g. a copy of config.example.yaml
├── provisioner.env          # secrets, not committed
└── schemas/
    └── orders.avsc
```

[`config.example.yaml`](../../config.example.yaml) and
[`schemas/orders.avsc`](../../schemas/orders.avsc) from this repository work as a starting point.

### 2. Keep the passwords in an env file

`config.example.yaml` reads the service user's password from `${ORDERS_SERVICE_PASSWORD}`. Put
the value in a file that stays out of Git:

```console
$ printf 'ORDERS_SERVICE_PASSWORD=%s\n' "$(openssl rand -hex 24)" > provisioner.env
$ echo 'provisioner.env' >> .gitignore
```

More options in [Keep passwords out of the config file](manage-secrets.md).

### 3. Write the Compose file

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v26.1.6
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
      - --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081
    ports:
      - "19092:19092"
      - "18081:18081"
    volumes: [redpandadata:/var/lib/redpanda/data]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      timeout: 5s
      retries: 20

  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
    depends_on:
      redpanda: { condition: service_healthy }
    environment:
      REDPANDA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      REDPANDA_CONFIG_PATH: /config/config.yaml
    env_file: ./provisioner.env
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro

  orders-app:
    image: <your-app-image>
    depends_on:
      redpanda-provisioner: { condition: service_completed_successfully }
    env_file: ./provisioner.env     # the app logs in as orders-service with the same password

volumes:
  redpandadata:
```

The parts that matter:

- **Pinned image tag.** `2.0.0` instead of `latest`, so an upgrade happens when you change the
  file, not when the registry moves. The image is also tagged `2.0` (latest patch of 2.0). See
  [Deployment](../deployment.md) and [Upgrading to 2.0](../upgrading-to-2.0.md).
- **One mount point for config and schemas.** The config lands at `/config/config.yaml`, so
  `schemas/orders.avsc` is read from `/config/schemas/orders.avsc`.
- **`REDPANDA_CONFIG_PATH`.** Without it the provisioner looks for `./config.yaml` in its
  working directory.
- **`service_healthy` on Redpanda.** The provisioner would retry for up to five minutes
  anyway, but waiting for the healthcheck keeps the log free of retry warnings.
- **`service_completed_successfully` on the provisioner.** Your app starts only after a run
  that exited with 0.

### 4. Start the stack

```console
$ docker compose up -d
```

## Check that it works

The output below was captured with this Compose file (with different host ports and a stand-in
app that runs `rpk topic list`) on 2026-11-15. It is safe to run on any development machine.

The provisioner has exited with 0, and the app ran after it:

```console
$ docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
SERVICE                STATUS
orders-app             Exited (0) 6 seconds ago
redpanda               Up 14 seconds (healthy)
redpanda-provisioner   Exited (0) 7 seconds ago
```

Your app would show `Up` instead of `Exited`. The provisioner's log ends with the success line
(trimmed to the first and last lines):

```console
$ docker compose logs --no-log-prefix redpanda-provisioner
{"time":"2026-11-15T17:00:55.049082402Z","level":"INFO","msg":"Starting redpanda-provisioner","version":"v2.0.0"}
…
{"time":"2026-11-15T17:00:55.86077419Z","level":"INFO","msg":"Provisioning complete"}
{"time":"2026-11-15T17:00:55.860847508Z","level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

The stand-in app saw the provisioned topics:

```text
orders-app-1  | NAME        PARTITIONS  REPLICAS
orders-app-1  | _schemas    1           1
orders-app-1  | orders      6           1
orders-app-1  | user-state  6           1
```

When the provisioner fails, the app is not started and `docker compose up` exits with 1. Here
`provisioner.env` was empty:

```text
 Container rpdocs-ops-redpanda-provisioner-1 Error service "redpanda-provisioner" didn't complete successfully: exit 1
 Container rpdocs-ops-orders-app-1 not started: condition "service_completed_successfully" of service "redpanda-provisioner" not met 
service "redpanda-provisioner" didn't complete successfully: exit 1
```

The reason is in the provisioner's last log line:

```text
{"time":"2026-11-15T17:01:17.805267487Z","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):\n  users[0].password: ${ORDERS_SERVICE_PASSWORD}"}
```

## Pitfalls

- **Every `up` runs it again.** `docker compose up -d` starts the exited provisioner container
  once more, so a changed `config.yaml` or schema takes effect on the next `up`. To run only the
  provisioner, without touching the other services:

  ```console
  $ docker compose run --rm redpanda-provisioner
  ```

  Every run is safe to repeat; existing topics, schemas, users and ACLs are reconciled, not
  duplicated. On Redpanda v26.1.6 each rerun logs `Updating topic config` for
  `min.insync.replicas` with `"current":""`, because the broker reports that value back empty.
- **Schema file not found.** Mounting only the config file, or mounting the schemas somewhere
  else, fails at provisioning time with
  `reading schema file "/config/schemas/orders.avsc": open /config/schemas/orders.avsc: no such file or directory`.
  Topics created before that point stay created.
- **`localhost` inside a container.** In Compose, the provisioner reaches Redpanda by service
  name (`redpanda:9092`), not `localhost:19092`. Use the listener advertised as
  `internal://redpanda:9092`.
- **Compose `secrets:` don't work for the provisioner.** They are mounted as files, and the
  provisioner reads secrets only from environment variables.
- **`latest` moves.** With `:latest`, a `docker compose pull` can bring a new major version with
  breaking config changes. Pin a version.

## Further reading

- [Deployment](../deployment.md) for image tags, environment variables and exit codes.
- [Getting started](../getting-started.md) for a guided first run.
- [Keep passwords out of the config file](manage-secrets.md).
- [Connect to a SASL- and TLS-secured cluster and Schema Registry](connect-to-a-secured-cluster.md).
- [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).
- [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/).
