# Deployment

The provisioner is a short-lived process. It reads its config, connects to Redpanda, creates or
updates topics, schemas, users and access control lists (ACLs), and exits: `0` on success, `1`
on any error. Run it wherever your application is deployed, as the step that has to succeed
before the application starts.

- [Container image](#container-image)
- [Docker Compose](#docker-compose)
- [Kubernetes Job](#kubernetes-job)
- [Standalone binary](#standalone-binary)
- [Environment variables](#environment-variables)
- [YAML or environment variables](#yaml-or-environment-variables)
- [Connection retry](#connection-retry)
- [Logging](#logging)
- [Exit codes](#exit-codes)
- [Permissions the provisioner's account needs](#permissions-the-provisioners-account-needs)

## Container image

Images are published to `ghcr.io/datarocks-ag/redpanda-provisioner` for `linux/amd64` and
`linux/arm64`, built from each release tag:

| Tag | Points to | Use it for |
|---|---|---|
| `2.0.0` | exactly that release | production: reproducible |
| `2.0` | latest patch release of 2.0 | production: picks up fixes |
| `latest` | latest release | trying things out |
| `sha-<commit>` | the build of one commit | debugging |

Pin `<major>.<minor>` or the full version in anything that matters. `latest` moves to the
next major release without warning, and a major release can change behaviour (see
[Upgrading to 2.0](upgrading-to-2.0.md)).

The image is built `FROM scratch`. It contains the static binary `/redpanda-provisioner` (the
entrypoint) and the CA certificate bundle used for TLS, and nothing else:

- **No shell.** You can't `docker exec` into it or run `sh -c` wrappers. Pass configuration
  through environment variables and mounted files.
- **Working directory `/`.** The default config path `./config.yaml` is therefore
  `/config.yaml`. Set `REDPANDA_CONFIG_PATH` to mount it elsewhere.
- **Runs as root** by default; the image sets no user. It doesn't need root: it runs fine with
  any user ID that can read the mounted config and schema files, for example
  `--user 65532:65532` or `runAsNonRoot` in Kubernetes.
- The version is logged at startup: `"msg":"Starting redpanda-provisioner","version":"v2.0.0"`.

## Docker Compose

The provisioner runs between Redpanda and the application. This is the repository's
[docker-compose.yaml](../docker-compose.yaml) with an application added:

```yaml
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
      - --schema-registry-addr internal://0.0.0.0:8081
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
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      REDPANDA_CONFIG_PATH: /config/config.yaml
      ORDERS_SERVICE_PASSWORD: orders-dev-password   # development only
    volumes:
      # Schema file paths resolve against the config file's directory,
      # so schemas/ is mounted next to the config.
      - ./config.example.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro

  orders-service:
    image: <your-registry>/orders-service
    depends_on:
      redpanda-provisioner:
        condition: service_completed_successfully
```

- `condition: service_completed_successfully` keeps the application from starting when
  provisioning failed. With the password variable missing, `docker compose up -d` ends with:

  ```
   Container rpdocs-core2-redpanda-provisioner-1 Error service "redpanda-provisioner" didn't complete successfully: exit 1
   Container rpdocs-core2-orders-service-1 not started: condition "service_completed_successfully" of service "redpanda-provisioner" not met
  service "redpanda-provisioner" didn't complete successfully: exit 1
  ```

  (`rpdocs-core2` is the Compose project name of the test run; yours is your directory name.)
- `condition: service_healthy` isn't strictly needed, because the provisioner
  [retries](#connection-retry) until Redpanda answers, but it keeps retry warnings out of the
  log.
- Don't set `restart: always` or `unless-stopped` on the provisioner. It is supposed to exit.
- Outside local development, keep passwords out of the Compose file; see
  [Keep passwords out of the config file](howto/manage-secrets.md).

The full walkthrough, including what to check, is in
[Run the provisioner before your app in Docker Compose](howto/run-in-docker-compose.md).

## Kubernetes Job

A Job fits deploy pipelines and Helm hooks (`helm.sh/hook: pre-install,pre-upgrade`): each
deploy runs it once, and the rollout of the application waits for it. Ship the config and
schemas as a ConfigMap and the credentials as a Secret:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: redpanda-provisioner
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
      containers:
        - name: redpanda-provisioner
          image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0
          env:
            - { name: REDPANDA_CONFIG_PATH, value: /config/config.yaml }
            - { name: REDPANDA_BROKERS, value: "<broker-host>:<port>" }
            - { name: REDPANDA_TLS_ENABLED, value: "true" }
            - name: REDPANDA_SASL_USERNAME
              valueFrom: { secretKeyRef: { name: redpanda-provisioner-admin, key: username } }
            - name: REDPANDA_SASL_PASSWORD
              valueFrom: { secretKeyRef: { name: redpanda-provisioner-admin, key: password } }
            - name: ORDERS_SERVICE_PASSWORD
              valueFrom: { secretKeyRef: { name: orders-service-kafka, key: password } }
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: [ALL] }
          volumeMounts:
            - { name: config, mountPath: /config, readOnly: true }
      volumes:
        - name: config
          configMap: { name: redpanda-provisioner-config }
```

Replace `<broker-host>:<port>` with your broker's address; the Secret and ConfigMap names are examples. Running it
as an init container of the application works too, but then every replica runs it on every
start. For the complete walkthrough (creating the ConfigMap with schema files, the Secrets,
and checking the result), see
[Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md).

## Standalone binary

Each release also has `tar.gz` archives for Linux and macOS (`amd64`, `arm64`) on the
[releases page](https://github.com/datarocks-ag/redpanda-provisioner/releases). The binary
takes no flags; it is configured through environment variables only:

```console
$ export REDPANDA_CONFIG_PATH=./config.yaml REDPANDA_BROKERS=localhost:19092
$ export SCHEMA_REGISTRY_URL=http://localhost:18081 ORDERS_SERVICE_PASSWORD=orders-dev-password
$ ./redpanda-provisioner
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDPANDA_CONFIG_PATH` | `./config.yaml` | Path to the YAML config. In the image, `./` is `/`. |
| `REDPANDA_BROKERS` | `localhost:9092` | Comma-separated broker addresses, e.g. `redpanda-0:9092,redpanda-1:9092`. Used when `broker.addresses` is empty. |
| `REDPANDA_SASL_USERNAME` | none | The provisioner's SASL username. Used when `broker.sasl.username` is empty. |
| `REDPANDA_SASL_PASSWORD` | none | Its password. Set both or neither. |
| `REDPANDA_SASL_MECHANISM` | `SCRAM-SHA-256` | `SCRAM-SHA-256` or `SCRAM-SHA-512`. Used when `broker.sasl.mechanism` is empty. |
| `REDPANDA_TLS_ENABLED` | `false` | `true`, `True`, `TRUE`, `t`, `T` or `1` to turn TLS on; `false`, `False`, `FALSE`, `f`, `F` or `0` to turn it off. Anything else, including `yes`, `on` or `enabled`, fails the run. Ignored when `broker.tls.enabled` is set in YAML. |
| `SCHEMA_REGISTRY_URL` | none | Schema Registry URL. Required when the config has `schemas:`. Used when `schema_registry.url` is empty. |
| `SCHEMA_REGISTRY_USERNAME` | none | HTTP basic auth user for the Schema Registry. |
| `SCHEMA_REGISTRY_PASSWORD` | none | Its password. Set both or neither. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`, lowercase. Any other value, including `DEBUG`, silently means `info`. |

All authentication variables are optional; a development cluster often has no authentication.
Variables that the config references with `${VAR}` (such as `ORDERS_SERVICE_PASSWORD`) are set
the same way; see the expansion rules in the [Configuration reference](configuration.md).

Invalid values are reported before the provisioner connects:

```
REDPANDA_TLS_ENABLED: invalid bool "yes": strconv.ParseBool: parsing "yes": invalid syntax
broker SASL username and password must both be set or both be empty (set broker.sasl.{username,password} or REDPANDA_SASL_{USERNAME,PASSWORD})
unsupported broker SASL mechanism "PLAIN" (must be SCRAM-SHA-256 or SCRAM-SHA-512)
```

## YAML or environment variables

Connection settings can come from the `broker:` and `schema_registry:` blocks of the config,
from the environment variables above, or both. A non-empty YAML value wins; an empty or missing
one is filled from the environment variable. `broker.tls.enabled` wins whenever it is present,
so `enabled: false` in YAML turns TLS off even with `REDPANDA_TLS_ENABLED=true`. A `${VAR:-}`
that resolves to an empty string counts as empty and falls through to the environment variable.
[How it works](concepts.md) explains the rules with examples.

## Connection retry

At startup, the provisioner retries the broker connection with exponential backoff (1s, 2s, 4s,
… capped at 30s) for at most 5 minutes, logging each attempt as `Redpanda not ready, retrying`
with the cause in its `error` attribute. The Schema Registry connection, when it is needed, is
retried the same way. Authentication failures are retried too, so a wrong password takes 5
minutes to fail; the cause is in the retry warnings, not in the final error. Details are in
[How it works](concepts.md).

## Logging

The provisioner logs JSON lines to standard output, one object per line with `time`, `level`
and `msg`, plus attributes for the resource it is working on:

```json
{"time":"2026-11-15T16:59:23.253617096Z","level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"time":"2026-11-15T16:59:23.25504096Z","level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
```

A successful run ends with `"msg":"redpanda-provisioner finished successfully"`, a failed one
with `"msg":"redpanda-provisioner failed"` and the reason in `error`. Passwords are never
logged. Set `LOG_LEVEL=debug` to also see resources that are already up to date (for example
`Topic config up to date`).

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Every resource in the config was provisioned. |
| `1` | Something failed: the config, the connection, or a resource. The last log line says what. |

The provisioner stops at the first failing resource. Resources provisioned before it in the
same run stay as they are; fix the problem and run it again. A run that is stopped from outside
(for example a Kubernetes `activeDeadlineSeconds` or Ctrl+C) also exits with `1`.

## Permissions the provisioner's account needs

Without SASL, the provisioner needs no account. On a cluster with SASL and authorization
enabled, it logs in with `REDPANDA_SASL_USERNAME` and needs:

| Work | ACLs |
|---|---|
| Topics: look up, create, add partitions, read and change configs | `create`, `describe`, `describe_configs`, `alter_configs`, `alter` on the topics it manages |
| Users: create and update SCRAM credentials | `alter` on the cluster |
| ACLs: create | `alter` on the cluster |

In a test against Redpanda v26.1.6 with authorization enabled, an account with exactly these
ACLs (topics by `*`) created and updated a topic, a user and an ACL. Without the cluster
`alter` ACL, the run failed at the first user with
`upserting SCRAM user: CLUSTER_AUTHORIZATION_FAILED: Cluster authorization failed.`
A Redpanda superuser needs no ACLs at all.

Schema Registry access is separate: with basic auth, the provisioner's Schema Registry user must
be allowed to register schemas and to read and set the compatibility level of the subjects.
This part was not tested with Schema Registry authorization enabled.

How to create this account is described in
[Set up the provisioner's admin account](howto/set-up-the-provisioner-admin-account.md).

## Next steps

- [Keep passwords out of the config file](howto/manage-secrets.md) before going to production.
- [Connect to a SASL- and TLS-secured cluster and Schema Registry](howto/connect-to-a-secured-cluster.md).
- [FAQ](faq.md) for operations questions.
