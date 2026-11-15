# Upgrading to 2.0

2.0 makes config loading fail-closed. In 1.x, an unset `${VAR}` stayed in the config as
literal text: a missing password became the password `${ORDERS_SERVICE_PASSWORD}`, and you
found out when the service couldn't log in. In 2.0, the same config fails **at load time,
before anything is changed**, and names the field.

Three more changes can affect an existing 1.x setup: schema file paths, the parsing of
`REDPANDA_TLS_ENABLED`, and the log messages on failure. Everything else is new and opt-in.

## The upgrade in four steps

1. **Pin the new image** (`ghcr.io/datarocks-ag/redpanda-provisioner:2.0`) in a staging or
   local environment, with the same config, mounts and environment variables as production.
2. **Go through the checklist** below and adjust the config, mounts and environment.
3. **Run it.** Config errors appear within milliseconds, before the provisioner connects. Fix
   them using the sections below and run again until it logs
   `redpanda-provisioner finished successfully`.
4. **Deploy** with the image pinned to `2.0` or `2.0.0`, not `latest`.

## Checklist

- [ ] Every `${VAR}` in the config is either set in every environment, or written as
  `${VAR:-default}` or `${VAR:-}`. See [Unresolved `${VAR}` fails](#unresolved-var-fails).
- [ ] No variable that should fall back to a default is set to an empty string.
- [ ] Schema files are mounted relative to the **config file's directory**, or referenced with
  absolute paths. See [Schema file paths](#schema-file-paths-are-relative-to-the-config-file).
- [ ] `REDPANDA_TLS_ENABLED`, if set, is one of `true`, `false`, `1`, `0` (or another value
  listed [below](#redpanda_tls_enabled-is-parsed-strictly)), and it means what you intend.
- [ ] Log-based alerts match `redpanda-provisioner failed` instead of the 1.x failure messages.
  See [Failure log messages](#failure-log-messages-changed).

## Breaking changes

### Unresolved `${VAR}` fails

**1.x:** a reference to an unset variable stayed in the value as literal text. A missing
`ORDERS_SERVICE_PASSWORD` set the user's password to the string `${ORDERS_SERVICE_PASSWORD}`.
`${VAR:-default}` wasn't supported either and was kept as literal text.

**2.0:** loading fails, and the error lists every unresolved reference with its config path.
With the repository's `config.example.yaml` and `ORDERS_SERVICE_PASSWORD` not set:

```json
{"time":"2026-11-15T16:59:36.421402105Z","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):\n  users[0].password: ${ORDERS_SERVICE_PASSWORD}"}
```

Nothing is created or changed, and the process exits with `1`.

**What to do:** for each reported reference, decide whether the value is required.

- **Required** (passwords, mostly): set the variable in every environment. Leave the reference
  as `${VAR}`, so a missing secret keeps failing loudly.
- **Has a sensible default:** write `${VAR:-default}`.
- **May be empty:** write `${VAR:-}`.

Before (1.x):

```yaml
topics:
  - name: ${ORDERS_TOPIC}
    partitions: 6
    replication_factor: 3
    config:
      retention.ms: ${ORDERS_RETENTION_MS}
```

After (2.0), with `orders` and seven days as defaults:

```yaml
topics:
  - name: ${ORDERS_TOPIC:-orders}
    partitions: 6
    replication_factor: 3
    config:
      retention.ms: ${ORDERS_RETENTION_MS:-604800000}
```

Two things work differently from a shell:

- **A variable that is set but empty does not fall back to the default.** With
  `ORDERS_TOPIC=""`, `${ORDERS_TOPIC:-orders}` resolves to an empty string, and loading fails
  with `topics[0].name: is required`. Remove the variable instead of setting it to empty, for
  example an empty `ORDERS_TOPIC:` entry in a Compose file.
- **Only string fields are expanded**, as in 1.x. `partitions`, `replication_factor`,
  `iterations` and `broker.tls.enabled` can't use `${VAR}`; the YAML parser rejects the text.

The full rules are in the [Configuration reference](configuration.md) and in
[Keep passwords out of the config file](howto/manage-secrets.md).

### Schema file paths are relative to the config file

**1.x:** `schemas[].file` was resolved against the process's working directory, which is `/`
in the container image.

**2.0:** a relative path is resolved against the **directory that contains the config file**.
Absolute paths are used as they are.

| Config path | `file:` | 1.x reads | 2.0 reads |
|---|---|---|---|
| `/config.yaml` | `schemas/orders.avsc` | `/schemas/orders.avsc` | `/schemas/orders.avsc` |
| `/config/config.yaml` | `schemas/orders.avsc` | `/schemas/orders.avsc` | `/config/schemas/orders.avsc` |
| `/config/config.yaml` | `/schemas/orders.avsc` | `/schemas/orders.avsc` | `/schemas/orders.avsc` |

With the config mounted at `/config.yaml`, as in the 1.x example Compose file, nothing changes.
Otherwise, a 1.x mount layout fails in 2.0 when the schema is provisioned, after the topics:

```
provisioning: provisioning schema "orders-value": reading schema file "/config/schemas/orders.avsc": open /config/schemas/orders.avsc: no such file or directory
```

**What to do:** mount the schemas next to the config. Before (1.x):

```yaml
    environment:
      REDPANDA_CONFIG_PATH: /config/config.yaml
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./schemas:/schemas:ro
```

After (2.0):

```yaml
    environment:
      REDPANDA_CONFIG_PATH: /config/config.yaml
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro
```

Alternatively, keep the mounts and change `file:` to the absolute path `/schemas/orders.avsc`.
In Kubernetes, put the schema files into the same ConfigMap as the config, or mount their
volume below the config's directory.

### `REDPANDA_TLS_ENABLED` is parsed strictly

**1.x:** only the exact string `true` turned TLS on. Every other value, including `TRUE`, `1`
and `yes`, silently meant off.

**2.0:** the value must be one of these, or the run fails before connecting:

| Turns TLS on | Turns TLS off |
|---|---|
| `true`, `True`, `TRUE`, `t`, `T`, `1` | `false`, `False`, `FALSE`, `f`, `F`, `0` |

An empty or unset variable means off. Any other value is an error:

```
REDPANDA_TLS_ENABLED: invalid bool "yes": strconv.ParseBool: parsing "yes": invalid syntax
```

**What to do:**

- `yes`, `on`, `enabled` or similar: replace with `true`, or with `false` if the 1.x setup ran
  without TLS (which it did, since 1.x ignored the value).
- `1`, `TRUE`, `True`, `t`: **these now turn TLS on.** In 1.x they meant off. If the cluster
  listener doesn't use TLS, the provisioner now retries the connection for 5 minutes and fails.
  Set the variable to `false` or remove it.

### Failure log messages changed

**1.x** logged failures with separate messages, such as `Failed to load configuration`,
`Failed to connect to Redpanda`, `Failed to connect to Schema Registry` and
`Provisioning failed`.

**2.0** logs every failure as `redpanda-provisioner failed`, with the reason in the `error`
attribute (see the example above). The exit code is `1` in both versions.

**What to do:** update log-based alerts or searches to match `redpanda-provisioner failed`.

## Behaviour changes

These don't make a 1.x config fail, but they change what happens:

- **The Schema Registry is only contacted when `schemas:` has entries.** In 1.x, a set
  `SCHEMA_REGISTRY_URL` made the provisioner connect, and fail if the registry was unreachable,
  even without schemas. 2.0 logs
  `Skipping Schema Registry connection — no schemas configured` and continues.
- **SCRAM users get an iteration count.** 1.x sent an iteration count of `0`, which the broker
  can reject with `UNACCEPTABLE_CREDENTIAL`. 2.0 sends `4096` unless `users[].iterations` says
  otherwise. No config change is needed.
- **Half-set broker credentials fail.** In 1.x, setting only one of `REDPANDA_SASL_USERNAME`
  and `REDPANDA_SASL_PASSWORD` silently connected without SASL. 2.0 fails with
  `broker SASL username and password must both be set or both be empty (set broker.sasl.{username,password} or REDPANDA_SASL_{USERNAME,PASSWORD})`.
- **Spaces around broker addresses are trimmed:** `REDPANDA_BROKERS="redpanda-0:9092, redpanda-1:9092"`
  works in 2.0.
- **Log order is stable.** Topic config changes are applied and logged sorted by key, and an
  ACL entry with several operations is created in one request.

## New in 2.0

Once you're upgraded, these are available. None of them is required.

- **Connection settings in YAML.** `broker:` (addresses, SASL, TLS) and `schema_registry:`
  (URL, basic auth) blocks. The environment variables still work and fill in any field the YAML
  leaves empty. An explicit `broker.tls.enabled: false` in YAML **overrides**
  `REDPANDA_TLS_ENABLED=true`; only a missing `enabled` falls back to the variable. See the
  [Configuration reference](configuration.md) and [How it works](concepts.md).
- **`${VAR:-default}` and `${VAR:-}`** for defaults and optional values.
- **`users[].iterations`** to raise the SCRAM iteration count above the default of `4096`
  (up to `16384`).
- **Schema Registry basic auth** through `schema_registry.username` and `.password`, or
  `SCHEMA_REGISTRY_USERNAME` and `SCHEMA_REGISTRY_PASSWORD`. See
  [Connect to a secured cluster](howto/connect-to-a-secured-cluster.md).

For example, an env-only 1.x deployment can move its connection settings into the config:

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

The [changelog](../CHANGELOG.md) has the complete list of changes in 2.0.0.
