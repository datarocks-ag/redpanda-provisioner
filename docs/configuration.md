# Configuration reference

The complete reference for the YAML config file. For a guided introduction, start with
[Getting started](getting-started.md); for what the provisioner does with each section, read
[How it works](concepts.md). A commented example is in
[config.example.yaml](../config.example.yaml).

- [Top level](#top-level)
- [broker](#broker)
- [schema_registry](#schema_registry)
- [topics](#topics)
- [schemas](#schemas)
- [users](#users)
- [acls](#acls)
- [Environment variable expansion](#environment-variable-expansion)
- [Validation](#validation)

The provisioner reads the file named by `REDPANDA_CONFIG_PATH` (default `./config.yaml`). All
values are **case-sensitive**: `update` is valid, `Update` is not; `BACKWARD` is valid,
`backward` is not.

## Top level

```yaml
strategy: update        # optional: "update" (default) or "create"
broker: {...}           # optional: connection to the Kafka API
schema_registry: {...}  # optional: connection to the Schema Registry
topics: [...]
schemas: [...]
users: [...]
acls: [...]
```

| Field | Type | Default | Description |
|---|---|---|---|
| `strategy` | string | `update` | `update` or `create`. Applies to **topics only**; schemas, users and ACLs are always created or updated. A topic's own `strategy` overrides it. |
| `broker` | mapping | empty | See [broker](#broker). |
| `schema_registry` | mapping | empty | See [schema_registry](#schema_registry). |
| `topics` | list | empty | See [topics](#topics). |
| `schemas` | list | empty | See [schemas](#schemas). |
| `users` | list | empty | See [users](#users). |
| `acls` | list | empty | See [acls](#acls). |

Every section is optional. A file with only `topics:` is a valid config. Resources are
provisioned in the order topics, schemas, users, ACLs; see [How it works](concepts.md).

## broker

How the provisioner connects to the Kafka API of Redpanda.

```yaml
broker:
  addresses:
    - redpanda-0.redpanda:9092
    - redpanda-1.redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-512
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}
  tls:
    enabled: true
```

| Field | Type | Default | Description |
|---|---|---|---|
| `addresses` | list of strings | `REDPANDA_BROKERS`, then `localhost:9092` | Seed broker addresses as `host:port`. Leading and trailing spaces are trimmed. An entry that is empty or only whitespace is a load error. |
| `sasl.mechanism` | string | `REDPANDA_SASL_MECHANISM`, then `SCRAM-SHA-256` | `SCRAM-SHA-256` or `SCRAM-SHA-512`. No other SASL mechanism (PLAIN, OAUTHBEARER, GSSAPI) is supported. |
| `sasl.username` | string | `REDPANDA_SASL_USERNAME` | The provisioner's own login. Set both `username` and `password`, or neither. |
| `sasl.password` | string | `REDPANDA_SASL_PASSWORD` | Its password. |
| `tls.enabled` | bool | `REDPANDA_TLS_ENABLED`, then `false` | `true` or `false`, unquoted. Turns on TLS 1.2 or newer, verified against the system CA certificates. |

Every field is optional. A field left empty in YAML is filled from its environment variable,
so an env-only deployment needs no `broker:` block at all. The full precedence rules are in
[How it works](concepts.md); the environment variables are listed in
[Deployment](deployment.md).

- **SASL** is used only when both username and password are set (from YAML or the
  environment). With neither, the provisioner connects without authentication. With only one
  of them, it fails before connecting.
- **TLS** is either on or off. A custom CA file, skipping verification, client certificates
  (mTLS) and a server name override are not supported. A YAML `enabled: false` wins over
  `REDPANDA_TLS_ENABLED=true`.
- `tls.enabled` can't come from `${VAR}`, because [expansion](#environment-variable-expansion)
  only applies to strings. Use `REDPANDA_TLS_ENABLED` instead and leave the field out.

The account the provisioner logs in with needs enough rights to create topics, users and ACLs;
see [Set up the provisioner's admin account](howto/set-up-the-provisioner-admin-account.md).
For a complete secured setup, see
[Connect to a secured cluster](howto/connect-to-a-secured-cluster.md).

## schema_registry

How the provisioner reaches the Schema Registry HTTP API. Only needed when `schemas:` has
entries.

```yaml
schema_registry:
  url: https://schema-registry.example.com:8081
  username: ${SR_USERNAME}
  password: ${SR_PASSWORD}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | `SCHEMA_REGISTRY_URL` | Base URL, `http://` or `https://`. `https://` is verified against the system CA certificates. |
| `username` | string | `SCHEMA_REGISTRY_USERNAME` | HTTP basic auth user. Set both `username` and `password`, or neither. |
| `password` | string | `SCHEMA_REGISTRY_PASSWORD` | HTTP basic auth password. |

- The provisioner connects to the Schema Registry **only when `schemas:` is not empty**.
  Without schemas, it logs `Skipping Schema Registry connection — no schemas configured` (if a
  URL is set) and never contacts it.
- With schemas but no URL from YAML or `SCHEMA_REGISTRY_URL`, the run fails with
  `schema registry URL required when schemas are configured (set schema_registry.url or SCHEMA_REGISTRY_URL)`.
  This check runs after the broker connection, not at load time.
- Basic auth is the only supported authentication.

## topics

```yaml
topics:
  - name: orders                 # required
    partitions: 6                # optional, >= 1
    replication_factor: 3        # optional, >= 1
    config:                      # optional, topic properties
      retention.ms: "604800000"
      cleanup.policy: delete
      min.insync.replicas: "2"
    strategy: update             # optional: "update" or "create"
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | **required** | Topic name. Must be unique in the file. |
| `partitions` | integer | `1` on creation | Number of partitions, `1` or more. |
| `replication_factor` | integer | `1` on creation | Replication factor, `1` or more. Must not exceed the number of brokers. |
| `config` | map of string to string | none | Topic properties such as `retention.ms`, `cleanup.policy`, `compression.type`. |
| `strategy` | string | top-level `strategy` | `update` or `create` for this topic only. |

**When the topic doesn't exist,** it is created with the given partitions, replication factor
and config. An omitted `partitions` or `replication_factor` is sent as `1`, **not** as the
broker's default. Set both explicitly in production.

**When the topic exists,** `strategy` decides:

| | `update` | `create` |
|---|---|---|
| Logged | changes, see below | `Skipping existing topic (strategy=create)` |
| More `partitions` than the topic has | partitions are added (`Increasing topic partitions`) | nothing |
| Fewer `partitions` than the topic has | nothing; warning `Topic has more partitions than configured (cannot decrease)` | nothing |
| Different `replication_factor` | nothing, and nothing is logged | nothing |
| `config` keys | each listed key that differs is set (`Updating topic config`) | nothing |

- Only the keys you list under `config` are compared and set. **Removing a key from the file
  doesn't reset it** on the topic; it keeps its last value.
- Values are compared as text with what the broker reports. On Redpanda v26.1.6,
  `min.insync.replicas` is reported back empty, so it is logged as `Updating topic config` on
  every run although nothing changes.
- Quote numbers in `config` (`"604800000"`) for readability. Unquoted numbers and booleans are
  accepted too and used as written.
- `partitions` and `replication_factor` must be YAML integers: `partitions: "6"` is a
  [parse error](#validation), and so is `partitions: ${PARTITIONS}`.

See [Change a topic's settings](howto/change-topic-settings.md) and
[Add partitions to a topic](howto/add-partitions-to-a-topic.md).

## schemas

```yaml
schemas:
  - subject: orders-value             # required
    type: avro                        # required: avro, protobuf or json
    file: schemas/orders.avsc         # required, relative to the config file's directory
    compatibility: BACKWARD           # optional
```

| Field | Type | Default | Description |
|---|---|---|---|
| `subject` | string | **required** | Schema Registry subject, e.g. `<topic>-value` for the default topic naming strategy. Must be unique in the file. |
| `type` | string | **required** | `avro`, `protobuf` or `json`, lowercase. |
| `file` | string | **required** | Path to the schema file. A relative path resolves against the **directory of the config file**, not the working directory. An absolute path is used as is. |
| `compatibility` | string | not set | `BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE` or `NONE`, uppercase. |

- With the config at `/config/config.yaml`, `file: schemas/orders.avsc` reads
  `/config/schemas/orders.avsc`. Mount schema files next to the config.
- Whether the file exists is checked when the schema is provisioned, not at load time. A
  missing file fails the run after topics have been provisioned:
  `provisioning schema "orders-value": reading schema file "/config/schemas/orders.avsc": open /config/schemas/orders.avsc: no such file or directory`.
- The schema is registered on every run. Unchanged content returns the existing schema ID;
  changed content creates a new version, if the subject's compatibility rules allow it.
- `compatibility` is set on the subject only when it differs from the current level, and only
  **after** the schema is registered. Removing `compatibility` from the file leaves the
  subject's level unchanged.
- Schema references (a schema importing another subject) are not supported.

See [Register Avro, Protobuf or JSON schemas](howto/register-a-schema.md) and
[Evolve a schema safely](howto/evolve-a-schema.md).

## users

SASL/SCRAM users for your services.

```yaml
users:
  - username: orders-service              # required
    password: ${ORDERS_SERVICE_PASSWORD}  # required
    mechanism: SCRAM-SHA-256              # required: SCRAM-SHA-256 or SCRAM-SHA-512
    iterations: 4096                      # optional
```

| Field | Type | Default | Description |
|---|---|---|---|
| `username` | string | **required** | User name. Must be unique in the file. |
| `password` | string | **required** | Must not be empty. Use `${VAR}` to keep it out of the file. |
| `mechanism` | string | **required**, no default | `SCRAM-SHA-256` or `SCRAM-SHA-512`, uppercase. Clients must log in with the same mechanism. |
| `iterations` | integer | `4096` | PBKDF2 iteration count from `4096` to `16384`. `0` or omitted means `4096`. |

- Users are created or updated on every run, so the password in the file is set again each
  time. Changing it in the file changes it in the cluster on the next run.
- `iterations` outside `4096` to `16384` fails validation before anything is provisioned:
  `validating config: users[0].iterations: must be <= 16384, got 20000`.
- Users removed from the file are **not** deleted.
- The password never appears in the log.

See [Create a producer service user](howto/create-a-producer-user.md),
[Keep passwords out of the config file](howto/manage-secrets.md) and
[Rotate a service user's password](howto/rotate-a-user-password.md).

## acls

Access control list (ACL) entries. Each entry allows or denies one principal a set of
operations on one resource.

```yaml
users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:orders-service"   # required
    operations: [write, describe]      # required, at least one
    resource_type: topic               # required
    resource_name: orders              # required
    pattern: literal                   # required: literal or prefixed
    permission: allow                  # required: allow or deny
```

| Field | Type | Default | Description |
|---|---|---|---|
| `principal` | string | **required** | Written as `User:<username>`. The `User:` prefix is **not** added for you, and the value is not checked: `orders-service` without the prefix is sent as is and matches nobody. |
| `operations` | list of strings | **required** | One or more of `all`, `read`, `write`, `create`, `delete`, `alter`, `describe`, `cluster_action`, `describe_configs`, `alter_configs`, `idempotent_write`, lowercase. |
| `resource_type` | string | **required** | `topic`, `group`, `cluster` or `transactional_id`. |
| `resource_name` | string | **required** | Topic, consumer group or transactional ID name, or a prefix of it with `pattern: prefixed`. Required for `cluster` too, where any value is accepted and ignored. |
| `pattern` | string | **required** | `literal` (exact name) or `prefixed` (every name starting with `resource_name`). |
| `permission` | string | **required** | `allow` or `deny`. |

- The host is always `*` (any host). There is no `host` field.
- Entries are created on every run; creating an existing ACL changes nothing.
- **Removing an entry, or an operation from an entry, doesn't remove it from the cluster.**
  See [Revoke an ACL or remove a user](howto/revoke-access.md).
- Duplicate entries are not detected and do no harm.
- The principal doesn't have to be a user from `users:`; it can be any principal the cluster
  knows.

See [Create a consumer service user](howto/create-a-consumer-user.md),
[Grant access to all topics with a prefix](howto/grant-access-by-topic-prefix.md) and
[Allow idempotent and transactional producers](howto/allow-transactional-producers.md).

## Environment variable expansion

String values can reference environment variables. They are resolved once, when the config is
loaded:

| Syntax | Variable set (even to empty) | Variable unset |
|---|---|---|
| `${VAR}` | its value | **load fails** |
| `${VAR:-default}` | its value, **even if empty** | `default` |
| `${VAR:-}` | its value | empty string |

```yaml
users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}   # required: load fails if unset
    mechanism: SCRAM-SHA-256

schema_registry:
  url: ${SCHEMA_REGISTRY_URL:-http://redpanda:8081}
  username: ${SR_USERNAME:-}               # optional: empty if unset
  password: ${SR_PASSWORD:-}

strategy: ${STRATEGY:-update}
```

- **Unset variables fail loading.** Every unresolved reference is listed in one error, with
  its config path:

  ```
  loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):
    users[0].password: ${ORDERS_SERVICE_PASSWORD}
  ```

  This is deliberate: a literal `${ORDERS_SERVICE_PASSWORD}` as a password would otherwise
  surface much later as an unrelated broker error.
- **A set but empty variable wins over the default.** Unlike in a shell, `${TOPIC_NAME:-orders}`
  with `TOPIC_NAME=""` resolves to an empty string, which then fails validation:
  `topics[0].name: is required`. Unset the variable instead of setting it to empty.
- **Only string fields are expanded.** Integers and booleans (`partitions`,
  `replication_factor`, `iterations`, `tls.enabled`) can't use `${VAR}`; the YAML parser
  rejects the text before expansion runs. Keys are not expanded either, including the property
  names under a topic's `config`.
- Names follow shell rules: a letter or `_`, then letters, digits or `_`. Anything else, such
  as `${not a var}`, is left as literal text.
- A default can't contain `}`, and there is no escape for a literal `${`.
- Expanded values are not expanded again: a variable whose value contains `${OTHER}` is used
  as is.
- Expansion happens before validation, so an expanded value must still be valid
  (`strategy: ${STRATEGY:-update}` fails if `STRATEGY=Update`).
- An empty connection field (for example `${SR_USERNAME:-}` with the variable unset) falls
  through to the matching environment variable, as described in [broker](#broker).

See [Keep passwords out of the config file](howto/manage-secrets.md).

## Validation

The config is parsed and validated before the provisioner connects to anything, so a broken
config fails in milliseconds and changes nothing. The error is logged as the `error` attribute
of the `redpanda-provisioner failed` line, and the process exits with `1`:

```json
{"time":"2026-11-15T17:58:21.066953165+01:00","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: strategy: invalid strategy \"Update\" (must be \"create\" or \"update\")"}
```

```json
{"time":"2026-11-15T17:58:21.05760169+01:00","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: acls[0].operations[0]: invalid value \"Write\""}
```

The message format is `loading configuration: validating config: <field path>: <problem>`.
Validation checks:

- required fields, and that `users[].password` is not empty,
- the allowed values of every enumerated field, case-sensitively,
- duplicate topic names, schema subjects and usernames,
- `partitions` and `replication_factor` of at least `1`, `iterations` in range,
- username and password pairs in `broker.sasl` and `schema_registry`,
- empty broker addresses and null bytes in names.

Things to know:

- **Only the first error is reported.** Fix it and run again to see the next one. (Unset
  `${VAR}` references are the exception: they are all listed together.)
- **Unknown keys are ignored.** A misspelt key such as `partitons: 6` is silently dropped and
  the topic is created with 1 partition. Compare your keys with this page when a setting has no
  effect.
- **Wrong YAML types fail parsing**, with a line number instead of a field path:

  ```
  loading configuration: parsing config YAML: yaml: unmarshal errors:
    line 3: cannot unmarshal !!str `6` into int32
  ```

- **Not checked at load time:** whether schema files exist, whether the principal of an ACL is
  well-formed, whether `replication_factor` fits the cluster, and whether the Schema Registry
  URL is set. These fail during the run, after earlier resources were already provisioned.
- There is no dry-run or validate-only mode. To check a config without a cluster, run the
  provisioner against an unreachable broker: config errors appear immediately. A valid config
  logs `Configuration loaded` and then `Redpanda not ready, retrying`; stop it with Ctrl+C.
  Checks that need the cluster (environment variable merging aside) are not covered this way.
