# FAQ

After reading this page you can answer the questions that come up once the provisioner runs
for real. The provisioner reads a YAML config and brings Redpanda topics, Schema Registry
subjects, SCRAM users and ACLs (access control lists) in line with it on every run; it only
creates and updates, it never deletes. [How it works](concepts.md) explains a run in full, and
the [configuration reference](configuration.md) lists every field.

Log output on this page is trimmed: the `time` field is left out of every line. Captures were
made against Redpanda v26.1.6.

- [Day-to-day behaviour](#day-to-day-behaviour)
  - [Is it safe to run on every deploy?](#is-it-safe-to-run-on-every-deploy)
  - [I removed a topic, user, ACL or schema from the config. Why does it still exist?](#i-removed-a-topic-user-acl-or-schema-from-the-config-why-does-it-still-exist)
  - [I removed a key from a topic's config. Why is the old value still set?](#i-removed-a-key-from-a-topics-config-why-is-the-old-value-still-set)
  - [Why didn't changing replication_factor do anything?](#why-didnt-changing-replication_factor-do-anything)
  - [Why was my topic created with replication factor 1?](#why-was-my-topic-created-with-replication-factor-1)
  - [Why does the log say "Topic has more partitions than configured (cannot decrease)"?](#why-does-the-log-say-topic-has-more-partitions-than-configured-cannot-decrease)
  - [Does strategy: create apply to users, ACLs or schemas?](#does-strategy-create-apply-to-users-acls-or-schemas)
  - [Does editing a schema file create a new version?](#does-editing-a-schema-file-create-a-new-version)
  - [Why does every run log "Updating topic config" for the same key?](#why-does-every-run-log-updating-topic-config-for-the-same-key)
  - [Is there a dry-run?](#is-there-a-dry-run)
  - [What do the exit codes mean, and what's already applied after a failure?](#what-do-the-exit-codes-mean-and-whats-already-applied-after-a-failure)
  - [What happens if the broker isn't up yet?](#what-happens-if-the-broker-isnt-up-yet)
  - [How do I get debug logs?](#how-do-i-get-debug-logs)
- [Configuration errors](#configuration-errors)
  - [Why does loading fail with "unresolved environment variables"?](#why-does-loading-fail-with-unresolved-environment-variables)
  - [Why did my ${VAR:-default} come out empty?](#why-did-my-var-default-come-out-empty)
  - [Why "cannot unmarshal !!str … into int32"?](#why-cannot-unmarshal-str--into-int32)
  - [Why "invalid value" for a value that looks right?](#why-invalid-value-for-a-value-that-looks-right)
  - [Why "users[0].mechanism: is required"?](#why-users0mechanism-is-required)
  - [Why does a cluster ACL need a resource_name?](#why-does-a-cluster-acl-need-a-resource_name)
  - [Why did my typo or extra field not cause an error?](#why-did-my-typo-or-extra-field-not-cause-an-error)
  - [Do YAML values or environment variables win?](#do-yaml-values-or-environment-variables-win)
  - [Why "schema registry URL required when schemas are configured"?](#why-schema-registry-url-required-when-schemas-are-configured)
  - [Why "reading schema file … no such file or directory"?](#why-reading-schema-file--no-such-file-or-directory)
- [Connection errors](#connection-errors)
  - [The run fails with "connection timeout: context deadline exceeded". Where's the real error?](#the-run-fails-with-connection-timeout-context-deadline-exceeded-wheres-the-real-error)
  - [Why "schema registry returned status 401"?](#why-schema-registry-returned-status-401)
  - [Can I use a private CA or skip TLS verification?](#can-i-use-a-private-ca-or-skip-tls-verification)
  - [Does it support mTLS, PLAIN or OAUTHBEARER?](#does-it-support-mtls-plain-or-oauthbearer)
- [Schemas and access](#schemas-and-access)
  - [Why "registering schema: unexpected status 409"?](#why-registering-schema-unexpected-status-409)
  - [Can I use schema references?](#can-i-use-schema-references)
  - [Do principals need the User: prefix?](#do-principals-need-the-user-prefix)
  - [Can I deny a principal access to a topic?](#can-i-deny-a-principal-access-to-a-topic)
- [Operations](#operations)
  - [Which image tag should I use?](#which-image-tag-should-i-use)
  - [Why does my Kubernetes pod fail runAsNonRoot?](#why-does-my-kubernetes-pod-fail-runasnonroot)

## Day-to-day behaviour

### Is it safe to run on every deploy?

Yes, that's what it's built for.

Every step either compares with the current state or uses an idempotent API: topics are
created only when missing, schemas that are already registered return their existing ID,
users are upserted and creating an existing ACL changes nothing. A second run on an unchanged
config leaves the cluster as it is. It's equally safe to re-run after a failure: whatever
succeeded stays, and the next run continues from there.

The second run of the bundled `config.example.yaml` against a local Redpanda creates nothing
(trimmed to the provisioning lines; the `Updating topic config` line is explained in
[Why does every run log "Updating topic config" for the same key?](#why-does-every-run-log-updating-topic-config-for-the-same-key)):

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"min.insync.replicas","current":"","desired":"1"}
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":1}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["write","describe"],"resource_type":"topic","resource_name":"orders","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Provisioning complete"}
```

Users are the one thing written on every run: the password in the config is set again each
time, so a password changed by hand in the cluster is overwritten.

Further reading: [How it works](concepts.md)

### I removed a topic, user, ACL or schema from the config. Why does it still exist?

Because the provisioner never deletes anything.

Removing a line from the config must never be able to drop a topic with its data or lock out
a service. There is no delete code for topics, Schema Registry subjects or versions, users or
ACLs: an entry you remove is no longer managed, and stays in the cluster exactly as it was.
Taking access away or removing a user is a separate procedure, described in its own how-to.

Further reading: [Revoke an ACL or remove a user](howto/revoke-access.md)

### I removed a key from a topic's config. Why is the old value still set?

Because only the keys listed under `config` are compared and set; a key you remove is no
longer managed, and the topic keeps the last value.

The provisioner changes topic settings with an incremental update that touches only the
listed keys, so it can't tell "I removed this key" from "I never managed this key". To go back
to the broker default, set the key to that default value explicitly, or delete the setting
with `rpk`.

After removing `retention.ms: "259200000"` from the `orders` topic and running again, the
value is still there. Check with `rpk` (read-only, safe in any environment):

```console
$ rpk topic describe orders -c | grep -E '^(KEY|retention.ms) '
KEY                                   VALUE          SOURCE
retention.ms                          259200000      DYNAMIC_TOPIC_CONFIG
```

`DYNAMIC_TOPIC_CONFIG` means the value was set on the topic rather than inherited.

Further reading: [Change a topic's settings](howto/change-topic-settings.md)

### Why didn't changing replication_factor do anything?

Because `replication_factor` is only used when the topic is created; for an existing topic it
is ignored, and nothing is logged.

Changing the replication factor of an existing topic means moving partition replicas between
brokers, which the provisioner doesn't do. Use Redpanda's own tooling for that. In this run,
`replication_factor` was raised from 1 to 3 on an existing topic: the run succeeded, logged
nothing about it, and `rpk` still shows one replica:

```console
$ rpk topic describe orders -s
NAME        orders
PARTITIONS  6
REPLICAS    1
```

Further reading: [Change a topic's settings](howto/change-topic-settings.md),
[Configuration reference](configuration.md)

### Why was my topic created with replication factor 1?

Because `replication_factor` was left out, and then the provisioner asks for 1, not for the
broker's default.

The same holds for `partitions`: when it's missing, the topic is created with 1 partition. Set
both explicitly for every topic you run in production. The log line shows what was requested:

```json
{"level":"INFO","msg":"Creating topic","topic":"payments","partitions":1,"replication_factor":1}
```

```yaml
topics:
  - name: payments
    partitions: 6
    replication_factor: 3   # needs at least 3 brokers
```

The replication factor can't be raised later through the config (see
[Why didn't changing replication_factor do anything?](#why-didnt-changing-replication_factor-do-anything)),
so get it right before the first run.

Further reading: [Configuration reference](configuration.md)

### Why does the log say "Topic has more partitions than configured (cannot decrease)"?

Because the topic already has more partitions than `partitions` in the config, and Kafka
topics can't lose partitions; the provisioner warns and leaves the count as it is.

The run still succeeds. To silence the warning, set `partitions` to the current count. Raising
`partitions` above the current count does add partitions.

```json
{"level":"WARN","msg":"Topic has more partitions than configured (cannot decrease)","topic":"orders","current":6,"desired":3}
```

Further reading: [Add partitions to a topic](howto/add-partitions-to-a-topic.md)

### Does strategy: create apply to users, ACLs or schemas?

No, only to topics. Schemas are registered, users upserted and ACLs created on every run,
whatever `strategy` says.

`strategy: create` (globally or per topic) makes the provisioner create a missing topic and
skip an existing one entirely: no partition increase, no config update. In this run with
`strategy: create`, the existing topic is skipped while the user is still written:

```yaml
strategy: create
topics:
  - name: orders
    partitions: 12
users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256
```

```json
{"level":"INFO","msg":"Skipping existing topic (strategy=create)","topic":"orders"}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
```

Further reading: [How it works](concepts.md)

### Does editing a schema file create a new version?

Yes, if the schema itself changes: the provisioner registers the file's content on every run,
and the Schema Registry adds a version when the content is new.

Registering content that is already in the subject returns the existing ID and adds nothing.
A new version must pass the subject's compatibility check, or the run fails with a `409`
(see [Why "registering schema: unexpected status 409"?](#why-registering-schema-unexpected-status-409)).
After adding an optional field with a default to `schemas/orders.avsc`, the run registers
version 2:

```json
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":2}
```

Reformatting the same Avro schema onto one line returned `"id":2` again and created no new
version. List the versions with the Schema Registry API (read-only, safe in any environment):

```console
$ curl -s http://localhost:18081/subjects/orders-value/versions
[1,2]
```

Further reading: [Evolve a schema safely](howto/evolve-a-schema.md)

### Why does every run log "Updating topic config" for the same key?

Because the broker reports a different value from the one in the config, so the provisioner
sets it again on every run. On Redpanda v26.1.6 this happens for `min.insync.replicas`.

The provisioner compares each listed key, as a string, with the value the broker reports.
Redpanda accepts `min.insync.replicas` but doesn't report a value for it, so the comparison
never matches. The repeated update is harmless. Every run of the bundled
`config.example.yaml` shows:

```json
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"min.insync.replicas","current":"","desired":"1"}
```

and `rpk` lists no `min.insync.replicas` row for the topic:

```console
$ rpk topic describe orders -c | grep -E '^(KEY|min.insync)'
KEY                                   VALUE          SOURCE
```

For any other key, the cause is usually a value written differently from how the broker
reports it. Compare the `current` and `desired` fields in the log line, and write the value in
the config exactly as `current` shows it.

Further reading: [Change a topic's settings](howto/change-topic-settings.md)

### Is there a dry-run?

No. There is no dry-run or validate-only mode and no command-line flags; every run applies
changes.

You can still check that a config loads without touching a cluster: the provisioner loads and
validates the config before it connects. Run the image with no network, so it can never reach
a broker. This is safe anywhere:

```console
$ docker run --rm --network none \
    -e REDPANDA_CONFIG_PATH=/config/config.yaml \
    -e ORDERS_SERVICE_PASSWORD=check \
    -v "$PWD/config.yaml:/config/config.yaml:ro" \
    ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
```

A valid config gets as far as connecting; stop it with Ctrl-C once you see the retry warning:

```json
{"level":"INFO","msg":"Starting redpanda-provisioner","version":"v2.0.0"}
{"level":"INFO","msg":"Loading configuration","path":"/config/config.yaml"}
{"level":"INFO","msg":"Configuration loaded","topics":2,"schemas":1,"users":1,"acls":2}
{"level":"INFO","msg":"Connecting to Redpanda","brokers":["localhost:9092"]}
{"level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"unable to dial: dial tcp [::1]:9092: connect: connection refused"}
```

An invalid config ends after `Loading configuration` with exit code `1`. All unresolved
`${VAR}` references are reported together; other validation errors stop at the first one.
This check doesn't cover everything: whether schema files exist and whether a Schema Registry
URL is set are only checked after the broker connection.

Further reading: [Configuration reference](configuration.md)

### What do the exit codes mean, and what's already applied after a failure?

`0`: everything was provisioned. `1`: something failed, whether config, connection or a
broker or Schema Registry call. Everything applied before the failure stays applied.

A run works in a fixed order (topics, then schemas, then users, then ACLs) and stops at the
first error. The last log line says what failed. Here the topic was created, the schema file
was missing, and the user after it was never written:

```yaml
topics:
  - name: invoices
    partitions: 3
schemas:
  - subject: invoices-value
    type: avro
    file: schemas/invoices.avsc   # doesn't exist
users:
  - username: billing-service
    password: ${BILLING_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256
```

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating topic","topic":"invoices","partitions":3,"replication_factor":1}
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"provisioning: provisioning schema \"invoices-value\": reading schema file \"<config-dir>/schemas/invoices.avsc\": open <config-dir>/schemas/invoices.avsc: no such file or directory"}
```

(`<config-dir>` stands for the absolute directory of the config file.) The process exited with
`1`. Fix the cause and run again; the existing topic is not created twice. Docker Compose's
`service_completed_successfully` and Kubernetes Jobs rely on the exit code.

Further reading: [Deployment](deployment.md), [How it works](concepts.md)

### What happens if the broker isn't up yet?

The provisioner retries with backoff (1 s, doubling up to 30 s) for up to 5 minutes before
giving up. The Schema Registry gets its own retry of the same length when schemas are
configured.

Each attempt is logged as a warning, with the reason in `error` and `delay` in nanoseconds:

```json
{"level":"INFO","msg":"Connecting to Redpanda","brokers":["localhost:31799"]}
{"level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"unable to dial: dial tcp [::1]:31799: connect: connection refused"}
{"level":"WARN","msg":"Redpanda not ready, retrying","attempt":2,"max_retries":15,"delay":2000000000,"error":"unable to dial: dial tcp [::1]:31799: connect: connection refused"}
```

In Docker Compose, start the provisioner after Redpanda's health check passes rather than
relying on the retry.

Further reading: [Run the provisioner before your app in Docker Compose](howto/run-in-docker-compose.md)

### How do I get debug logs?

Set `LOG_LEVEL=debug`, in lower case.

The provisioner logs JSON to stdout. `LOG_LEVEL` accepts `debug`, `info`, `warn` and `error`;
any other value, including `DEBUG`, falls back to `info` without a warning. Debug adds the
"nothing to do" lines, such as `Topic config up to date` and `Compatibility level up to date`:

```console
$ LOG_LEVEL=debug REDPANDA_CONFIG_PATH=./config.yaml redpanda-provisioner
```

```json
{"level":"WARN","msg":"Topic has more partitions than configured (cannot decrease)","topic":"orders","current":6,"desired":3}
{"level":"DEBUG","msg":"Topic config up to date","topic":"orders"}
```

Further reading: [Deployment](deployment.md)

## Configuration errors

### Why does loading fail with "unresolved environment variables"?

Because the config refers to a `${VAR}` that isn't set and has no default.

Every unresolved reference is listed with its field, so you can fix them all at once:

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):\n  users[0].password: ${ORDERS_SERVICE_PASSWORD}"}
```

Set the variable, or give a non-secret field a default with `${VAR:-default}`. `${VAR:-}`
means "may be empty".

Further reading: [Keep passwords out of the config file](howto/manage-secrets.md),
[Configuration reference](configuration.md)

### Why did my ${VAR:-default} come out empty?

Because the variable is set to an empty string; the default is only used when the variable is
not set at all.

This is different from the shell, where `${VAR:-default}` also replaces an empty value. With
this config and `KAFKA_BROKER=` (set but empty) in the environment:

```yaml
broker:
  addresses:
    - ${KAFKA_BROKER:-localhost:9092}
```

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: broker.addresses[0]: must not be empty or whitespace-only"}
```

Unset the variable (`unset KAFKA_BROKER`, or remove it from the Compose `environment:` list)
instead of setting it to empty.

Further reading: [Configuration reference](configuration.md)

### Why "cannot unmarshal !!str … into int32"?

Because a number is quoted: `partitions` and `replication_factor` must be plain YAML numbers.

```yaml
topics:
  - name: orders
    partitions: "6"
```

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: parsing config YAML: yaml: unmarshal errors:\n  line 3: cannot unmarshal !!str `6` into int32"}
```

Write `partitions: 6`. This also means `${VAR}` can't be used in number fields: references are
only expanded in string fields. Topic `config` values are the opposite: they're strings, so
quote numbers there (`retention.ms: "259200000"`).

Further reading: [Configuration reference](configuration.md)

### Why "invalid value" for a value that looks right?

Because values are case-sensitive, and the case differs between fields.

Lower case: `strategy`, schema `type` (`avro`, `protobuf`, `json`), ACL `operations`,
`resource_type`, `pattern` and `permission`. Upper case: `compatibility` (`BACKWARD`, …) and
`mechanism` (`SCRAM-SHA-256`, `SCRAM-SHA-512`).

```yaml
schemas:
  - subject: orders-value
    type: AVRO
    file: schemas/orders.avsc
```

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: schemas[0].type: invalid value \"AVRO\" (must be avro, protobuf, or json)"}
```

For ACL operations the message doesn't list the allowed values:
`acls[0].operations[0]: invalid value "Write"`.

Further reading: [Configuration reference](configuration.md)

### Why "users[0].mechanism: is required"?

Because a user's SCRAM mechanism has no default; set `mechanism` on every user.

SCRAM (Salted Challenge Response Authentication Mechanism) stores a credential per mechanism,
and the client must use the same one. Pick `SCRAM-SHA-256` or `SCRAM-SHA-512` to match your
clients:

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: users[0].mechanism: is required"}
```

```yaml
users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256
```

Further reading: [Create a producer service user](howto/create-a-producer-user.md)

### Why does a cluster ACL need a resource_name?

Because validation requires `resource_name` on every ACL; for `resource_type: cluster` the
value is ignored.

Kafka has a single cluster resource, so there is nothing to name. Write `kafka-cluster`, the
name Kafka uses for it, so the entry reads clearly:

```yaml
acls:
  - principal: "User:orders-service"
    operations: [idempotent_write]
    resource_type: cluster
    resource_name: kafka-cluster   # ignored for cluster, but required
    pattern: literal
    permission: allow
```

Without it, loading fails:

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: acls[0].resource_name: is required"}
```

Further reading: [Allow idempotent and transactional producers](howto/allow-transactional-producers.md)

### Why did my typo or extra field not cause an error?

Because the YAML is read leniently: unknown and misspelt keys are ignored without a warning.

A misspelt optional field falls back to its default. Here `partitons` is ignored, and the
topic is created with 1 partition:

```yaml
topics:
  - name: payments
    partitons: 6
```

```json
{"level":"INFO","msg":"Creating topic","topic":"payments","partitions":1,"replication_factor":1}
```

Check the counts in the `Configuration loaded` line and the `Creating topic` lines after a
config change. A misspelt required field (`mechansim`) is caught, as the field then counts as
missing.

Further reading: [Configuration reference](configuration.md)

### Do YAML values or environment variables win?

A non-empty YAML value wins; the environment variables (`REDPANDA_BROKERS`,
`REDPANDA_SASL_*`, `REDPANDA_TLS_ENABLED`, `SCHEMA_REGISTRY_*`) only fill fields the YAML
leaves empty.

With `broker.addresses: [localhost:31792]` in the config and `REDPANDA_BROKERS=redpanda:9092`
in the environment, the YAML address is used:

```json
{"level":"INFO","msg":"Connecting to Redpanda","brokers":["localhost:31792"]}
```

A `${VAR:-}` that resolves to empty leaves the field empty, so the environment variable fills
it. `broker.tls.enabled` is the exception to "non-empty": an explicit `false` in YAML also
overrides `REDPANDA_TLS_ENABLED`.

Further reading: [How it works](concepts.md), [Deployment](deployment.md)

### Why "schema registry URL required when schemas are configured"?

Because the config has `schemas:` entries but neither `schema_registry.url` nor
`SCHEMA_REGISTRY_URL` is set.

The check runs after the broker connection, so a broker problem shows up first. Without
schemas, no URL is needed and the Schema Registry isn't contacted.

```json
{"level":"INFO","msg":"Connected to Redpanda","brokers":["localhost:31792"]}
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"schema registry URL required when schemas are configured (set schema_registry.url or SCHEMA_REGISTRY_URL)"}
```

```yaml
schema_registry:
  url: http://redpanda:8081
```

Further reading: [Register Avro, Protobuf or JSON schemas](howto/register-a-schema.md)

### Why "reading schema file … no such file or directory"?

Because the schema file isn't at the path shown in the error. A relative `file` is resolved
against the directory of the config file, not the working directory.

The error shows the absolute path the provisioner tried. In a container, mount the schema
directory next to the config, as the bundled `docker-compose.yaml` does:

```yaml
    environment:
      REDPANDA_CONFIG_PATH: /config/config.yaml
    volumes:
      - ./config.example.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro      # file: schemas/orders.avsc → /config/schemas/orders.avsc
```

The file is only read during provisioning, after topics are applied, so a missing file isn't
caught when the config loads. The full error is shown under
[What do the exit codes mean, and what's already applied after a failure?](#what-do-the-exit-codes-mean-and-whats-already-applied-after-a-failure).

Further reading: [Register Avro, Protobuf or JSON schemas](howto/register-a-schema.md),
[../docker-compose.yaml](../docker-compose.yaml)

## Connection errors

### The run fails with "connection timeout: context deadline exceeded". Where's the real error?

In the `error` field of the `Redpanda not ready, retrying` (or `Schema Registry not ready,
retrying`) warnings logged before it.

Every connection failure is retried for up to 5 minutes, including wrong credentials, so the
final error only says the time ran out:

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"connecting to Redpanda: connection timeout: context deadline exceeded"}
```

The warnings carry the cause. A wrong SASL password, for example:

```json
{"level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"SASL_AUTHENTICATION_FAILED: SASL Authentication failed.: SASL authentication failed: security: Invalid credentials"}
```

and a broker that isn't listening on that address:

```json
{"level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"unable to dial: dial tcp [::1]:31799: connect: connection refused"}
```

To find them in a long log: `docker compose logs redpanda-provisioner | grep 'not ready'`.

Further reading: [Connect to a SASL- and TLS-secured cluster and Schema Registry](howto/connect-to-a-secured-cluster.md)

### Why "schema registry returned status 401"?

Because the Schema Registry requires HTTP basic auth and the provisioner sent no credentials,
or wrong ones.

Set `schema_registry.username` and `schema_registry.password` (or `SCHEMA_REGISTRY_USERNAME`
and `SCHEMA_REGISTRY_PASSWORD`). The broker's SASL credentials are not reused for the Schema
Registry. `"auth":false` in the connect line shows that no credentials were configured:

```json
{"level":"INFO","msg":"Connecting to Schema Registry","url":"http://localhost:31881","auth":false}
{"level":"WARN","msg":"Schema Registry not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"schema registry returned status 401"}
```

```yaml
schema_registry:
  url: http://redpanda:8081
  username: ${SR_USERNAME}
  password: ${SR_PASSWORD}
```

Further reading: [Connect to a SASL- and TLS-secured cluster and Schema Registry](howto/connect-to-a-secured-cluster.md)

### Can I use a private CA or skip TLS verification?

No. TLS (Transport Layer Security) is on or off: when on, the server certificate must be
signed by a CA (certificate authority) in the system trust store, which in the image is the
Alpine CA bundle.

There is no setting for a CA file, for skipping verification, or for the server name. A
cluster whose certificate comes from a private CA can't be reached over TLS by the released
image. The Schema Registry is the same: an `https://` URL is verified against the system trust
store. Richer TLS settings are on the backlog.

```yaml
broker:
  tls:
    enabled: true
```

Further reading: [Connect to a SASL- and TLS-secured cluster and Schema Registry](howto/connect-to-a-secured-cluster.md),
[the TLS setting in the code](../internal/config/config.go)

### Does it support mTLS, PLAIN or OAUTHBEARER?

No. For the broker it supports SASL (Simple Authentication and Security Layer) with
`SCRAM-SHA-256` or `SCRAM-SHA-512` only, or no authentication; for the Schema Registry, HTTP
basic auth or none.

Client certificates (mTLS, mutual TLS) can't be configured, and any other mechanism is
rejected:

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: broker.sasl.mechanism: invalid value \"PLAIN\" (must be SCRAM-SHA-256 or SCRAM-SHA-512)"}
```

SASL is used when a username and password are set, and off when both are empty.

Further reading: [Set up the provisioner's admin account](howto/set-up-the-provisioner-admin-account.md)

## Schemas and access

### Why "registering schema: unexpected status 409"?

Because the Schema Registry rejected the new schema as incompatible with the subject's
earlier versions under its compatibility level.

The registry's own message follows the status and names the problem. Here a field without a
default was added under `BACKWARD` (trimmed with `…`):

```json
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"provisioning: provisioning schema \"orders-value\": registering schema: unexpected status 409: {\"error_code\":409,\"message\":\"Schema being registered is incompatible with an earlier schema for subject \\\"orders-value\\\", details: [{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field 'region' at path '/fields/5' in the new schema has no default value and is missing in the old schema', additionalInfo:'region'}, …, {compatibility: 'BACKWARD'}]\"}"}
```

Usually the fix is in the schema: here, give the new field a default. Topics provisioned
before the schema stay applied; users and ACLs after it are not written until the run
succeeds. Changing the compatibility level in the same run doesn't help, because the level is
set after registering; that change is a procedure of its own.

Further reading: [Evolve a schema safely](howto/evolve-a-schema.md)

### Can I use schema references?

No. A schema is registered with its type and content only; references to other subjects
can't be configured.

For Avro, put the referenced types inline in the same file. Each `schemas:` entry is one
self-contained file:

```yaml
schemas:
  - subject: orders-value
    type: avro
    file: schemas/orders.avsc
```

Further reading: [Register Avro, Protobuf or JSON schemas](howto/register-a-schema.md),
[the Schema Registry client](../internal/client/client.go)

### Do principals need the User: prefix?

Yes. Write `principal: "User:<username>"`; the provisioner passes the value to the broker as
it is and adds no prefix.

A principal without the prefix is accepted by validation, but the ACL then doesn't match the
SCRAM user, so the user gets no access. Quote the value, because of the colon:

```yaml
acls:
  - principal: "User:orders-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow
```

Further reading: [Create a producer service user](howto/create-a-producer-user.md)

### Can I deny a principal access to a topic?

Yes, with `permission: deny`. A deny ACL wins over any allow ACL that matches the same
operation; that is how Kafka and Redpanda authorization work, not something the provisioner
decides.

ACLs only take effect when the cluster has authorization enabled. This config lets `reporting`
read every topic starting with `orders.` except `orders.internal`:

```yaml
topics:
  - name: orders.public
    partitions: 1
  - name: orders.internal
    partitions: 1
users:
  - username: reporting
    password: ${REPORTING_PASSWORD}
    mechanism: SCRAM-SHA-256
acls:
  - principal: "User:reporting"
    operations: [read, describe]
    resource_type: topic
    resource_name: orders.
    pattern: prefixed
    permission: allow
  - principal: "User:reporting"
    operations: [read]
    resource_type: topic
    resource_name: orders.internal
    pattern: literal
    permission: deny
```

Check as the `reporting` user with `rpk` (reads one message; safe in a test cluster):

```console
$ rpk topic consume orders.public -n 1 -f '%v\n'
hello
$ rpk topic consume orders.internal -n 1 -f '%v\n'
ERR: topic orders.internal partition 0: TOPIC_AUTHORIZATION_FAILED: Not authorized to access topics: [Topic authorization failed.]
```

The deny entry is never removed by the provisioner, even when you delete it from the config.

Further reading: [Revoke an ACL or remove a user](howto/revoke-access.md),
[Kafka authorization and ACLs](https://kafka.apache.org/documentation/#security_authz)

## Operations

### Which image tag should I use?

A pinned release (`2.0.0`) or minor version (`2.0`).

Each release tag `v<version>` publishes `ghcr.io/datarocks-ag/redpanda-provisioner` as
`<version>` (`2.0.0`), `<major>.<minor>` (`2.0`), `sha-<commit>` and `latest`, for
`linux/amd64` and `linux/arm64`. `latest` moves with every release, including major ones with
breaking changes; the bundled `docker-compose.yaml` uses it for a quick start only.

```yaml
  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
```

Further reading: [Deployment](deployment.md), [../CHANGELOG.md](../CHANGELOG.md)

### Why does my Kubernetes pod fail runAsNonRoot?

Because the image sets no user, so it runs as root unless the pod says otherwise; with
`runAsNonRoot: true` alone, Kubernetes refuses to start it.

Set a numeric non-root user in the security context. The image is built `FROM scratch` with a
single static binary, so any UID works, provided it can read the mounted config and schema
files:

```yaml
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
```

The released `2.0.0` image runs normally as UID 65532:

```console
$ docker run --rm --user 65532:65532 -e REDPANDA_BROKERS=<broker> \
    -e REDPANDA_CONFIG_PATH=/config/config.yaml \
    -v "$PWD/config.yaml:/config/config.yaml:ro" \
    ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
```

```json
{"level":"INFO","msg":"Starting redpanda-provisioner","version":"v2.0.0"}
…
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

Further reading: [Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md)
