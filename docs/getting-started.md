# Getting started

After this guide you have run the provisioner against an empty Redpanda, know what each part of
its config does, and have checked the result with `rpk`.

**The setup:** an `orders-service` publishes order events. It needs an `orders` topic, an Avro
schema for the events in the Schema Registry, its own login, and access control lists (ACLs)
that let it write to `orders` and use its consumer group. The repository ships exactly this as
[`docker-compose.yaml`](../docker-compose.yaml) and
[`config.example.yaml`](../config.example.yaml), so you write no files of your own.

You need:

- Docker with Compose v2,
- a clone of this repository (the guide runs every command from its root directory),
- host ports `19092` and `18081` free, because the Compose file publishes Redpanda's Kafka and
  Schema Registry ports there.

> **Versions:** the Compose file uses the image `ghcr.io/datarocks-ag/redpanda-provisioner:latest`
> and Redpanda `v26.1.6`. The output below was captured on 2026-11-15 with provisioner `v2.0.0`.
> All image tags are listed in [Deployment](deployment.md).

## Step 1: The stack

The Compose file has two services. `redpanda` is a single-broker Redpanda with the Schema
Registry built in. `redpanda-provisioner` waits until Redpanda reports healthy, applies the
config once, and exits:

```yaml
# docker-compose.yaml (provisioner service)
  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:latest
    depends_on:
      redpanda: { condition: service_healthy }
    environment:
      REDPANDA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      REDPANDA_CONFIG_PATH: /config/config.yaml
      LOG_LEVEL: info
      ORDERS_SERVICE_PASSWORD: orders-dev-password   # development only; use a secret elsewhere
    volumes:
      - ./config.example.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro
```

The environment tells the provisioner where the broker and the Schema Registry are, and where
the config file is mounted. `ORDERS_SERVICE_PASSWORD` is not a provisioner setting: the config
file refers to it as `${ORDERS_SERVICE_PASSWORD}`, so the password stays out of the file. The
`schemas/` directory is mounted next to the config because schema file paths are resolved
relative to the config file's directory.

This Redpanda runs without authentication. The provisioner connects without credentials, and
the users and ACLs it creates are stored but not enforced. For a cluster that enforces them,
see [Connect to a secured cluster](howto/connect-to-a-secured-cluster.md) and
[Set up the provisioner's admin account](howto/set-up-the-provisioner-admin-account.md).

## Step 2: The config, part by part

[`config.example.yaml`](../config.example.yaml) has four sections. The provisioner applies them
in this order: topics, schemas, users, ACLs.

### Topics

```yaml
topics:
  - name: orders
    partitions: 6
    replication_factor: 1         # the bundled docker-compose.yaml runs one broker; use 3 in production
    config:
      retention.ms: "259200000"
      cleanup.policy: delete
      min.insync.replicas: "1"     # keep at or below replication_factor; use 2 with replication_factor 3
      compression.type: zstd

  - name: user-state
    partitions: 6
    replication_factor: 1
    config:
      cleanup.policy: compact
      min.insync.replicas: "1"
```

`orders` keeps events for three days (`259200000` ms) and compresses them with zstd.
`user-state` is a compacted topic, which keeps the latest record per key. The replication
factor (RF) is `1` because this stack has one broker; a topic can't have more replicas than
there are brokers.

Values under `config` are strings, so quote numbers. Partitions can be increased later but
never decreased, and the replication factor of an existing topic is never changed. See
[Change a topic's settings](howto/change-topic-settings.md) and
[Add partitions to a topic](howto/add-partitions-to-a-topic.md).

### The `orders-value` schema

```yaml
schemas:
  - subject: orders-value
    type: avro
    file: schemas/orders.avsc   # relative to this file's directory
    compatibility: BACKWARD
```

A *subject* is the name a schema is registered under in the Schema Registry; by convention
`<topic>-value` holds the schema of a topic's message values. The file
[`schemas/orders.avsc`](../schemas/orders.avsc) defines an Avro record `Order` with
`order_id`, `customer_id`, `amount_cents` and `created_at`.

`compatibility: BACKWARD` tells the registry to accept a new version only if consumers that
use it can still read data written with the previous version. That is what protects the topic
once the schema starts to change; see [Evolve a schema safely](howto/evolve-a-schema.md).

### The `orders-service` user

```yaml
users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256      # required: SCRAM-SHA-256 | SCRAM-SHA-512
```

This creates a SCRAM (Salted Challenge Response Authentication Mechanism) user, the login a
service uses with SASL (Simple Authentication and Security Layer). `${ORDERS_SERVICE_PASSWORD}`
is read from the environment when the config is loaded. If the variable is not set, the run
stops before it connects to anything:

```text
loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):
  users[0].password: ${ORDERS_SERVICE_PASSWORD}
```

`mechanism` has no default and must match what the client of `orders-service` uses. See
[Keep passwords out of the config file](howto/manage-secrets.md).

### The ACLs

```yaml
acls:
  - principal: "User:orders-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow

  - principal: "User:orders-service"
    operations: [read, describe]
    resource_type: group
    resource_name: order-processors
    pattern: literal
    permission: allow
```

An ACL grants a principal some operations on one resource. The first entry lets
`orders-service` produce to `orders`. The second lets it join and commit offsets in the
consumer group `order-processors`. Reading a topic needs `read` on the topic as well, which
this config does not grant; [Create a consumer service user](howto/create-a-consumer-user.md)
shows the full set.

Write the principal with its `User:` prefix. The provisioner passes it to the broker as it is.
`pattern: literal` matches exactly the name given; `prefixed` matches every name that starts
with it (see [Grant access to all topics with a prefix](howto/grant-access-by-topic-prefix.md)).

## Step 3: Run it

Start the stack. Compose starts Redpanda, waits until its health check passes, then starts the
provisioner:

```console
$ docker compose up -d
```

The provisioner exits within a few seconds. Check that it exited with code `0`:

```console
$ docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
SERVICE                STATUS
redpanda               Up 7 seconds (healthy)
redpanda-provisioner   Exited (0) Less than a second ago
```

Then read its log:

```console
$ docker compose logs --no-log-prefix redpanda-provisioner
{"level":"INFO","msg":"Starting redpanda-provisioner","version":"v2.0.0"}
{"level":"INFO","msg":"Loading configuration","path":"/config/config.yaml"}
{"level":"INFO","msg":"Configuration loaded","topics":2,"schemas":1,"users":1,"acls":2}
{"level":"INFO","msg":"Connecting to Redpanda","brokers":["redpanda:9092"]}
{"level":"INFO","msg":"Connected to Redpanda","brokers":["redpanda:9092"]}
{"level":"INFO","msg":"Connecting to Schema Registry","url":"http://redpanda:8081","auth":false}
{"level":"INFO","msg":"Connected to Schema Registry","url":"http://redpanda:8081"}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating topic","topic":"orders","partitions":6,"replication_factor":1}
{"level":"INFO","msg":"Creating topic","topic":"user-state","partitions":6,"replication_factor":1}
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":1}
{"level":"INFO","msg":"Setting compatibility level","subject":"orders-value","current":"","desired":"BACKWARD"}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["write","describe"],"resource_type":"topic","resource_name":"orders","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["read","describe"],"resource_type":"group","resource_name":"order-processors","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

(Log lines are JSON on stdout. The `time` field is trimmed from every line here for
readability.)

The log follows the order from step 2: two topics, one schema with its compatibility level,
one user, two ACLs. The password does not appear in any line.

## Step 4: Check the result

`rpk`, Redpanda's command-line tool, is installed in the Redpanda container. Run it there with
`docker compose exec`.

The topics exist with six partitions each. `_schemas` is the Schema Registry's own topic:

```console
$ docker compose exec redpanda rpk topic list
NAME        PARTITIONS  REPLICAS
_schemas    1           1
orders      6           1
user-state  6           1
```

The three settings the config gives `orders` show up as `DYNAMIC_TOPIC_CONFIG`; everything
else keeps the broker's defaults:

```console
$ docker compose exec redpanda rpk topic describe orders
SUMMARY
=======
NAME        orders
PARTITIONS  6
REPLICAS    1

CONFIGS
=======
KEY                                   VALUE          SOURCE
cleanup.policy                        delete         DYNAMIC_TOPIC_CONFIG
compression.type                      zstd           DYNAMIC_TOPIC_CONFIG
…
retention.ms                          259200000      DYNAMIC_TOPIC_CONFIG
…
```

(Trimmed: the `DEFAULT_CONFIG` rows are left out.) `min.insync.replicas` is missing from the
list: Redpanda `v26.1.6` accepts the setting but does not report it back. Step 5 shows what
that means for later runs.

The schema is registered as version 1 with the compatibility level from the config:

```console
$ docker compose exec redpanda rpk registry subject list
orders-value

$ docker compose exec redpanda rpk registry schema get orders-value --schema-version latest
SUBJECT       VERSION  ID    TYPE
orders-value  1        1     AVRO

$ docker compose exec redpanda rpk registry compatibility-level get orders-value
SUBJECT       LEVEL     ERROR
orders-value  BACKWARD
```

The user and its ACLs exist. Each operation in the config's `operations` list becomes one ACL
row, and the host is always `*`:

```console
$ docker compose exec redpanda rpk security user list
USERNAME
orders-service

$ docker compose exec redpanda rpk security acl list
PRINCIPAL            HOST  RESOURCE-TYPE  RESOURCE-NAME     RESOURCE-PATTERN-TYPE  OPERATION  PERMISSION  ERROR
User:orders-service  *     GROUP          order-processors  LITERAL                DESCRIBE   ALLOW
User:orders-service  *     GROUP          order-processors  LITERAL                READ       ALLOW
User:orders-service  *     TOPIC          orders            LITERAL                DESCRIBE   ALLOW
User:orders-service  *     TOPIC          orders            LITERAL                WRITE      ALLOW
```

## Step 5: Run it again

Run the provisioner a second time against the same Redpanda:

```console
$ docker compose run --rm redpanda-provisioner
…
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"min.insync.replicas","current":"","desired":"1"}
{"level":"INFO","msg":"Updating topic config","topic":"user-state","key":"min.insync.replicas","current":"","desired":"1"}
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":1}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["write","describe"],"resource_type":"topic","resource_name":"orders","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-service","operations":["read","describe"],"resource_type":"group","resource_name":"order-processors","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

(Trimmed: the start-up and connection lines, the same as in step 3, are replaced by `…`, and
the `time` field is removed. Compose's own `Container … Created` lines go to stderr and are
left out.)

The run succeeds, and nothing is created twice:

- **Topics** already exist, so they are compared with the config instead of created. The
  partition counts match. The only difference the provisioner sees is `min.insync.replicas`,
  because Redpanda does not report it back (`"current":""`). It sets the value again, which
  changes nothing; expect these two lines on every run with this config.
- **The schema** has the same content, so the registry returns the existing ID `1` and no new
  version. The compatibility level already matches, so there is no "Setting compatibility
  level" line.
- **The user** is upserted: its password is set to the configured value on every run.
- **The ACLs** are requested again. The broker already has them, so `rpk security acl list`
  still shows the same four rows.

This is what lets you run the provisioner on every deploy: it brings the cluster to what the
config says instead of failing on things that already exist. It never deletes anything,
though. Removing a topic, user or ACL from the file leaves it in the cluster;
[How it works](concepts.md) explains what each run changes and what it leaves alone.

## Step 6: Clean up

Stop the stack and delete Redpanda's data volume:

```console
$ docker compose down -v
```

## Where you are now

You have:

- a stack that provisions Redpanda before anything else uses it,
- a topic with explicit partitions, replication factor and settings,
- a registered Avro schema with a compatibility rule,
- a service user whose password comes from the environment,
- ACLs that give that user only what it needs,
- a provisioning step you can run again and again.

## Next steps

- **Understand what a run does.** [How it works](concepts.md) covers the provisioning order,
  what is updated and what is never touched, `strategy: create`, retries and the precedence of
  YAML over environment variables.
- **Look up a field.** [Configuration reference](configuration.md) lists every field and its
  validation rules.
- **Start your application after the provisioner.**
  [Run the provisioner before your app in Docker Compose](howto/run-in-docker-compose.md), or
  [Run the provisioner as a Kubernetes Job](howto/run-as-a-kubernetes-job.md).
- **Secure the cluster.** [Set up the provisioner's admin account](howto/set-up-the-provisioner-admin-account.md)
  and [Connect to a secured cluster](howto/connect-to-a-secured-cluster.md).
- **Add more services.** [Create a producer service user](howto/create-a-producer-user.md) and
  [Create a consumer service user with a consumer group](howto/create-a-consumer-user.md).
- **Deploy it.** [Deployment](deployment.md) covers image tags, environment variables, broker
  permissions, logging and exit codes.
- **Questions?** See the [FAQ](faq.md).
