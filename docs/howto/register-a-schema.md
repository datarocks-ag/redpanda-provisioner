# Register Avro, Protobuf or JSON schemas

After this you can register Avro, Protobuf and JSON Schema definitions in the Redpanda Schema
Registry from the provisioner config, each under the subject your serializers expect.

## When you need this

Your producers and consumers use Schema Registry serializers, and the schemas should exist
before the applications start, under version control, instead of being registered by whichever
producer starts first. This is also the case when producers run with automatic schema
registration turned off.

## What you need to know first

- **Schema Registry.** A service, built into Redpanda, that stores schemas and gives each one a
  numeric id. Serializers put that id in front of every message. See
  [Schema Registry overview](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/)
  in the Redpanda documentation.
- **Subject.** The name a schema is registered under; a subject holds a list of versions.
  Serializers choose the subject with a naming strategy. The default, `TopicNameStrategy`, uses
  `<topic>-value` for message values and `<topic>-key` for message keys. This is a convention of
  the serializers, not of the registry or this tool: the provisioner registers exactly the
  subject you write. See
  [Subject name strategy](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy).
- **Compatibility level.** A per-subject rule the registry applies when a new version is
  registered, for example `BACKWARD`. See
  [Evolve a schema safely](evolve-a-schema.md).
- **Connection.** The provisioner needs the registry URL in `schema_registry.url` or
  `SCHEMA_REGISTRY_URL`, and only connects to it when `schemas:` is not empty. See the
  [configuration reference](../configuration.md).

## Steps

The topics `orders`, `payments` and `customers` and their schemas are examples; use your own.

### 1. Put the schema files next to the config file

A relative `file:` path is resolved against the directory that contains the config file, not
against the working directory. An absolute path is used as it is. This layout keeps the schema
files in a `schemas/` directory beside `config.yaml`:

```text
config.yaml
schemas/
  orders.avsc
  payment.proto
  customer.schema.json
```

The bundled [docker-compose.yaml](../../docker-compose.yaml) follows this layout: it mounts the
config at `/config/config.yaml` and the schemas at `/config/schemas`. When you run the
provisioner in a container, mount both the same way.

`schemas/orders.avsc` (Avro), the file shipped as [schemas/orders.avsc](../../schemas/orders.avsc):

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "customer_id", "type": "string" },
    { "name": "amount_cents", "type": "long" },
    { "name": "created_at", "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

`schemas/payment.proto` (Protobuf):

```protobuf
syntax = "proto3";

package com.example.payments;

message Payment {
  string payment_id = 1;
  string order_id = 2;
  int64 amount_cents = 3;
  string currency = 4;
}
```

`schemas/customer.schema.json` (JSON Schema):

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Customer",
  "type": "object",
  "properties": {
    "customer_id": { "type": "string" },
    "email": { "type": "string" },
    "created_at": { "type": "string", "format": "date-time" }
  },
  "required": ["customer_id", "email"]
}
```

Each file must be self-contained. The provisioner sends no schema references, so every type a
schema uses must be defined in the same file. A Protobuf `import` of another file is rejected
by the registry (see [Pitfalls](#pitfalls)).

### 2. Declare the topics and the schemas

Declare the topics too, so the config creates everything the applications need. `type` is
lowercase (`avro`, `protobuf` or `json`); `compatibility` is uppercase and optional.

```yaml
topics:
  - name: orders
    partitions: 3
    replication_factor: 1
  - name: payments
    partitions: 3
    replication_factor: 1
  - name: customers
    partitions: 3
    replication_factor: 1

schemas:
  - subject: orders-value              # <topic>-value: the value schema of topic "orders"
    type: avro
    file: schemas/orders.avsc          # relative to this file's directory
    compatibility: BACKWARD
  - subject: payments-value
    type: protobuf
    file: schemas/payment.proto
    compatibility: BACKWARD
  - subject: customers-value
    type: json
    file: schemas/customer.schema.json
    compatibility: FULL
```

For a key schema, add another entry with the subject `<topic>-key`. If you leave out
`compatibility`, the subject uses the registry's global level (`BACKWARD` on a default
Redpanda).

### 3. Run the provisioner

Run it the way you deploy it, for example as in
[Run the provisioner before your app in Docker Compose](run-in-docker-compose.md) or
[Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md). Schemas are registered
after all topics, in the order of the `schemas:` list. For each one the provisioner registers
the file's content, then sets the compatibility level if you gave one and it differs (log lines
trimmed to the relevant keys):

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating topic","topic":"orders","partitions":3,"replication_factor":1}
{"level":"INFO","msg":"Creating topic","topic":"payments","partitions":3,"replication_factor":1}
{"level":"INFO","msg":"Creating topic","topic":"customers","partitions":3,"replication_factor":1}
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":1}
{"level":"INFO","msg":"Setting compatibility level","subject":"orders-value","current":"","desired":"BACKWARD"}
{"level":"INFO","msg":"Registering schema","subject":"payments-value","type":"PROTOBUF"}
{"level":"INFO","msg":"Schema registered","subject":"payments-value","id":2}
{"level":"INFO","msg":"Setting compatibility level","subject":"payments-value","current":"","desired":"BACKWARD"}
{"level":"INFO","msg":"Registering schema","subject":"customers-value","type":"JSON"}
{"level":"INFO","msg":"Schema registered","subject":"customers-value","id":3}
{"level":"INFO","msg":"Setting compatibility level","subject":"customers-value","current":"","desired":"FULL"}
{"level":"INFO","msg":"Provisioning complete"}
```

`current` is empty on the first run because the subject has no level of its own yet; it was
using the global one.

## Check that it works

List the subjects and their versions:

```console
$ rpk registry subject list
customers-value
orders-value
payments-value
$ rpk registry schema list
SUBJECT          VERSION  ID    TYPE      ERROR
customers-value  1        3     JSON
orders-value     1        1     AVRO
payments-value   1        2     PROTOBUF
```

Print a registered schema to compare it with your file, and check the compatibility levels:

```console
$ rpk registry schema get payments-value --schema-version latest --print-schema
syntax = "proto3";

package com.example.payments;

message Payment {
  string payment_id = 1;
  string order_id = 2;
  int64 amount_cents = 3;
  string currency = 4;
}

$ rpk registry compatibility-level get orders-value payments-value customers-value
SUBJECT          LEVEL     ERROR
orders-value     BACKWARD
payments-value   BACKWARD
customers-value  FULL
```

Run the provisioner again: each schema logs `Registering schema` and `Schema registered` with
the same id, and no new version is created. A compatibility level that already matches is only
logged at `LOG_LEVEL=debug`.

## Pitfalls

- **`type` is lowercase, `compatibility` is uppercase.** `type: AVRO` fails at startup with
  `loading configuration: validating config: schemas[0].type: invalid value "AVRO" (must be avro, protobuf, or json)`,
  and `compatibility: full` with
  `schemas[2].compatibility: invalid value "full" (must be BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, or NONE)`.
- **The subject must match what the serializer looks up.** The provisioner does not derive the
  subject from a topic and does not check that the topic exists. A typo such as `order-value`
  registers a subject nobody uses, and the serializer does not find your schema.
  If your serializers use `RecordNameStrategy` or `TopicRecordNameStrategy`, name the subject
  accordingly.
- **Schema references are not supported.** A Protobuf file with `import "payment.proto";`
  fails with
  `registering schema: unexpected status 422: {"error_code":422,"message":"Invalid schema subject: refunds-value, type: PROTOBUF, definition: … references: [], metadata: {nullopt}"}`
  (body trimmed). Inline the imported types into one file instead. Topics are created before
  schemas, so topics from the same run already exist when this error stops the run.
- **A missing file is only found at run time.** The config loads, topics are created, and then
  the run fails with
  `provisioning schema "<subject>": reading schema file "<absolute path>": open <absolute path>: no such file or directory`.
  The absolute path in the message shows where the provisioner looked; in a container, check
  that the schemas directory is mounted next to the config file.
- **Removing a schema from the YAML does not delete it.** The provisioner never deletes
  subjects or versions. Removing `compatibility` leaves the subject's current level in place.

## Further reading

- [Evolve a schema safely](evolve-a-schema.md) for changing a registered schema.
- [Configuration reference](../configuration.md) for the `schemas` and `schema_registry` fields.
- [How it works](../concepts.md) for the order of steps and when the Schema Registry is
  contacted.
- [Getting started](../getting-started.md) for a first run with a schema.
- [FAQ](../faq.md).
- [Schema Registry overview](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/)
  in the Redpanda documentation, and
  [Subject name strategy](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy)
  in the Confluent documentation.
- The code: [internal/provisioner/schemas.go](../../internal/provisioner/schemas.go).
