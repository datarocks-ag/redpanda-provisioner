# Evolve a schema safely

After this you can change a registered schema through the config file, know when a run creates
a new version, and get an incompatible change through when you really need one.

## When you need this

A schema registered by the provisioner needs a new field, or a field has to go. Producers and
consumers already use the current version, so the change must not break them, and you want the
Schema Registry, not a reviewer, to catch a change that would.

## What you need to know first

- **Versions.** Each subject in the Schema Registry holds a list of versions. Registering
  content the subject already has returns the existing id; registering new content adds a
  version. See [Register Avro, Protobuf or JSON schemas](register-a-schema.md) for subjects
  and the `schemas:` config.
- **Compatibility level.** The rule the registry checks a new version against before it accepts
  it. `BACKWARD` (the Redpanda default) means consumers using the new schema can read data
  written with the previous one; for Avro, a new field therefore needs a default. `NONE`
  turns the check off. See
  [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
  in the Confluent documentation, which Redpanda's registry follows.
- **Order inside one run.** For each schema the provisioner first registers the file, then
  sets the `compatibility` level. The level in your YAML therefore only takes effect from the
  next run on. See [How it works](../concepts.md).

## Steps

The subject `orders-value` and the file `schemas/orders.avsc` are examples, as in
[Register Avro, Protobuf or JSON schemas](register-a-schema.md). The config used throughout:

```yaml
topics:
  - name: orders
    partitions: 3
    replication_factor: 1

schemas:
  - subject: orders-value
    type: avro
    file: schemas/orders.avsc   # relative to this file's directory
    compatibility: BACKWARD
```

### 1. Make a compatible change to the schema file

Add a field with a default, which `BACKWARD` allows. This is `schemas/orders.avsc` after the
change:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    { "name": "order_id", "type": "string" },
    { "name": "customer_id", "type": "string" },
    { "name": "amount_cents", "type": "long" },
    { "name": "created_at", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "currency", "type": "string", "default": "EUR" }
  ]
}
```

To check a change before you run the provisioner, ask the registry directly. `rpk` reads the
file from the machine it runs on:

```console
$ rpk registry schema check-compatibility orders-value --schema schemas/orders.avsc --schema-version latest
Schema is compatible.
```

When the change would be rejected, it answers `Schema is not compatible.` followed by the
reasons, the same `details` you see in step 3.

### 2. Run the provisioner

Run it the way you deploy it, for example as in
[Run the provisioner before your app in Docker Compose](run-in-docker-compose.md) or
[Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md). The provisioner sends
the file's content on every run; because the content changed, the registry adds version 2 with
a new id (log lines trimmed to the relevant keys):

```json
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":4}
{"level":"INFO","msg":"Provisioning complete"}
```

The id is unique across the whole registry, not per subject, so it need not be 2. A second run
with the unchanged file logs the same id, 4, and adds no version.

### 3. Get an incompatible change through, if you must

An incompatible change breaks consumers that still read old data, so first make sure that is
acceptable, for example because no data has been written yet or every consumer is upgraded at
once. This change adds a field without a default:

```json
    { "name": "currency", "type": "string", "default": "EUR" },
    { "name": "channel", "type": "string" }
```

With `compatibility: BACKWARD` the registry rejects it, and the run fails (trimmed; the `oldSchema`
value in the error is shortened to `…`):

```json
{"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"provisioning: provisioning schema \"orders-value\": registering schema: unexpected status 409: {\"error_code\":409,\"message\":\"Schema being registered is incompatible with an earlier schema for subject \\\"orders-value\\\", details: [{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field 'channel' at path '/fields/5' in the new schema has no default value and is missing in the old schema', additionalInfo:'channel'}, {oldSchemaVersion: 2}, {oldSchema: '…'}, {compatibility: 'BACKWARD'}]\"}"}
```

The `details` name the rule that failed (`READER_FIELD_MISSING_DEFAULT_VALUE`), the field, and
the version it was checked against.

Changing `compatibility` to `NONE` in the same edit does not help: the provisioner registers
the schema before it sets the level, so the registry still checks against `BACKWARD` and the
run fails with the same 409. Split the change into two runs:

1. Put the schema file back to the version the registry already has (here, the one from
   step 1), change only `compatibility: NONE`, and run. The schema is unchanged, so the
   registration returns the existing id, and then the level is relaxed (trimmed):

   ```json
   {"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
   {"level":"INFO","msg":"Schema registered","subject":"orders-value","id":4}
   {"level":"INFO","msg":"Setting compatibility level","subject":"orders-value","current":"BACKWARD","desired":"NONE"}
   {"level":"INFO","msg":"Provisioning complete"}
   ```

2. Now apply the incompatible schema change and run again. The registry accepts it as a new
   version (trimmed):

   ```json
   {"level":"INFO","msg":"Registering schema","subject":"orders-value","type":"AVRO"}
   {"level":"INFO","msg":"Schema registered","subject":"orders-value","id":5}
   {"level":"INFO","msg":"Provisioning complete"}
   ```

Then set `compatibility` back to `BACKWARD` and run a third time, so later changes are checked
again (trimmed):

```json
{"level":"INFO","msg":"Schema registered","subject":"orders-value","id":5}
{"level":"INFO","msg":"Setting compatibility level","subject":"orders-value","current":"NONE","desired":"BACKWARD"}
```

## Check that it works

List the subject's versions and its compatibility level:

```console
$ rpk registry schema list orders-value
SUBJECT       VERSION  ID    TYPE  ERROR
orders-value  1        1     AVRO
orders-value  2        4     AVRO
orders-value  3        5     AVRO
$ rpk registry compatibility-level get orders-value
SUBJECT       LEVEL     ERROR
orders-value  BACKWARD
```

## Pitfalls

- **Every content change is a new version.** The provisioner registers the file on every run
  and the registry decides whether the content is new. Keep the file stable between releases;
  an unchanged file returns the existing id.
- **Subjects and versions are never deleted.** Reverting the file to an older content does not
  remove the newer version, and removing a schema from the YAML leaves the subject in place.
  Deleting subjects or versions is a manual registry operation outside this tool.
- **A failed run leaves earlier steps applied.** Topics are created before schemas, and schemas
  listed before the failing one are already registered. Users and ACLs after it are not
  provisioned until the run succeeds; fix the schema or the compatibility level and run again.
- **Removing `compatibility` from the YAML does not reset the level.** The subject keeps the
  level it has. To change it, set the new value explicitly.

## Further reading

- [Register Avro, Protobuf or JSON schemas](register-a-schema.md).
- [Configuration reference](../configuration.md) for the `schemas` fields.
- [How it works](../concepts.md) for the order of steps and what the provisioner never deletes.
- [FAQ](../faq.md).
- [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
  in the Confluent documentation,
  [Schema Registry overview](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/)
  and [rpk registry schema check-compatibility](https://docs.redpanda.com/current/reference/rpk/rpk-registry/rpk-registry-schema-check-compatibility/)
  in the Redpanda documentation.
- The code: [internal/provisioner/schemas.go](../../internal/provisioner/schemas.go).
