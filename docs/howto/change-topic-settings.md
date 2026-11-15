# Change a topic's settings

After this you can change a topic's settings, such as its retention time or maximum message
size, by editing the config file and running the provisioner again.

## When you need this

A topic already exists and one of its settings no longer fits: you need to keep data for three
days instead of seven, or a producer now sends messages larger than the 1 MiB default. You want
the change recorded in the config file, not typed by hand into `rpk` on each cluster.

## What you need to know first

- **Topic settings.** Each topic has properties such as `retention.ms`, `cleanup.policy` or
  `max.message.bytes`. Values you set on a topic override the cluster default. See
  [Topic properties](https://docs.redpanda.com/current/reference/properties/topic-properties/)
  in the Redpanda documentation.
- **`topics[].config`.** A map of setting name to value in the provisioner config. Values are
  strings; quote numbers (`"259200000"`) so the file reads the same way the broker reports
  them. See the [configuration reference](../configuration.md).
- **Strategy.** With `strategy: update` (the default) the provisioner updates existing topics;
  with `strategy: create` it only creates missing topics and leaves existing ones alone. See
  [How it works](../concepts.md).

## Steps

The topic `orders` is an example; use your own topic name.

### 1. Start from the topic as it is declared today

This is the config that created the topic, with a seven-day retention:

```yaml
topics:
  - name: orders
    partitions: 3
    replication_factor: 1
    config:
      retention.ms: "604800000"
      cleanup.policy: delete
      min.insync.replicas: "1"
```

Record the current values so you can compare them later. `rpk topic describe -c` lists every
setting and where its value comes from (`DYNAMIC_TOPIC_CONFIG` means set on the topic,
`DEFAULT_CONFIG` means the cluster default):

```console
$ rpk topic describe orders -c | grep -E 'KEY|retention.ms|max.message.bytes|cleanup.policy'
KEY                                   VALUE          SOURCE
cleanup.policy                        delete         DYNAMIC_TOPIC_CONFIG
delete.retention.ms                   -1             DEFAULT_CONFIG
max.message.bytes                     1048576        DEFAULT_CONFIG
retention.ms                          604800000      DYNAMIC_TOPIC_CONFIG
```

### 2. Edit the values in `config:`

Change `retention.ms` to three days and add `max.message.bytes` to allow 2 MiB messages:

```yaml
topics:
  - name: orders
    partitions: 3
    replication_factor: 1
    config:
      retention.ms: "259200000"       # was "604800000"
      cleanup.policy: delete
      min.insync.replicas: "1"
      max.message.bytes: "2097152"    # new
```

Make sure the topic, or the global `strategy`, is not set to `create`. A `create` topic is
skipped once it exists, and your change would never reach the broker.

### 3. Run the provisioner

Run it the way you deploy it, for example as in
[Run the provisioner before your app in Docker Compose](run-in-docker-compose.md) or
[Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).

For each topic, the provisioner reads the current settings, compares every key you listed in
`config:` with the broker's value as a string, and sends only the keys that differ in one
incremental alter request. Keys you did not list are not part of the request, so the broker
keeps their current values. Each changed key is logged as `Updating topic config` (log lines
trimmed to the relevant keys):

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"max.message.bytes","current":"1048576","desired":"2097152"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"min.insync.replicas","current":"","desired":"1"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"retention.ms","current":"604800000","desired":"259200000"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

The `min.insync.replicas` line appears on every run, even when nothing changed; see
[Pitfalls](#pitfalls).

## Check that it works

Describe the topic again. The changed keys show the new values with source
`DYNAMIC_TOPIC_CONFIG`:

```console
$ rpk topic describe orders -c | grep -E 'KEY|retention.ms|max.message.bytes|cleanup.policy'
KEY                                   VALUE          SOURCE
cleanup.policy                        delete         DYNAMIC_TOPIC_CONFIG
delete.retention.ms                   -1             DEFAULT_CONFIG
max.message.bytes                     2097152        DYNAMIC_TOPIC_CONFIG
retention.ms                          259200000      DYNAMIC_TOPIC_CONFIG
```

Run the provisioner a second time. Only the `min.insync.replicas` line is left (trimmed):

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Updating topic config","topic":"orders","key":"min.insync.replicas","current":"","desired":"1"}
{"level":"INFO","msg":"Provisioning complete"}
```

Settings that already match are only logged as `Topic config up to date` when `LOG_LEVEL` is
`debug`.

## Pitfalls

- **Removing a key from `config:` does not reset it.** The provisioner only sets the keys you
  list; it never deletes a topic setting. After you remove `max.message.bytes` from the YAML
  and run again, the topic still has `2097152`. To return a setting to the cluster default,
  delete it by hand and keep it out of the YAML:

  ```console
  $ rpk topic alter-config orders --delete max.message.bytes
  TOPIC   STATUS
  orders  OK
  $ rpk topic describe orders -c | grep -E 'KEY|max.message.bytes'
  KEY                                   VALUE          SOURCE
  max.message.bytes                     1048576        DEFAULT_CONFIG
  ```

  If you delete it by hand but leave it in the YAML, the next run sets it again.
- **`strategy: create` topics are skipped.** Once the topic exists, the provisioner logs
  `Skipping existing topic (strategy=create)` and changes nothing, neither settings nor
  partitions. Set the topic's `strategy` to `update`, or remove it so the global strategy
  applies.
- **`min.insync.replicas` is "updated" on every run.** Redpanda v26.1.6 reports this setting
  back with an empty value, so the comparison never matches and every run logs
  `Updating topic config ... "key":"min.insync.replicas","current":"","desired":"1"` and sets
  it again. The run still succeeds; treat the line as noise.
- **`replication_factor` is not changed on existing topics.** It is only used when the topic is
  created. Raising it in the YAML has no effect and logs nothing; `rpk topic describe <topic> -p`
  still shows the old replica list. Changing the replication factor of an existing topic is a
  separate operation in Redpanda, outside this tool.
- **A misspelt setting name fails the run.** The provisioner does not check setting names; the
  broker does. `retention.msx` fails with
  `provisioning: provisioning topic "orders": altering config for topic "orders": INVALID_CONFIG: Configuration is invalid.`
  Topics listed before the failing one are already updated; nothing after it runs.
- **A misspelt YAML key is ignored.** A typo in a field name such as `confg:` is silently
  dropped when the file is parsed, so the run succeeds without changing anything. If a change
  does not show up in `Updating topic config` lines, check the spelling.

## Further reading

- [Configuration reference](../configuration.md) for every field under `topics`.
- [How it works](../concepts.md) for strategy and what the provisioner never deletes.
- [Add partitions to a topic](add-partitions-to-a-topic.md) for the `partitions` field.
- [FAQ](../faq.md).
- [Topic properties](https://docs.redpanda.com/current/reference/properties/topic-properties/)
  and [rpk topic alter-config](https://docs.redpanda.com/current/reference/rpk/rpk-topic/rpk-topic-alter-config/)
  in the Redpanda documentation.
- The code: [internal/provisioner/topics.go](../../internal/provisioner/topics.go).
