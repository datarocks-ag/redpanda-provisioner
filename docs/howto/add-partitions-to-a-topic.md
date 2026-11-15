# Add partitions to a topic

After this you can raise the number of partitions of an existing topic from the config file,
and you know what that does to records with keys.

## When you need this

A topic was created with few partitions and its consumers can no longer keep up: a consumer
group can run at most one active consumer per partition, so more partitions let you run more
consumers in parallel. You want to raise the count in the config file instead of running
`rpk topic add-partitions` on each cluster.

## What you need to know first

- **Partitions.** A topic is split into partitions; each partition is an ordered log. Order is
  only guaranteed within one partition. See
  [Manage topics](https://docs.redpanda.com/current/develop/manage-topics/config-topics/) in
  the Redpanda documentation.
- **Keys decide the partition.** A record with a key goes to the partition chosen by hashing
  the key over the current number of partitions. That is what keeps all records of one key, for
  example one customer, in order. Change the number of partitions and the same key can land on a
  different partition.
- **`topics[].partitions`.** The partition count in the provisioner config. With
  `strategy: update` (the default) the provisioner raises it on existing topics; it never lowers
  it. See the [configuration reference](../configuration.md) and [How it works](../concepts.md).

## Steps

The topic `payments` is an example; use your own topic name.

### 1. Decide whether key ordering matters to you

Adding partitions does not move records that are already stored. New records with a key are
hashed over the new partition count, so a key's new records can go to a different partition
than its old ones. For a short time, a consumer can then see a key's new records before it has
finished the old ones on another partition.

This is what happened on a test topic going from 3 to 6 partitions, producing the same three
keys before and after:

| Key | Partition with 3 partitions | Partition with 6 partitions |
|---|---|---|
| `customer-17` | 1 | 4 |
| `customer-42` | 0 | 3 |
| `customer-99` | 1 | 4 |

If your consumers rely on per-key order, either let them drain the topic before you add
partitions and restart producers afterwards, or create a new topic with the new partition count
and move producers and consumers over. Records without a key are not affected in this way.

### 2. Raise `partitions`

```yaml
topics:
  - name: payments
    partitions: 6          # was 3
    replication_factor: 1
```

The value is the total number of partitions, not the number to add. Write it as a number, not
as a quoted string. The topic, or the global `strategy`, must not be `create`, or the topic is
skipped.

### 3. Run the provisioner

Run it the way you deploy it, for example as in
[Run the provisioner before your app in Docker Compose](run-in-docker-compose.md) or
[Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md). It compares the
configured count with the topic's current count and adds partitions when the configured count
is higher (log lines trimmed to the relevant keys):

```json
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Increasing topic partitions","topic":"payments","from":3,"to":6}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

## Check that it works

List the topic and check the `PARTITIONS` column:

```console
$ rpk topic list
NAME      PARTITIONS  REPLICAS
orders    3           1
payments  6           1
```

A second run with the same config logs no partition line for `payments`.

## Pitfalls

- **You cannot decrease partitions.** Neither Kafka nor Redpanda can remove partitions from a
  topic. If the config asks for fewer partitions than the topic has, the provisioner logs a
  warning, changes nothing and still exits with status 0 (trimmed):

  ```json
  {"level":"WARN","msg":"Topic has more partitions than configured (cannot decrease)","topic":"payments","current":6,"desired":4}
  {"level":"INFO","msg":"Provisioning complete"}
  ```

  `rpk topic list` still shows 6 partitions. Watch for this warning after you copy a config
  between environments; set the YAML to the real count to silence it. To get fewer partitions,
  create a new topic and copy the data, as described in
  [Manage topics](https://docs.redpanda.com/current/develop/manage-topics/config-topics/).
- **`strategy: create` topics are skipped.** Once the topic exists, the provisioner logs
  `Skipping existing topic (strategy=create)` and does not add partitions.
- **Existing records are not redistributed.** The new partitions start empty. Old records stay
  where they are until retention removes them.
- **Removing `partitions` from the YAML changes nothing.** Without the field the provisioner
  does not compare partition counts on existing topics. It only matters on creation, where a
  missing value creates the topic with 1 partition.

## Further reading

- [Change a topic's settings](change-topic-settings.md) for the other fields of an existing
  topic.
- [Configuration reference](../configuration.md) and [How it works](../concepts.md).
- [FAQ](../faq.md).
- [Manage topics](https://docs.redpanda.com/current/develop/manage-topics/config-topics/) in the
  Redpanda documentation, for how keys are assigned to partitions and why partitions cannot be
  removed.
- The code: [internal/provisioner/topics.go](../../internal/provisioner/topics.go).
