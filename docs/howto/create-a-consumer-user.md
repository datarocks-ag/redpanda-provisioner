# Create a consumer service user with a consumer group

After this a service has its own Redpanda login that may read one topic as a member of its own
consumer group, and can't write anywhere.

## When you need this

A service reads events another service writes, say a billing service that consumes `orders`.
It runs several instances that share the work through a consumer group and commit their
offsets. Without the right ACL on the group, the service can fetch metadata but can't join the
group, and the error looks like a topic problem at first sight.

## What you need to know first

- **Consumer groups.** Instances that use the same group ID (`group.id` in most Kafka clients)
  split a topic's partitions between them, and the group stores how far each partition has been
  read (committed offsets). The group is a resource of its own with its own ACLs. See
  [Consumer groups](https://docs.redpanda.com/current/develop/consume-data/consumer-offsets/)
  in the Redpanda docs.
- **What a consumer needs.** `read` on the topic to fetch records, `describe` on the topic for
  metadata, and `read` on the group to join it, send heartbeats and commit offsets. In Kafka's
  model `read` on a resource also allows `describe`, so `read` on the group covers looking up the
  group and fetching its committed offsets as well. Kafka's table of request → operation is in
  [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz_primitives).
- **SCRAM users and `User:` principals.** As for a producer: the user logs in with SASL/SCRAM,
  and its ACL principal is `User:<name>`. See
  [Create a producer service user](create-a-producer-user.md).
- **Enforcement.** Redpanda checks ACLs only when SASL and authorization are enabled. See
  [Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md).

The names `orders`, `billing-service` and `billing` below are placeholders.

## Steps

### 1. Pick the group ID

Use the group ID the service already configures. The ACL names it exactly, so a service that
builds its group ID at runtime (with a hostname or a version in it) needs a prefixed ACL
instead; see [Grant access to all topics with a prefix](grant-access-by-topic-prefix.md).

### 2. Declare the topic, the user and two ACLs

One ACL for the topic, one for the group:

```yaml
broker:
  addresses:
    - redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}

topics:
  - name: orders
    partitions: 6
    replication_factor: 1           # use 3 on a production cluster

users:
  - username: billing-service
    password: ${BILLING_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:billing-service"
    operations: [read, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal
    permission: allow

  - principal: "User:billing-service"
    operations: [read]
    resource_type: group
    resource_name: billing           # the service's group.id
    pattern: literal
    permission: allow
```

Declaring `orders` here is harmless if another config already does: with the default strategy
the provisioner compares it with the existing topic and changes only what differs. Keep the
two declarations identical, or keep the topic in one config only.

### 3. Run the provisioner

```console
$ export REDPANDA_ADMIN_USERNAME=<admin-user> REDPANDA_ADMIN_PASSWORD='<admin-password>'
$ export BILLING_SERVICE_PASSWORD='<billing-service-password>'
$ redpanda-provisioner
```

The run ends with these lines (trimmed to `msg` and its keys):

```text
{"level":"INFO","msg":"Ensuring SCRAM user","username":"billing-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"billing-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:billing-service","operations":["read","describe"],"resource_type":"topic","resource_name":"orders","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:billing-service","operations":["read"],"resource_type":"group","resource_name":"billing","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

## Check that it works

Consume as the new user, in its group. The captures ran inside a Redpanda v26.1.6 container
with SASL and authorization enabled (`docker exec <container> rpk …`); from another machine add
`-X brokers=<broker-host>:<port>`.

Reading with group `billing` works and commits the offset:

```console
$ rpk topic consume orders -g billing -n 1 -X user=billing-service -X pass='<billing-service-password>' -X sasl.mechanism=SCRAM-SHA-256
{
  "topic": "orders",
  "value": "order-1",
  "timestamp": 1794761936931,
  "partition": 1,
  "offset": 0
}
```

The same user with another group ID is refused. `rpk` repeats the line until you stop it with
Ctrl+C (trimmed):

```console
$ rpk topic consume orders -g reporting -n 1 -X user=billing-service -X pass='<billing-service-password>' -X sasl.mechanism=SCRAM-SHA-256
ERR: topic  partition 0: unable to join group session: GROUP_AUTHORIZATION_FAILED: Not authorized to access group: Group authorization failed.
…
```

Producing is refused as well. The error names the cluster, not the topic, because the client
first asks for a producer ID, and a user that may write to no topic at all doesn't get one:

```console
$ echo x | rpk topic produce orders -X user=billing-service -X pass='<billing-service-password>' -X sasl.mechanism=SCRAM-SHA-256
unable to produce record: CLUSTER_AUTHORIZATION_FAILED: Cluster authorization failed.
```

## Pitfalls

- **`GROUP_AUTHORIZATION_FAILED`.** The group ACL is missing or names a different group ID than
  the client uses. Compare `resource_name` with the client's `group.id`; both are
  case-sensitive.
- **`TOPIC_AUTHORIZATION_FAILED` while consuming.** The topic ACL is missing `read`, or names
  another topic.
- **Consuming without a group works but the service doesn't.** Reading specific partitions
  without a group needs only the topic ACL. A service using a group needs the group ACL too.
- **Group ACL with `describe` only.** Listing the group works, joining it doesn't; joining and
  committing need `read`.
- **Removing the ACLs from the YAML doesn't revoke them.** See
  [Revoke an ACL or remove a user](revoke-access.md).

## Further reading

- [Configuration reference](../configuration.md) for `users` and `acls`.
- [Grant access to all topics with a prefix](grant-access-by-topic-prefix.md) for groups or
  topics named by a common prefix.
- [Create a producer service user](create-a-producer-user.md) for the writing side.
- Kafka docs: [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz_primitives).
- Redpanda docs: [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/).
