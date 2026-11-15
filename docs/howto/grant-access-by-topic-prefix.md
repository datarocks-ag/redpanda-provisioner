# Grant access to all topics with a prefix

After this one ACL entry gives a service access to every topic, or every consumer group, whose
name starts with a given prefix, including topics created later.

## When you need this

Your topics follow a naming scheme such as `orders.created`, `orders.cancelled` and
`orders.refunded`, and a service needs all of them. Listing each topic means a new ACL entry
whenever a topic is added, and the service fails on the new topic until someone remembers. The
same happens with consumer groups whose ID carries a suffix, such as `analytics-daily` and
`analytics-backfill`.

## What you need to know first

- **Pattern types.** Each ACL has a pattern type. With `pattern: literal`, `resource_name` must
  equal the resource's name. With `pattern: prefixed`, it matches every resource whose name
  starts with `resource_name`. The match is a plain string prefix, with no separator or
  wildcard logic. See
  [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/)
  in the Redpanda docs.
- **Topic and group ACLs.** Reading topics in a group needs ACLs on both resources; see
  [Create a consumer service user with a consumer group](create-a-consumer-user.md). Each can be
  literal or prefixed on its own.
- **Enforcement.** Redpanda checks ACLs only when SASL and authorization are enabled. See
  [Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md).

The names `orders.`, `analytics-` and `analytics-service` below are placeholders.

## Steps

### 1. Choose the prefix, including its separator

End the prefix with the separator your names use. `orders.` matches `orders.created` but not
`orders` or `ordersarchive`; `orders` without the dot would match all three.

### 2. Declare the prefixed ACLs

Set `pattern: prefixed` on the entries. The topics you declare here are created by this run;
topics with the prefix that exist already, or are created later, are covered too:

```yaml
broker:
  addresses:
    - redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}

topics:
  - name: orders.created
    partitions: 6
    replication_factor: 1           # use 3 on a production cluster
  - name: orders.cancelled
    partitions: 6
    replication_factor: 1

users:
  - username: analytics-service
    password: ${ANALYTICS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:analytics-service"
    operations: [read, describe]
    resource_type: topic
    resource_name: orders.          # every topic starting with "orders."
    pattern: prefixed
    permission: allow

  - principal: "User:analytics-service"
    operations: [read]
    resource_type: group
    resource_name: analytics-       # every group starting with "analytics-"
    pattern: prefixed
    permission: allow
```

The same works for producers (`operations: [write, describe]` on a topic prefix) and for
transactional IDs ([Allow idempotent and transactional producers](allow-transactional-producers.md)).

### 3. Run the provisioner

```console
$ export REDPANDA_ADMIN_USERNAME=<admin-user> REDPANDA_ADMIN_PASSWORD='<admin-password>'
$ export ANALYTICS_SERVICE_PASSWORD='<analytics-service-password>'
$ redpanda-provisioner
```

The log shows each entry with its pattern (trimmed to `msg` and its keys):

```text
{"level":"INFO","msg":"Ensuring ACL","principal":"User:analytics-service","operations":["read","describe"],"resource_type":"topic","resource_name":"orders.","pattern":"prefixed","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:analytics-service","operations":["read"],"resource_type":"group","resource_name":"analytics-","pattern":"prefixed","permission":"allow"}
```

## Check that it works

The captures ran inside a Redpanda v26.1.6 container with SASL and authorization enabled
(`docker exec <container> rpk …`). The cluster also held the topics `orders` and `payments`,
and a topic `orders.refunded` that was created by hand after the provisioner ran.

The ACLs show the pattern type `PREFIXED`:

```console
$ rpk security acl list --allow-principal User:analytics-service -X user=<admin-user> -X pass='<admin-password>' -X sasl.mechanism=SCRAM-SHA-256
PRINCIPAL               HOST  RESOURCE-TYPE  RESOURCE-NAME  RESOURCE-PATTERN-TYPE  OPERATION  PERMISSION  ERROR
User:analytics-service  *     GROUP          analytics-     PREFIXED               READ       ALLOW
User:analytics-service  *     TOPIC          orders.        PREFIXED               DESCRIBE   ALLOW
User:analytics-service  *     TOPIC          orders.        PREFIXED               READ       ALLOW
```

The user sees exactly the topics with the prefix, including the one created later, and not
`orders` or `payments`:

```console
$ rpk topic list -X user=analytics-service -X pass='<analytics-service-password>' -X sasl.mechanism=SCRAM-SHA-256
NAME              PARTITIONS  REPLICAS
orders.cancelled  6           1
orders.created    6           1
orders.refunded   1           1
```

It can read the later topic in a group that matches the group prefix:

```console
$ rpk topic consume orders.refunded -g analytics-daily -n 1 -X user=analytics-service -X pass='<analytics-service-password>' -X sasl.mechanism=SCRAM-SHA-256
{
  "topic": "orders.refunded",
  "value": "order-7 refunded",
  "timestamp": 1794762062695,
  "partition": 0,
  "offset": 0
}
```

A topic outside the prefix is refused:

```console
$ rpk topic consume payments -g analytics-daily -n 1 -X user=analytics-service -X pass='<analytics-service-password>' -X sasl.mechanism=SCRAM-SHA-256
unable to check topic existence: TOPIC_AUTHORIZATION_FAILED: Not authorized to access topics: [Topic authorization failed.]
```

So is a group outside the group prefix (`rpk` repeats the line until you press Ctrl+C;
trimmed):

```console
$ rpk topic consume orders.cancelled -g billing -n 1 -X user=analytics-service -X pass='<analytics-service-password>' -X sasl.mechanism=SCRAM-SHA-256
ERR: topic  partition 0: unable to join group session: GROUP_AUTHORIZATION_FAILED: Not authorized to access group: Group authorization failed.
…
```

## Pitfalls

- **Prefix too short.** `orders` without a separator also matches `orders-archive` or
  `orderservice-internal`. Check with `rpk topic list` as the user, as above.
- **`pattern: Prefixed` or `PREFIXED`.** Values are case-sensitive; only `literal` and
  `prefixed` load.
- **Changing `literal` to `prefixed` in the YAML.** The provisioner creates the new prefixed
  ACL and leaves the old literal one in place, because it never deletes. Remove the old one by
  hand: [Revoke an ACL or remove a user](revoke-access.md).
- **Access wider than intended.** A prefixed ACL also covers topics other teams create later
  under the same prefix. Agree on prefixes per owner before you rely on them.

## Further reading

- [Configuration reference](../configuration.md) for `acls[].pattern`.
- [Create a consumer service user with a consumer group](create-a-consumer-user.md) and
  [Create a producer service user](create-a-producer-user.md) for literal ACLs.
- Redpanda docs: [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/).
