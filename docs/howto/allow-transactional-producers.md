# Allow idempotent and transactional producers

After this a producer service may use idempotent writes and Kafka transactions with its own
transactional IDs, and no other service can take those IDs over.

## When you need this

A service needs exactly-once processing, or writes to several topics and wants the writes to
become visible together. Its Kafka client sets a `transactional.id` and starts failing on
startup with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, although producing without transactions
works.

## What you need to know first

- **Idempotent producer.** The client gets a producer ID from the broker and numbers its
  batches, so a retried batch is not written twice. Many clients enable it by default; `rpk` does.
  Kafka's ACL table lists `IdempotentWrite` on the cluster for getting a producer ID; Redpanda
  v26.1.6 also accepted `write` on a topic instead (see
  [Check that it works](#check-that-it-works)).
- **Transactional ID.** A transactional producer has a stable `transactional.id` that the
  broker uses to fence off older instances of the same producer. It is an ACL resource of its
  own (`resource_type: transactional_id`); using one takes `write` and `describe` on it. See
  [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz_primitives)
  in the Kafka docs and
  [Transactions](https://docs.redpanda.com/current/develop/transactions/) in the Redpanda docs.
- **Cluster resource.** There is one cluster resource. The provisioner requires
  `resource_name` on every ACL entry, but ignores it for `resource_type: cluster`; Redpanda
  stores the name as `kafka-cluster` whatever you write.
- **Producer basics.** The user and its topic ACL are the same as for any producer; see
  [Create a producer service user](create-a-producer-user.md).

The names `payments`, `payments-service` and `payments-` below are placeholders.

## Steps

### 1. Agree on a transactional ID prefix

Each instance of the service needs a unique transactional ID that stays the same across
restarts, for example `payments-0`, `payments-1`. Give them a common prefix so one prefixed ACL
covers all of them.

### 2. Declare the ACLs

Next to the usual topic ACL, add one for the transactional IDs and one for idempotent writes
on the cluster:

```yaml
broker:
  addresses:
    - redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}

topics:
  - name: payments
    partitions: 6
    replication_factor: 1           # use 3 on a production cluster

users:
  - username: payments-service
    password: ${PAYMENTS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:payments-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: payments
    pattern: literal
    permission: allow

  - principal: "User:payments-service"
    operations: [write, describe]
    resource_type: transactional_id
    resource_name: payments-        # every transactional.id starting with "payments-"
    pattern: prefixed
    permission: allow

  - principal: "User:payments-service"
    operations: [idempotent_write]
    resource_type: cluster
    resource_name: kafka-cluster    # required by validation, ignored for cluster
    pattern: literal
    permission: allow
```

The cluster entry is not needed on Redpanda v26.1.6 as long as the user may write to a topic
(see below). Keep it if your clients or another Kafka-compatible cluster expect it.

### 3. Run the provisioner

```console
$ export REDPANDA_ADMIN_USERNAME=<admin-user> REDPANDA_ADMIN_PASSWORD='<admin-password>'
$ export PAYMENTS_SERVICE_PASSWORD='<payments-service-password>'
$ redpanda-provisioner
```

The ACL lines of the log (trimmed to `msg` and its keys):

```text
{"level":"INFO","msg":"Ensuring ACL","principal":"User:payments-service","operations":["write","describe"],"resource_type":"topic","resource_name":"payments","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:payments-service","operations":["write","describe"],"resource_type":"transactional_id","resource_name":"payments-","pattern":"prefixed","permission":"allow"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:payments-service","operations":["idempotent_write"],"resource_type":"cluster","resource_name":"kafka-cluster","pattern":"literal","permission":"allow"}
```

### 4. Configure the client

Set the service's `transactional.id` to a value with the prefix, for example
`payments-<instance-number>`.

## Check that it works

List the user's ACLs (captured inside a Redpanda v26.1.6 container with SASL and authorization
enabled, `docker exec <container> rpk …`):

```console
$ rpk security acl list --allow-principal User:payments-service -X user=<admin-user> -X pass='<admin-password>' -X sasl.mechanism=SCRAM-SHA-256
PRINCIPAL              HOST  RESOURCE-TYPE     RESOURCE-NAME  RESOURCE-PATTERN-TYPE  OPERATION         PERMISSION  ERROR
User:payments-service  *     CLUSTER           kafka-cluster  LITERAL                IDEMPOTENT_WRITE  ALLOW
User:payments-service  *     TOPIC             payments       LITERAL                DESCRIBE          ALLOW
User:payments-service  *     TOPIC             payments       LITERAL                WRITE             ALLOW
User:payments-service  *     TRANSACTIONAL_ID  payments-      PREFIXED               DESCRIBE          ALLOW
User:payments-service  *     TRANSACTIONAL_ID  payments-      PREFIXED               WRITE             ALLOW
```

`rpk` can't produce in a transaction, so the transactional checks were made with a small
Go program using the [franz-go](https://github.com/twmb/franz-go) client (the library the
provisioner uses), producing one record to `payments` as `payments-service`. Against Redpanda
v26.1.6 it showed:

| ACLs of `payments-service` | Idempotent produce | Transactional produce with `payments-tx-1` |
|---|---|---|
| topic `write`, `describe` only | works | fails: `TRANSACTIONAL_ID_AUTHORIZATION_FAILED: Transactional ID authorization failed.` |
| + transactional ID prefix `payments-` | works | commits |
| + cluster `idempotent_write` | works | commits |

With all three ACLs, a transactional ID outside the prefix (`billing-tx-1`) still fails with
`TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, which is what keeps other services from fencing yours.

To test from your own service, start it and look for the first committed transaction in its
logs; an authorization problem shows up while the producer initialises, before any record is
sent.

## Pitfalls

- **`TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.** The client's `transactional.id` doesn't start
  with the ACL's `resource_name`, or the entry is `literal` while the IDs carry a suffix.
- **`CLUSTER_AUTHORIZATION_FAILED` when producing.** The user may write to no topic at all, so
  it gets no producer ID. Check the topic ACL first; a consumer-only user sees exactly this
  ([Create a consumer service user with a consumer group](create-a-consumer-user.md)).
- **Empty `resource_name` on the cluster entry.** Validation stops the run with
  `acls[<n>].resource_name: is required`, although the value is ignored. Write `kafka-cluster`.

## Further reading

- [Configuration reference](../configuration.md) for `acls[].resource_type` and
  `acls[].operations`.
- [Grant access to all topics with a prefix](grant-access-by-topic-prefix.md) for how prefixed
  ACLs match.
- Kafka docs: [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz_primitives).
- Redpanda docs: [Transactions](https://docs.redpanda.com/current/develop/transactions/).
