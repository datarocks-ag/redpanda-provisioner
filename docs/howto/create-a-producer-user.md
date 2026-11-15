# Create a producer service user

After this a service has its own Redpanda login that may write to its topic and nothing else,
declared in the provisioner's config next to the topic itself.

## When you need this

A new service publishes events, say an order service that writes to `orders`. Sharing an
existing login gives it every right that login has, and a user created by hand with `rpk` is
missing in the next environment. Declaring the user and its access control lists (ACLs) in the
config makes every environment get the same login with the same rights.

## What you need to know first

- **SCRAM users.** A service logs in with SASL/SCRAM (Simple Authentication and Security Layer,
  Salted Challenge Response Authentication Mechanism): a username, a password and a mechanism,
  `SCRAM-SHA-256` or `SCRAM-SHA-512`. The provisioner creates the user if it is missing and sets
  the password again on every run. See [Configuration reference](../configuration.md).
- **ACLs.** An ACL entry allows (or denies) a principal some operations on a resource. The
  principal of the SCRAM user `<name>` is `User:<name>`; the provisioner does not add the
  `User:` prefix for you. See
  [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/)
  in the Redpanda docs.
- **What a producer needs.** Producing to a topic takes `write` on that topic; `describe` lets
  the client look up the topic's metadata. Kafka's table of which request needs which
  operation is in [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz_primitives).
- **Enforcement.** Redpanda checks ACLs only when SASL and authorization are enabled. The
  provisioner itself needs an admin account there; see
  [Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md).

The names `orders` and `orders-producer` below are placeholders for your topic and service.

## Steps

### 1. Declare the topic, the user and the ACL

Add all three to the config. The provisioner works through topics, then users, then ACLs, so
the topic and the user exist before the ACL that names them:

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
  - username: orders-producer
    password: ${ORDERS_PRODUCER_PASSWORD}
    mechanism: SCRAM-SHA-256        # required, no default

acls:
  - principal: "User:orders-producer"
    operations: [write, describe]
    resource_type: topic
    resource_name: orders
    pattern: literal                # exactly this topic
    permission: allow
```

The password comes from an environment variable, so the file can be committed. See
[Keep passwords out of the config file](manage-secrets.md).

If the service writes to several topics, add one ACL entry per topic, or one entry with
`pattern: prefixed` ([Grant access to all topics with a prefix](grant-access-by-topic-prefix.md)).

### 2. Run the provisioner

Set the variables and run it (output trimmed to `msg` and its keys):

```console
$ export REDPANDA_ADMIN_USERNAME=<admin-user> REDPANDA_ADMIN_PASSWORD='<admin-password>'
$ export ORDERS_PRODUCER_PASSWORD='<orders-producer-password>'
$ redpanda-provisioner
…
{"level":"INFO","msg":"Creating topic","topic":"orders","partitions":6,"replication_factor":1}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"orders-producer","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"orders-producer"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:orders-producer","operations":["write","describe"],"resource_type":"topic","resource_name":"orders","pattern":"literal","permission":"allow"}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

### 3. Give the service its credentials

Configure the service's Kafka client with SASL mechanism `SCRAM-SHA-256`, username
`orders-producer` and the same password. The provisioner does not hand the password to the
service; both read it from your secret store.

## Check that it works

Log in as the new user with `rpk` and produce a record. The captures below ran inside a
Redpanda v26.1.6 container with SASL and authorization enabled
(`docker exec <container> rpk …`); from another machine add `-X brokers=<broker-host>:<port>`.

The ACLs are in place:

```console
$ rpk security acl list --allow-principal User:orders-producer -X user=<admin-user> -X pass='<admin-password>' -X sasl.mechanism=SCRAM-SHA-256
PRINCIPAL             HOST  RESOURCE-TYPE  RESOURCE-NAME  RESOURCE-PATTERN-TYPE  OPERATION  PERMISSION  ERROR
User:orders-producer  *     TOPIC          orders         LITERAL                DESCRIBE   ALLOW
User:orders-producer  *     TOPIC          orders         LITERAL                WRITE      ALLOW
```

Producing to `orders` works:

```console
$ echo 'order-1' | rpk topic produce orders -X user=orders-producer -X pass='<orders-producer-password>' -X sasl.mechanism=SCRAM-SHA-256
Produced to partition 1 at offset 0 with timestamp 1794761936931.
```

Producing to another topic is refused:

```console
$ echo 'x' | rpk topic produce payments -X user=orders-producer -X pass='<orders-producer-password>' -X sasl.mechanism=SCRAM-SHA-256
unable to produce record: TOPIC_AUTHORIZATION_FAILED: Not authorized to access topics: [Topic authorization failed.]
```

Reading its own topic is refused too, because the user has no `read`. `rpk topic consume`
repeats this line for each partition until you stop it with Ctrl+C (trimmed):

```console
$ rpk topic consume orders -n 1 -X user=orders-producer -X pass='<orders-producer-password>' -X sasl.mechanism=SCRAM-SHA-256
ERR: topic orders partition 0: TOPIC_AUTHORIZATION_FAILED: Not authorized to access topics: [Topic authorization failed.]
…
```

## Pitfalls

- **Principal without `User:`.** `principal: "orders-producer"` passes config validation, but
  Redpanda rejects the ACL and the run stops:
  `provisioning ACL for "orders-producer" on topic "orders": creating ACL for "orders-producer": INVALID_REQUEST: …`.
- **Everything is allowed in development.** On a cluster without SASL and authorization, ACLs
  are not checked, so a missing ACL only shows up on the secured cluster. Test against one; the
  admin account how-to shows a throwaway container for that.
- **Typo in a key.** Unknown keys in the YAML are ignored without a warning. A misspelt
  `resource_name` makes the entry fail validation (`acls[0].resource_name: is required`); a
  misspelt optional key such as `iterations` is silently dropped.
- **Removing the ACL from the YAML doesn't revoke it.** The provisioner only adds. See
  [Revoke an ACL or remove a user](revoke-access.md).
- **Transactions or exactly-once.** Those need extra ACLs; see
  [Allow idempotent and transactional producers](allow-transactional-producers.md).

## Further reading

- [Configuration reference](../configuration.md) for `users` and `acls`.
- [Create a consumer service user with a consumer group](create-a-consumer-user.md) for the
  reading side.
- [Rotate a service user's password](rotate-a-user-password.md)
- [Getting started](../getting-started.md) for a guided first run.
- Redpanda docs: [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/).
