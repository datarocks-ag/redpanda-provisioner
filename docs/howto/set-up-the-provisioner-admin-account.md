# Set up the provisioner's admin account

After this the provisioner logs in to a SASL-secured Redpanda cluster with its own account
and has the rights to create topics, users and access control lists (ACLs).

## When you need this

Your cluster requires clients to authenticate, and you want the provisioner to run against it,
for example as a step before your application starts. The provisioner needs an account of its
own, and that account needs more rights than any of the service users it creates.

## What you need to know first

- **SASL/SCRAM.** SASL (Simple Authentication and Security Layer) with SCRAM (Salted Challenge
  Response Authentication Mechanism) is how Kafka clients log in with a username and password.
  The provisioner supports `SCRAM-SHA-256` and `SCRAM-SHA-512` only. See
  [Configure Authentication](https://docs.redpanda.com/current/manage/security/authentication/)
  in the Redpanda docs.
- **Authorization and ACLs.** With SASL and authorization turned on, Redpanda checks every
  request against ACLs: rules of the form "principal may do operation on resource". The
  principal of a SCRAM user `<name>` is `User:<name>`. See
  [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/).
- **Superusers.** Redpanda's cluster property `superusers` lists usernames that bypass ACL
  checks. A superuser can do everything a provisioning run needs without any ACL of its own;
  a plain user needs the ACLs listed in step 2. [Deployment](../deployment.md) has the same
  list as a table.
- **Connection settings.** The provisioner reads its own credentials from `broker.sasl` in the
  config file, or, for every field left empty there, from the environment variables
  `REDPANDA_SASL_USERNAME`, `REDPANDA_SASL_PASSWORD` and `REDPANDA_SASL_MECHANISM` (default
  `SCRAM-SHA-256`). See [Configuration reference](../configuration.md) and
  [How it works](../concepts.md).

## Steps

### 1. Create the SCRAM user

Create the account with `rpk`, logged in as an existing superuser. Use a long random password
and store it in your secret store, not in the config file
([Keep passwords out of the config file](manage-secrets.md)):

```console
$ rpk security user create provisioner --mechanism SCRAM-SHA-256 -p '<provisioner-password>' \
    -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
Created user "provisioner".
```

The provisioner cannot create this account itself: it has to log in before it can do anything.

### 2. Give it the rights to provision

A provisioning run creates and alters topics, upserts SCRAM users and creates ACLs. You have
two ways to allow that; pick one.

**Option A: make it a superuser (simplest).** A superuser needs no ACLs, and nothing breaks
when you add a new kind of resource to the config later. The project's end-to-end test runs the
provisioner this way. The price is that the account can do everything on the cluster.

**Option B: grant it a set of ACLs (least privilege).** A plain SCRAM user with `alter` on the
cluster and `create`, `describe`, `describe_configs`, `alter_configs` and `alter` on the topics
ran the whole flow against Redpanda v26.1.6: it created a topic, added partitions, changed a
topic config, upserted a SCRAM user and created an ACL. This set is tested to work; it is not
proven to be the smallest one. You create these ACLs by hand with `rpk`, because the
provisioner can't grant rights to itself before it has them.

[Check that it works](#check-that-it-works) shows what fails with neither.

#### Option A: superuser

Read the current list first, because `rpk cluster config set superusers` replaces the whole
list. Removing an existing entry locks that account out of everything ACLs don't grant it.

```console
$ rpk cluster config get superusers -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
- admin
$ rpk cluster config set superusers '["admin","provisioner"]' \
    -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
Successfully updated configuration. New configuration version is 3.
$ rpk cluster config get superusers -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
- admin
- provisioner
```

#### Option B: ACLs

Grant `alter` on the cluster (for users and ACLs) and the topic operations on every topic the
provisioner manages. `*` stands for all topics; use `--resource-pattern-type prefixed` with a
prefix instead if the provisioner manages only some of them:

```console
$ rpk security acl create --allow-principal User:provisioner --operation alter --cluster \
    -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
PRINCIPAL         HOST  RESOURCE-TYPE  RESOURCE-NAME  RESOURCE-PATTERN-TYPE  OPERATION  PERMISSION  ERROR
User:provisioner  *     CLUSTER        kafka-cluster  LITERAL                ALTER      ALLOW
$ rpk security acl create --allow-principal User:provisioner \
    --operation create,describe,describe_configs,alter_configs,alter --topic '*' \
    -X user=<superuser> -X pass='<superuser-password>' -X sasl.mechanism=SCRAM-SHA-256
PRINCIPAL         HOST  RESOURCE-TYPE  RESOURCE-NAME  RESOURCE-PATTERN-TYPE  OPERATION         PERMISSION  ERROR
User:provisioner  *     TOPIC          *              LITERAL                ALTER             ALLOW
User:provisioner  *     TOPIC          *              LITERAL                ALTER_CONFIGS     ALLOW
User:provisioner  *     TOPIC          *              LITERAL                CREATE            ALLOW
User:provisioner  *     TOPIC          *              LITERAL                DESCRIBE          ALLOW
User:provisioner  *     TOPIC          *              LITERAL                DESCRIBE_CONFIGS  ALLOW
```

Cluster `alter` lets the account create any user and any ACL, including ACLs for itself, so
treat its password with the same care as a superuser's.

Schema Registry access is separate and was not tested with Schema Registry authorization
enabled; see [Deployment](../deployment.md) for the permissions table.

### 3. Pass the credentials to the provisioner

Either put them in the config file through environment variables:

```yaml
broker:
  addresses:
    - redpanda:9092
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}

topics:
  - name: invoices
    partitions: 3
    replication_factor: 1

users:
  - username: invoices-service
    password: ${INVOICES_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256

acls:
  - principal: "User:invoices-service"
    operations: [write, describe]
    resource_type: topic
    resource_name: invoices
    pattern: literal
    permission: allow
```

Or leave `broker` out of the file and set the provisioner's own environment variables:

```console
$ export REDPANDA_BROKERS=redpanda:9092
$ export REDPANDA_SASL_USERNAME=provisioner
$ export REDPANDA_SASL_PASSWORD='<provisioner-password>'
$ export REDPANDA_SASL_MECHANISM=SCRAM-SHA-256
```

A non-empty value in the file wins over the environment variable. The mechanism must match the
one the user was created with.

If you register schemas and Schema Registry requires HTTP basic authentication, the provisioner
needs credentials there as well (`schema_registry.username`/`password` or
`SCHEMA_REGISTRY_USERNAME`/`SCHEMA_REGISTRY_PASSWORD`). The end-to-end test uses the same
superuser account for both. TLS and Schema Registry are covered in
[Connect to a SASL- and TLS-secured cluster and Schema Registry](connect-to-a-secured-cluster.md).

## Check that it works

Run the provisioner. A run as a superuser ends with `redpanda-provisioner finished successfully`
(output trimmed to the `msg` and a few keys; captured against Redpanda v26.1.6):

```text
{"level":"INFO","msg":"Connected to Redpanda",…}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Creating topic","topic":"invoices","partitions":3,"replication_factor":1}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"invoices-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"invoices-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:invoices-service","operations":["write","describe"],…}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

With the Option B ACLs, a second run after raising `partitions` from 3 to 6 and changing
`retention.ms` on a topic `receipts` also succeeds (trimmed the same way):

```text
{"level":"INFO","msg":"Starting provisioning"}
{"level":"INFO","msg":"Increasing topic partitions","topic":"receipts","from":3,"to":6}
{"level":"INFO","msg":"Updating topic config","topic":"receipts","key":"retention.ms","current":"86400000","desired":"172800000"}
{"level":"INFO","msg":"Ensuring SCRAM user","username":"receipts-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"level":"INFO","msg":"SCRAM user ensured","username":"receipts-service"}
{"level":"INFO","msg":"Ensuring ACL","principal":"User:receipts-service","operations":["write","describe"],…}
{"level":"INFO","msg":"Provisioning complete"}
{"level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

The same run as `provisioner` *before* step 2, with no ACLs, connects fine and then fails at
the first topic (exit code 1):

```text
{"level":"INFO","msg":"Connected to Redpanda",…}
{"level":"INFO","msg":"Starting provisioning"}
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"provisioning: provisioning topic \"invoices\": listing topics: TOPIC_AUTHORIZATION_FAILED: Not authorized to access topics: [Topic authorization failed.]"}
```

With rights on every topic but no cluster `alter`, it gets past the topics and fails at the
users step:

```text
{"level":"ERROR","msg":"redpanda-provisioner failed","error":"provisioning: provisioning user \"invoices-service\": upserting SCRAM user: CLUSTER_AUTHORIZATION_FAILED: Cluster authorization failed."}
```

To try this without touching a real cluster, start a throwaway Redpanda with SASL and
authorization on. Save this as `bootstrap.yaml`; Redpanda reads it once, on the first start:

```yaml
superusers:
  - admin
enable_sasl: true
```

Then start the container, wait until `docker exec redpanda-sasl rpk cluster health` shows
`Healthy: true`, and create the `admin` user (port 34711 is an example; the advertised external
port must equal the published one):

```console
$ docker run -d --name redpanda-sasl -p 34711:29092 \
    -v "$PWD/bootstrap.yaml:/etc/redpanda/.bootstrap.yaml:ro" \
    redpandadata/redpanda:v26.1.6 redpanda start --smp 1 --memory 512M --overprovisioned \
    --mode dev-container \
    --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:29092 \
    --advertise-kafka-addr internal://localhost:9092,external://localhost:34711
$ docker exec redpanda-sasl rpk security user create admin -p '<admin-password>' --mechanism SCRAM-SHA-256
Created user "admin".
```

Point the provisioner at `localhost:34711`. Run the `rpk` commands of these how-tos inside the
container (`docker exec redpanda-sasl rpk …`). Remove the container with
`docker rm -f redpanda-sasl` when you are done.

## Pitfalls

- **Wrong password or unknown user.** The provisioner retries the connection for up to five
  minutes before it gives up; the cause is only in the `WARN` lines (trimmed):

  ```text
  {"level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"SASL_AUTHENTICATION_FAILED: SASL Authentication failed.: SASL authentication failed: security: Invalid credentials"}
  ```

- **No credentials set against a SASL listener.** Also retried; the `WARN` lines say
  `"error":"broker closed the connection immediately after a request was issued, which often happens when SASL is required but not provided: is SASL missing?"`.
- **Only the username or only the password set.** The run stops before connecting:
  `broker SASL username and password must both be set or both be empty (set broker.sasl.{username,password} or REDPANDA_SASL_{USERNAME,PASSWORD})`.
- **`superusers` overwritten.** `rpk cluster config set superusers` replaces the list; always
  include the existing entries.
- **ACLs not enforced.** On a cluster without SASL and authorization, every ACL check passes,
  so a missing ACL only shows up in production. Test access rules against a secured cluster
  such as the throwaway one above.

## Further reading

- [Configuration reference](../configuration.md) for `broker.sasl`, and
  [Deployment](../deployment.md) for the environment variables and the broker permissions.
- [Connect to a SASL- and TLS-secured cluster and Schema Registry](connect-to-a-secured-cluster.md)
- [Create a producer service user](create-a-producer-user.md), the first thing to do with the
  new account.
- Redpanda docs: [Configure Authentication](https://docs.redpanda.com/current/manage/security/authentication/)
  and [Configure Access Control Lists](https://docs.redpanda.com/current/manage/security/authorization/acl/).
- The end-to-end test that runs the provisioner as a superuser:
  [integration_e2e_test.go](../../cmd/redpanda-provisioner/integration_e2e_test.go).
