# Connect to a SASL- and TLS-secured cluster and Schema Registry

After this the provisioner logs in to a Redpanda cluster that requires SASL/SCRAM and TLS, and
to a Schema Registry that requires HTTP basic authentication.

## When you need this

Your production cluster does not accept anonymous plaintext connections the way the bundled
[`docker-compose.yaml`](../../docker-compose.yaml) does. The Kafka listener requires a login,
traffic is encrypted, and the Schema Registry asks for a username and password. Without the
right settings the provisioner does not fail at once: it retries for up to five minutes per
target and then exits with a timeout.

## What you need to know first

**SASL/SCRAM.** SASL (Simple Authentication and Security Layer) is how Kafka clients log in.
The provisioner supports only the SCRAM (Salted Challenge Response Authentication Mechanism)
variants `SCRAM-SHA-256` and `SCRAM-SHA-512`. SASL is on when both a username and a password
are set, and off when both are empty. See
[Redpanda: authentication](https://docs.redpanda.com/current/manage/security/authentication/).

**TLS.** TLS (Transport Layer Security) is a single switch, `broker.tls.enabled`. When it is on,
the provisioner requires TLS 1.2 or newer and verifies the broker certificate against the system
certificate authorities (CAs). The image ships the Alpine CA bundle, so certificates from public
CAs verify.

**Schema Registry.** The provisioner talks to the Schema Registry over plain HTTP or HTTPS,
whatever the URL says, with HTTP basic auth when a username and password are set. HTTPS uses the
same system CAs. It connects only when the config has `schemas:` entries.

**Not supported.** There is no setting for any of these, and no workaround inside the
provisioner:

- a custom CA file or private CA (only the system CAs in the image are trusted),
- skipping certificate verification,
- client certificates (mTLS, mutual TLS),
- overriding the TLS server name,
- SASL mechanisms `PLAIN`, `OAUTHBEARER` or `GSSAPI` (Kerberos).

If your cluster needs one of these, the provisioner cannot connect to it today.

**YAML or environment.** Every connection setting can sit in the config file or in an
environment variable. A non-empty value in the file wins; an empty one is filled from the
environment:

| Config file | Environment variable |
|---|---|
| `broker.addresses` | `REDPANDA_BROKERS` (comma-separated) |
| `broker.sasl.mechanism` | `REDPANDA_SASL_MECHANISM` (default `SCRAM-SHA-256`) |
| `broker.sasl.username` / `password` | `REDPANDA_SASL_USERNAME` / `REDPANDA_SASL_PASSWORD` |
| `broker.tls.enabled` | `REDPANDA_TLS_ENABLED` |
| `schema_registry.url` | `SCHEMA_REGISTRY_URL` |
| `schema_registry.username` / `password` | `SCHEMA_REGISTRY_USERNAME` / `SCHEMA_REGISTRY_PASSWORD` |

`broker.tls.enabled` is the exception: an explicit `false` in the file also wins over the
environment variable. Details in [How it works](../concepts.md) and [Deployment](../deployment.md).

## Steps

### 1. Get an account that may manage the cluster

The provisioner creates topics, users and ACLs (access control lists), so its login needs those
rights: a superuser, or a user with ACLs for cluster `alter` and the topic operations it
needs. A login without cluster `alter` fails at the first user with
`upserting SCRAM user: CLUSTER_AUTHORIZATION_FAILED: Cluster authorization failed.` The exact
list is in [Deployment](../deployment.md) and
[Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md). The examples
below call it `admin`.

### 2. Configure the broker connection

Add the connection to the config file and keep the credentials in environment variables:

```yaml
broker:
  addresses:
    - <broker-host-1>:9092
    - <broker-host-2>:9092
  sasl:
    mechanism: SCRAM-SHA-256          # or SCRAM-SHA-512, matching how the admin user was created
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}
  tls:
    enabled: true
```

The mechanism is case-sensitive. Use the address and port of the listener that has SASL (and
TLS) enabled; on a cluster with several listeners, that is often not 9092.

If you prefer environment variables only, leave out the `broker:` block and set:

```text
REDPANDA_BROKERS=<broker-host-1>:9092,<broker-host-2>:9092
REDPANDA_SASL_MECHANISM=SCRAM-SHA-256
REDPANDA_SASL_USERNAME=admin
REDPANDA_SASL_PASSWORD=<admin-password>
REDPANDA_TLS_ENABLED=true
```

`REDPANDA_TLS_ENABLED` accepts `true`, `1`, `t` and `false`, `0`, `f` in the usual case variants;
`yes` stops the run with `REDPANDA_TLS_ENABLED: invalid bool "yes"`.

### 3. Configure the Schema Registry connection

Point the URL at the HTTPS endpoint and add the basic-auth credentials:

```yaml
schema_registry:
  url: https://<schema-registry-host>:8081
  username: ${SR_USERNAME}
  password: ${SR_PASSWORD}
```

On Redpanda, Schema Registry basic auth checks the same SCRAM users as the Kafka API, so the
admin account usually works for both. Keep credentials out of the URL: the URL is written to
the log as it is.

### 4. Pass the secrets

Set `REDPANDA_ADMIN_USERNAME`, `REDPANDA_ADMIN_PASSWORD`, `SR_USERNAME` and `SR_PASSWORD` from
your secret store, as described in [Keep passwords out of the config file](manage-secrets.md).

## Check that it works

A successful run logs `Connected to Redpanda` and `Connected to Schema Registry` with
`"auth":true`. This was captured against a throwaway Redpanda v26.1.6 container with SASL,
authorization and Schema Registry basic auth enabled, but **without TLS**, using the config from
the steps with `tls` left out and `http://` URLs (trimmed to the connection lines):

```text
{"time":"2026-11-15T17:59:13.327326148+01:00","level":"INFO","msg":"Connecting to Redpanda","brokers":["localhost:34192"]}
{"time":"2026-11-15T17:59:13.330356111+01:00","level":"INFO","msg":"Connected to Redpanda","brokers":["localhost:34192"]}
{"time":"2026-11-15T17:59:13.330368204+01:00","level":"INFO","msg":"Connecting to Schema Registry","url":"http://localhost:34181","auth":true}
{"time":"2026-11-15T17:59:13.331912676+01:00","level":"INFO","msg":"Connected to Schema Registry","url":"http://localhost:34181"}
…
{"time":"2026-11-15T17:59:13.543749832+01:00","level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

With TLS on, the lines are the same, with your broker addresses and the `https://` URL.

When something is wrong, the run does **not** stop at the first failure. It logs a WARN line per
attempt, waiting 1, 2, 4, 8 … up to 30 seconds between attempts, and gives up after five minutes
per target. The cause is only in the `error` attribute of the WARN lines; the final ERROR line
says only `connection timeout`. Read the first WARN line instead of waiting. (`delay` is in
nanoseconds.)

Wrong broker password:

```text
{"time":"2026-11-15T17:59:42.7960109+01:00","level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"SASL_AUTHENTICATION_FAILED: SASL Authentication failed.: SASL authentication failed: security: Invalid credentials"}
{"time":"2026-11-15T17:59:43.798575451+01:00","level":"WARN","msg":"Redpanda not ready, retrying","attempt":2,"max_retries":15,"delay":2000000000,"error":"SASL_AUTHENTICATION_FAILED: SASL Authentication failed.: SASL authentication failed: security: Invalid credentials"}
```

The same error appears when the password is right but the mechanism does not match the one the
user was created with.

No SASL credentials against a SASL listener:

```text
{"time":"2026-11-15T18:00:02.804294774+01:00","level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"broker closed the connection immediately after a request was issued, which often happens when SASL is required but not provided: is SASL missing?"}
```

TLS enabled against a listener without TLS:

```text
{"time":"2026-11-15T18:01:42.711219697+01:00","level":"WARN","msg":"Redpanda not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"broker closed the connection immediately after a dial, which often happens if the client is using TLS when the broker is not expecting it: is TLS misconfigured on the client or the broker?"}
```

Wrong or missing Schema Registry credentials:

```text
{"time":"2026-11-15T17:59:50.800890707+01:00","level":"WARN","msg":"Schema Registry not ready, retrying","attempt":1,"max_retries":15,"delay":1000000000,"error":"schema registry returned status 401"}
```

The final line, after five minutes, looks like this (written from the code, not captured):

```text
{"time":"…","level":"ERROR","msg":"redpanda-provisioner failed","error":"connecting to Redpanda: connection timeout: context deadline exceeded"}
```

## Pitfalls

- **Unsupported mechanism.** `PLAIN` and other mechanisms are rejected before any connection is
  made:
  `unsupported broker SASL mechanism "PLAIN" (must be SCRAM-SHA-256 or SCRAM-SHA-512)`.
- **Half-set credentials.** A username without a password (or the reverse), after YAML and
  environment are merged, stops the run:
  `broker SASL username and password must both be set or both be empty (set broker.sasl.{username,password} or REDPANDA_SASL_{USERNAME,PASSWORD})`.
- **TLS against a private CA.** With a certificate the system CAs don't know, the TLS handshake
  fails on every attempt, the WARN lines carry the error, and the run times out. There is no CA-file setting; this is
  not supported. (Not captured: no TLS test cluster was available.)
- **`REDPANDA_TLS_ENABLED` seems to be ignored.** An explicit `tls.enabled: false` in the
  config file wins over `REDPANDA_TLS_ENABLED=true`. Remove the YAML line to let the environment
  decide.
- **Schema Registry URL without schemas.** With no `schemas:` entries, the provisioner skips the
  Schema Registry and logs `Skipping Schema Registry connection — no schemas configured`. Wrong
  Schema Registry credentials then go unnoticed until you add the first schema.
- **Five minutes per target.** A wrong Schema Registry password is discovered only after the
  broker connection succeeded, and costs another five minutes. Set a Job deadline with that in
  mind; see [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).

## Further reading

- [Configuration reference](../configuration.md) for the `broker` and `schema_registry` fields.
- [Deployment](../deployment.md) for the environment variables and exit codes.
- [Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md).
- [Keep passwords out of the config file](manage-secrets.md).
- [Redpanda: configure TLS](https://docs.redpanda.com/current/manage/security/encryption/) and
  [Schema Registry authorization](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-authorization/).
- The connection code: [`internal/client/client.go`](../../internal/client/client.go).
