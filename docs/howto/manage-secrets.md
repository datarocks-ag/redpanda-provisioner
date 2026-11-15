# Keep passwords out of the config file

After this, your config file holds only `${VAR}` references, and every password reaches the
provisioner as an environment variable from Docker Compose or a Kubernetes Secret.

## When you need this

The config file describes topics, users and ACLs (access control lists). You want to commit it
to Git, but it also needs the passwords of the service users it creates, the provisioner's own
broker login, and maybe Schema Registry credentials. Those must not end up in the repository.

## What you need to know first

**`${VAR}` references.** Any string value in the config file can contain `${VAR}` or
`${VAR:-default}`. The provisioner replaces them with environment variables when it loads the
file, before it validates anything. Only string fields are expanded, so this works for
passwords, usernames, addresses and URLs, but not for numbers such as `partitions`. Full rules:
[Configuration reference](../configuration.md).

**Fail closed.** If a `${VAR}` has no default and the variable is not set, the run stops before
it connects to anything and lists every missing variable at once:

```json
{"time":"2026-11-15T17:58:59.384982496+01:00","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: unresolved environment variables (set the env var or provide a default with ${VAR:-fallback}):\n  broker.sasl.username: ${REDPANDA_ADMIN_USERNAME}\n  broker.sasl.password: ${REDPANDA_ADMIN_PASSWORD}\n  users[0].password: ${ORDERS_SERVICE_PASSWORD}"}
```

The exit code is 1, so Docker Compose and Kubernetes see the run as failed.

**Two kinds of credentials.** Passwords of the users the provisioner *creates* live under
`users[].password`; they can only come from the config file, so use `${VAR}` there. The
provisioner's *own* logins (`broker.sasl.*`, `schema_registry.username`/`password`) can come
from the config file or from fixed environment variables (`REDPANDA_SASL_USERNAME`,
`REDPANDA_SASL_PASSWORD`, `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_PASSWORD`). A non-empty
value in the file wins; an empty one is filled from the environment. See
[How it works](../concepts.md) and [Deployment](../deployment.md).

**Environment variables only.** The provisioner has no `<NAME>_FILE` convention and the image
has no shell, so it cannot read a password from a mounted file. Every secret has to arrive as
an environment variable.

## Steps

### 1. Replace every password in the config file with a reference

Use `${VAR}` without a default for anything that must be set, so a missing secret stops the
run. Use `${VAR:-}` (empty default) for optional values that may also come from the fixed
environment variables:

```yaml
broker:
  sasl:
    mechanism: SCRAM-SHA-256
    username: ${REDPANDA_ADMIN_USERNAME}
    password: ${REDPANDA_ADMIN_PASSWORD}

schema_registry:
  username: ${SR_USERNAME:-}
  password: ${SR_PASSWORD:-}

users:
  - username: orders-service
    password: ${ORDERS_SERVICE_PASSWORD}
    mechanism: SCRAM-SHA-256
```

`${SR_USERNAME:-}` resolves to an empty string when the variable is unset. The provisioner then
falls back to `SCHEMA_REGISTRY_USERNAME`, and to no authentication if that is unset too. The
same pattern is in [`config.example.yaml`](../../config.example.yaml).

### 2. Pass the variables in Docker Compose

Keep the values in an env file that is not committed, and reference it with `env_file`:

```console
$ printf 'ORDERS_SERVICE_PASSWORD=%s\n' "$(openssl rand -hex 24)" > provisioner.env
$ echo 'provisioner.env' >> .gitignore
```

```yaml
  redpanda-provisioner:
    image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
    environment:
      REDPANDA_BROKERS: redpanda:9092
      SCHEMA_REGISTRY_URL: http://redpanda:8081
      REDPANDA_CONFIG_PATH: /config/config.yaml
    env_file: ./provisioner.env
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./schemas:/config/schemas:ro
```

Compose `secrets:` do not help here: they mount files under `/run/secrets/`, and the
provisioner does not read files. The complete Compose setup is in
[Run the provisioner before your app in Docker Compose](run-in-docker-compose.md).

### 3. Or pass them from a Kubernetes Secret

Put the values in a Secret whose keys are the variable names, and load all of them with
`envFrom`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: redpanda-provisioner-credentials
type: Opaque
stringData:
  REDPANDA_ADMIN_USERNAME: admin
  REDPANDA_ADMIN_PASSWORD: <admin-password>
  ORDERS_SERVICE_PASSWORD: <orders-service-password>
```

```yaml
      containers:
        - name: redpanda-provisioner
          image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
          envFrom:
            - secretRef:
                name: redpanda-provisioner-credentials
```

To pick single keys from a Secret with other names, use `env[].valueFrom.secretKeyRef`
instead. The complete Job is in [Run the provisioner as a Kubernetes Job](run-as-a-kubernetes-job.md).

### 4. Give the application the same secret

The provisioner sets `orders-service`'s password to whatever `ORDERS_SERVICE_PASSWORD` holds,
on every run. Your application must log in with that same value, so feed it from the same env
file or Secret instead of a second copy.

## Check that it works

Run the provisioner with the variables set (in Compose: `docker compose run --rm
redpanda-provisioner`) and look for the user lines. Running it again is safe, because it only
re-sets the password to the value it already has. This output is from a throwaway Redpanda
v26.1.6 container with SASL (Simple Authentication and Security Layer) enabled, trimmed to the
relevant lines:

```text
…
{"time":"2026-11-15T17:59:13.54244361+01:00","level":"INFO","msg":"Ensuring SCRAM user","username":"orders-service","mechanism":"SCRAM-SHA-256","iterations":4096}
{"time":"2026-11-15T17:59:13.543449524+01:00","level":"INFO","msg":"SCRAM user ensured","username":"orders-service"}
…
{"time":"2026-11-15T17:59:13.543749832+01:00","level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

Then log in as the user with the password from your secret, here with `rpk` (Redpanda's CLI):

```console
$ rpk topic list -X brokers=<broker> -X user=orders-service -X pass=<orders-service-password> -X sasl.mechanism=SCRAM-SHA-256
NAME    PARTITIONS  REPLICAS
orders  6           1
```

`SASL_AUTHENTICATION_FAILED: … Invalid credentials` means the provisioner has not run since the
secret changed, or the application reads a different value.

## Pitfalls

- **Set but empty is not unset.** A variable that exists with an empty value wins over the
  default, unlike in a shell. With `password: ${ORDERS_SERVICE_PASSWORD:-orders-dev-password}`
  and `ORDERS_SERVICE_PASSWORD=` (for example an empty line in an env file), the default is
  ignored and validation rejects the empty password:

  ```json
  {"time":"2026-11-15T18:00:14.953203064+01:00","level":"ERROR","msg":"redpanda-provisioner failed","error":"loading configuration: validating config: users[0].password: is required"}
  ```

  For broker and Schema Registry credentials, an empty value falls through to the fixed
  environment variables instead, and a half-filled pair stops the run with
  `broker.sasl: username and password must both be set or both be empty` or
  `schema_registry: username and password must both be set or both be empty`.
- **No escape for `${`.** Text written in the config file that looks like `${NAME}` is always
  expanded; there is no `$${` escape. A password containing `${…}` must therefore come from an
  environment variable. Values read from environment variables are not expanded again, so
  `ORDERS_SERVICE_PASSWORD='pw-with-${HOME}-inside'` is stored exactly as written.
- **Numbers and booleans can't be referenced.** `partitions: ${PARTITIONS}` fails to parse,
  because `${…}` is a string. Only string fields take references.
- **What is logged.** No log line contains a password: the user lines carry `username`,
  `mechanism` and `iterations`, and `Connecting to Schema Registry` carries only
  `"auth":true` or `false`. Broker addresses and the Schema Registry URL *are* logged as
  written, so never put credentials into `schema_registry.url`; use the username and password
  fields.

## Further reading

- [Configuration reference](../configuration.md) for the full `${VAR}` syntax and validation.
- [Deployment](../deployment.md) for all environment variables the provisioner reads.
- [Rotate a service user's password](rotate-a-user-password.md).
- [Connect to a SASL- and TLS-secured cluster and Schema Registry](connect-to-a-secured-cluster.md).
- [FAQ](../faq.md).
