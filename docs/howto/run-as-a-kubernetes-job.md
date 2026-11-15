# Run the provisioner as a Kubernetes Job

After this the provisioner runs as a Kubernetes Job with its config and schemas from a
ConfigMap, its passwords from a Secret, as a non-root user, and again on every deploy.

## When you need this

Your Redpanda cluster and your applications run on Kubernetes, and you want topics, schemas,
users and ACLs (access control lists) applied as part of each deployment, the same way every
time, without anyone running `rpk` by hand.

## What you need to know first

**A one-shot run.** The provisioner runs one pass and exits: code 0 on success, 1 on any error.
That fits a Kubernetes [Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/),
which runs Pods until one succeeds. See [Deployment](../deployment.md).

**Schema paths are relative to the config file.** `file: schemas/orders.avsc` is resolved
against the directory that holds the config file. ConfigMap keys cannot contain `/`, so you map
them to paths with `items` when you mount the ConfigMap.

**Built-in retries.** Before it gives up, the provisioner itself retries the broker and the
Schema Registry for up to five minutes each. Kubernetes retries (`backoffLimit`) come on top of
that, so keep them low.

**The image runs as root unless you say otherwise.** The image is built `FROM scratch` with no
`USER`, so without a `securityContext` the process runs as UID 0. It needs no privileges and
writes no files, so any non-root UID works.

**Jobs are immutable.** You cannot change the Pod template of an existing Job, and Kubernetes
does not rerun a Job that has completed. To run the provisioner again on the next deploy, the
old Job must go and a new one must be created.

## Steps

### 1. Put the config and the schemas in a ConfigMap

Keep the passwords as `${VAR}` references; the values come from the Secret in step 2. Replace
the `<…>` hosts with your broker and Schema Registry addresses.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: redpanda-provisioner-config
data:
  config.yaml: |
    broker:
      addresses:
        - <broker-host>:9093
      sasl:
        mechanism: SCRAM-SHA-256
        username: ${REDPANDA_ADMIN_USERNAME}
        password: ${REDPANDA_ADMIN_PASSWORD}
    schema_registry:
      url: http://<schema-registry-host>:8081
      username: ${REDPANDA_ADMIN_USERNAME}
      password: ${REDPANDA_ADMIN_PASSWORD}
    topics:
      - name: orders
        partitions: 6
        replication_factor: 3
        config:
          min.insync.replicas: "2"
    schemas:
      - subject: orders-value
        type: avro
        file: schemas/orders.avsc
        compatibility: BACKWARD
    users:
      - username: orders-service
        password: ${ORDERS_SERVICE_PASSWORD}
        mechanism: SCRAM-SHA-256
    acls:
      - principal: "User:orders-service"
        operations: [write, describe]
        resource_type: topic
        resource_name: orders
        pattern: literal
        permission: allow
  orders.avsc: |
    {
      "type": "record",
      "name": "Order",
      "namespace": "com.example.orders",
      "fields": [
        { "name": "id", "type": "string" }
      ]
    }
```

With many files, generate it instead:
`kubectl create configmap redpanda-provisioner-config --from-file=config.yaml --from-file=schemas/ --dry-run=client -o yaml`.
Each schema file becomes a key named after the file, and needs its own `items` entry in step 3.

For a cluster with TLS, see
[Connect to a SASL- and TLS-secured cluster and Schema Registry](connect-to-a-secured-cluster.md).

### 2. Put the passwords in a Secret

The keys are the environment variable names the config references:

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

Create it from your secret store rather than committing it. The provisioner reads secrets only
from environment variables, not from mounted files; see
[Keep passwords out of the config file](manage-secrets.md). The admin account is described in
[Set up the provisioner's admin account](set-up-the-provisioner-admin-account.md).

### 3. Define the Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: redpanda-provisioner
spec:
  backoffLimit: 2
  activeDeadlineSeconds: 1200
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 65532
        runAsGroup: 65532
        runAsNonRoot: true
      containers:
        - name: redpanda-provisioner
          image: ghcr.io/datarocks-ag/redpanda-provisioner:2.0.0
          env:
            - name: REDPANDA_CONFIG_PATH
              value: /config/config.yaml
          envFrom:
            - secretRef:
                name: redpanda-provisioner-credentials
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: redpanda-provisioner-config
            items:
              - key: config.yaml
                path: config.yaml
              - key: orders.avsc
                path: schemas/orders.avsc
```

Why each setting is there:

- **`runAsUser` and `runAsNonRoot`.** The image has no `USER`, so `runAsNonRoot: true` alone
  would make Kubernetes refuse to start the container: it cannot prove a non-root user. Setting
  `runAsUser` gives it one. The UID is arbitrary.
- **`restartPolicy: Never`.** A failed run leaves its Pod behind, so you can read its log.
  With `OnFailure`, the container restarts inside the same Pod.
- **`backoffLimit: 2`.** Up to three Pods in total. Each one can already spend five minutes per
  target on its own retries.
- **`activeDeadlineSeconds: 1200`.** An upper bound for the whole Job, including retries, so a
  deploy that waits for the Job does not hang.
- **`ttlSecondsAfterFinished`.** Kubernetes deletes the finished Job after a day. Set it to
  what you need for reading logs.
- **`items`.** Maps the flat ConfigMap keys to `/config/config.yaml` and
  `/config/schemas/orders.avsc`, so the relative schema path resolves.
- **`REDPANDA_CONFIG_PATH`.** Without it the provisioner looks for `./config.yaml`.
- **Pinned image tag.** `2.0.0` (or `2.0` for the latest patch), not `latest`. See
  [Deployment](../deployment.md).

### 4. Rerun the Job on every deploy

Because a completed Job never runs again and its template cannot be changed, delete it before
you apply it again:

```console
$ kubectl apply -f configmap.yaml -f secret.yaml
$ kubectl delete job redpanda-provisioner --ignore-not-found
$ kubectl apply -f job.yaml
$ kubectl wait --for=condition=complete job/redpanda-provisioner --timeout=20m
```

Other ways that achieve the same:

- **Helm hook.** Annotate the Job with `helm.sh/hook: pre-install,pre-upgrade` and
  `helm.sh/hook-delete-policy: before-hook-creation`. Helm then deletes the previous Job,
  creates a new one, and waits for it before installing the rest of the release. Pre-install
  hooks run before the chart's normal resources exist, so the ConfigMap and the Secret must be
  hooks too, with a lower `helm.sh/hook-weight` than the Job. See
  [Helm chart hooks](https://helm.sh/docs/topics/charts_hooks/).
- **A new name per deploy.** Give each run a unique name, for example with a release number
  suffix (`redpanda-provisioner-<release>`), and rely on `ttlSecondsAfterFinished` to clean up.

Make the application's rollout wait for the Job, so it starts only after a successful run.

## Check that it works

The commands below are written from Kubernetes and provisioner behaviour; they were **not
captured** against a cluster.

```console
$ kubectl wait --for=condition=complete job/redpanda-provisioner --timeout=20m
job.batch/redpanda-provisioner condition met
$ kubectl logs job/redpanda-provisioner | tail -2
{"time":"…","level":"INFO","msg":"Provisioning complete"}
{"time":"…","level":"INFO","msg":"redpanda-provisioner finished successfully"}
```

The provisioner logs one JSON object per line to stdout. On failure, the last line has
`"level":"ERROR","msg":"redpanda-provisioner failed"` and the reason in `error`. When the
connection is the problem, read the `error` attribute of the
`Redpanda not ready, retrying` or `Schema Registry not ready, retrying` WARN lines instead of
waiting for the timeout. With `restartPolicy: Never`, each failed attempt keeps its own Pod:

```console
$ kubectl get pods -l job-name=redpanda-provisioner
$ kubectl logs <pod-name>
```

The image itself running as non-root with a read-only filesystem was checked with Docker
(`docker run --user 65532:65532 --read-only --cap-drop ALL …` with image `2.0.0`): the run
ended with `redpanda-provisioner finished successfully`.

## Pitfalls

- **`field is immutable` on `kubectl apply`.** You changed the Job (for example the image tag)
  while the old Job still exists. Delete it first, as in step 4.
- **Nothing happens on deploy.** The old Job is still there in state `Complete`, so applying
  the unchanged manifest is a no-op. Delete it first, as in step 4.
- **`container has runAsNonRoot and image will run as root`.** `runAsNonRoot: true` without
  `runAsUser`. Add `runAsUser`.
- **Schema file not found.** The schema key is mounted, but not under the path the config
  expects. The run fails at the schema step with
  `reading schema file "/config/schemas/orders.avsc": open /config/schemas/orders.avsc: no such file or directory`,
  after the topics are already created. Check the `items` paths.
- **Changed ConfigMap, same result.** A ConfigMap update does not trigger anything. Only a new
  Job run picks it up.
- **Job deadline shorter than the provisioner's retries.** With `activeDeadlineSeconds` below
  five minutes, Kubernetes kills the Pod before the provisioner logs its final error. The WARN
  lines are still in the log.

## Further reading

- [Deployment](../deployment.md) for environment variables, image tags, logging and exit codes.
- [Keep passwords out of the config file](manage-secrets.md).
- [Run the provisioner before your app in Docker Compose](run-in-docker-compose.md) for local
  development.
- [Rotate a service user's password](rotate-a-user-password.md).
- Kubernetes: [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/),
  [Configure a Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/),
  [ConfigMaps as volumes](https://kubernetes.io/docs/concepts/configuration/configmap/).
