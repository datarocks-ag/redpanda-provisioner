# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] — 2026-04-25

> **Breaking change.** Unresolved `${VAR}` references in YAML config now
> fail config-load instead of being kept as literal strings. See the
> *Security* section below for migration guidance.

### Added

- **YAML connection config** — new top-level `broker:` and `schema_registry:`
  blocks let connection details (broker addresses, SASL credentials, TLS
  toggle, SR URL, SR HTTP basic auth) live in the YAML config. The legacy
  `REDPANDA_*` and `SCHEMA_REGISTRY_*` env vars still work as a fallback
  when a YAML field is empty, so existing env-only deployments are
  unaffected. (#26)
- **`${VAR:-default}` expansion** — string values in the config can now
  fall back to a literal default when the env var is unset; `${VAR:-}` is
  the explicit "may be empty" escape hatch. (#26)
- **Lazy Schema Registry connect** — the SR client is only dialed when
  the config actually has `schemas:` entries. A misconfigured or
  unreachable `SCHEMA_REGISTRY_URL` no longer burns the full 5-minute
  retry window when there is no schema work to do. (#24)
- **End-to-end test fixture** mirroring production posture
  (SASL/SCRAM-SHA-256 + HTTP-basic-auth Schema Registry on Redpanda v26)
  drives `Run` directly, so credential and SR-auth regressions surface
  at PR time. (#24, #26)

### Changed

- **Schema file paths** are now resolved against the directory containing
  the loaded config file, not the process working directory. A config
  mounted at `/config/config.yaml` referencing `schemas/orders.avsc`
  reads from `/config/schemas/orders.avsc` regardless of where the
  binary was started. (#25)
- **Topic config reconciliation is deterministic.** The `AlterConfig`
  batch and log output for a topic with multiple config keys are now
  ordered by sorted key, so reconciliation is reproducible across runs
  (Go map iteration is randomized). (#25)
- **ACL operations are batched.** A config entry like
  `operations: [read, describe, write]` now issues one `CreateACLs`
  round-trip instead of one per operation. (#25)
- **`broker.tls.enabled` is tri-state.** An explicit YAML `false` now
  overrides `REDPANDA_TLS_ENABLED=true`, matching the documented
  "YAML wins when set" rule. (#26)
- **SCRAM PBKDF2 iteration count** is configurable per user via
  `users[].iterations`; defaults to the RFC 5802 minimum of 4096 and is
  validated against the int32 wire-protocol ceiling. (#24)
- **`REDPANDA_TLS_ENABLED`** is now parsed via `strconv.ParseBool`, so
  `1`/`true`/`TRUE`/`yes` are all accepted; an invalid value is a
  config error rather than a silent default-to-false. (#26)

### Fixed

- **SCRAM user upserts** now send a non-zero iteration count, fixing a
  broker-side `UNACCEPTABLE_CREDENTIAL` rejection introduced when the
  default of `0` was sent on the wire. (#24)
- **Schema Registry HTTP basic auth** is now applied on every request
  (ping, register, get/set compatibility), and partial credentials
  (one of username/password set, the other empty) are rejected at the
  client boundary. (#24)
- **Stale franz-go metadata cache** no longer breaks topic idempotency:
  if the cache reports a topic missing right after a previous run
  created it, a `TopicAlreadyExists` response now triggers a cache
  purge + refetch into the update path instead of failing the run. (#23)
- **Half-set broker SASL credentials** after YAML/env merge now fail
  fast in `Run` with a config-pointing error, instead of silently
  downgrading auth in `client.connect`. (#26)
- **Whitespace-only broker addresses** in YAML are rejected at
  config-load instead of being silently dropped at runtime. (#26)
- **CI coverage badge step** was reading `coverage.out` while the test
  step writes `cover.out`; aligned the filenames so badge publishing
  no longer aborts the whole `Test` job. (#27)
- **Dead schema-type switch** that re-mapped values that
  `strings.ToUpper` had already produced — removed for clarity. (#25)

### Security

- **Fail-closed `${VAR}` expansion** is a deliberate behavior change:
  an unresolved `${VAR}` in any config field is now a load error
  pointing at the offending field, instead of being kept as a literal
  string. The previous keep-as-literal fallback was a footgun — an
  unset password would silently become the literal `"${PASSWORD}"`
  and the broker would reject it with an unrelated
  `UNACCEPTABLE_CREDENTIAL` error. Use `${VAR:-default}` (or
  `${VAR:-}` for "may be empty") to opt back into a permissive
  behavior on a per-reference basis. **Breaking change** for any
  config that relied on the old keep-as-literal semantics. (#26)
- The env-var name pattern is tightened to POSIX rules
  (`[A-Za-z_][A-Za-z0-9_]*`), so accidental matches like
  `${not a var}` are left as literals rather than expanded. (#26)

### Maintenance

- Bump dependencies and GitHub Actions across the toolchain
  (testcontainers-go, codecov-action, docker/login-action,
  docker/build-push-action, docker/metadata-action,
  docker/setup-buildx-action, actions/upload-artifact,
  goreleaser-action). (#7, #8, #9, #10, #11, #16, #17, #18, #22)

## [1.0.0] — 2026-02-20

Initial release.

[Unreleased]: https://github.com/datarocks-ag/redpanda-provisioner/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/datarocks-ag/redpanda-provisioner/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/datarocks-ag/redpanda-provisioner/releases/tag/v1.0.0
