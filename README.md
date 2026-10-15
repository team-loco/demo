# demo

e2e deployment and observability tests for loco.

The test uses released Loco binaries, Helm charts, and public APIs. It creates a local OIDC identity, obtains CLI credentials through Loco's login APIs, creates the demo organization/workspace, deploys shop apps, and checks builds and observability through the APIs. The harness never seeds Loco's database or queries application/telemetry databases directly.

This harness targets shared-secret automatic cluster attachment and telemetry queries at the regional control-plane API. Its real artifact pins still point to v0.0.103, which predates those contracts. A preflight therefore exits nonzero before creating infrastructure. A compatible published Loco release is required before the complete run can be verified; this branch does not substitute local builds or invent release tags.

## Run

Install [mise](https://mise.jdx.dev/) and start Docker. The pinned Loco platform images require Linux AMD64; native Apple Silicon is blocked by [loco#716](https://github.com/team-loco/loco/issues/716). GitHub Actions uses an AMD64 runner.

```sh
mise trust
mise run setup
mise run test
mise run validate
mise run e2e
```

The runner creates its own `loco-demo` kind cluster and deletes infrastructure owned by this checkout on exit. It refuses an existing cluster or conflicting container names, and never uses the current kube context. `artifacts/` holds sanitized diagnostics and API evidence; ignored `.state/` holds runtime credentials and identifiers. The downloaded CLI runs in a container with its own credential store.

```sh
mise run e2e -- --keep
mise run down
```

`mise run up`, `mise run deploy`, and `mise run verify` expose the individual phases. `deploy` accepts service names to repeat selected deployments after bootstrap is available. The complete E2E always tests the whole matrix.

## Public interface checks

| Operation | Interface |
| --- | --- |
| Authenticate demo user | Local Dex OIDC provider, `UserService/WhoAmI` |
| Obtain CLI session | `AuthService/ApproveCLILogin` and `ExchangeCLICode` with PKCE |
| Create organization/workspace/environment | `OrgService`, `WorkspaceService`, `EnvironmentService` |
| Discover resource IDs and configure deployments | `ResourceService` and `loco deploy` |
| Verify source/prebuilt build matrix | `BuildService/ListBuilds` and `GetBuild` |
| Verify resource health | `ResourceService/GetResource` |
| Attach cluster | Agent shared secret and `kube-system` namespace UID; healthy region through `ResourceService/ListRegions` |
| Issue scoped telemetry credential | `TokenService/CreateToken`, restricted to the demo workspace |
| Verify scoped logs and Kafka consumption | `ObservabilityProxyService/QueryLogs` |
| Verify payment transaction counter increases after checkout | `ObservabilityProxyService/QueryMetrics` |
| Verify connected checkout trace across five services | `ObservabilityProxyService/QueryTraces` |
| Reject queries outside the token workspace | HTTP 403 `permission_denied` from all three telemetry methods |

The agent and control plane receive the same randomly generated operator secret. The agent reads Kubernetes identity itself; attachment does not involve user login. Dex OIDC authenticates the demo user for normal workspace and deployment APIs.

The API and private observability proxy run in one pod. Only the API port is exposed. The API forwards telemetry RPCs to its loopback proxy, which holds private reader and migrator credentials for one regional ClickHouse instance. Queries use the API-issued workspace token at the ordinary API origin; there is no worker endpoint discovery or cross-region fanout.

Kubernetes APIs install infrastructure, inspect builder-node placement, provide local port forwarding, and collect failure diagnostics. Product data and credentials come from Loco's APIs.

The regression tests reject database clients/seeding in the runner and cover API pagination, resource scopes, strict authorization errors, connected trace graphs, counter increases, release preflight, infrastructure ownership, and startup retries. Missing or unavailable APIs cannot satisfy the authorization checks.

## App matrix

| Cluster source builds | Prebuilt images |
| --- | --- |
| accounting | ad |
| cart | currency |
| checkout | email |
| payment | fraud-detection |
| product-catalog | frontend |
| quote | frontend-proxy |
| recommendation | image-provider |
| shipping | load-generator |

Every app has its own `loco.yaml` and partial. Source contexts are staged under `.state/work/<service>/source`; shared upstream build files remain available. The product-catalog image receives the upstream OTel configuration file that the original deployment mounts through a ConfigMap. Source and license are retained under `upstream/`.

The upstream demo chart supplies only Kafka, Postgres, Valkey, and feature flags. The render check rejects any extra workload. Its Collector, Jaeger, Prometheus, Grafana, OpenSearch, AI, mobile, and application deployments are disabled. Loco supplies observability.

Prebuilt deployments explicitly pass the immutable image to `loco deploy --image`; source deployments omit that flag. Callers resolve Loco-assigned resource IDs through `ResourceService` and use the corresponding private service addresses. Stable logical discovery is tracked in [loco#712](https://github.com/team-loco/loco/issues/712).

## Pins and blockers

`versions.json` pins CLI v0.0.103 with binary checksums, its source commit and platform image digests, Astronomy Shop 3.1.0, upstream chart 0.42.3 with its archive checksum, and supporting images. Loco charts come from the exact CLI release commit and their Helm dependency lockfiles; this is a release source pin, not a separately published chart version. `mise.toml`, `mise.lock`, and `uv.lock` pin tools and Python dependencies.

The next pin update must use published binaries with verified checksums and matching immutable platform image digests. It must include these contracts:

- Shared-secret attachment with cluster metadata: [loco#734](https://github.com/team-loco/loco/pull/734), with lifecycle and rotation in [loco#737](https://github.com/team-loco/loco/pull/737).
- Same-origin regional observability forwarding and typed log region filters; the proxy stays private.
- Trace querying ([loco#732](https://github.com/team-loco/loco/pull/732)) and cumulative counter querying ([loco#736](https://github.com/team-loco/loco/pull/736)).
- Explicit image deployment ([loco#733](https://github.com/team-loco/loco/pull/733)) and omitted HTTP health checks ([loco#731](https://github.com/team-loco/loco/pull/731)).
- ARM64 platform images ([loco#735](https://github.com/team-loco/loco/pull/735)) for native Apple Silicon runs.

These links identify dependencies; they do not assert that a PR is merged or its artifacts released. Updating the pins requires rerunning the full 16-service E2E. Until then, passing unit/render checks validate harness logic and manifests, not the deployed system.

Blocked or failed checks make the complete run fail. CI retains evidence; check Actions for the current result. Staging automation and the public deployment are tracked in [demo#1](https://github.com/team-loco/demo/issues/1).

## Failure reports

```sh
mise run report
```

Reports go to `team-loco/loco` with pinned versions, reproduction steps, and the failing check. Known blockers link their existing issues. Open issues with the same check title are reused.

Automatic reporting from trusted main/manual runs requires `LOCO_ISSUES_TOKEN` with Issues write access to `team-loco/loco`. PR jobs never receive it. Without that secret, CI still fails and retains evidence, but does not automatically create issues.
