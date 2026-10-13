# demo

e2e deployment and observability tests for loco.

The test uses released Loco binaries, Helm charts, and public APIs. It creates a local OIDC identity, obtains CLI credentials through Loco's login APIs, creates the demo organization/workspace, deploys shop apps, and checks builds and observability through the APIs. The harness never seeds Loco's database or queries application/telemetry databases directly.

Fresh installation is currently blocked by [cluster enrollment](https://github.com/team-loco/loco/issues/719). The pinned release has no public API to create a cluster and issue an agent credential. The E2E stops at that boundary and exits nonzero. Application deployment and telemetry checks remain unverified; missing APIs never count as passing tests.

## Run

Install [mise](https://mise.jdx.dev/) and start Docker. The pinned Loco platform images require Linux AMD64; native Apple Silicon is blocked by [loco#716](https://github.com/team-loco/loco/issues/716). GitHub Actions uses an AMD64 runner.

```sh
mise trust
mise run setup
mise run test
mise run validate
mise run e2e
```

The runner creates its own `loco-demo` kind cluster and deletes it on exit. It never uses the current kube context. `artifacts/` holds sanitized diagnostics and API evidence; ignored `.state/` holds runtime credentials and identifiers. The downloaded CLI runs in a container with its own credential store.

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
| Find observability endpoint | `ObservabilityAccessService/GetObservabilityAccess` |
| Verify scoped logs and Kafka consumption | `ObservabilityProxyService/QueryLogs` |
| Verify payment transaction counter | `ObservabilityProxyService/QueryMetrics` |
| Verify connected traces | Blocked: [trace-query API missing](https://github.com/team-loco/loco/issues/720) |

Kubernetes APIs install infrastructure, inspect builder-node placement, provide local port forwarding, and collect failure diagnostics. Product data and credentials come from Loco's APIs.

The API-only regression tests reject database clients/seeding in the runner and cover API pagination, resource scopes, missing-API blockers, and startup retries.

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

The CLI supports prebuilt deployments through `image:` in `loco.yaml`; `deploy --image` is tracked in [loco#713](https://github.com/team-loco/loco/issues/713). Callers resolve Loco-assigned resource IDs through `ResourceService` and use the corresponding private service addresses. Stable logical discovery is tracked in [loco#712](https://github.com/team-loco/loco/issues/712).

## Pins and blockers

`versions.json` pins CLI v0.0.102 with binary checksums, its source commit and platform image digests, Astronomy Shop 3.1.0, upstream chart 0.42.3 with its archive checksum, and supporting images. Loco charts come from the exact CLI release commit and their Helm dependency lockfiles; this is a release source pin, not a separately published chart version. `mise.toml`, `mise.lock`, and `uv.lock` pin tools and Python dependencies.

Current release gaps:

- [#719](https://github.com/team-loco/loco/issues/719): fresh cluster enrollment and agent credentials have no public API.
- [#720](https://github.com/team-loco/loco/issues/720): the observability proxy exposes no trace-query API.
- [#722](https://github.com/team-loco/loco/issues/722): `QueryMetrics` queries gauges only, so it cannot retrieve the payment counter.
- [#711](https://github.com/team-loco/loco/issues/711): omitted health checks acquire HTTP checks in the planning path. The demo keeps health omitted on gRPC services/workers and verifies that intent through `GetResource`.
- [#716](https://github.com/team-loco/loco/issues/716): released platform images lack ARM64 manifests.

Blocked or failed checks make the complete run fail. CI retains evidence; check Actions for the current result. Staging automation and the public deployment are tracked in [demo#1](https://github.com/team-loco/demo/issues/1).

## Failure reports

```sh
mise run report
```

Reports go to `team-loco/loco` with pinned versions, reproduction steps, and the failing check. Known blockers link their existing issues. Open issues with the same check title are reused.

Automatic reporting from trusted main/manual runs requires `LOCO_ISSUES_TOKEN` with Issues write access to `team-loco/loco`. PR jobs never receive it. Without that secret, CI still fails and retains evidence, but does not automatically create issues.
