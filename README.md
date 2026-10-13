# demo

e2e deployment and observability tests for loco.

This repo runs the OpenTelemetry Astronomy Shop on Loco. Eight apps build from source on a kind worker selected for Loco build jobs; eight deploy from digest-pinned upstream images. Every app has a `loco.yaml` and deploys through Loco. Kafka, Postgres, Valkey, and feature flags come from the upstream demo Helm chart. Loco supplies the Collector, ClickHouse, and observability proxy.

## Run

Install [mise](https://mise.jdx.dev/) and start Docker. The current pinned Loco platform images require Linux AMD64; GitHub Actions runs on AMD64. Native Apple Silicon bootstrap is blocked by [loco#716](https://github.com/team-loco/loco/issues/716).

```sh
mise trust
mise run setup
mise run test
mise run validate
mise run e2e
```

The E2E command creates its own `loco-demo` cluster and deletes it on exit. It never uses your current kube context. It writes diagnostics, deployment logs, check results, and telemetry evidence to `artifacts/`. Runtime tokens and CLI credentials live in ignored `.state/`; the CLI runs in a container with its own credential store.

Keep the cluster to investigate failures:

```sh
mise run e2e -- --keep
mise run deploy -- payment
mise run verify
mise run down
```

Run the phases separately with `mise run up`, `mise run deploy`, and `mise run verify`. `up` requires a fresh cluster. `deploy` accepts service names to rerun selected deployments. The complete E2E command always deploys the whole matrix.

## Checks

- Download the released CLI and verify its checksum against `versions.json`.
- Install Loco networking, operator/build controller, core/agent, and observability Helm charts; create an isolated demo user, organization, workspace, session, and cluster registration.
- Render the upstream Helm chart and require exactly four infrastructure workloads. Its Collector, Jaeger, Prometheus, Grafana, OpenSearch, AI, mobile, and application deployments are disabled.
- Deploy each app through `loco deploy`. Require successful cluster builds for source apps and no Build records for prebuilt apps.
- Require ready pods and preserve the requested health semantics, including omitted health on gRPC services and workers.
- Exercise product browsing, cart, and checkout with a sampled trace context.
- Require frontend, checkout, payment, and shipping spans with the same trace ID and Loco workspace attribution, plus app logs and counter metrics in ClickHouse.
- Require both Kafka consumers to record the checkout order and accounting to persist it in Postgres.

Failed and blocked checks make the run fail. An infrastructure failure prevents app tests from running; it is never counted as a successful deployment run.

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

The CLI currently accepts image deployments through `image:` in `loco.yaml`. `deploy --image` is tracked in [loco#713](https://github.com/team-loco/loco/issues/713).

Each checked-in config has one service and its own partial. The harness stages source under `.state/work/<service>/source` so the shared upstream Dockerfile contexts remain valid. It copies the upstream declarative OTel config into the product-catalog image, matching the upstream chart's ConfigMap mount. Upstream application source and license are retained under `upstream/`.

Loco names Kubernetes Services `resource-<UUID>:80`. The harness creates resource identities through Loco's API, resolves caller environment addresses to those Services, then imports each resource into its partial through `loco deploy --confirm-import`. It does not create service aliases or patch Loco-owned Deployments. Stable discovery is tracked in [loco#712](https://github.com/team-loco/loco/issues/712).

OTLP exporters use Loco's Collector. Apps that require gRPC explicitly select port 4317; other apps use Loco's injected HTTP endpoint. Tracing is enabled for the test workload, and the Collector derives workspace/resource attribution from Kubernetes pod metadata.

## Pins and current blockers

`versions.json` pins CLI v0.0.102, its source commit and image digests, Astronomy Shop 3.1.0, upstream chart 0.42.3 with its archive checksum, and infrastructure images. Loco charts are installed from the exact CLI release commit with their existing Helm dependency lockfiles; anonymously readable packaged Loco charts were unavailable during initial setup. These are release-tagged source charts, not a claimed separate chart release. Tool versions and downloads are pinned in `mise.toml` and `mise.lock`; Python dependencies are pinned in `uv.lock`.

[Omitted health checks](https://github.com/team-loco/loco/issues/711) currently acquire HTTP probes in Loco's planning path. The demo deliberately leaves health absent on gRPC services and workers and fails if probes appear. It does not hide this gap by patching probes or adding an unrelated HTTP server.

The initial local run passed config validation, chart rendering, kind creation, and Cilium installation, then stopped at the ARM64 platform image pull. App runtime checks remain unverified until the AMD64 workflow runs. Check the latest Actions evidence for the current deployment result.

## Failure reports

```sh
mise run report
```

This files failed checks in `team-loco/loco` with pinned versions and reproduction steps. It links known blockers and reuses open issues with the same check title. Review the evidence before treating an integration failure as a Loco defect.

For automatic reporting from trusted main/workflow-dispatch runs, configure the repository secret `LOCO_ISSUES_TOKEN` with Issues write access to `team-loco/loco`. PR jobs never receive it. Without that secret, failures still fail CI and retain evidence, but automatic issue creation is unavailable.
