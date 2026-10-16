# Loco Demo

The Loco implementation of the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo): Astronomy Shop, deployed and observed through [Loco](https://github.com/team-loco/loco).

Astronomy Shop is a microservices application where users browse products, add items to a cart, and place orders. This repository brings 16 of its services to Loco, with a `loco.yaml` for each service and an end-to-end test that exercises deployment, checkout, and telemetry through Loco's public APIs.

Use it to see how a multi-service application fits together on Loco, explore its deployment configuration, or test Loco against a complete application.

## What Loco provides

Loco builds and deploys the application services, connects them within a workspace, and collects their logs, metrics, and distributed traces. The demo exercises both source builds on cluster workers and deployments of prebuilt images.

The application code and instrumentation come from OpenTelemetry's Astronomy Shop. Kafka, Postgres, Valkey, and feature flags come from the upstream demo chart. Loco supplies the application deployment and observability infrastructure; the upstream chart's application workloads and telemetry backends are disabled.

The test runs a local Loco installation alongside the shop. It creates a demo user and workspace, deploys the services, generates shopping traffic, and checks the results through the same APIs available to Loco clients. It does not seed Loco's database or query application or telemetry databases directly.

## Run locally

Install [mise](https://mise.jdx.dev/) and start Docker, then run:

```sh
git clone https://github.com/team-loco/demo.git
cd demo
mise trust
mise run setup
mise run test
mise run validate
mise run e2e
```

`setup` installs pinned tools and Python dependencies and prepares the upstream sources and Loco release artifacts. `test` runs the runner's unit tests. `validate` checks the application configurations and rendered infrastructure. `e2e` provisions the local installation and runs the complete deployment and observability test.

**Current status:** the runner pins published Loco v0.0.112 CLI binaries and six matching platform images from commit `5efe322d`. This includes local Gateway Service configuration and numeric runtime user support. Setup and chart/application configuration checks pass; the complete 16-service E2E has not passed.

The pinned platform images include Linux AMD64 and ARM64 manifests. GitHub Actions uses an AMD64 runner. A native Apple Silicon run also requires compatible application images.

## What the test checks

| Area | Check |
| --- | --- |
| Authentication | Sign in a demo user through local Dex OIDC and obtain CLI credentials through Loco's login APIs. |
| Cluster attachment | Attach the worker cluster using an operator-provided shared secret and its Kubernetes identity. |
| Builds and deployments | Deploy all 16 services, verify eight source builds and eight prebuilt deployments, and check resource health. |
| Application behavior | Browse the shop, populate a cart, complete checkout, and observe Kafka consumption. |
| Logs | Retrieve application logs through `QueryLogs`, scoped to the demo workspace, environment, and resources. |
| Metrics | Confirm the payment transaction counter increases after checkout through `QueryMetrics`. |
| Traces | Retrieve a connected checkout trace spanning frontend, checkout, payment, shipping, and product catalog through `QueryTraces`. |
| Access control | Reject log, metric, and trace queries outside the credential's workspace. |

User login and cluster attachment are separate. Dex authenticates the demo user; the agent attaches using the shared secret configured on the agent and control plane.

Telemetry queries go to the regional control-plane API. The observability proxy and ClickHouse stay private to that control plane. The runner queries one region at a time.

Missing APIs, failed checks, and blocked checks make the complete run fail. [GitHub Actions](https://github.com/team-loco/demo/actions) retains diagnostic artifacts and API evidence.

## Application services

Each service has its own deployment configuration under [`apps/`](apps/). [`apps.json`](apps.json) records the build mode, port, protocol, and source Dockerfile.

| Built from source on Loco | Deployed from prebuilt images |
| --- | --- |
| accounting | ad |
| cart | currency |
| checkout | email |
| payment | fraud-detection |
| product-catalog | frontend |
| quote | frontend-proxy |
| recommendation | image-provider |
| shipping | load-generator |

Source deployments use the retained upstream Dockerfiles. Prebuilt deployments pass an immutable image reference to `loco deploy --image`. The runner resolves Loco resource IDs to wire private service addresses between applications.

Services whose images default to root or declare a named user specify positive numeric `runAsUser` and `runAsGroup` values in their `loco.yaml`. The IDs use existing image accounts where available, including .NET's `app`, quote's `www-data`, and the proxy's `envoy`. Currency, email, recommendation, and load-generator run as `65534:65534`. Loco still enforces non-root execution. The pinned Loco v0.0.112 CLI, API, controller and CRDs support these declarations.

The retained upstream sources are in [`upstream/`](upstream/). Source contexts are staged under `.state/work/<service>/source` with shared build files available. The product-catalog build includes its upstream OpenTelemetry configuration file. See [`UPSTREAM.md`](UPSTREAM.md) for provenance and update instructions.

## Keep an installation or run individual phases

The application services use private Loco addresses. The frontend binds to all container interfaces for local port forwarding; the frontend proxy does not request a public domain during this isolated test.

The runner creates its own `loco-demo` kind cluster and does not use your current Kubernetes context. It refuses an existing cluster or conflicting container names. A normal E2E run deletes infrastructure owned by this checkout on exit.

To retain the installation for inspection:

```sh
mise run e2e -- --keep
```

The runner installs Kubernetes Metrics Server for the collector's CPU autoscaler and waits for a CPU sample and an active HPA. Metrics Server supplies Kubernetes' resource metrics API; application observability continues through OpenTelemetry Collector and ClickHouse. Metrics Server connects to node IP addresses, which kind's kubelet certificates do not include. The local configuration disables kubelet certificate verification for Metrics Server; this setting is confined to [`config/metrics-server.yaml`](config/metrics-server.yaml).

The `loco-control-plane` Helm chart installs the API, dashboard, and private observability proxy. `loco-core` installs the worker agent and Gateway, with `certManager.issuer.type=selfSigned` for local certificates. PostgreSQL, registry, source storage, and Dex remain external Compose fixtures.

The runner verifies the dashboard HTML, its runtime configuration, and Config RPC through the dashboard's own `/api` route before authenticating. Agent, CLI, and telemetry queries use that Gateway route; the API and observability proxy have no public Services. To open the retained dashboard, resolve `app.platform.test.local` to `127.0.0.1` on the host and visit `http://app.platform.test.local:18000`.

The phases are also available separately:

```sh
mise run up
mise run deploy
mise run verify
mise run down
```

After a successful setup, repeat a deployment for selected services with:

```sh
mise run deploy -- cart checkout
```

The complete E2E always tests all 16 services. Sanitized diagnostics and API evidence go to `artifacts/`; runtime credentials and identifiers stay in the ignored `.state/` directory. The downloaded CLI runs in a container with its own credential store.

## Versions and configuration

| Path | Contents |
| --- | --- |
| [`apps/`](apps/) | Per-service `loco.yaml` files. |
| [`config/`](config/) | Local cluster, Loco, networking, observability, and supporting-service configuration. |
| [`scripts/demo.py`](scripts/demo.py) | Setup, deployment, verification, cleanup, and failure reporting. |
| [`tests/`](tests/) | Regression tests for the runner. |
| [`versions.json`](versions.json) | Release versions, commits, image digests, and artifact checksums. |

The repository pins Astronomy Shop 3.1.0 and upstream chart 0.42.3. Loco CLI binaries retain their release tag, source commit, and published checksums. Loco Helm charts use a separately pinned immutable platform commit and its dependency lockfiles; all six platform images, including the dashboard, use verified published digests from that platform commit. `mise.toml`, `mise.lock`, and `uv.lock` pin the tools and Python dependencies.

Updating Loco requires published CLI binaries with verified checksums and immutable platform images matching the chart source commit. The release must support shared-secret cluster attachment, regional observability queries, trace and counter queries, `deploy --image`, and services without HTTP health checks. Rerun the complete E2E after changing release pins.

## Report a failure

```sh
mise run report
```

The runner reports failures to [`team-loco/loco`](https://github.com/team-loco/loco/issues) with the pinned versions, reproduction steps, and failing check. It links known issues and reuses open issues with the same check title.

Automatic reporting from trusted main or manual CI runs requires `LOCO_ISSUES_TOKEN` with Issues write access to `team-loco/loco`. Pull-request jobs do not receive this token. Without it, CI still fails and retains evidence.

## Upstream attribution

Astronomy Shop comes from the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo). This repository adapts its application services for Loco and retains their [Apache 2.0 license](upstream/LICENSE) and copyright headers. See [`UPSTREAM.md`](UPSTREAM.md) for the exact source commit and retained components.
