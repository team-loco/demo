# Loco Demo

The Loco implementation of the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo): Astronomy Shop, installed, deployed and observed through [Loco](https://github.com/team-loco/loco).

Astronomy Shop is a microservices application where users browse products, add items to a cart and place orders. This repository installs Loco on a local kind cluster with the operator CLI, `locoadm`, deploys 16 of the shop's services with the user CLI, `loco`, and checks the deployment, checkout and telemetry through Loco's public APIs.

## Install Loco

An operator installs Loco from one file, [`config/installation.yaml`](config/installation.yaml):

```sh
locoadm validate  --file installation.yaml
locoadm apply     --file installation.yaml   # stops until the certificate issuer exists
kubectl apply     -f config/issuer.yaml
locoadm apply     --file installation.yaml   # installs the control plane
locoadm bootstrap --file installation.yaml --email demo@example.test
locoadm apply     --file installation.yaml   # attaches the worker
locoadm status    --file installation.yaml
```

The first `apply` installs cert-manager and Envoy Gateway, then stops because the ClusterIssuer it names does not exist yet. `bootstrap` signs the first administrator in through the identity provider and saves a CLI session; the last `apply` uses that session to enroll the worker cluster. `status` reports `running` once the worker is ready.

`locoadm` reads the pinned Loco release, the Kubernetes contexts, and references to Secrets the operator creates beforehand. The demo supplies these external services as Docker containers next to the kind cluster ([`config/fixtures.yaml`](config/fixtures.yaml)):

| Service | Fixture | Address |
| --- | --- | --- |
| PostgreSQL | `postgres` | `postgres.loco-demo.localhost:5432` |
| OCI registry with `api`, `push` and `pull` identities | `zot` | `registry.loco-demo.localhost:5000` |
| S3-compatible source and backup buckets | `versitygw` | `s3.loco-demo.localhost:19000` |
| OIDC provider with the user `demo@example.test` and a public `loco` client | Dex | `dex.loco-demo.localhost:15556` |

Their credentials are in [`config/inputs.yaml`](config/inputs.yaml), the input Secret `locoadm` reads. The API, dashboard and OTLP ingest listen on `api`, `console` and `ingest.loco-demo.localhost` through the control-plane Gateway.

Every `*.localhost` name resolves to `127.0.0.1` on the host, so `locoadm`, `loco` and the browser reach the API, Dex and storage through ports kind and Docker publish on the loopback address; inside the cluster, CoreDNS resolves the same names to the fixtures and to [`config/gateway.yaml`](config/gateway.yaml), so URLs, tokens and presigned storage links work the same from both sides.

## Run locally

Install [mise](https://mise.jdx.dev/) and start Docker. Ports 80, 15556 and 19000 on `127.0.0.1` must be free. Then run:

```sh
git clone https://github.com/team-loco/demo.git
cd demo
mise trust
mise run setup
mise run e2e
```

| Task | What it does |
| --- | --- |
| `mise run setup` | Installs the pinned tools, downloads the `loco` and `locoadm` release binaries for the host and checks them against the release's `checksums.txt`, and downloads the upstream chart. |
| `mise run test` | Runs the runner's unit tests. |
| `mise run validate` | Validates `loco.yaml`, the rendered upstream chart and `installation.yaml`. |
| `mise run up` | Creates the `loco-demo` kind cluster and fixtures, runs the operator commands above, and checks that the dashboard serves. |
| `mise run deploy` | Creates the `demo` organization and `astronomy-shop` workspace with `loco`, installs the shop's Kafka, Valkey, PostgreSQL and flagd into the workspace namespace, and deploys the 16 services with one `loco deploy`. The pods then pull their images and start. |
| `mise run verify` | Runs the checks below. |
| `mise run down` | Deletes the cluster and fixtures this checkout created. |
| `mise run e2e` | Runs `up`, `deploy` and `verify`, then `down`. `mise run e2e -- --keep` keeps the installation. |
| `mise run report` | Files failed checks as issues in `team-loco/loco`. |

The CLIs run on the host with `HOME` set to `.state/home` and `LOCO_CREDENTIAL_STORE=file`, so the session `bootstrap` saves lives in `.state/home/.loco` and never touches your own `~/.loco` or keychain. To use it yourself:

```sh
export HOME=$PWD/.state/home LOCO_CREDENTIAL_STORE=file
.cache/bin/*/loco whoami
```

Open `http://console.loco-demo.localhost` and sign in as `demo@example.test` with password `demo-password` to see the shop in the dashboard.

The runner creates its own `loco-demo` kind cluster and never uses your current Kubernetes context. It refuses to start when that cluster or its fixture containers already exist, and `down` removes only infrastructure recorded as owned by this checkout.

## What the test checks

| Area | Check |
| --- | --- |
| Installation | `locoadm` installs the control plane, bootstraps the administrator through a Dex password login, attaches the worker, and `status` reports `running`. |
| Deployments | All 16 services deploy from prebuilt images without a build, and every resource reports healthy. |
| Application behavior | The runner browses the shop, adds to the cart and checks out; both Kafka consumers record the order. |
| Logs | `QueryLogs` returns the checkout order from both SDK-pushed OTLP logs and container stdout. |
| Metrics | `QueryMetrics` reports the payment transaction counter increasing after checkout. |
| Traces | `GetTrace` returns a connected checkout trace across frontend, checkout, product-catalog, payment and shipping. |
| Access control | A token scoped to the demo workspace cannot query another workspace's logs, metrics or traces. |

Telemetry checks use `loco.observability.v1.ObservabilityService` with a workspace-scoped token from `TokenService`. The demo does not run `locoadm verify`: it needs the application domain's self-signed certificate trusted by `locoadm`, and on macOS that means adding it to the keychain. The deployment checks cover the same deploy and log paths.

Evidence and per-step logs go to `artifacts/`; credentials and identifiers stay in the ignored `.state/` directory.

## Application services

[`loco.yaml`](loco.yaml) declares the 16 services in one partial, `astronomy-shop`, and the deploy step applies it in one plan:

```sh
loco deploy --file loco.yaml --env production --yes --confirm-import
```

Every service except product-catalog runs the upstream release image `ghcr.io/open-telemetry/demo:3.1.0-<service>`, pinned by digest in `loco.yaml`. Each service names its image, so the deploy builds nothing. The 16 services are accounting, ad, cart, checkout, currency, email, fraud-detection, frontend, frontend-proxy, image-provider, load-generator, payment, product-catalog, quote, recommendation and shipping.

Services reach each other by name, as in `cart:7070`: Loco gives every service a private address under its own name in the workspace. Services whose images default to root or to a named user declare numeric `runAsUser` and `runAsGroup` values in their `loco.yaml`.

product-catalog configures its OpenTelemetry SDK only from the file `OTEL_CONFIG_FILE` names, and upstream mounts that file into the container. `loco.yaml` cannot mount files ([team-loco/loco#1843](https://github.com/team-loco/loco/issues/1843)), so [`images/product-catalog`](images/product-catalog/) builds the upstream image with upstream's `otel-config.yml` added at `/otel-config.yml`. The [images workflow](.github/workflows/images.yml) publishes it as `ghcr.io/team-loco/demo/product-catalog`, and `loco.yaml` pins it by digest. The derived image goes away once `loco.yaml` can mount files.

See [`UPSTREAM.md`](UPSTREAM.md) for provenance and update instructions.

## Versions

[`versions.json`](versions.json) pins one Loco release, which names both the CLI binaries and the platform bundle `locoadm` installs, plus Astronomy Shop 3.1.0, its chart 0.42.3 and the kind node image. `mise.toml`, `mise.lock` and `uv.lock` pin the tools and Python dependencies. Renovate opens a pull request when Loco publishes a release; the E2E run on that pull request is the compatibility check.

## Report a failure

```sh
mise run report
```

The runner files each failed check in [`team-loco/loco`](https://github.com/team-loco/loco/issues) with the Loco version and reproduction steps, and reuses an open issue with the same title. CI on `main` and manual runs reports automatically with `LOCO_ISSUES_TOKEN`, which needs Issues write access to `team-loco/loco`.

## Upstream attribution

Astronomy Shop comes from the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo). This repository runs its published images and chart under their [Apache 2.0 license](https://github.com/open-telemetry/opentelemetry-demo/blob/main/LICENSE).
