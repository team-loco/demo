# Loco Demo

The Loco implementation of the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo): Astronomy Shop, installed, deployed and observed through [Loco](https://github.com/team-loco/loco).

Astronomy Shop is a microservices application where users browse products, add items to a cart and place orders. This repository installs Loco on a local kind cluster with the operator CLI, `locoadm`, deploys 16 of the shop's services with the user CLI, `loco`, and checks the deployment, checkout and telemetry through Loco's public APIs.

## Install Loco

An operator installs Loco from one file, [`loco-install.yaml`](loco-install.yaml), which `locoadm` reads from the current directory:

```sh
locoadm validate
locoadm apply            # stops until the certificate issuer exists
kubectl apply -f config/issuer.yaml
locoadm apply            # installs the control plane
locoadm login --device   # signs in as demo@example.test
locoadm cluster attach   # attaches the worker
locoadm status
```

The first `apply` installs cert-manager and Envoy Gateway, then stops because the ClusterIssuer it names does not exist yet. `controlPlane.admins` lists `demo@example.test`, so its first sign-in becomes a system admin. `login --device` prints a code; the runner approves it through the API's `ApproveDeviceLogin`, the call the dashboard makes, with an ID token from Dex's password grant. `cluster attach` uses that session to enroll the worker cluster. `status` reports `running` once the worker is ready, with no unclaimed admins.

The runner writes a copy of `loco-install.yaml` with the kind cluster's UID to `.state/` and runs `locoadm` there.

`locoadm` reads the pinned Loco release, the Kubernetes contexts, and references to Secrets the operator creates beforehand. The demo supplies these external services as Docker containers next to the kind cluster ([`config/fixtures.yaml`](config/fixtures.yaml)):

| Service | Fixture | Address |
| --- | --- | --- |
| PostgreSQL | `postgres` | `postgres.loco-demo.localhost:5432` |
| OCI registry with `api`, `push` and `pull` identities | `zot` | `registry.loco-demo.localhost:5000` |
| S3-compatible source and backup buckets | `versitygw` | `s3.loco-demo.localhost:19000` |
| OIDC provider with the user `demo@example.test` and a public `loco` client | Dex | `dex.loco-demo.localhost:15556` |

Their credentials are in [`config/inputs.yaml`](config/inputs.yaml), the input Secret `locoadm` reads. The API, dashboard and OTLP ingest listen on `api`, `console` and `ingest.loco-demo.localhost` through the control-plane Gateway.

On the host, every `*.localhost` name resolves to `127.0.0.1`, where kind and Docker publish the API, Dex and storage ports. Inside the cluster, CoreDNS resolves the same names to the fixtures and to [`config/gateway.yaml`](config/gateway.yaml).

## Run locally

Install [mise](https://mise.jdx.dev/) and start Docker. Ports 80, 15000, 15556 and 19000 on `127.0.0.1` must be free. Then run:

```sh
git clone https://github.com/team-loco/demo.git
cd demo
mise trust
mise run setup
mise run e2e
```

| Task | What it does |
| --- | --- |
| `mise run setup` | Installs the pinned tools, downloads the `loco` and `locoadm` release binaries for the host and checks them against the release's `checksums.txt`, and downloads the upstream chart. `-- --loco` selects another Loco; see [Run unreleased Loco](#run-unreleased-loco). |
| `mise run test` | Runs the runner's unit tests. |
| `mise run validate` | Validates `loco.yaml`, the rendered upstream chart and `loco-install.yaml`. |
| `mise run up` | Creates the `loco-demo` kind cluster and fixtures, runs the operator commands above, and checks that the dashboard serves. |
| `mise run deploy` | Creates the `demo` organization and `astronomy-shop` workspace with `loco`, installs the shop's Kafka, Valkey, PostgreSQL and flagd into the workspace namespace, and deploys the 16 services with one `loco deploy`. The pods then pull their images and start. |
| `mise run verify` | Runs the checks below. |
| `mise run down` | Deletes the cluster and fixtures this checkout created. |
| `mise run e2e` | Runs `up`, `deploy` and `verify`, then `down`. `mise run e2e -- --keep` keeps the installation. |
| `mise run report` | Files failed checks as issues in `team-loco/loco`. |

The CLIs run on the host with `HOME` set to `.state/home` and `LOCO_CREDENTIAL_STORE=file`, so the session `locoadm login` saves lives in `.state/home/.loco` and never touches your own `~/.loco` or keychain. To use it yourself:

```sh
export HOME=$PWD/.state/home LOCO_CREDENTIAL_STORE=file
.cache/bin/*/loco whoami
```

Open `http://console.loco-demo.localhost` and sign in as `demo@example.test` with password `demo-password` to see the shop in the dashboard.

The runner creates its own `loco-demo` kind cluster and never uses your current Kubernetes context. It refuses to start when that cluster or its fixture containers already exist, and `down` removes only infrastructure recorded as owned by this checkout.

## What the test checks

| Area | Check |
| --- | --- |
| Installation | `locoadm` installs the control plane, signs in the configured admin, attaches the worker, and `status` reports `running` with no unclaimed admins. |
| Deployments | All 16 services deploy and every resource reports healthy. |
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

Every service except product-catalog runs the upstream release image `ghcr.io/open-telemetry/demo:3.1.0-<service>`, pinned by digest in `loco.yaml`. The 16 services are accounting, ad, cart, checkout, currency, email, fraud-detection, frontend, frontend-proxy, image-provider, load-generator, payment, product-catalog, quote, recommendation and shipping.

Services reach each other by name, as in `cart:7070`: Loco gives every service a private address under its own name in the workspace. Services whose images default to root or to a named user declare numeric `runAsUser` and `runAsGroup` values in their `loco.yaml`.

product-catalog runs `ghcr.io/team-loco/demo/product-catalog`: the upstream image with upstream's `otel-config.yml` at `/otel-config.yml`, built from [`images/product-catalog`](images/product-catalog/) by the [images workflow](.github/workflows/images.yml) and pinned by digest in `loco.yaml`.

See [`UPSTREAM.md`](UPSTREAM.md) for provenance and update instructions.

## Run unreleased Loco

`mise run setup -- --loco <source>` selects the Loco that later tasks install and use, until the next `setup`. The default source is `release`.

| Source | CLIs | Platform |
| --- | --- | --- |
| `release` | The release binaries `versions.json` pins. | The release's platform bundle. |
| `main` | Built from the newest `main` commit whose images CI published, in `.cache/loco`. | That commit's published charts, CRD bundles and images, from Loco's `release:main` task. |
| A core checkout's path | Built from the checkout. | Built from the checkout by its `release:local` task during `up`, uncommitted changes included, and pushed to the registry fixture. |

```sh
mise run setup -- --loco main
mise run e2e

mise run setup -- --loco ../loco
mise run e2e
```

For both, `setup` builds the CLIs with Go, and the runner writes the platform's `release.json` to `.state/` and points `release.manifest` in the copy of `loco-install.yaml` at it. `main` verifies each image's attestation with `gh`, which must be signed in. A checkout builds six images and needs its own `mise` tools installed. The registry fixture listens on `127.0.0.1:15000`, where `locoadm` pulls the charts and CRD bundles as `registry.loco-demo.localhost:15000`, and each node's containerd routes that name to the fixture. `report` names the commit for `main` and files nothing for a checkout.

## Versions

[`versions.json`](versions.json) pins one Loco release, which names both the CLI binaries and the platform bundle `locoadm` installs, plus Astronomy Shop 3.1.0, its chart 0.42.3 and the kind node image. `mise.toml`, `mise.lock` and `uv.lock` pin the tools and Python dependencies. Renovate opens a pull request when Loco publishes a release; the E2E run on that pull request is the compatibility check.

## Report a failure

```sh
mise run report
```

The runner files each failed check in [`team-loco/loco`](https://github.com/team-loco/loco/issues) with the Loco version and reproduction steps, and reuses an open issue with the same title. CI on `main` and manual runs reports automatically with `LOCO_ISSUES_TOKEN`, which needs Issues write access to `team-loco/loco`.

## Upstream attribution

Astronomy Shop comes from the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo). This repository runs its published images and chart under their [Apache 2.0 license](https://github.com/open-telemetry/opentelemetry-demo/blob/main/LICENSE).
