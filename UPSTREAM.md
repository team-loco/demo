# Upstream source

The application fixtures under `upstream/` come from [open-telemetry/opentelemetry-demo](https://github.com/open-telemetry/opentelemetry-demo) release 3.1.0, commit `dedc0178918e260823323b8d95005a8cb924b007`.

Only shop services, their shared protobuf definitions, Kafka, feature-flag fixtures, Postgres initialization, and the product-catalog OTel config are retained. AI, mobile, upstream telemetry backends, dashboards, and repository tooling are excluded. The copied application sources retain their upstream copyright headers and [Apache 2.0 license](upstream/LICENSE).

The E2E harness adds `COPY otel-config.yml /otel-config.yml` to the staged product-catalog Dockerfile. This supplies the file that upstream mounts with Kubernetes or Compose. Runtime wiring lives in `apps/*/loco.yaml` and `scripts/demo.py`.

To update, choose an upstream release, copy the same fixture directories, update `versions.json` and prebuilt app/configuration digests, then run `mise run test`, `mise run validate`, and the complete E2E workflow. Keep source fixtures, prebuilt images, and upstream chart appVersion aligned.
