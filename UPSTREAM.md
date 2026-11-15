# Upstream

Astronomy Shop comes from [open-telemetry/opentelemetry-demo](https://github.com/open-telemetry/opentelemetry-demo) release 3.1.0, commit `dedc0178918e260823323b8d95005a8cb924b007`.

The demo keeps no upstream source. It runs the release's published images, `ghcr.io/open-telemetry/demo:3.1.0-<service>`, pinned by digest in `apps/*/loco.yaml`, and installs Kafka, Valkey, PostgreSQL and flagd from the upstream chart that `versions.json` pins by version and SHA-256. product-catalog is the exception: `images/product-catalog/Dockerfile` pins its upstream image by digest and adds upstream's OpenTelemetry config file for it, until team-loco/loco#1843 lets `loco.yaml` mount it. Runtime wiring lives in `apps/*/loco.yaml` and `scripts/demo.py`; the checkout's customer details come from upstream's `src/load-generator/people.json`.

To update, choose an upstream release, update the `upstream` entry in `versions.json`, the image tags and digests in `apps/*/loco.yaml`, `images/product-catalog/Dockerfile` and `config/demo-infrastructure.yaml`, then run `mise run test`, `mise run validate`, and the complete E2E workflow. After the images workflow publishes the new product-catalog image, pin `apps/product-catalog/loco.yaml` to the digest in its job summary. Keep the images and the chart's appVersion on the same release.
