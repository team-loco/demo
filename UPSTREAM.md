# Upstream

Astronomy Shop comes from [open-telemetry/opentelemetry-demo](https://github.com/open-telemetry/opentelemetry-demo) release 3.1.0, commit `dedc0178918e260823323b8d95005a8cb924b007`.

The demo runs the release's published images, `ghcr.io/open-telemetry/demo:3.1.0-<service>`, pinned by digest in `loco.yaml`, and installs Kafka, Valkey, PostgreSQL and flagd from the upstream chart that `versions.json` pins by version and SHA-256. product-catalog runs an image built from `images/product-catalog/Dockerfile`: its upstream image, pinned by digest, with upstream's OpenTelemetry config file added. Runtime wiring lives in `loco.yaml` and `scripts/demo.py`; the checkout's customer details come from upstream's `src/load-generator/people.json`.

To update, choose an upstream release, update the `upstream` entry in `versions.json`, the image tags and digests in `loco.yaml`, `images/product-catalog/Dockerfile` and `config/demo-infrastructure.yaml`, then run `mise run validate` and the complete E2E workflow. Loco builds product-catalog from its Dockerfile during deployment. Keep the images and the chart's appVersion on the same release.
