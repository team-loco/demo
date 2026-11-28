# Demo GitOps repository

This repository installs the demo control plane and worker on one local cluster through Flux 2.10.0.

`releases/control-plane.yaml` pins the platform bundle. `clusters/local/kustomization.yaml` selects that cohort and `bases/all`, which uses the bundle's `./all` entry point. Shared role defaults live under `bases/`; demo overrides live in `clusters/local/values/`.

Run `mise run up` from the repository root. The runner generates `.state/gitops/` with `locoadm init --single-cluster`, overlays this directory while retaining the generated API environment Secret and OpenBao seal key, and enables the chart-managed PostgreSQL database and token-authenticated Zot registry. Tracked Secret manifests contain placeholders. The runner uploads a complete revision of the working repository to the in-cluster S3 service and points a Flux Bucket source at it. This keeps bootstrap configuration independent of the application registry.

The control plane reconciles before sign-in and worker registration. `locoadm cluster create --kube-context kind-loco-demo` delivers the join token directly to the cluster; then the runner selects `./all` and reconciles the worker.

See [the local flow](../README.md#install-loco) for fixture addresses and commands. For a persistent installation, encrypt plaintext Secrets before committing or supply them through an operator-managed secret delivery mechanism. Keep the seal key in offline custody.
