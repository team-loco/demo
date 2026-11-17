# Demo GitOps repository

This repository installs the demo control plane and worker on one local cluster through Flux 2.10.0.

`releases/control-plane.yaml` pins the platform bundle. `clusters/local/kustomization.yaml` selects that cohort and `bases/all`, which uses the bundle's `./all` entry point. Shared role defaults live under `bases/`; demo overrides live in `clusters/local/values/`.

Run `mise run up` from the repository root. The runner copies this directory into `.state/gitops/`, generates the API environment Secret and OpenBao seal key with `locoadm init --single-cluster`, and supplies the fixture registry Secrets. Tracked Secret manifests contain placeholders. The runner publishes the working repository to the local OCI registry and points Flux at it.

The control plane reconciles before sign-in and worker registration. `locoadm cluster create --kube-context kind-loco-demo` delivers the join token directly to the cluster; then the runner selects `./all` and reconciles the worker.

See [the local flow](../README.md#install-loco) for fixture addresses and commands. For a persistent installation, encrypt plaintext Secrets before committing or supply them through an operator-managed secret delivery mechanism. Keep the seal key in offline custody.
