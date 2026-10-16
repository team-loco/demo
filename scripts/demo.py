from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import yaml

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".state"
CACHE = ROOT / ".cache"
OUT = ROOT / "artifacts"
VERSIONS = json.loads((ROOT / "versions.json").read_text())
APPS = json.loads((ROOT / "apps.json").read_text())
CLUSTER = "loco-demo"
PROJECT = "loco-demo"
CONTEXT = "kind-" + CLUSTER
CORE = CACHE / "loco"
CLI_SOURCE = CACHE / "loco-cli-source"
REGION = "us-east-1"
API_NAMESPACE = "observability"
GATEWAY_NODE_PORT = 30080
DASHBOARD_HOST = "app.platform.test.local"
CLI_PORT = 18000
DEX_PORT = 18056
DEX_ISSUER = f"http://{DASHBOARD_HOST}:{CLI_PORT}/dex"
READY_SECONDS = 600
TELEMETRY_SECONDS = 240
BUILD_SECONDS = 2100
PROGRESS_SECONDS = 15
RESULTS = []
INFRA_STARTED = False
KNOWN = {
    "health": "https://github.com/team-loco/loco/issues/711",
    "discovery": "https://github.com/team-loco/loco/issues/712",
    "image-flag": "https://github.com/team-loco/loco/issues/713",
    "enrollment": "https://github.com/team-loco/loco/issues/719",
    "trace-api": "https://github.com/team-loco/loco/issues/720",
    "metrics-api": "https://github.com/team-loco/loco/issues/722",
}


class RPCError(RuntimeError):
    def __init__(self, procedure, status, payload):
        self.status = status
        self.code = payload.get("code", "unknown")
        super().__init__(f"{procedure}: HTTP {status}: {redact(json.dumps(payload))}")


class PublicAPIGap(RuntimeError):
    def __init__(self, message, issue):
        super().__init__(message)
        self.issue = issue


def identity():
    return json.loads((STATE / "identity.json").read_text())


def workspace_id():
    return identity()["workspaceId"]


def app_namespace():
    return "ws-" + workspace_id()


def run(args, *, cwd=ROOT, input=None, check=True, timeout=READY_SECONDS, log=None, stream=False):
    args = [str(a) for a in args]
    env = os.environ.copy()
    if args[0] == "helm":
        (CACHE / "helm").mkdir(parents=True, exist_ok=True)
        env["HELM_REPOSITORY_CONFIG"] = str(CACHE / "helm/repositories.yaml")
        env["HELM_REPOSITORY_CACHE"] = str(CACHE / "helm/cache")
    if stream:
        result = run_streamed(args, cwd=cwd, input=input, timeout=timeout, env=env, log=log)
    else:
        result = subprocess.run(args, cwd=cwd, input=input, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout, env=env)
        if log:
            Path(log).parent.mkdir(parents=True, exist_ok=True)
            Path(log).write_text(redact(result.stdout))
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {result.returncode}):\n{redact(result.stdout[-6000:])}")
    return result.stdout if check else result


def run_streamed(args, *, cwd, input, timeout, env, log):
    if log:
        Path(log).parent.mkdir(parents=True, exist_ok=True)
    output = []
    with contextlib.ExitStack() as stack:
        logfile = stack.enter_context(Path(log).open("w")) if log else None
        process = stack.enter_context(subprocess.Popen(
            args, cwd=cwd, env=env, stdin=subprocess.PIPE if input is not None else None,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            start_new_session=True))
        def read_output():
            for line in process.stdout:
                output.append(line)
                sanitized = redact(line)
                print(sanitized, end="", flush=True)
                if logfile:
                    logfile.write(sanitized)
                    logfile.flush()
        def write_input():
            try:
                process.stdin.write(input)
            except BrokenPipeError:
                pass
            finally:
                process.stdin.close()
        reader = threading.Thread(target=read_output)
        reader.start()
        writer = threading.Thread(target=write_input) if input is not None else None
        if writer:
            writer.start()
        started = time.monotonic()
        deadline = started + timeout
        label = Path(log).stem if log else Path(args[0]).name
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(args[0], timeout)
                try:
                    process.wait(timeout=min(PROGRESS_SECONDS, remaining))
                    break
                except subprocess.TimeoutExpired:
                    elapsed = time.monotonic() - started
                    print(f"RUN {label} ({elapsed:.0f}s elapsed)", flush=True)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            reader.join()
            raise subprocess.TimeoutExpired(args[0], timeout, output=redact("".join(output))) from None
        finally:
            reader.join()
            if writer:
                writer.join()
    return subprocess.CompletedProcess(args, process.returncode, "".join(output))


@contextlib.contextmanager
def step(name):
    started = time.monotonic()
    print(f"START {name}", flush=True)
    try:
        yield
    except Exception:
        print(f"FAIL {name} ({time.monotonic() - started:.0f}s)", flush=True)
        raise
    print(f"DONE {name} ({time.monotonic() - started:.0f}s)", flush=True)


def redact(text):
    if (STATE / "secrets.json").exists():
        for value in credentials().values():
            if isinstance(value, str) and len(value) >= 16:
                text = text.replace(value, "[REDACTED]")
    return text


def credentials():
    return json.loads((STATE / "secrets.json").read_text())


def write_yaml(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    if path.is_relative_to(STATE):
        path.chmod(0o600)


def kube(*args, **kwargs):
    return run(["kubectl", "--kubeconfig", STATE / "kubeconfig", "--context", CONTEXT, *args], **kwargs)


def apply(data):
    documents = data if isinstance(data, list) else [data]
    kube("apply", "-f", "-", input=yaml.safe_dump_all(documents))


def compose(*args, **kwargs):
    return run(["docker", "compose", "-p", PROJECT, "-f", STATE / "compose.yaml", *args], **kwargs)


def rpc(url, procedure, data, token=None, *, host=None):
    headers = {"Content-Type": "application/json", "Connect-Protocol-Version": "1"}
    if host:
        headers["Host"] = host
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(
        url.rstrip("/") + "/" + procedure,
        data=json.dumps(data).encode(),
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"message": body}
        raise RPCError(procedure, exc.code, payload) from exc


def api(procedure, data, token="session"):
    bearer = credentials()["session"] if token == "session" else token
    return rpc(f"http://127.0.0.1:{CLI_PORT}/api", procedure, data, bearer, host=DASHBOARD_HOST)


def list_api(procedure, request, key):
    records = []
    seen = set()
    request = dict(request)
    while True:
        response = api(procedure, request)
        records.extend(response.get(key, []))
        cursor = response.get("nextPageToken")
        if not cursor:
            return records
        if cursor in seen:
            raise RuntimeError(f"{procedure}: repeated pagination cursor")
        seen.add(cursor)
        request["pageToken"] = cursor


def list_builds(resource_id):
    return list_api("loco.build.v1.BuildService/ListBuilds", {"resourceId": resource_id, "pageSize": 200}, "builds")


def observability_rpc(method, request, token=None):
    scoped = dict(request)
    if method in ("QueryLogs", "TailLogs"):
        scoped["regions"] = [REGION]
    elif method == "QueryMetrics":
        scoped["region"] = REGION
    return api("loco.observability.v1.ObservabilityProxyService/" + method,
               scoped, token=token or credentials()["telemetry"])


def require_release_contracts():
    required = {
        "proto/loco/agent/v1/agent.proto": ["string cluster_key =", "string display_name =", "string tier ="],
        "api/config.go": ["LOCO_AGENT_CONNECTION_SECRETS", "OBSERVABILITY_PROXY_URL"],
        "proto/loco/observability/v1/observability.proto": ["rpc QueryTraces(", "repeated string regions ="],
        "internal/session/config.go": ['"config.yaml"'],
        "cmd/loco/infra/deploy.go": ['cmd.Flags().String("image"'],
        "observability-proxy/pkg/clickhouse/metrics.go": ["FROM otel_metrics_sum"],
        "charts/loco-control-plane/templates/ui.yaml": ['window.locoConfig = {"apiURL":"/api"};', "ReplacePrefixMatch"],
        "charts/loco-control-plane/templates/api-deployment.yaml": ["api.existingSecret"],
        "charts/loco-core/templates/cm-issuer.yaml": ["selfSigned"],
    }
    missing = []
    for name, symbols in required.items():
        source = CLI_SOURCE if name.startswith(("cmd/", "internal/session/")) else CORE
        path = source / name
        content = path.read_text() if path.exists() else ""
        missing.extend(name + ": " + symbol for symbol in symbols if symbol not in content)
    if missing:
        raise PublicAPIGap(
            f"CLI {VERSIONS['loco']['cli']}, platform source {VERSIONS['loco'].get('platformCommit', VERSIONS['loco']['commit'])} lacks required contracts: "
            + "; ".join(missing) + ". Update versions.json only after required contracts are merged and artifacts are published; "
            "unmerged PRs and local source builds are not released artifacts.",
            KNOWN["enrollment"])


def wait_for(name, predicate, seconds=READY_SECONDS):
    started = time.monotonic()
    deadline = started + seconds
    next_update = started + PROGRESS_SECONDS
    print(f"WAIT {name} (timeout {seconds}s)", flush=True)
    last = None
    while True:
        try:
            if predicate():
                print(f"READY {name} ({time.monotonic() - started:.0f}s)", flush=True)
                return
        except PublicAPIGap:
            raise
        except (RuntimeError, urllib.error.URLError, OSError, IndexError, KeyError) as exc:
            last = exc
        now = time.monotonic()
        if now >= deadline:
            raise RuntimeError(redact(f"Timed out waiting for {name}. {last or ''}"))
        if now >= next_update:
            detail = f"; latest error: {redact(str(last))}" if last else ""
            print(f"WAIT {name} ({now - started:.0f}s elapsed){detail}", flush=True)
            next_update = now + PROGRESS_SECONDS
        time.sleep(3)


def record(name, fn, issue=None):
    print(f"CHECK {name}", flush=True)
    started = time.monotonic()
    try:
        detail = fn()
        result = {"name": name, "status": "PASS", "detail": detail or ""}
    except Exception as exc:
        result = {"name": name, "status": "FAIL", "detail": redact(str(exc)), "issue": issue}
        if isinstance(exc, PublicAPIGap):
            result["status"] = "BLOCKED"
            result["issue"] = exc.issue
        if "no matching manifest for linux/arm64" in result["detail"]:
            result["issue"] = "https://github.com/team-loco/loco/issues/716"
    result["seconds"] = round(time.monotonic() - started, 2)
    RESULTS.append(result)
    OUT.mkdir(exist_ok=True)
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2) + "\n")
    print(f"{result['status']} {name}: {result['detail']}", flush=True)
    return result["status"] == "PASS"


def checkout(repository, ref, commit, dest):
    if not dest.exists():
        run(["git", "init", dest])
        run(["git", "remote", "add", "origin", f"https://github.com/{repository}.git"], cwd=dest)
    current = run(["git", "rev-parse", "HEAD"], cwd=dest, check=False)
    if current.returncode == 0 and current.stdout.strip() == commit:
        return
    reference = ref if ref == commit else f"refs/tags/{ref}"
    print(f"Updating cached {repository} checkout to {ref}", flush=True)
    run(["git", "fetch", "--depth", "1", "origin", reference], cwd=dest)
    fetched = run(["git", "rev-parse", "FETCH_HEAD^{commit}"], cwd=dest).strip()
    if fetched != commit:
        raise RuntimeError(f"{dest}: expected {commit}, got {fetched} for {ref}")
    run(["git", "checkout", "--detach", commit], cwd=dest)


def prepare():
    CACHE.mkdir(exist_ok=True)
    OUT.mkdir(exist_ok=True)
    checkout("team-loco/loco", VERSIONS["loco"]["cli"], VERSIONS["loco"]["commit"], CLI_SOURCE)
    platform_commit = VERSIONS["loco"]["platformCommit"]
    checkout("team-loco/loco", platform_commit, platform_commit, CORE)
    bindir = CACHE / "bin" / VERSIONS["loco"]["cli"]
    bindir.mkdir(parents=True, exist_ok=True)
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    if arch is None:
        raise RuntimeError(f"Unsupported architecture: {platform.machine()}")
    systems = {"linux", platform.system().lower()}
    checksums = urllib.request.urlopen(
        f"https://github.com/team-loco/loco/releases/download/{VERSIONS['loco']['cli']}/checksums.txt"
    ).read().decode()
    for system in systems:
        name = f"loco-{system}-{arch}"
        dest = bindir / name
        expected = VERSIONS["loco"]["cliChecksums"][name]
        published = next(line.split()[0] for line in checksums.splitlines() if line.split()[-1] == name)
        if published != expected:
            raise RuntimeError(f"Published checksum changed for pinned CLI {name}")
        if not dest.exists():
            with urllib.request.urlopen(
                f"https://github.com/team-loco/loco/releases/download/{VERSIONS['loco']['cli']}/{name}"
            ) as response:
                dest.write_bytes(response.read())
        if hashlib.sha256(dest.read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Checksum mismatch for {name}")
        dest.chmod(0o755)
    (bindir / "checksums.txt").write_text(checksums)
    run(["helm", "repo", "add", "open-telemetry", "https://open-telemetry.github.io/opentelemetry-helm-charts"])
    run(["helm", "repo", "add", "altinity", "https://helm.altinity.com"])
    run(["helm", "repo", "add", "cilium", "https://helm.cilium.io/"])
    run(["helm", "repo", "add", "jetstack", "https://charts.jetstack.io"])
    chart = CACHE / "charts" / "opentelemetry-demo"
    if not chart.exists():
        archive = CACHE / "charts" / "opentelemetry-demo.tgz"
        archive.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(VERSIONS["upstream"]["chartUrl"], timeout=60) as response:
            archive.write_bytes(response.read())
        if hashlib.sha256(archive.read_bytes()).hexdigest() != VERSIONS["upstream"]["chartSha256"]:
            raise RuntimeError("Upstream chart checksum mismatch")
        with tarfile.open(archive) as source:
            source.extractall(archive.parent, filter="data")
    for name in ["loco-core", "loco-networking", "loco-obs", "loco-control-plane"]:
        run(["helm", "dependency", "build", CORE / "charts" / name], timeout=READY_SECONDS)
    print("CLI checksums and release source verified; immutable platform source and chart dependencies prepared", flush=True)


def native_cli():
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return CACHE / "bin" / VERSIONS["loco"]["cli"] / f"loco-{platform.system().lower()}-{arch}"


def cli_api_url():
    return json.loads((STATE / "endpoint.json").read_text())["cliApiUrl"]


def cli(*args, **kwargs):
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return run([
        "docker", "run", "--rm", "--network", "kind",
        "--add-host", DASHBOARD_HOST + ":" + json.loads((STATE / "endpoint.json").read_text())["nodeIP"],
        "--mount", f"type=bind,source={ROOT},target=/demo",
        "--mount", f"type=bind,source={STATE / 'cli'},target=/root/.loco",
        "--workdir", "/demo", "--env", "LOCO_CREDENTIAL_STORE=file",
        "--env", "LOCO_HOST=" + cli_api_url(),
        "--entrypoint", f"/demo/.cache/bin/{VERSIONS['loco']['cli']}/loco-linux-{arch}",
        VERSIONS["infrastructureImages"]["postgres"], *args,
    ], **kwargs)


def stage_app(name, resources=None):
    doc = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
    svc = doc["services"][name]
    if resources:
        svc["env"] = resolve_env(svc["env"], resources)
    dest = STATE / "work" / name
    dest.mkdir(parents=True, exist_ok=True)
    write_yaml(dest / "loco.yaml", doc)
    (dest / ".dockerignore").write_text("source/src/**/node_modules\nsource/**/__pycache__\n")
    if APPS[name]["mode"] == "source" and not (dest / "source").exists():
        shutil.copytree(ROOT / "upstream", dest / "source")
        if name == "product-catalog":
            dockerfile = dest / "source" / APPS[name]["dockerfile"]
            with dockerfile.open("a") as stream:
                stream.write("\nCOPY otel-config.yml /otel-config.yml\n")
    return dest


def resolve_env(env, resources):
    resolved = dict(env)
    for key, value in env.items():
        for name, resource_id in resources.items():
            host = "resource-" + resource_id
            value = value.replace(f"{name}:{APPS[name]['port']}", host + ":80")
        resolved[key] = value
    for key, value in env.items():
        if key.endswith("_HOST") and value in resources:
            resolved[key] = "resource-" + resources[value]
            port_key = key[:-5] + "_PORT"
            if port_key in env:
                resolved[port_key] = "80"
    return resolved


def validate():
    if not native_cli().exists():
        prepare()
    for name in APPS:
        stage_app(name)
        result = run([native_cli(), "infra", "validate", STATE / "work" / name / "loco.yaml"], check=False)
        if result.returncode:
            raise RuntimeError(f"{name}: {result.stdout}")
    rendered = run(["helm", "template", "astronomy", CACHE / "charts" / "opentelemetry-demo",
                    "--namespace", "demo-chart-validation", "-f", ROOT / "config/demo-infrastructure.yaml"])
    docs = [d for d in yaml.safe_load_all(rendered) if d]
    assert_infrastructure_only(docs)
    (OUT / "demo-infrastructure.yaml").write_text(rendered)
    validate_platform_charts()
    print("Validated all 16 loco.yaml files, infrastructure-only chart and control-plane charts", flush=True)


def control_plane_values():
    images = VERSIONS["loco"]["images"]
    if "ui" not in images:
        raise RuntimeError("Pin a published immutable UI image matching platformCommit before installing the control-plane chart")
    return {
        "controlPlaneHostname": DASHBOARD_HOST,
        "ui": {"hostname": DASHBOARD_HOST, "image": image_values(images["ui"])},
        "api": {"image": image_values(images["api"]), "existingSecret": "loco-demo-api-env"},
        "obsProxy": {
            "image": image_values(images["obs-proxy"]),
            "clickhouse": {
                "existingSecret": {"name": "loco-demo-clickhouse", "key": "reader"},
                "migratorExistingSecret": {"name": "loco-demo-clickhouse", "key": "migrator"},
            },
        },
    }


def validate_platform_charts():
    require_release_contracts()
    values = {"agent": {"clusterName": CLUSTER, "clusterRegion": REGION, "clusterTier": "production",
                        "image": image_values(VERSIONS["loco"]["images"]["agent"])}}
    write_yaml(STATE / "core-render-values.yaml", values)
    rendered = run(["helm", "template", "loco-core", CORE / "charts/loco-core", "--namespace", "loco-system",
                    "-f", ROOT / "config/core.yaml", "-f", STATE / "core-render-values.yaml"])
    gateway = next(doc for doc in yaml.safe_load_all(rendered) if doc and doc.get("kind") == "EnvoyProxy")
    service = gateway["spec"].get("provider", {}).get("kubernetes", {}).get("envoyService")
    configured = yaml.safe_load((ROOT / "config/core.yaml").read_text())["envoyProxy"]["service"]
    if service != configured:
        raise RuntimeError("Pinned loco-core chart does not render envoyProxy.service for local Gateway NodePort access")
    write_yaml(STATE / "control-plane-render-values.yaml", control_plane_values())
    rendered += "\n---\n" + run([
        "helm", "template", "loco-control-plane", CORE / "charts/loco-control-plane",
        "--namespace", API_NAMESPACE, "-f", STATE / "control-plane-render-values.yaml"])
    (OUT / "platform-charts.yaml").write_text(rendered)


def assert_infrastructure_only(docs):
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "DaemonSet", "StatefulSet")]
    names = {d["metadata"]["name"] for d in workloads}
    if names != {"flagd", "kafka", "valkey-cart", "astronomy-db"}:
        raise RuntimeError(f"Unexpected upstream workloads: {sorted(names)}")


def helm_install(name, chart, namespace, *values, timeout="10m"):
    with step(f"Install Helm release {name} in {namespace}"):
        run(["helm", "upgrade", "--install", name, chart, "--namespace", namespace,
             "--create-namespace", "--kubeconfig", STATE / "kubeconfig", "--kube-context", CONTEXT,
             *values, "--wait", "--timeout", timeout], timeout=READY_SECONDS + 120,
            log=OUT / f"helm-{name}.log", stream=True)


def bootstrap_compose():
    STATE.mkdir(exist_ok=True)
    (STATE / "cli").mkdir(exist_ok=True)
    if not (STATE / "secrets.json").exists():
        data = {n: secrets.token_hex(24) for n in
                ["database", "migrator", "ingest", "reader", "secretkey", "agent"]}
        data["secretkey"] = base64.b64encode(secrets.token_bytes(32)).decode()
        (STATE / "secrets.json").write_text(json.dumps(data))
        (STATE / "secrets.json").chmod(0o600)
    c = credentials()
    source = yaml.safe_load((CORE / "compose.yaml").read_text())
    services = {n: source["services"][n] for n in ["postgres", "registry", "s3"]}
    for name, svc in services.items():
        svc["image"] = VERSIONS["infrastructureImages"][name]
        svc["container_name"] = PROJECT + "-" + name
        svc["networks"] = {"default": {}, "kind": {"aliases": [PROJECT + "-" + name]}}
        svc.pop("ports", None)
    services["postgres"]["environment"] = {
        "POSTGRES_USER": "loco_demo", "POSTGRES_PASSWORD": c["database"], "POSTGRES_DB": "loco_demo"}
    services["registry"]["volumes"] = [
        str(CORE / "registry/local/config.json") + ":/etc/zot/config.json:ro",
        str(CORE / "registry/local/htpasswd") + ":/etc/zot/htpasswd:ro",
        "registry-data:/var/lib/registry"]
    services["s3"]["environment"] = {
        "ROOT_ACCESS_KEY": "loco-demo", "ROOT_SECRET_KEY": "loco-demo-source-key",
        "VGW_REGION": "us-east-1", "VGW_PORT": ":9000", "SOURCE_BUCKET": "loco-demo-sources"}
    api_env = {
        "DATABASE_URL": f"postgresql://loco_demo:{c['database']}@loco-demo-postgres:5432/loco_demo?sslmode=disable",
        "APP_PORT": "0.0.0.0:8000", "APP_ENV": "test", "LOG_LEVEL": "debug",
        "DEFAULT_PLATFORM_DOMAIN": "demo.test.local",
        "AUTH_ISSUERS": json.dumps([{
            "issuer": DEX_ISSUER, "jwksUrl": "http://loco-demo-dex:5556/dex/keys", "audience": "loco",
            "web": {"clientId": "loco", "scopes": "openid email profile"},
        }]),
        "LOCO_AGENT_CONNECTION_SECRETS": c["agent"],
        "WEB_URL": f"http://{DASHBOARD_HOST}:{CLI_PORT}",
        "LOCO_REGISTRY_HOST": "loco-demo-registry:5000",
        "LOCO_REGISTRY_URL": "http://loco-demo-registry:5000",
        "LOCO_REGISTRY_USERNAME": "api", "LOCO_REGISTRY_PASSWORD": "loco-dev-api",
        "LOCO_SOURCE_BUCKET": "loco-demo-sources", "LOCO_SOURCE_BUCKET_ENDPOINT": "http://loco-demo-s3:9000",
        "LOCO_SOURCE_BUCKET_ACCESS_KEY_ID": "loco-demo", "LOCO_SOURCE_BUCKET_SECRET_ACCESS_KEY": "loco-demo-source-key",
        "LOCO_SOURCE_BUCKET_REGION": "us-east-1", "LOCO_SOURCE_BUCKET_FORCE_PATH_STYLE": "true",
        "LOCO_SECRETS_KEY_PROVIDER": "local", "LOCO_SECRETS_LOCAL_KEYS": "demo:" + c["secretkey"],
    }
    (STATE / "api-env.json").write_text(json.dumps(api_env))
    (STATE / "api-env.json").chmod(0o600)
    dex = yaml.safe_load((CORE / "env/conformance/dex.yaml").read_text())
    dex["issuer"] = DEX_ISSUER
    dex["staticClients"] = [{
        "id": "loco", "name": "Loco Demo", "public": True,
        "redirectURIs": [f"http://{DASHBOARD_HOST}:{CLI_PORT}/auth/callback"],
    }]
    dex["staticPasswords"][0]["email"] = "demo@example.test"
    dex["staticPasswords"][0]["username"] = "demo"
    write_yaml(STATE / "dex.yaml", dex)
    services["dex"] = {
        "image": VERSIONS["infrastructureImages"]["dex"], "container_name": "loco-demo-dex",
        "command": ["dex", "serve", "/etc/dex/config.yaml"],
        "volumes": [str(STATE / "dex.yaml") + ":/etc/dex/config.yaml:ro"],
        "ports": [f"127.0.0.1:{DEX_PORT}:5556"],
        "networks": {"default": {}, "kind": {"aliases": ["loco-demo-dex"]}},
    }
    write_yaml(STATE / "compose.yaml", {
        "services": services, "volumes": {"postgres-data": {}, "registry-data": {}, "s3-data": {}},
        "networks": {"kind": {"external": True}}})
    compose("up", "-d", "postgres", "registry", "s3", "dex", timeout=READY_SECONDS, stream=True)
    wait_for("OIDC provider", lambda: urllib.request.urlopen(
        f"http://127.0.0.1:{DEX_PORT}/dex/.well-known/openid-configuration", timeout=5).status == 200)


def create_identity():
    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "loco",
        "username": "demo@example.test", "password": "carolpw", "scope": "openid email profile",
    }).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{DEX_PORT}/dex/token", data=form,
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(request, timeout=30) as response:
        oidc_token = json.load(response)["id_token"]
    c = credentials()
    c["oidc"] = oidc_token
    (STATE / "secrets.json").write_text(json.dumps(c))
    current_user = api("loco.user.v1.UserService/WhoAmI", {}, token=oidc_token)["user"]
    api("loco.user.v1.UserService/UpdateUser", {"userId": current_user["id"], "name": "Demo User",
                                               "updateMask": "name"}, token=oidc_token)
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    code = api("loco.auth.v1.AuthService/ApproveCLILogin",
               {"state": secrets.token_urlsafe(24), "codeChallenge": challenge}, token=oidc_token)["code"]
    tokens = api("loco.auth.v1.AuthService/ExchangeCLICode",
                 {"code": code, "codeVerifier": verifier}, token=None)["tokens"]
    c = credentials()
    c["session"] = tokens["accessToken"]
    c["refresh"] = tokens["refreshToken"]
    (STATE / "secrets.json").write_text(json.dumps(c))
    orgs = list_api("loco.org.v1.OrgService/ListUserOrgs", {"userId": current_user["id"], "pageSize": 200}, "orgs")
    org_id = next((o["id"] for o in orgs if o["name"] == "demo"), None)
    if org_id is None:
        org_id = api("loco.org.v1.OrgService/CreateOrg", {"name": "demo"})["orgId"]
    workspaces = list_api("loco.workspace.v1.WorkspaceService/ListOrgWorkspaces",
                          {"orgId": org_id, "pageSize": 200}, "workspaces")
    ws_id = next((w["id"] for w in workspaces if w["name"] == "astronomy-shop"), None)
    if ws_id is None:
        ws_id = api("loco.workspace.v1.WorkspaceService/CreateWorkspace",
                    {"orgId": org_id, "name": "astronomy-shop", "description": "Loco deployment E2E"})["workspaceId"]
    environments = api("loco.environment.v1.EnvironmentService/ListEnvironments", {"workspaceId": ws_id}).get("environments", [])
    env_id = next((e["id"] for e in environments if e["name"] == "production"), None)
    if env_id is None:
        env_id = api("loco.environment.v1.EnvironmentService/CreateEnvironment",
                     {"workspaceId": ws_id, "name": "production", "type": "ENVIRONMENT_TYPE_PRODUCTION"})["environmentId"]
    data = {"userId": current_user["id"], "orgId": org_id, "workspaceId": ws_id, "environmentId": env_id}
    (STATE / "identity.json").write_text(json.dumps(data))
    (STATE / "cli/credentials.json").write_text(json.dumps({
        "host": cli_api_url(), "token": c["session"],
        "expiresAt": (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=int(tokens["expiresIn"]))).isoformat(),
        "refreshToken": c["refresh"]}))
    write_yaml(STATE / "cli/config.yaml", {
        "locoHost": cli_api_url(), "currentScope": "default", "scopes": {"default": {
            "organization": {"name": "demo", "id": org_id},
            "workspace": {"name": "astronomy-shop", "id": ws_id},
        }},
    })
    (STATE / "cli/config.yaml").chmod(0o600)
    (STATE / "cli/credentials.json").chmod(0o600)
    c["telemetry"] = api("loco.token.v1.TokenService/CreateToken", {
        "name": "demo-observability", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": ws_id,
        "scopes": [{"scope": "SCOPE_READ", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": ws_id}],
        "expiresInSec": 86400,
    })["token"]
    (STATE / "secrets.json").write_text(json.dumps(c))
    (OUT / "identity-api.json").write_text(json.dumps(data, indent=2) + "\n")
    print("OIDC login, CLI token exchange, organization and workspace API calls passed", flush=True)



def secret(namespace, name, values, secret_type="Opaque"):
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": namespace, "name": name},
           "type": secret_type, "stringData": values})


def setup_external_hosts():
    addresses = {}
    for name in ["postgres", "registry", "s3", "dex"]:
        addresses["loco-demo-" + name] = run([
            "docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
            "loco-demo-" + name]).strip()
    addresses[DASHBOARD_HOST] = run([
        "docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
        CLUSTER + "-control-plane"]).strip()
    cm = json.loads(kube("-n", "kube-system", "get", "configmap", "coredns", "-o", "json"))
    hosts = "\n".join(f"        {ip} {name}" for name, ip in addresses.items())
    cm["data"]["Corefile"] = cm["data"]["Corefile"].replace(
        "    forward .", f"    hosts {{\n{hosts}\n        fallthrough\n    }}\n    forward .")
    apply(cm)
    kube("-n", "kube-system", "rollout", "restart", "deployment/coredns")
    nodes = run(["kind", "get", "nodes", "--name", CLUSTER]).splitlines()
    for node in nodes:
        run(["docker", "exec", node, "mkdir", "-p", "/etc/containerd/certs.d/loco-demo-registry:5000"])
        run(["docker", "exec", "-i", node, "tee",
             "/etc/containerd/certs.d/loco-demo-registry:5000/hosts.toml"],
            input='[host."http://loco-demo-registry:5000"]\n  capabilities = ["pull", "resolve"]\n')
    return addresses


def image_values(image):
    repository, tag = image.split(":", 1)
    return {"repository": repository, "tag": tag}


def install_control_plane():
    c = credentials()
    secret(API_NAMESPACE, "loco-demo-api-env", json.loads((STATE / "api-env.json").read_text()))
    host = "clickhouse-loco-obs-clickhouse.observability.svc.cluster.local:9000"
    secret(API_NAMESPACE, "loco-demo-clickhouse", {
        "reader": f"clickhouse://loco_reader:{c['reader']}@{host}/loco_obs",
        "migrator": f"clickhouse://loco_migrator:{c['migrator']}@{host}/loco_obs",
    })
    write_yaml(STATE / "control-plane.yaml", control_plane_values())
    helm_install("loco-control-plane", CORE / "charts/loco-control-plane", API_NAMESPACE,
                 "-f", STATE / "control-plane.yaml")


def install_identity_route(address):
    name = "loco-demo-dex"
    apply({"apiVersion": "v1", "kind": "Service",
           "metadata": {"name": name, "namespace": API_NAMESPACE},
           "spec": {"ports": [{"name": "http", "port": 5556, "targetPort": 5556}]}})
    apply({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
           "metadata": {"name": name, "namespace": API_NAMESPACE,
                        "labels": {"kubernetes.io/service-name": name, "endpointslice.kubernetes.io/managed-by": PROJECT}},
           "addressType": "IPv4", "ports": [{"name": "http", "port": 5556, "protocol": "TCP"}],
           "endpoints": [{"addresses": [address], "conditions": {"ready": True}}]})
    apply({"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
           "metadata": {"name": name, "namespace": API_NAMESPACE},
           "spec": {"parentRefs": [{"name": "eg", "namespace": "loco-system"}],
                    "hostnames": [DASHBOARD_HOST], "rules": [{
                        "matches": [{"path": {"type": "PathPrefix", "value": "/dex"}}],
                        "backendRefs": [{"name": name, "port": 5556}],
                    }]}})


def dashboard_get(path):
    request = urllib.request.Request(f"http://127.0.0.1:{CLI_PORT}" + path,
                                     headers={"Host": DASHBOARD_HOST})
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read().decode(), response.headers


def check_dashboard():
    html, _ = dashboard_get("/")
    runtime, headers = dashboard_get("/runtime-config.js")
    if '<script src="/runtime-config.js"></script>' not in html:
        raise RuntimeError("Dashboard HTML does not load its runtime configuration")
    if html.index('/runtime-config.js') > html.index('type="module"'):
        raise RuntimeError("Dashboard runtime configuration loads after the application")
    if runtime.strip() != 'window.locoConfig = {"apiURL":"/api"};':
        raise RuntimeError("Dashboard API runtime configuration is not same-origin /api")
    if "immutable" in headers.get("Cache-Control", "").lower():
        raise RuntimeError("Dashboard runtime configuration must not be immutable")
    config = api("loco.config.v1.ConfigService/GetConfig", {}, token=None)
    if "serviceDefaults" not in config:
        raise RuntimeError("Dashboard /api route did not reach the Config RPC")
    auth = config.get("auth", {})
    if auth.get("issuer") != DEX_ISSUER or auth.get("clientId") != "loco":
        raise RuntimeError("Dashboard Config RPC does not configure demo browser sign-in")
    discovery, _ = dashboard_get("/dex/.well-known/openid-configuration")
    if json.loads(discovery).get("issuer") != DEX_ISSUER:
        raise RuntimeError("Dashboard Gateway does not serve the demo OIDC discovery document")
    (OUT / "dashboard-oidc.json").write_text(discovery)
    (OUT / "dashboard.html").write_text(html)
    (OUT / "dashboard-runtime.js").write_text(runtime)
    (OUT / "dashboard-config.json").write_text(json.dumps(config, indent=2) + "\n")
    return "Published dashboard loads runtime configuration and Config RPC through its same-origin /api route"


def connect_gateway():
    node_ip = run(["docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
                   CLUSTER + "-control-plane"]).strip()
    (STATE / "endpoint.json").write_text(json.dumps({
        "cliApiUrl": f"http://{DASHBOARD_HOST}:{GATEWAY_NODE_PORT}/api", "nodeIP": node_ip}))
    wait_for("public Gateway and dashboard API route", lambda: bool(
        api("loco.config.v1.ConfigService/GetConfig", {}, token=None).get("serviceDefaults")))
    check_dashboard()


def check_attachment():
    key = json.loads(kube("get", "namespace", "kube-system", "-o", "json"))["metadata"]["uid"]
    wait_for("automatically attached healthy cluster", lambda: any(
        region.get("region") == REGION and region.get("healthStatus") == "healthy"
        for region in api("loco.resource.v1.ResourceService/ListRegions", {}).get("regions", [])))
    (OUT / "cluster-attachment.json").write_text(json.dumps({
        "clusterKey": key, "region": REGION, "credentialSource": "operator-shared-secret",
        "discovery": "ResourceService/ListRegions", "healthStatus": "healthy",
    }, indent=2) + "\n")


def up():
    global INFRA_STARTED
    with step("Verify release artifacts and application configuration"):
        prepare()
        require_release_contracts()
        validate()
    STATE.mkdir(exist_ok=True)
    clusters = run(["kind", "get", "clusters"]).splitlines()
    if CLUSTER in clusters:
        raise RuntimeError("loco-demo already exists. Reuse it with deploy/verify, or run mise run down before up.")
    containers = set(run(["docker", "ps", "-a", "--format", "{{.Names}}"]).splitlines())
    collisions = containers.intersection(PROJECT + "-" + name for name in ["postgres", "registry", "s3", "dex"])
    if collisions:
        raise RuntimeError("Existing demo containers must be removed by their owner: " + ", ".join(sorted(collisions)))
    write_yaml(STATE / "owner.yaml", {"root": str(ROOT)})
    INFRA_STARTED = True
    with step("Create isolated kind cluster"):
        run(["kind", "create", "cluster", "--name", CLUSTER, "--image", VERSIONS["kindImage"],
             "--config", ROOT / "config/kind.yaml", "--kubeconfig", STATE / "kubeconfig"],
            timeout=READY_SECONDS, log=OUT / "kind.log", stream=True)
    helm_install("loco-networking", CORE / "charts/loco-networking", "kube-system",
                 "-f", ROOT / "config/networking.yaml")
    for ns in ["loco-system", "observability", "observability-node"]:
        apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})
    with step("Start PostgreSQL, registry, source storage and Dex"):
        bootstrap_compose()
    with step("Configure private infrastructure DNS and registry access"):
        addresses = setup_external_hosts()
    c = credentials()
    for name, user, password in [("loco-demo-pull", "nodes", "loco-dev-nodes"),
                                  ("loco-demo-push", "builder", "loco-dev-builder")]:
        auth = base64.b64encode(f"{user}:{password}".encode()).decode()
        secret("loco-system", name, {".dockerconfigjson": json.dumps({
            "auths": {"loco-demo-registry:5000": {"auth": auth}}})}, "kubernetes.io/dockerconfigjson")
    operator = {
        "controller": {"image": image_values(VERSIONS["loco"]["images"]["controller"])},
        "builds": {"builderImage": image_values(VERSIONS["loco"]["images"]["builder"]),
                   "privateEgressCIDRs": [addresses["loco-demo-registry"] + "/32", addresses["loco-demo-s3"] + "/32"]},
    }
    write_yaml(STATE / "operator.yaml", operator)
    helm_install("loco-operator", CORE / "charts/loco-operator", "loco-system",
                 "-f", ROOT / "config/operator.yaml", "-f", STATE / "operator.yaml")
    helm_install("cert-manager", "jetstack/cert-manager", "cert-manager",
                 "--version", VERSIONS["certManagerChart"], "--set", "crds.enabled=true")
    obs = {"global": {"region": REGION},
           "clickhouseUserPasswords": {"loco_migrator": c["migrator"],
                                      "loco_ingest": c["ingest"], "loco_reader": c["reader"]}}
    write_yaml(STATE / "observability.yaml", obs)
    helm_install("loco-obs", CORE / "charts/loco-obs", API_NAMESPACE,
                 "-f", ROOT / "config/observability.yaml", "-f", STATE / "observability.yaml")
    core = {"agent": {"image": image_values(VERSIONS["loco"]["images"]["agent"]),
                      "clusterName": CLUSTER, "clusterRegion": REGION, "clusterTier": "production"},
            "env": {"CONTROL_PLANE_URL": f"http://{DASHBOARD_HOST}:{GATEWAY_NODE_PORT}/api",
                    "AGENT_TOKEN": c["agent"]}}
    write_yaml(STATE / "core.yaml", core)
    helm_install("loco-core", CORE / "charts/loco-core", "loco-system",
                 "-f", ROOT / "config/core.yaml", "-f", STATE / "core.yaml")
    with step("Install regional API, dashboard and private observability proxy"):
        install_control_plane()
        install_identity_route(addresses["loco-demo-dex"])
    with step("Verify dashboard and same-origin public API routing"):
        connect_gateway()
    with step("Authenticate demo user and create workspace"):
        create_identity()
    with step("Wait for automatic cluster attachment"):
        check_attachment()
    helm_install("astronomy", CACHE / "charts/opentelemetry-demo", app_namespace(),
                 "-f", ROOT / "config/demo-infrastructure.yaml")
    print("Demo organization, workspace, user, builder worker, Loco charts, and shop infrastructure ready", flush=True)


def provision():
    resources = {}
    listed = list_api("loco.resource.v1.ResourceService/ListWorkspaceResources",
                      {"workspaceId": workspace_id(), "pageSize": 200}, "resources")
    existing = {resource["name"]: resource["id"] for resource in listed}
    for name, meta in APPS.items():
        if name in existing:
            resources[name] = existing[name]
            continue
        config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())["services"][name]
        target = config["regions"]["us-east-1"]
        resources[name] = api("loco.resource.v1.ResourceService/CreateResource", {
            "workspaceId": workspace_id(), "name": name, "type": "RESOURCE_TYPE_SERVICE",
            "spec": {"service": {
                "routing": {"port": meta["port"], "pathPrefix": "/", "idleTimeout": 60},
                "observability": {"tracing": {"enabled": True, "sampleRate": 1}},
                "regions": {"us-east-1": {"enabled": True, "cpu": target["cpu"], "memory": target["memory"],
                                         "minReplicas": 1, "maxReplicas": 1}},
            }},
        })["resourceId"]
    (STATE / "resources.json").write_text(json.dumps(resources, indent=2))
    return resources


def deploy_one(name, resources):
    stage_app(name, resources)
    image_args = []
    if APPS[name]["mode"] == "image":
        image = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())["services"][name]["image"]
        image_args = ["--image", image]
    with step(f"Deploy {name} ({APPS[name]['mode']})"):
        output = cli("deploy", "--file", f".state/work/{name}/loco.yaml", *image_args,
                     "--yes", "--confirm-import", "--env", "production",
                     timeout=BUILD_SECONDS, log=OUT / f"deploy-{name}.log", stream=True)
    if APPS[name]["mode"] == "source":
        builds = [b for b in list_builds(resources[name]) if b["status"] == "BUILD_STATUS_SUCCEEDED"]
        if not builds:
            raise RuntimeError(f"{name}: BuildService reports no successful build")
        build_id = max(builds, key=lambda b: b["createdAt"])["id"]
        build = api("loco.build.v1.BuildService/GetBuild", {"buildId": build_id})["build"]
        if build["status"] != "BUILD_STATUS_SUCCEEDED":
            raise RuntimeError(f"{name}: GetBuild did not confirm success")
        pods = json.loads(kube("-n", "loco-builds", "get", "pods", "-l", "loco.io/build-id=" + build_id, "-o", "json"))["items"]
        if not pods:
            raise RuntimeError(f"{name}: successful build has no pod to verify builder placement")
        evidence = []
        for pod in pods:
            node = pod["spec"]["nodeName"]
            labels = json.loads(kube("get", "node", node, "-o", "json"))["metadata"]["labels"]
            if labels.get("loco.build/demo-builder") != "true":
                raise RuntimeError(f"{name}: build ran on {node}, which is not a builder worker")
            evidence.append({"buildId": build_id, "pod": pod["metadata"]["name"], "node": node})
        (OUT / f"build-{name}.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return output[-1500:]


def deploy(names=None):
    resources = provision()
    for name in names or APPS:
        record("deploy:" + name, lambda name=name: deploy_one(name, resources))
    return all(r["status"] == "PASS" for r in RESULTS)



def check_resource(name, resource_id):
    configured = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())["services"][name]
    def healthy():
        resource = api("loco.resource.v1.ResourceService/GetResource", {"resourceId": resource_id})["resource"]
        service = resource.get("spec", {}).get("service", {})
        if "health" not in configured and service.get("healthCheck"):
            raise PublicAPIGap(f"{name} omits health but GetResource reports an HTTP health check: " +
                               json.dumps(service["healthCheck"]), KNOWN["health"])
        environments = [e for e in resource.get("environmentStatuses", [])
                        if e["environmentId"] == identity()["environmentId"]]
        return bool(environments) and all(e.get("status") == "RESOURCE_STATUS_HEALTHY" for e in environments)
    wait_for(name + " health through GetResource", healthy, seconds=90)
    return "GetResource reports healthy with the configured health semantics"


def check_builds(resources):
    for name, meta in APPS.items():
        builds = list_builds(resources[name])
        if meta["mode"] == "source" and not any(b["status"] == "BUILD_STATUS_SUCCEEDED" for b in builds):
            raise RuntimeError(f"{name} has no succeeded cluster build: {builds}")
        if meta["mode"] == "image" and builds:
            raise RuntimeError(f"{name} should use a prebuilt image but created builds: {builds}")
        if meta["mode"] == "source" and not (OUT / f"build-{name}.json").exists():
            raise RuntimeError(f"{name}: missing evidence that the cluster build ran on a builder worker")
    jobs = json.loads(kube("-n", "loco-builds", "get", "jobs", "-o", "json"))
    for job in jobs["items"]:
        if job["spec"]["template"]["spec"].get("nodeSelector", {}).get("loco.build/demo-builder") != "true":
            raise RuntimeError("Build job did not select the builder worker")
    return "Eight source apps have successful builds; eight image apps have no builds"


def traffic():
    resources = json.loads((STATE / "resources.json").read_text())
    baseline, _ = payment_counter()
    (STATE / "metric-baseline.json").write_text(json.dumps({"value": baseline}))
    trace_id = secrets.token_hex(16)
    user_id = "demo-" + uuid.uuid4().hex
    (STATE / "trace.json").write_text(json.dumps({"traceId": trace_id, "userId": user_id}))
    port = 18080
    log = (OUT / "frontend-forward.log").open("w")
    proc = subprocess.Popen(["kubectl", "--kubeconfig", str(STATE / "kubeconfig"), "--context", CONTEXT,
                             "-n", app_namespace(), "port-forward", "service/resource-" + resources["frontend"],
                             f"{port}:80"], stdout=log, stderr=subprocess.STDOUT)
    def request(path, body=None, checkout=False):
        request_trace = trace_id if checkout else secrets.token_hex(16)
        req = urllib.request.Request("http://127.0.0.1:" + str(port) + path,
              data=json.dumps(body).encode() if body is not None else None,
              headers={"Content-Type": "application/json", "traceparent": f"00-{request_trace}-{secrets.token_hex(8)}-01",
                       "baggage": "synthetic_request=true"})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    try:
        wait_for("frontend port forward", lambda: request("/api/products"), seconds=30)
        products = request("/api/products")
        product_id = products[0]["id"]
        cart = request("/api/cart", {"userId": user_id, "item": {"productId": product_id, "quantity": 1}})
        if not cart.get("items"):
            raise RuntimeError("Add-to-cart response contains no items")
        person = json.loads((ROOT / "upstream/src/load-generator/people.json").read_text())[0]
        person["userId"] = user_id
        recommendations = request("/api/recommendations?currencyCode=USD")
        if not recommendations:
            raise RuntimeError("Recommendation service returned no products")
        order = request("/api/checkout?currencyCode=USD", person, checkout=True)
        if not order.get("orderId"):
            raise RuntimeError("Checkout returned no orderId: " + json.dumps(order))
        (STATE / "order.json").write_text(json.dumps(order))
        (OUT / "checkout.json").write_text(json.dumps({"orderId": order["orderId"], "traceId": trace_id}))
        return {"orderId": order["orderId"], "traceId": trace_id}
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        log.close()


def query_logs(resource_ids):
    now = dt.datetime.now(dt.UTC)
    environment_id = identity()["environmentId"]
    request = {"workspaceId": workspace_id(), "resourceIds": resource_ids,
               "startTime": (now - dt.timedelta(minutes=10)).isoformat(), "endTime": now.isoformat(),
               "limit": 200, "order": "LOG_ORDER_NEWEST_FIRST"}
    entries = []
    seen = set()
    while True:
        response = observability_rpc("QueryLogs", request)
        page = response.get("entries", [])
        if any(entry["resourceId"] not in resource_ids for entry in page):
            raise RuntimeError("QueryLogs returned a resource outside the requested scope")
        entries.extend(entry for entry in page
                       if entry.get("resourceAttributes", {}).get("loco.environment.id") == environment_id)
        cursor = response.get("nextCursor")
        if not cursor:
            return entries
        if cursor in seen:
            raise RuntimeError("QueryLogs repeated pagination cursor")
        seen.add(cursor)
        request["cursor"] = cursor


def order_in_logs(resource_ids, order_id):
    entries = query_logs(resource_ids)
    return any(order_id in entry.get("body", "") + json.dumps(entry.get("logAttributes", {}))
               for entry in entries)


def check_logs():
    resources = json.loads((STATE / "resources.json").read_text())
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    ids = [resources[n] for n in ["checkout", "email"]]
    wait_for("checkout order logs through QueryLogs", lambda: order_in_logs(ids, order_id), TELEMETRY_SECONDS)
    (OUT / "logs-api.json").write_text(redact(json.dumps(query_logs(ids), indent=2)) + "\n")
    return "QueryLogs returns the checkout order with the requested resource scope"


def payment_counter():
    resources = json.loads((STATE / "resources.json").read_text())
    now = dt.datetime.now(dt.UTC)
    response = observability_rpc("QueryMetrics", {
        "workspaceId": workspace_id(), "resourceIds": [resources["payment"]],
        "environmentId": identity()["environmentId"], "metricName": "demo.payment.transactions",
        "startTime": (now - dt.timedelta(minutes=10)).isoformat(), "endTime": now.isoformat(),
        "intervalSeconds": 60, "aggregation": "max"})
    series = response.get("series", [])
    if any(s["resourceId"] != resources["payment"] for s in series):
        raise RuntimeError("QueryMetrics returned a resource outside the requested scope")
    values = [float(point["value"]) for item in series for point in item.get("points", [])]
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise RuntimeError("QueryMetrics returned an invalid payment counter value")
    return max(values, default=0), response


def check_metrics():
    baseline = json.loads((STATE / "metric-baseline.json").read_text())["value"]
    def observed():
        value, response = payment_counter()
        if value <= baseline:
            return False
        (OUT / "metrics-api.json").write_text(json.dumps({
            "baseline": baseline, "observed": value, "response": response,
        }, indent=2) + "\n")
        return True
    wait_for("payment counter increase through QueryMetrics", observed, TELEMETRY_SECONDS)
    return "QueryMetrics reports a payment counter increase after the checkout"


def query_trace(trace_id, resource_ids):
    now = dt.datetime.now(dt.UTC)
    request = {"workspaceId": workspace_id(), "resourceIds": resource_ids,
               "environmentId": identity()["environmentId"], "traceId": trace_id,
               "startTime": (now - dt.timedelta(minutes=10)).isoformat(), "endTime": now.isoformat(),
               "limit": 200}
    spans = []
    seen = set()
    while True:
        response = observability_rpc("QueryTraces", request)
        spans.extend(response.get("spans", []))
        cursor = response.get("nextCursor")
        if not cursor:
            return spans
        if cursor in seen:
            raise RuntimeError("QueryTraces repeated pagination cursor")
        seen.add(cursor)
        request["cursor"] = cursor


def connected_checkout_trace(spans, trace_id, resources):
    allowed = set(resources.values())
    expected_workspace = workspace_id()
    expected_environment = identity()["environmentId"]
    by_id = {}
    for span in spans:
        if (span.get("traceId") != trace_id or span.get("resourceId") not in allowed
                or span.get("workspaceId") != expected_workspace
                or span.get("environmentId") != expected_environment
                or span.get("resourceAttributes", {}).get("loco.region") != REGION):
            raise RuntimeError("QueryTraces returned a span outside the requested trace or tenant scope")
        span_id = span.get("spanId", "")
        if not re.fullmatch(r"[0-9a-f]{16}", span_id) or span_id == "0" * 16:
            raise RuntimeError("QueryTraces returned an invalid span ID")
        signature = (span.get("parentSpanId", ""), span["resourceId"])
        if span_id in by_id and by_id[span_id] != signature:
            raise RuntimeError("QueryTraces returned conflicting duplicate span IDs")
        by_id[span_id] = signature
    required = {resources[name] for name in ["frontend", "checkout", "payment", "shipping", "product-catalog"]}
    neighbors = {span_id: set() for span_id in by_id}
    for span_id, (parent, _) in by_id.items():
        if parent in by_id:
            neighbors[span_id].add(parent)
            neighbors[parent].add(span_id)
    for anchor, (_, resource) in by_id.items():
        if resource != resources["checkout"]:
            continue
        reached, pending = set(), [anchor]
        while pending:
            current = pending.pop()
            if current in reached:
                continue
            reached.add(current)
            pending.extend(neighbors[current] - reached)
        if required <= {by_id[span_id][1] for span_id in reached}:
            return True
    return False


def check_traces():
    resources = json.loads((STATE / "resources.json").read_text())
    trace_id = json.loads((STATE / "trace.json").read_text())["traceId"]
    def observed():
        spans = query_trace(trace_id, list(resources.values()))
        if not connected_checkout_trace(spans, trace_id, resources):
            return False
        (OUT / "traces-api.json").write_text(json.dumps({"traceId": trace_id, "spans": spans}, indent=2) + "\n")
        return True
    wait_for("connected checkout trace through QueryTraces", observed, TELEMETRY_SECONDS)
    return "QueryTraces links frontend, checkout, payment, shipping and product-catalog spans by parent IDs"


def check_observability_scope():
    other_workspace = api("loco.workspace.v1.WorkspaceService/CreateWorkspace", {
        "orgId": identity()["orgId"], "name": "telemetry-isolation-" + secrets.token_hex(4),
    })["workspaceId"]
    now = dt.datetime.now(dt.UTC)
    for method in ["QueryLogs", "QueryMetrics", "QueryTraces"]:
        request = {"workspaceId": other_workspace,
                   "startTime": (now - dt.timedelta(minutes=1)).isoformat(), "endTime": now.isoformat()}
        if method == "QueryMetrics":
            request.update({"metricName": "demo.payment.transactions", "intervalSeconds": 60, "aggregation": "max"})
        try:
            observability_rpc(method, request)
        except RPCError as exc:
            if exc.code != "permission_denied" or exc.status != 403:
                raise RuntimeError(f"{method} did not enforce workspace authorization: {exc}") from exc
        else:
            raise RuntimeError(f"{method} allowed a token scoped to another workspace")
    return "Regional API rejects all three telemetry queries for a workspace outside the issued token scope"


def check_kafka():
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    resources = json.loads((STATE / "resources.json").read_text())
    for name in ["accounting", "fraud-detection"]:
        wait_for(name + " Kafka order consumption through QueryLogs",
                 lambda name=name: order_in_logs([resources[name]], order_id), TELEMETRY_SECONDS)
    return "QueryLogs confirms both Kafka consumers recorded the checkout order"


def verify():
    resources = json.loads((STATE / "resources.json").read_text())
    for name, resource_id in resources.items():
        record("health:" + name, lambda name=name, rid=resource_id: check_resource(name, rid))
    record("build-matrix", lambda: check_builds(resources))
    record("observability-scope", check_observability_scope)
    if record("shop-checkout", traffic):
        record("logs-api", check_logs)
        record("metrics-api", check_metrics)
        record("traces-api", check_traces)
        record("kafka-consumers", check_kafka)
    else:
        for check in ["logs-api", "metrics-api", "traces-api"]:
            RESULTS.append({"name": check, "status": "BLOCKED", "detail": "Checkout must pass first"})
        RESULTS.append({"name": "kafka-consumers", "status": "BLOCKED", "detail": "Checkout must pass first"})
    save_report()
    return all(r["status"] == "PASS" for r in RESULTS)


def diagnostics():
    OUT.mkdir(exist_ok=True)
    if not owns_infrastructure():
        return
    if (STATE / "kubeconfig").exists():
        for args, name in [
            (["get", "pods", "-A", "-o", "wide"], "pods"),
            (["get", "events", "-A", "--sort-by=.lastTimestamp"], "events"),
            (["-n", "loco-system", "get", "applications", "-o", "yaml"], "applications"),
            (["-n", "loco-builds", "get", "builds", "-o", "yaml"], "builds"),
        ]:
            try:
                kube(*args, check=False, timeout=30, log=OUT / (name + ".log"))
            except Exception:
                pass
        for ns in ["loco-system", API_NAMESPACE, "loco-builds"] + ([app_namespace()] if (STATE / "identity.json").exists() else []):
            try:
                items = json.loads(kube("-n", ns, "get", "pods", "-o", "json"))["items"]
                for pod in items:
                    name = pod["metadata"]["name"]
                    kube("-n", ns, "logs", name, "--all-containers", "--tail=100",
                         check=False, timeout=20, log=OUT / "pods" / (name + ".log"))
            except Exception:
                pass
    if (STATE / "compose.yaml").exists():
        try:
            compose("logs", "--no-color", "--tail=100", check=False, timeout=30, log=OUT / "control-plane.log")
        except Exception:
            pass


def save_report():
    OUT.mkdir(exist_ok=True)
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2) + "\n")
    lines = ["# Loco demo E2E evidence", "", "CLI: " + VERSIONS["loco"]["cli"],
             "Core/charts commit: " + VERSIONS["loco"].get("platformCommit", VERSIONS["loco"]["commit"]),
             "Astronomy Shop: " + VERSIONS["upstream"]["release"], ""]
    for result in RESULTS:
        lines.extend([f"## {result['status']}: {result['name']}", "", str(result["detail"]), ""])
        if result.get("issue"):
            lines.extend(["Tracked in " + result["issue"], ""])
    (OUT / "evidence.md").write_text("\n".join(lines))


def report():
    data = json.loads((OUT / "results.json").read_text())
    for failure in data:
        if failure["status"] not in ("FAIL", "BLOCKED"):
            continue
        if failure["status"] == "BLOCKED" and not failure.get("issue"):
            continue
        if failure.get("issue"):
            print(f"{failure['name']}: {failure['issue']}")
            continue
        marker = "[demo-e2e] " + failure["name"]
        existing = json.loads(run(["gh", "issue", "list", "-R", "team-loco/loco", "--state", "open",
                                   "--search", marker, "--json", "title,url"]))
        exact = [i for i in existing if i["title"] == marker]
        if exact:
            print(exact[0]["url"])
            continue
        body = OUT / "issue.md"
        body.write_text(f"""The pinned Astronomy Shop E2E check `{failure['name']}` fails.

CLI: `{VERSIONS['loco']['cli']}`. CLI source: `{VERSIONS['loco']['commit']}`. Platform and Helm chart source: `{VERSIONS['loco'].get('platformCommit', VERSIONS['loco']['commit'])}`.
Upstream demo: `{VERSIONS['upstream']['release']}`; chart `{VERSIONS['upstream']['chart']}`.

Reproduce from https://github.com/team-loco/demo with Docker running:

```sh
mise trust
mise run setup
mise run e2e -- --keep
```

Observed failure:

```text
{redact(str(failure['detail']))}
```

Expected: all 16 apps deploy through Loco, eight cluster builds and eight prebuilt images; checkout succeeds and its trace, metrics and logs reach Loco's Collector and ClickHouse. The harness must exit nonzero on a failed or blocked check.

Local evidence is in `artifacts/evidence.md` and the per-step logs. CI retains these artifacts. This report identifies the failing check; its cause may be a harness integration defect and needs triage before changing Loco.
""")
        print(run(["gh", "issue", "create", "-R", "team-loco/loco", "--title", marker, "--body-file", body]))


def owns_infrastructure():
    owner = STATE / "owner.yaml"
    return owner.exists() and yaml.safe_load(owner.read_text()) == {"root": str(ROOT)}


def down():
    if not owns_infrastructure():
        print("No infrastructure owned by this checkout; nothing to remove", flush=True)
        return
    if (STATE / "compose.yaml").exists():
        compose("down", "--volumes", "--remove-orphans", check=False, timeout=120)
    run(["kind", "delete", "cluster", "--name", CLUSTER, "--kubeconfig", STATE / "kubeconfig"], check=False, timeout=120)
    if STATE.exists():
        shutil.rmtree(STATE)


def main():
    global INFRA_STARTED
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare", "validate", "up", "deploy", "verify", "e2e", "down", "report"])
    parser.add_argument("services", nargs="*", choices=list(APPS))
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--report-issues", action="store_true")
    args = parser.parse_args()
    if args.services and args.command != "deploy":
        parser.error("Service selection is supported by deploy; e2e always tests all 16 apps")
    if args.command == "e2e":
        INFRA_STARTED = False
        success = False
        try:
            if record("bootstrap", up):
                deploy(args.services or None)
                verify()
            success = bool(RESULTS) and all(r["status"] == "PASS" for r in RESULTS)
        finally:
            diagnostics()
            save_report()
            if args.report_issues:
                report()
            if not args.keep and INFRA_STARTED:
                down()
        return 0 if success else 1
    if args.command == "deploy":
        return 0 if deploy(args.services or None) else 1
    if args.command == "verify":
        return 0 if verify() else 1
    globals()[args.command]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
