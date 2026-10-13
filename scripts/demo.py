from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
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
WORKSPACE = "00000000-0000-7000-8000-000000000003"
ORG = "00000000-0000-7000-8000-000000000002"
USER = "00000000-0000-7000-8000-000000000001"
ENVIRONMENT = "00000000-0000-7000-8000-000000000004"
NAMESPACE = "ws-" + WORKSPACE
CORE = CACHE / "loco"
CLI_PORT = 18000
READY_SECONDS = 600
TELEMETRY_SECONDS = 240
BUILD_SECONDS = 2100
RESULTS = []
KNOWN = {
    "health": "https://github.com/team-loco/loco/issues/711",
    "discovery": "https://github.com/team-loco/loco/issues/712",
    "image-flag": "https://github.com/team-loco/loco/issues/713",
}


def run(args, *, cwd=ROOT, input=None, check=True, timeout=READY_SECONDS, log=None):
    args = [str(a) for a in args]
    env = os.environ.copy()
    if args[0] == "helm":
        (CACHE / "helm").mkdir(parents=True, exist_ok=True)
        env["HELM_REPOSITORY_CONFIG"] = str(CACHE / "helm/repositories.yaml")
        env["HELM_REPOSITORY_CACHE"] = str(CACHE / "helm/cache")
    result = subprocess.run(args, cwd=cwd, input=input, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout, env=env)
    if log:
        Path(log).parent.mkdir(parents=True, exist_ok=True)
        Path(log).write_text(redact(result.stdout))
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed (exit {result.returncode}):\n{redact(result.stdout[-6000:])}")
    return result.stdout if check else result


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


def kube(*args, **kwargs):
    return run(["kubectl", "--kubeconfig", STATE / "kubeconfig", "--context", CONTEXT, *args], **kwargs)


def apply(data):
    documents = data if isinstance(data, list) else [data]
    kube("apply", "-f", "-", input=yaml.safe_dump_all(documents))


def compose(*args, **kwargs):
    return run(["docker", "compose", "-p", PROJECT, "-f", STATE / "compose.yaml", *args], **kwargs)


def sql(query):
    return compose("exec", "-T", "postgres", "psql", "-U", "loco_demo", "-d", "loco_demo",
                   "-At", "-v", "ON_ERROR_STOP=1", input=query).strip()


def api(procedure, data):
    req = urllib.request.Request(
        f"http://127.0.0.1:{CLI_PORT}/{procedure}",
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + credentials()["session"]},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{procedure}: {redact(exc.read().decode())}") from exc


def wait_for(name, predicate, seconds=READY_SECONDS):
    deadline = time.monotonic() + seconds
    last = None
    while True:
        try:
            if predicate():
                return
        except (RuntimeError, urllib.error.URLError, OSError, IndexError, KeyError) as exc:
            last = exc
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for {name}. {last or ''}")
        time.sleep(3)


def record(name, fn, issue=None):
    print(f"CHECK {name}", flush=True)
    started = time.monotonic()
    try:
        detail = fn()
        result = {"name": name, "status": "PASS", "detail": detail or ""}
    except Exception as exc:
        result = {"name": name, "status": "FAIL", "detail": redact(str(exc)), "issue": issue}
        if "no matching manifest for linux/arm64" in result["detail"]:
            result["issue"] = "https://github.com/team-loco/loco/issues/716"
    result["seconds"] = round(time.monotonic() - started, 2)
    RESULTS.append(result)
    OUT.mkdir(exist_ok=True)
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2) + "\n")
    print(f"{result['status']} {name}: {result['detail']}", flush=True)
    return result["status"] == "PASS"


def checkout(repository, tag, commit, dest):
    if not dest.exists():
        run(["git", "clone", "--depth", "1", "--branch", tag,
             f"https://github.com/{repository}.git", dest])
    actual = run(["git", "rev-parse", "HEAD"], cwd=dest).strip()
    if actual != commit:
        raise RuntimeError(f"{dest}: expected {commit}, got {actual}; remove this cache before preparing")


def prepare():
    CACHE.mkdir(exist_ok=True)
    OUT.mkdir(exist_ok=True)
    checkout("team-loco/loco", VERSIONS["loco"]["cli"], VERSIONS["loco"]["commit"], CORE)
    bindir = CACHE / "bin"
    bindir.mkdir(exist_ok=True)
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
    for name in ["loco-core", "loco-networking", "loco-obs"]:
        run(["helm", "dependency", "build", CORE / "charts" / name], timeout=READY_SECONDS)
    print("CLI checksums and release source verified; chart dependencies prepared", flush=True)


def native_cli():
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return CACHE / "bin" / f"loco-{platform.system().lower()}-{arch}"


def cli(*args, **kwargs):
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return run([
        "docker", "run", "--rm", "--network", "kind",
        "--mount", f"type=bind,source={ROOT},target=/demo",
        "--mount", f"type=bind,source={STATE / 'cli'},target=/root/.loco",
        "--workdir", "/demo", "--env", "LOCO_CREDENTIAL_STORE=file",
        "--env", "LOCO_HOST=http://loco-demo-api:8000",
        "--entrypoint", f"/demo/.cache/bin/loco-linux-{arch}",
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
                    "--namespace", NAMESPACE, "-f", ROOT / "config/demo-infrastructure.yaml"])
    docs = [d for d in yaml.safe_load_all(rendered) if d]
    assert_infrastructure_only(docs)
    (OUT / "demo-infrastructure.yaml").write_text(rendered)
    print("Validated all 16 loco.yaml files and infrastructure-only chart", flush=True)


def assert_infrastructure_only(docs):
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "DaemonSet", "StatefulSet")]
    names = {d["metadata"]["name"] for d in workloads}
    if names != {"flagd", "kafka", "valkey-cart", "astronomy-db"}:
        raise RuntimeError(f"Unexpected upstream workloads: {sorted(names)}")


def helm_install(name, chart, namespace, *values, timeout="10m"):
    run(["helm", "upgrade", "--install", name, chart, "--namespace", namespace,
         "--create-namespace", "--kubeconfig", STATE / "kubeconfig", "--kube-context", CONTEXT,
         *values, "--wait", "--timeout", timeout], timeout=READY_SECONDS + 120,
        log=OUT / f"helm-{name}.log")


def bootstrap_compose():
    STATE.mkdir(exist_ok=True)
    (STATE / "cli").mkdir(exist_ok=True)
    if not (STATE / "secrets.json").exists():
        data = {n: secrets.token_hex(24) for n in
                ["session", "agent", "database", "migrator", "ingest", "reader", "secretkey"]}
        data["session"] = "loco_s_" + data["session"]
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
        "DATABASE_URL": f"postgresql://loco_demo:{c['database']}@postgres:5432/loco_demo?sslmode=disable",
        "APP_PORT": "0.0.0.0:8000", "APP_ENV": "test", "LOG_LEVEL": "debug",
        "DEFAULT_PLATFORM_DOMAIN": "demo.test.local",
        "LOCO_REGISTRY_HOST": "loco-demo-registry:5000",
        "LOCO_REGISTRY_URL": "http://loco-demo-registry:5000",
        "LOCO_REGISTRY_USERNAME": "api", "LOCO_REGISTRY_PASSWORD": "loco-dev-api",
        "LOCO_SOURCE_BUCKET": "loco-demo-sources", "LOCO_SOURCE_BUCKET_ENDPOINT": "http://loco-demo-s3:9000",
        "LOCO_SOURCE_BUCKET_ACCESS_KEY_ID": "loco-demo", "LOCO_SOURCE_BUCKET_SECRET_ACCESS_KEY": "loco-demo-source-key",
        "LOCO_SOURCE_BUCKET_REGION": "us-east-1", "LOCO_SOURCE_BUCKET_FORCE_PATH_STYLE": "true",
        "LOCO_SECRETS_KEY_PROVIDER": "local", "LOCO_SECRETS_LOCAL_KEYS": "demo:" + c["secretkey"],
    }
    services["api"] = {
        "image": VERSIONS["loco"]["images"]["api"], "container_name": "loco-demo-api",
        "environment": api_env, "ports": [f"127.0.0.1:{CLI_PORT}:8000"],
        "networks": {"default": {}, "kind": {"aliases": ["loco-demo-api"]}},
        "depends_on": {"postgres": {"condition": "service_healthy"}},
    }
    write_yaml(STATE / "compose.yaml", {
        "services": services, "volumes": {"postgres-data": {}, "registry-data": {}, "s3-data": {}},
        "networks": {"kind": {"external": True}}})
    compose("up", "-d", "postgres", "registry", "s3", "api", timeout=READY_SECONDS)
    wait_for("API migrations and health", lambda: urllib.request.urlopen(
        f"http://127.0.0.1:{CLI_PORT}/health", timeout=5).status == 200)


def seed():
    c = credentials()
    seed = (CORE / "api/seed/e2e.sql").read_text()
    seed = seed.replace("e2e@test.local", "demo@example.test").replace("E2E Test User", "Demo User")
    seed = seed.replace("e2e-test-org", "demo").replace("e2e-test-workspace", "astronomy-shop")
    seed = seed.replace("e2e-test-cluster", CLUSTER).replace("e2e.test.local", "demo.test.local")
    seed = seed.replace(":'agent_token'", "'" + c["agent"] + "'")
    sql(seed)
    sql(f"""
INSERT INTO user_scopes (user_id, scope, entity_type, entity_id)
VALUES ('{USER}', 'read', 'workspace', '{WORKSPACE}'),
       ('{USER}', 'write', 'workspace', '{WORKSPACE}'),
       ('{USER}', 'admin', 'workspace', '{WORKSPACE}'),
       ('{USER}', 'read', 'organization', '{ORG}'),
       ('{USER}', 'write', 'organization', '{ORG}')
ON CONFLICT DO NOTHING;
INSERT INTO session_tokens (id, access_token_hash, refresh_token_hash, user_id, access_expires_at, refresh_expires_at)
VALUES ('00000000-0000-7000-8000-000000000007',
        encode(sha256(convert_to('{c["session"]}', 'UTF8')), 'hex'),
        encode(sha256(convert_to('refresh-{c["session"]}', 'UTF8')), 'hex'),
        '{USER}', NOW() + INTERVAL '1 day', NOW() + INTERVAL '1 day')
ON CONFLICT DO NOTHING;
""")
    (STATE / "cli/credentials.json").write_text(json.dumps({
        "Host": "http://loco-demo-api:8000", "Token": c["session"],
        "ExpiresAt": (dt.datetime.now(dt.UTC) + dt.timedelta(hours=23)).isoformat(),
        "RefreshToken": ""}))
    (STATE / "cli/config.toml").write_text(f"""currentScope = "default"
[scopes.default.organization]
name = "demo"
id = "{ORG}"
[scopes.default.workspace]
name = "astronomy-shop"
id = "{WORKSPACE}"
""")


def secret(namespace, name, values, secret_type="Opaque"):
    apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": namespace, "name": name},
           "type": secret_type, "stringData": values})


def setup_external_hosts():
    addresses = {}
    for name in ["api", "registry", "s3"]:
        addresses["loco-demo-" + name] = run([
            "docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
            "loco-demo-" + name]).strip()
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


def up():
    prepare()
    validate()
    STATE.mkdir(exist_ok=True)
    clusters = run(["kind", "get", "clusters"]).splitlines()
    if CLUSTER in clusters:
        raise RuntimeError("loco-demo already exists. Reuse it with deploy/verify, or run mise run down before up.")
    run(["kind", "create", "cluster", "--name", CLUSTER, "--image", VERSIONS["kindImage"],
         "--config", ROOT / "config/kind.yaml", "--kubeconfig", STATE / "kubeconfig"],
        timeout=READY_SECONDS, log=OUT / "kind.log")
    helm_install("loco-networking", CORE / "charts/loco-networking", "kube-system",
                 "-f", ROOT / "config/networking.yaml")
    for ns in ["loco-system", "observability", "observability-node", NAMESPACE]:
        apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})
    bootstrap_compose()
    seed()
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
    core = {"agent": {"image": image_values(VERSIONS["loco"]["images"]["agent"])},
            "env": {"CONTROL_PLANE_URL": "http://loco-demo-api:8000", "AGENT_TOKEN": c["agent"]}}
    write_yaml(STATE / "core.yaml", core)
    helm_install("loco-core", CORE / "charts/loco-core", "loco-system",
                 "-f", ROOT / "config/core.yaml", "-f", STATE / "core.yaml")
    wait_for("registered, healthy demo agent", lambda: sql(
        "SELECT count(*) FROM clusters WHERE name='loco-demo' AND health_status='healthy'") == "1")
    token = api("loco.token.v1.TokenService/CreateToken", {
        "name": "demo-observability", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": WORKSPACE,
        "scopes": [{"scope": "SCOPE_READ", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": WORKSPACE}],
        "expiresInSec": 86400})["token"]
    secret("observability", "loco-demo-proxy-auth", {"token": token})
    obs = {"clickhouseUserPasswords": {"loco_migrator": c["migrator"],
                                      "loco_ingest": c["ingest"], "loco_reader": c["reader"]},
           "obsProxy": {"image": image_values(VERSIONS["loco"]["images"]["obs-proxy"]) }}
    write_yaml(STATE / "observability.yaml", obs)
    helm_install("loco-obs", CORE / "charts/loco-obs", "observability",
                 "-f", ROOT / "config/observability.yaml", "-f", STATE / "observability.yaml")
    helm_install("astronomy", CACHE / "charts/opentelemetry-demo", NAMESPACE,
                 "-f", ROOT / "config/demo-infrastructure.yaml")
    print("Demo organization, workspace, user, builder worker, Loco charts, and shop infrastructure ready", flush=True)


def provision():
    resources = {}
    for name, meta in APPS.items():
        existing = sql(f"SELECT id FROM resources WHERE workspace_id='{WORKSPACE}' AND name='{name}'")
        if existing:
            resources[name] = existing
            continue
        config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())["services"][name]
        target = config["regions"]["us-east-1"]
        resources[name] = api("loco.resource.v1.ResourceService/CreateResource", {
            "workspaceId": WORKSPACE, "name": name, "type": "RESOURCE_TYPE_SERVICE",
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
    output = cli("deploy", "--file", f".state/work/{name}/loco.yaml",
                 "--yes", "--confirm-import", "--env", "production",
                 timeout=BUILD_SECONDS, log=OUT / f"deploy-{name}.log")
    return output[-1500:]


def deploy(names=None):
    resources = provision()
    for name in names or APPS:
        record("deploy:" + name, lambda name=name: deploy_one(name, resources))
    return all(r["status"] == "PASS" for r in RESULTS)


def ch(query):
    pods = json.loads(kube("-n", "observability", "get", "pods", "-l", "clickhouse.altinity.com/chi", "-o", "json"))
    pod = pods["items"][0]["metadata"]["name"]
    return kube("-n", "observability", "exec", pod, "-c", "clickhouse", "--", "clickhouse-client",
                "--user", "loco_reader", "--password", credentials()["reader"],
                "--database", "loco_obs", "--format", "JSONEachRow", "-q", query).strip()


def check_pods(name, resource_id):
    selector = f"loco.io/resource-id={resource_id}"
    wait_for(name + " readiness", lambda: all(
        any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
        for p in json.loads(kube("-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "json"))["items"]
    ) and bool(json.loads(kube("-n", NAMESPACE, "get", "pods", "-l", selector, "-o", "json"))["items"]),
        seconds=90)
    deployment = json.loads(kube("-n", NAMESPACE, "get", "deployment", "resource-" + resource_id, "-o", "json"))
    configured = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())["services"][name]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    if "health" not in configured:
        if "livenessProbe" in container or "readinessProbe" in container:
            raise RuntimeError(f"{name} omits health but Loco installed probes: " +
                               json.dumps({k: container[k] for k in ["livenessProbe", "readinessProbe"] if k in container}))
    return "Ready; configured health semantics preserved"


def check_builds(resources):
    rows = sql(f"SELECT r.name, b.status FROM builds b JOIN resources r ON r.id=b.resource_id WHERE r.workspace_id='{WORKSPACE}'")
    for name, meta in APPS.items():
        builds = [line.split("|")[1] for line in rows.splitlines() if line.split("|")[0] == name]
        if meta["mode"] == "source" and "succeeded" not in builds:
            raise RuntimeError(f"{name} has no succeeded cluster build: {builds}")
        if meta["mode"] == "image" and builds:
            raise RuntimeError(f"{name} should use a prebuilt image but created builds: {builds}")
    jobs = json.loads(kube("-n", "loco-builds", "get", "jobs", "-o", "json"))
    for job in jobs["items"]:
        if job["spec"]["template"]["spec"].get("nodeSelector", {}).get("loco.build/demo-builder") != "true":
            raise RuntimeError("Build job did not select the builder worker")
    return "Eight source apps have successful builds; eight image apps have no builds"


def traffic():
    resources = json.loads((STATE / "resources.json").read_text())
    trace_id = secrets.token_hex(16)
    user_id = "demo-" + uuid.uuid4().hex
    (STATE / "trace.json").write_text(json.dumps({"traceId": trace_id, "userId": user_id}))
    port = 18080
    log = (OUT / "frontend-forward.log").open("w")
    proc = subprocess.Popen(["kubectl", "--kubeconfig", str(STATE / "kubeconfig"), "--context", CONTEXT,
                             "-n", NAMESPACE, "port-forward", "service/resource-" + resources["frontend"],
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


def connected_trace(rows, resources):
    by_id = {row["SpanId"]: row for row in rows}
    frontend_spans = {row["SpanId"] for row in rows if row["ResourceId"] == resources["frontend"]}
    for name in ["checkout", "payment", "shipping"]:
        candidates = [row for row in rows if row["ResourceId"] == resources[name]]
        connected = False
        for span in candidates:
            parent = span["ParentSpanId"]
            visited = set()
            while parent in by_id and parent not in visited:
                if parent in frontend_spans:
                    connected = True
                    break
                visited.add(parent)
                parent = by_id[parent]["ParentSpanId"]
        if not connected:
            return False
    return bool(frontend_spans)


def check_telemetry():
    trace_id = json.loads((STATE / "trace.json").read_text())["traceId"]
    query = f"SELECT SpanId, ParentSpanId, ResourceId, ServiceName FROM otel_traces WHERE TraceId='{trace_id}' AND WorkspaceId='{WORKSPACE}'"
    resources = json.loads((STATE / "resources.json").read_text())
    def connected():
        rows = [json.loads(line) for line in ch(query).splitlines() if line]
        return connected_trace(rows, resources)
    wait_for("connected checkout trace with Loco workspace attribution", connected, TELEMETRY_SECONDS)
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    log_query = f"SELECT count(*) AS count FROM otel_logs WHERE WorkspaceId='{WORKSPACE}' AND ResourceId IN ('{resources['checkout']}', '{resources['email']}') AND position(concat(Body, toString(LogAttributes)), '{order_id}') > 0"
    metric_query = f"SELECT count(*) AS count FROM otel_metrics_sum WHERE WorkspaceId='{WORKSPACE}' AND ResourceId='{resources['payment']}' AND MetricName='demo.payment.transactions'"
    for name, sql_query in [("checkout order logs", log_query), ("payment transaction counter", metric_query)]:
        wait_for(name, lambda sql_query=sql_query: int(json.loads(ch(sql_query))["count"]) > 0, TELEMETRY_SECONDS)
    (OUT / "trace-evidence.jsonl").write_text(ch(query) + "\n")
    return "Connected frontend/checkout/payment/shipping spans, logs and counter metrics attributed to the demo workspace"


def check_kafka():
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    resources = json.loads((STATE / "resources.json").read_text())
    for name in ["accounting", "fraud-detection"]:
        def processed(name=name):
            return bool(ch(f"SELECT Body FROM otel_logs WHERE ResourceAttributes['loco.workspace.id']='{WORKSPACE}' AND ResourceAttributes['loco.resource.id']='{resources[name]}' AND position(Body, '{order_id}') > 0 LIMIT 1"))
        wait_for(name + " Kafka order consumption", processed, TELEMETRY_SECONDS)
    def persisted():
        return kube("-n", NAMESPACE, "exec", "deployment/astronomy-db", "--", "psql",
                    "-U", "astronomy_user", "-d", "astronomy_db", "-At", "-c",
                    f"SELECT count(*) FROM accounting.\"order\" WHERE order_id='{order_id}'").strip() == "1"
    wait_for("accounting order persistence", persisted, TELEMETRY_SECONDS)
    return "Both Kafka consumers record the order; accounting persists it in Postgres"


def verify():
    resources = json.loads((STATE / "resources.json").read_text())
    for name, resource_id in resources.items():
        record("health:" + name, lambda name=name, rid=resource_id: check_pods(name, rid), KNOWN["health"])
    record("build-matrix", lambda: check_builds(resources))
    if record("shop-checkout", traffic):
        record("telemetry", check_telemetry)
        record("kafka-consumers", check_kafka)
    else:
        RESULTS.append({"name": "telemetry", "status": "BLOCKED", "detail": "Checkout must pass first"})
        RESULTS.append({"name": "kafka-consumers", "status": "BLOCKED", "detail": "Checkout must pass first"})
    save_report()
    return all(r["status"] == "PASS" for r in RESULTS)


def diagnostics():
    OUT.mkdir(exist_ok=True)
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
        for ns in ["loco-system", "observability", "loco-builds", NAMESPACE]:
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
             "Core/charts commit: " + VERSIONS["loco"]["commit"],
             "Astronomy Shop: " + VERSIONS["upstream"]["release"], ""]
    for result in RESULTS:
        lines.extend([f"## {result['status']}: {result['name']}", "", str(result["detail"]), ""])
        if result.get("issue"):
            lines.extend(["Tracked in " + result["issue"], ""])
    (OUT / "evidence.md").write_text("\n".join(lines))


def report():
    data = json.loads((OUT / "results.json").read_text())
    for failure in data:
        if failure["status"] != "FAIL":
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

CLI: `{VERSIONS['loco']['cli']}`. Core and Helm chart source: `{VERSIONS['loco']['commit']}`.
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


def down():
    if (STATE / "compose.yaml").exists():
        compose("down", "--volumes", "--remove-orphans", check=False, timeout=120)
    run(["kind", "delete", "cluster", "--name", CLUSTER, "--kubeconfig", STATE / "kubeconfig"], check=False, timeout=120)
    if STATE.exists():
        shutil.rmtree(STATE)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare", "validate", "up", "deploy", "verify", "e2e", "down", "report"])
    parser.add_argument("services", nargs="*", choices=list(APPS))
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--report-issues", action="store_true")
    args = parser.parse_args()
    if args.services and args.command != "deploy":
        parser.error("Service selection is supported by deploy; e2e always tests all 16 apps")
    if args.command == "e2e":
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
            if not args.keep:
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
