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
CLI_PORT = 18000
DEX_PORT = 18056
READY_SECONDS = 600
TELEMETRY_SECONDS = 240
BUILD_SECONDS = 2100
RESULTS = []
KNOWN = {
    "health": "https://github.com/team-loco/loco/issues/711",
    "discovery": "https://github.com/team-loco/loco/issues/712",
    "image-flag": "https://github.com/team-loco/loco/issues/713",
    "enrollment": "https://github.com/team-loco/loco/issues/719",
    "trace-api": "https://github.com/team-loco/loco/issues/720",
    "metrics-api": "https://github.com/team-loco/loco/issues/722",
}


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


def rpc(url, procedure, data, token=None):
    headers = {"Content-Type": "application/json", "Connect-Protocol-Version": "1"}
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
        raise RuntimeError(f"{procedure}: {redact(exc.read().decode())}") from exc


def api(procedure, data, token="session"):
    bearer = credentials()["session"] if token == "session" else token
    return rpc(f"http://127.0.0.1:{CLI_PORT}", procedure, data, bearer)


def list_api(procedure, request, key):
    records = []
    request = dict(request)
    while True:
        response = api(procedure, request)
        records.extend(response.get(key, []))
        cursor = response.get("nextPageToken")
        if not cursor:
            return records
        request["pageToken"] = cursor


def list_builds(resource_id):
    return list_api("loco.build.v1.BuildService/ListBuilds", {"resourceId": resource_id, "pageSize": 200}, "builds")


def observability_rpc(method, request):
    access = api("loco.observability.v1.ObservabilityAccessService/GetObservabilityAccess",
                 {"workspaceId": workspace_id(), "resourceIds": request.get("resourceIds", [])})
    endpoints = [c["proxyUrl"] for c in access.get("clusters", []) if c.get("proxyUrl")]
    if not endpoints:
        raise PublicAPIGap("GetObservabilityAccess returns no usable proxy endpoint",
                           "https://github.com/team-loco/loco/issues/642")
    return rpc(endpoints[0], "loco.observability.v1.ObservabilityProxyService/" + method,
               request, credentials()["session"])


def wait_for(name, predicate, seconds=READY_SECONDS):
    deadline = time.monotonic() + seconds
    last = None
    while True:
        try:
            if predicate():
                return
        except PublicAPIGap:
            raise
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
    for name in ["loco-core", "loco-networking", "loco-obs"]:
        run(["helm", "dependency", "build", CORE / "charts" / name], timeout=READY_SECONDS)
    print("CLI checksums and release source verified; chart dependencies prepared", flush=True)


def native_cli():
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return CACHE / "bin" / VERSIONS["loco"]["cli"] / f"loco-{platform.system().lower()}-{arch}"


def cli(*args, **kwargs):
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    return run([
        "docker", "run", "--rm", "--network", "kind",
        "--mount", f"type=bind,source={ROOT},target=/demo",
        "--mount", f"type=bind,source={STATE / 'cli'},target=/root/.loco",
        "--workdir", "/demo", "--env", "LOCO_CREDENTIAL_STORE=file",
        "--env", "LOCO_HOST=http://loco-demo-api:8000",
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
                ["database", "migrator", "ingest", "reader", "secretkey"]}
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
        "AUTH_ISSUERS": json.dumps([{"issuer": "http://loco-demo-dex:5556/dex", "audience": "loco"}]),
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
    dex = yaml.safe_load((CORE / "env/conformance/dex.yaml").read_text())
    dex["issuer"] = "http://loco-demo-dex:5556/dex"
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
    compose("up", "-d", "postgres", "registry", "s3", "dex", "api", timeout=READY_SECONDS)
    wait_for("API migrations and health", lambda: urllib.request.urlopen(
        f"http://127.0.0.1:{CLI_PORT}/health", timeout=5).status == 200)
    wait_for("OIDC provider", lambda: urllib.request.urlopen(
        f"http://127.0.0.1:{DEX_PORT}/dex/.well-known/openid-configuration", timeout=5).status == 200)


def create_identity():
    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "loco", "client_secret": "loco-conformance",
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
        "Host": "http://loco-demo-api:8000", "Token": c["session"],
        "ExpiresAt": (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=int(tokens["expiresIn"]))).isoformat(),
        "RefreshToken": c["refresh"]}))
    (STATE / "cli/config.toml").write_text(f"""currentScope = "default"
[scopes.default.organization]
name = "demo"
id = "{org_id}"
[scopes.default.workspace]
name = "astronomy-shop"
id = "{ws_id}"
""")
    (OUT / "identity-api.json").write_text(json.dumps(data, indent=2) + "\n")
    print("OIDC login, CLI token exchange, organization and workspace API calls passed", flush=True)


def require_cluster_enrollment():
    raise PublicAPIGap(
        "The pinned release has no public cluster enrollment or agent credential issuance API. "
        "AgentService/Register authenticates an already-enrolled cluster. Fresh setup stops here.",
        KNOWN["enrollment"])


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
    for ns in ["loco-system", "observability", "observability-node"]:
        apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})
    bootstrap_compose()
    create_identity()
    require_cluster_enrollment()
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
    wait_for("registered, healthy demo agent", lambda: any(
        r.get("region") == "us-east-1" and r.get("healthStatus") == "healthy"
        for r in api("loco.resource.v1.ResourceService/ListRegions", {}).get("regions", [])))
    token = api("loco.token.v1.TokenService/CreateToken", {
        "name": "demo-observability", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": workspace_id(),
        "scopes": [{"scope": "SCOPE_READ", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": workspace_id()}],
        "expiresInSec": 86400})["token"]
    secret("observability", "loco-demo-proxy-auth", {"token": token})
    obs = {"clickhouseUserPasswords": {"loco_migrator": c["migrator"],
                                      "loco_ingest": c["ingest"], "loco_reader": c["reader"]},
           "obsProxy": {"image": image_values(VERSIONS["loco"]["images"]["obs-proxy"]) }}
    write_yaml(STATE / "observability.yaml", obs)
    helm_install("loco-obs", CORE / "charts/loco-obs", "observability",
                 "-f", ROOT / "config/observability.yaml", "-f", STATE / "observability.yaml")
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
    output = cli("deploy", "--file", f".state/work/{name}/loco.yaml",
                 "--yes", "--confirm-import", "--env", "production",
                 timeout=BUILD_SECONDS, log=OUT / f"deploy-{name}.log")
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
    request = {"workspaceId": workspace_id(), "resourceIds": resource_ids,
               "startTime": (now - dt.timedelta(minutes=10)).isoformat(), "endTime": now.isoformat(),
               "limit": 200, "order": "LOG_ORDER_NEWEST_FIRST"}
    entries = []
    while True:
        response = observability_rpc("QueryLogs", request)
        entries.extend(response.get("entries", []))
        cursor = response.get("nextCursor")
        if not cursor:
            return entries
        request["cursor"] = cursor


def order_in_logs(resource_ids, order_id):
    entries = query_logs(resource_ids)
    if any(entry["resourceId"] not in resource_ids for entry in entries):
        raise RuntimeError("QueryLogs returned a resource outside the requested scope")
    return any(order_id in entry.get("body", "") + json.dumps(entry.get("logAttributes", {}))
               for entry in entries)


def check_logs():
    resources = json.loads((STATE / "resources.json").read_text())
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    ids = [resources[n] for n in ["checkout", "email"]]
    wait_for("checkout order logs through QueryLogs", lambda: order_in_logs(ids, order_id), TELEMETRY_SECONDS)
    (OUT / "logs-api.json").write_text(redact(json.dumps(query_logs(ids), indent=2)) + "\n")
    return "QueryLogs returns the checkout order with the requested resource scope"


def check_metrics():
    resources = json.loads((STATE / "resources.json").read_text())
    def observed():
        now = dt.datetime.now(dt.UTC)
        response = observability_rpc("QueryMetrics", {
            "workspaceId": workspace_id(), "resourceIds": [resources["payment"]],
            "environmentId": identity()["environmentId"], "metricName": "demo.payment.transactions",
            "startTime": (now - dt.timedelta(minutes=10)).isoformat(), "endTime": now.isoformat(),
            "intervalSeconds": 60, "aggregation": "max"})
        series = response.get("series", [])
        if any(s["resourceId"] != resources["payment"] for s in series):
            raise RuntimeError("QueryMetrics returned a resource outside the requested scope")
        found = any(float(p["value"]) > 0 for s in series for p in s.get("points", []))
        if found:
            (OUT / "metrics-api.json").write_text(json.dumps(response, indent=2) + "\n")
        return found
    wait_for("payment transaction counter through QueryMetrics", observed, TELEMETRY_SECONDS)
    return "QueryMetrics returns payment counter points in the demo environment"


def check_traces():
    raise PublicAPIGap("The pinned ObservabilityProxyService exposes no trace-query API; "
                       "connected distributed traces cannot be verified through the public interface",
                       KNOWN["trace-api"])


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
        record("health:" + name, lambda name=name, rid=resource_id: check_resource(name, rid), KNOWN["health"])
    record("build-matrix", lambda: check_builds(resources))
    if record("shop-checkout", traffic):
        record("logs-api", check_logs)
        record("metrics-api", check_metrics, KNOWN["metrics-api"])
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
        for ns in ["loco-system", "observability", "loco-builds"] + ([app_namespace()] if (STATE / "identity.json").exists() else []):
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
