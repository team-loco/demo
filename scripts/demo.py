import argparse
import contextlib
import copy
import datetime as dt
import hashlib
import html.parser
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
CONFIG = ROOT / "config"
VERSIONS = json.loads((ROOT / "versions.json").read_text())
LOCO_FILE = ROOT / "loco.yaml"
SERVICES = yaml.safe_load(LOCO_FILE.read_text())["services"]
LOCO_VERSION = VERSIONS["loco"]
INSTALLATION = yaml.safe_load((CONFIG / "installation.yaml").read_text())
CONTROL_PLANE = INSTALLATION["controlPlane"]
WORKER = INSTALLATION["workers"][0]
API_URL = CONTROL_PLANE["apiURL"]
REGION = WORKER["region"]
CLUSTER = "loco-demo"
PROJECT = "loco-demo"
CONTEXT = "kind-" + CLUSTER
DOMAIN = "loco-demo.localhost"
FIXTURES = ["postgres", "registry", "s3", "dex"]
DEMO_EMAIL = "demo@example.test"
DEMO_PASSWORD = "demo-password"
# The first shopper in upstream's load-generator people.json.
CUSTOMER = {
    "email": "larry_sergei@example.com",
    "address": {"streetAddress": "1600 Amphitheatre Parkway", "zipCode": "94043", "city": "Mountain View",
                "state": "CA", "country": "United States"},
    "userCurrency": "USD",
    "creditCard": {"creditCardNumber": "4432-8015-6152-0454", "creditCardExpirationMonth": 1,
                   "creditCardExpirationYear": 2039, "creditCardCvv": 672},
}
# Defaults of the loco-worker chart.
WORKSPACE_NAMESPACE_PREFIX = "loco-ws-"
READY_SECONDS = 600
INSTALL_SECONDS = 2400
TELEMETRY_SECONDS = 240
PROGRESS_SECONDS = 15
STALL_SECONDS = 120
RETRYABLE = (RuntimeError, OSError, IndexError, KeyError)
RESULTS = []
INFRA_STARTED = False


class RPCError(RuntimeError):
    def __init__(self, procedure, status, payload):
        self.status = status
        self.code = payload.get("code", "unknown")
        super().__init__(f"{procedure}: HTTP {status}: {redact(json.dumps(payload))}")


def identity():
    return json.loads((STATE / "identity.json").read_text())


def workspace_id():
    return identity()["workspaceId"]


def app_namespace():
    return WORKSPACE_NAMESPACE_PREFIX + workspace_id()


def resources():
    return json.loads((STATE / "resources.json").read_text())


def run(args, *, input=None, check=True, timeout=READY_SECONDS, log=None, stream=False, env=None):
    args = [str(a) for a in args]
    env = {**os.environ, **(env or {})}
    if log:
        Path(log).parent.mkdir(parents=True, exist_ok=True)
    if stream:
        result = run_streamed(args, timeout=timeout, env=env, log=log)
    else:
        result = subprocess.run(args, cwd=ROOT, input=input, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout, env=env)
        if log:
            Path(log).write_text(redact(result.stdout))
    if check and result.returncode:
        raise RuntimeError(f"{Path(args[0]).name} {' '.join(args[1:3])} failed (exit {result.returncode}):\n"
                           f"{redact(result.stdout[-6000:])}")
    return result.stdout if check else result


def run_streamed(args, *, timeout, env, log):
    output = []
    with Path(log).open("w") as logfile, subprocess.Popen(
            args, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            start_new_session=True) as process:
        def read_output():
            for line in process.stdout:
                output.append(line)
                print(redact(line), end="", flush=True)
                logfile.write(redact(line))
                logfile.flush()
        reader = threading.Thread(target=read_output)
        reader.start()
        started = time.monotonic()
        while True:
            try:
                process.wait(timeout=max(0, min(PROGRESS_SECONDS, started + timeout - time.monotonic())))
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() >= started + timeout:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    reader.join()
                    raise subprocess.TimeoutExpired(args[0], timeout, output=redact("".join(output))) from None
                print(f"RUN {Path(log).stem} ({time.monotonic() - started:.0f}s elapsed)", flush=True)
        reader.join()
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


def secret_values():
    values = []
    for path, keys in [(STATE / "secrets.json", None), (credentials_path(), ["token", "refreshToken"])]:
        if path.exists():
            data = json.loads(path.read_text())
            values.extend(data[key] for key in keys or data if isinstance(data.get(key), str))
    return [value for value in values if len(value) >= 16]


def redact(text):
    for value in secret_values():
        text = text.replace(value, "[REDACTED]")
    return text


def write_yaml(path, data):
    Path(path).write_text(yaml.safe_dump(data, sort_keys=False))


def kube(*args, **kwargs):
    return run(["kubectl", "--kubeconfig", STATE / "kubeconfig", "--context", CONTEXT, *args], **kwargs)


def compose(*args, **kwargs):
    return run(["docker", "compose", "-p", PROJECT, "-f", CONFIG / "fixtures.yaml", *args], **kwargs)


def binary(tool):
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine().lower())
    if arch is None:
        raise RuntimeError(f"Unsupported architecture: {platform.machine()}")
    return CACHE / "bin" / LOCO_VERSION / f"{tool}-{platform.system().lower()}-{arch}"


def release_asset(name):
    return f"https://github.com/team-loco/loco/releases/download/v{LOCO_VERSION}/{name}"


def download_release_binaries(checksums):
    for tool in ["loco", "locoadm"]:
        dest = binary(tool)
        if dest.name not in checksums:
            raise RuntimeError(f"Loco {LOCO_VERSION} publishes no {dest.name}")
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(release_asset(dest.name), timeout=300) as response:
                dest.write_bytes(response.read())
        if hashlib.sha256(dest.read_bytes()).hexdigest() != checksums[dest.name]:
            dest.unlink()
            raise RuntimeError(f"{dest.name} does not match the checksum Loco {LOCO_VERSION} publishes")
        dest.chmod(0o755)


def cli_env():
    # A file store keeps the demo session out of the OS keychain.
    return {"HOME": str(STATE / "home"), "LOCO_CREDENTIAL_STORE": "file", "KUBECONFIG": str(STATE / "kubeconfig")}


def credentials_path():
    return STATE / "home/.loco/credentials.json"


def loco(*args, log=None, timeout=READY_SECONDS):
    return run([binary("loco"), *args], env=cli_env(), log=log, timeout=timeout, stream=log is not None)


def locoadm(*args, log, check=True, timeout=INSTALL_SECONDS):
    return run([binary("locoadm"), *args], env=cli_env(), log=OUT / f"locoadm-{log}.log",
               check=check, timeout=timeout, stream=True)


def prepare():
    with urllib.request.urlopen(release_asset("checksums.txt"), timeout=60) as response:
        lines = response.read().decode().splitlines()
    download_release_binaries({line.split()[-1]: line.split()[0] for line in lines if line.strip()})
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
    print(f"Loco {LOCO_VERSION} CLIs match the release checksums; upstream chart prepared", flush=True)


def installation(cluster_uid):
    doc = copy.deepcopy(INSTALLATION)
    doc["release"] = {"version": LOCO_VERSION}
    for target in [doc["controlPlane"], *doc["workers"]]:
        target["kubernetes"]["clusterUID"] = cluster_uid
    return doc


def validate():
    result = run([binary("loco"), "infra", "validate", LOCO_FILE], check=False)
    if result.returncode:
        raise RuntimeError(result.stdout)
    rendered = run(["helm", "template", "astronomy", CACHE / "charts" / "opentelemetry-demo",
                    "--namespace", "demo-chart-validation", "-f", CONFIG / "demo-infrastructure.yaml"])
    assert_infrastructure_only([d for d in yaml.safe_load_all(rendered) if d])
    (OUT / "demo-infrastructure.yaml").write_text(rendered)
    write_yaml(STATE / "installation-check.yaml", installation(str(uuid.uuid4())))
    run([binary("locoadm"), "validate", "--file", STATE / "installation-check.yaml"])
    print("Validated loco.yaml, the infrastructure-only chart and installation.yaml", flush=True)


def assert_infrastructure_only(docs):
    workloads = [d for d in docs if d.get("kind") in ("Deployment", "DaemonSet", "StatefulSet")]
    names = {d["metadata"]["name"] for d in workloads}
    if names != {"flagd", "kafka", "valkey-cart", "astronomy-db"}:
        raise RuntimeError(f"Unexpected upstream workloads: {sorted(names)}")


def fixture_addresses():
    return {name: run(["docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "kind").IPAddress}}',
                       f"{PROJECT}-{name}"]).strip() for name in FIXTURES}


def cluster_hosts(addresses, gateway):
    hosts = {addresses[name]: [f"{name}.{DOMAIN}"] for name in FIXTURES}
    storage = urllib.parse.urlsplit(CONTROL_PLANE["sourceBucket"]["endpoint"]).hostname
    for bucket in [CONTROL_PLANE["sourceBucket"]["bucket"], CONTROL_PLANE["openbao"]["snapshots"]["bucket"]]:
        hosts[addresses["s3"]].append(f"{bucket}.{storage}")
    hosts[gateway] = [urllib.parse.urlsplit(CONTROL_PLANE[key]).hostname for key in ["apiURL", "webURL"]]
    hosts[gateway].append(urllib.parse.urlsplit(CONTROL_PLANE["observability"]["ingestURL"]).hostname)
    return hosts


def configure_cluster_dns(hosts):
    cm = json.loads(kube("-n", "kube-system", "get", "configmap", "coredns", "-o", "json"))
    entries = "\n".join(f"        {ip} {' '.join(names)}" for ip, names in hosts.items())
    cm["data"]["Corefile"] = cm["data"]["Corefile"].replace(
        "    kubernetes cluster.local", f"    hosts {{\n{entries}\n        fallthrough\n    }}\n    kubernetes cluster.local", 1)
    kube("apply", "-f", "-", input=json.dumps(cm))
    kube("-n", "kube-system", "rollout", "restart", "deployment/coredns")
    kube("-n", "kube-system", "rollout", "status", "deployment/coredns", "--timeout=120s")


def start_fixtures():
    with step("Start PostgreSQL, registry, S3 storage and Dex"):
        compose("up", "-d", timeout=READY_SECONDS, log=OUT / "fixtures.log", stream=True)
        discovery = CONTROL_PLANE["auth"]["issuer"] + "/.well-known/openid-configuration"
        wait_for("Dex discovery", lambda: urllib.request.urlopen(discovery, timeout=5).status == 200, 30)
    addresses = fixture_addresses()
    with step("Route fixture and Loco hostnames inside the cluster"):
        kube("apply", "-f", CONFIG / "inputs.yaml", "-f", CONFIG / "gateway.yaml")
        gateway = kube("-n", "loco-system", "get", "service", "loco-demo-gateway", "-o", "jsonpath={.spec.clusterIP}")
        configure_cluster_dns(cluster_hosts(addresses, gateway.strip()))


class LoginForm(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.action = None
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form" and self.action is None:
            self.action = attrs.get("action") or ""
        elif tag == "input" and attrs.get("name"):
            self.fields[attrs["name"]] = attrs.get("value") or ""


def sign_in(authorization_url):
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
    with opener.open(authorization_url, timeout=30) as page:
        form = LoginForm()
        form.feed(page.read().decode())
        login_url = page.geturl()
    if form.action is None or not {"login", "password"} <= form.fields.keys():
        raise RuntimeError("Dex did not offer its password login form")
    fields = {**form.fields, "login": DEMO_EMAIL, "password": DEMO_PASSWORD}
    with opener.open(urllib.parse.urljoin(login_url, form.action),
                     data=urllib.parse.urlencode(fields).encode(), timeout=30) as callback:
        if callback.geturl().split("?")[0] != CONTROL_PLANE["auth"]["bootstrapRedirectURI"]:
            raise RuntimeError("Dex sign-in did not return to locoadm's callback")


def bootstrap(file):
    args = [binary("locoadm"), "bootstrap", "--file", file, "--email", DEMO_EMAIL, "--no-browser"]
    with step("locoadm bootstrap"):
        process = subprocess.Popen([str(a) for a in args], env={**os.environ, **cli_env()},
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        output = []
        try:
            for line in process.stdout:
                output.append(line)
                print(line, end="", flush=True)
                if line.startswith(("http://", "https://")):
                    sign_in(line.strip())
                    break
            rest, _ = process.communicate(timeout=READY_SECONDS)
            output.append(rest)
            print(rest, end="", flush=True)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            (OUT / "locoadm-bootstrap.log").write_text(redact("".join(output)))
        if process.returncode:
            raise RuntimeError("locoadm bootstrap failed:\n" + redact("".join(output)[-3000:]))


def install_loco(file):
    locoadm("validate", "--file", file, log="validate")
    first = locoadm("apply", "--file", file, log="apply-1", check=False)
    if first.returncode == 0 or "certificate issuer" not in first.stdout:
        raise RuntimeError("The first apply should stop at the certificate issuer checkpoint:\n" + first.stdout[-3000:])
    with step("Create the self-signed ClusterIssuer apply asked for"):
        kube("apply", "-f", CONFIG / "issuer.yaml")
        kube("wait", "--for=condition=Ready", "clusterissuer/loco-demo-selfsigned", "--timeout=120s")
    locoadm("apply", "--file", file, log="apply-2")
    bootstrap(file)
    locoadm("apply", "--file", file, log="apply-3")
    status = json.loads(locoadm("status", "--file", file, log="status"))
    (OUT / "installation-status.json").write_text(json.dumps(status, indent=2) + "\n")
    if status["phase"] != "running" or not all(worker["ready"] for worker in status["workers"]):
        raise RuntimeError("locoadm status does not report a running installation: " + json.dumps(status))
    with urllib.request.urlopen(CONTROL_PLANE["webURL"], timeout=30) as response:
        if "<html" not in response.read().decode().lower():
            raise RuntimeError("The dashboard did not serve its page")


def up():
    global INFRA_STARTED
    with step("Verify release binaries and application configuration"):
        prepare()
        validate()
    if CLUSTER in run(["kind", "get", "clusters"]).splitlines():
        raise RuntimeError("loco-demo already exists. Reuse it with deploy/verify, or run mise run down before up.")
    containers = set(run(["docker", "ps", "-a", "--format", "{{.Names}}"]).splitlines())
    collisions = containers.intersection(f"{PROJECT}-{name}" for name in FIXTURES)
    if collisions:
        raise RuntimeError("Existing demo containers must be removed by their owner: " + ", ".join(sorted(collisions)))
    write_yaml(STATE / "owner.yaml", {"root": str(ROOT)})
    INFRA_STARTED = True
    with step("Create kind cluster"):
        run(["kind", "create", "cluster", "--name", CLUSTER, "--image", VERSIONS["kindImage"],
             "--config", CONFIG / "kind.yaml", "--kubeconfig", STATE / "kubeconfig"],
            timeout=READY_SECONDS, log=OUT / "kind.log", stream=True)
    start_fixtures()
    uid = kube("get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}").strip()
    write_yaml(STATE / "installation.yaml", installation(uid))
    install_loco(STATE / "installation.yaml")
    print(f"Loco is running. Dashboard: {CONTROL_PLANE['webURL']} ({DEMO_EMAIL} / {DEMO_PASSWORD})", flush=True)


def rpc(procedure, data, token=None):
    headers = {"Content-Type": "application/json", "Connect-Protocol-Version": "1"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(f"{API_URL}/{procedure}", data=json.dumps(data).encode(), headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"message": body}
        raise RPCError(procedure, exc.code, payload) from exc


def session_token():
    stored = json.loads(credentials_path().read_text())
    now = dt.datetime.now(dt.UTC)
    if dt.datetime.fromisoformat(stored["expiresAt"]) - now > dt.timedelta(minutes=5):
        return stored["token"]
    tokens = rpc("loco.auth.v1.AuthService/RefreshCLIToken", {"refreshToken": stored["refreshToken"]})["tokens"]
    lifetime = dt.timedelta(seconds=int(tokens["expiresIn"])) - dt.timedelta(minutes=10)
    stored.update(token=tokens["accessToken"], refreshToken=tokens["refreshToken"], expiresAt=(now + lifetime).isoformat())
    credentials_path().write_text(json.dumps(stored))
    return stored["token"]


def api(procedure, data, token=None):
    return rpc(procedure, data, token or session_token())


def paginate(fetch, request, key, next_field="nextPageToken", cursor_field="pageToken"):
    records, seen, request = [], set(), dict(request)
    while True:
        response = fetch(request)
        records.extend(response.get(key, []))
        cursor = response.get(next_field)
        if not cursor:
            return records
        if cursor in seen:
            raise RuntimeError(f"{key}: repeated pagination cursor")
        seen.add(cursor)
        request[cursor_field] = cursor


def list_api(procedure, request, key):
    return paginate(lambda page: api(procedure, page), request, key)


def created_id(output):
    match = re.search(r"ID: ([0-9a-f-]{36})", output)
    if not match:
        raise RuntimeError("The CLI did not report the created ID: " + output)
    return match.group(1)


def create_workspace():
    if (STATE / "identity.json").exists():
        return
    with step("Create the demo organization and workspace"):
        org_id = created_id(loco("org", "create", "demo"))
        ws_id = created_id(loco("workspace", "create", "astronomy-shop", "--org-id", org_id))
        loco("use", f"{org_id}/{ws_id}")
        environments = api("loco.environment.v1.EnvironmentService/ListEnvironments", {"workspaceId": ws_id})
        env_id = next(e["id"] for e in environments.get("environments", []) if e["name"] == "production")
        telemetry = api("loco.token.v1.TokenService/CreateToken", {
            "name": "demo-observability", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": ws_id,
            "scopes": [{"scope": "SCOPE_READ", "entityType": "ENTITY_TYPE_WORKSPACE", "entityId": ws_id}],
            "expiresInSec": 86400,
        })["token"]
    (STATE / "secrets.json").write_text(json.dumps({"telemetry": telemetry}))
    (STATE / "secrets.json").chmod(0o600)
    data = {"orgId": org_id, "workspaceId": ws_id, "environmentId": env_id}
    (STATE / "identity.json").write_text(json.dumps(data))
    (OUT / "identity-api.json").write_text(json.dumps(data, indent=2) + "\n")


def record_resources():
    listed = list_api("loco.resource.v1.ResourceService/ListWorkspaceResources",
                      {"workspaceId": workspace_id(), "pageSize": 200}, "resources")
    ids = {resource["name"]: resource["id"] for resource in listed if resource["name"] in SERVICES}
    (STATE / "resources.json").write_text(json.dumps(ids, indent=2))


def deploy_services():
    with step("Deploy the 16 services"):
        output = loco("deploy", "--file", LOCO_FILE, "--yes", "--confirm-import", "--env", "production",
                      log=OUT / "deploy.log")
    return output[-1500:]


def deploy():
    create_workspace()
    with step("Install the shop's Kafka, Valkey, PostgreSQL and flagd"):
        run(["helm", "upgrade", "--install", "astronomy", CACHE / "charts/opentelemetry-demo",
             "--namespace", app_namespace(), "--create-namespace", "-f", CONFIG / "demo-infrastructure.yaml",
             "--kubeconfig", STATE / "kubeconfig", "--kube-context", CONTEXT, "--wait", "--timeout", "10m"],
            timeout=READY_SECONDS + 120, log=OUT / "helm-astronomy.log", stream=True)
    record("deploy", deploy_services)
    record_resources()
    return all(r["status"] == "PASS" for r in RESULTS)


def wait_for(name, predicate, seconds):
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
        except RETRYABLE as exc:
            last = exc
        now = time.monotonic()
        if now >= deadline:
            raise RuntimeError(redact(f"Timed out waiting for {name}. {last or ''}"))
        if now >= next_update:
            detail = f"; latest error: {redact(str(last))}" if last else ""
            print(f"WAIT {name} ({now - started:.0f}s elapsed){detail}", flush=True)
            next_update = now + PROGRESS_SECONDS
        time.sleep(3)


def record(name, fn):
    print(f"CHECK {name}", flush=True)
    started = time.monotonic()
    try:
        return save_result(name, "PASS", fn() or "", started)
    except Exception as exc:
        return save_result(name, "FAIL", str(exc), started)


def save_result(name, status, detail, started):
    result = {"name": name, "status": status, "detail": redact(detail),
              "seconds": round(time.monotonic() - started, 2)}
    RESULTS.append(result)
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2) + "\n")
    print(f"{result['status']} {name}: {result['detail']}", flush=True)
    return status == "PASS"


def observability_rpc(method, request, token=None):
    scoped = dict(request)
    if method == "QueryLogs":
        scoped["regions"] = [REGION]
    elif method == "QueryMetrics":
        scoped["region"] = REGION
    return api("loco.observability.v1.ObservabilityService/" + method, scoped,
               token=token or json.loads((STATE / "secrets.json").read_text())["telemetry"])


def get_resource(resource_id):
    return api("loco.resource.v1.ResourceService/GetResource", {"resourceId": resource_id})["resource"]


def check_health_semantics(name, resource_id):
    reported = get_resource(resource_id).get("spec", {}).get("service", {}).get("healthCheck")
    if "health" not in SERVICES[name] and reported:
        raise RuntimeError(f"{name} omits health but GetResource reports an HTTP health check: {json.dumps(reported)}")


def resource_healthy(resource_id):
    environments = [e for e in get_resource(resource_id).get("environmentStatuses", [])
                    if e["environmentId"] == identity()["environmentId"]]
    return bool(environments) and all(e.get("status") == "RESOURCE_STATUS_HEALTHY" for e in environments)


def namespace_pods(namespace):
    return json.loads(kube("-n", namespace, "get", "pods", "-o", "json", timeout=30))["items"]


def scheduling_failure(pod):
    for condition in pod["status"].get("conditions", []):
        # The scheduler writes the message of its latest FailedScheduling event here.
        if condition["type"] == "PodScheduled" and condition.get("reason") == "Unschedulable":
            return condition.get("message", "").split(" preemption:")[0]
    return None


def pod_reason(pod):
    status = pod["status"]
    parts = [failure] if (failure := scheduling_failure(pod)) else []
    for container in status.get("initContainerStatuses", []) + status.get("containerStatuses", []):
        state = container.get("state", {})
        reason = (state.get("waiting") or state.get("terminated") or {}).get("reason")
        if reason == "Completed" or (not reason and container.get("ready")):
            continue
        reason = reason or "not ready"
        last = container.get("lastState", {}).get("terminated", {}).get("reason")
        if last and last != reason:
            reason += ", last " + last
        parts.append(f"{container['name']} {reason}, restarts {container.get('restartCount', 0)}")
    return status.get("phase", "Unknown") + (": " + "; ".join(parts) if parts else "")


def pod_reasons(ids):
    """Describes each service's pods, and names the services whose every pod the scheduler cannot place."""
    pods = {name: [] for name in ids}
    for pod in namespace_pods(app_namespace()):
        for name, resource_id in ids.items():
            if pod["metadata"]["name"].startswith(f"resource-{resource_id}-"):
                pods[name].append(pod)
    reasons = {name: "; ".join(dict.fromkeys(map(pod_reason, found))) or "no pods" for name, found in pods.items()}
    unschedulable = {name for name, found in pods.items() if found and all(map(scheduling_failure, found))}
    return reasons, unschedulable


def check_health(ids):
    """Waits for every service under one deadline and records a health result for each."""
    started = time.monotonic()
    deadline = started + READY_SECONDS
    pending, errors = {}, {}
    for name, resource_id in ids.items():
        try:
            check_health_semantics(name, resource_id)
            pending[name] = resource_id
        except Exception as exc:
            save_result("health:" + name, "FAIL", str(exc), started)
    print(f"WAIT {len(pending)} services healthy through GetResource (timeout {READY_SECONDS}s)", flush=True)
    next_update, stalled, stalled_since = started + PROGRESS_SECONDS, None, None
    while pending:
        for name, resource_id in list(pending.items()):
            try:
                if resource_healthy(resource_id):
                    del pending[name]
                    save_result("health:" + name, "PASS",
                                "GetResource reports healthy with the configured health semantics", started)
            except RETRYABLE as exc:
                errors[name] = exc
        now = time.monotonic()
        if not pending:
            break
        if now < next_update and now < deadline:
            time.sleep(3)
            continue
        print(f"WAIT {len(ids) - len(pending)}/{len(ids)} healthy ({now - started:.0f}s elapsed); "
              f"waiting for {', '.join(pending)}", flush=True)
        next_update = now + PROGRESS_SECONDS
        try:
            reasons, unschedulable = pod_reasons(pending)
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
            reasons, unschedulable = dict.fromkeys(pending, f"pod status unavailable: {exc}"), set()
        if unschedulable != set(pending):
            stalled = None
        elif reasons != stalled:
            stalled, stalled_since = reasons, now
        stuck = {name for name, reason in reasons.items() if "OOMKilled" in reason or "ImagePullBackOff" in reason}
        if now >= deadline or (stalled and now - stalled_since >= STALL_SECONDS):
            stuck = set(pending)
        for name in stuck:
            error = f"; latest error: {errors[name]}" if name in errors else ""
            save_result("health:" + name, "FAIL", reasons[name] + error, started)
            del pending[name]


def traffic():
    baseline, _ = payment_counter()
    (STATE / "metric-baseline.json").write_text(json.dumps({"value": baseline}))
    user_id = "demo-" + uuid.uuid4().hex
    port = 18080
    log = (OUT / "frontend-forward.log").open("w")
    proc = subprocess.Popen(["kubectl", "--kubeconfig", str(STATE / "kubeconfig"), "--context", CONTEXT,
                             "-n", app_namespace(), "port-forward", "service/frontend", f"{port}:{SERVICES['frontend']['port']}"],
                            stdout=log, stderr=subprocess.STDOUT)
    def request(path, body=None, trace_id=None):
        request_trace = trace_id or secrets.token_hex(16)
        req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
              data=json.dumps(body).encode() if body is not None else None,
              headers={"Content-Type": "application/json", "traceparent": f"00-{request_trace}-{secrets.token_hex(8)}-01",
                       "baggage": "synthetic_request=true"})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)
    try:
        wait_for("frontend port forward", lambda: request("/api/products"), seconds=30)
        products = request("/api/products")
        cart = request("/api/cart", {"userId": user_id, "item": {"productId": products[0]["id"], "quantity": 1}})
        if not cart.get("items"):
            raise RuntimeError("Add-to-cart response contains no items")
        person = {**CUSTOMER, "userId": user_id}
        if not request("/api/recommendations?currencyCode=USD"):
            raise RuntimeError("Recommendation service returned no products")
        order = {}
        def checkout():
            trace_id = secrets.token_hex(16)
            order.update(request("/api/checkout?currencyCode=USD", person, trace_id), traceId=trace_id)
            return bool(order.get("orderId"))
        # Payment connects to flagd on its first charge and can miss that request's deadline.
        wait_for("checkout", checkout, seconds=60)
        (STATE / "order.json").write_text(json.dumps(order))
        result = {"orderId": order["orderId"], "traceId": order["traceId"]}
        (OUT / "checkout.json").write_text(json.dumps(result))
        return f"order {result['orderId']}, trace {result['traceId']}"
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        log.close()


def time_range(minutes=10):
    now = dt.datetime.now(dt.UTC)
    return {"startTime": (now - dt.timedelta(minutes=minutes)).isoformat(), "endTime": now.isoformat()}


def query_logs(resource_ids):
    request = {"workspaceId": workspace_id(), "environmentId": identity()["environmentId"],
               "resourceIds": resource_ids, **time_range(), "limit": 200, "order": "LOG_ORDER_NEWEST_FIRST"}
    entries = paginate(lambda page: observability_rpc("QueryLogs", page), request, "entries", "nextCursor", "cursor")
    if any(entry["resourceId"] not in resource_ids for entry in entries):
        raise RuntimeError("QueryLogs returned a resource outside the requested scope")
    return entries


def order_log_sources(resource_ids, order_id):
    sources = set()
    for entry in query_logs(resource_ids):
        attributes = entry.get("logAttributes", {})
        if order_id not in entry.get("body", "") + json.dumps(attributes):
            continue
        sources.add(attributes.get("log.iostream", "file") if attributes.get("log.file.path") else "otlp")
    return sources


def check_logs():
    ids = [resources()[n] for n in ["checkout", "email"]]
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    wait_for("checkout OTLP and stdout order logs through QueryLogs",
             lambda: {"otlp", "stdout"}.issubset(order_log_sources(ids, order_id)), TELEMETRY_SECONDS)
    (OUT / "logs-api.json").write_text(redact(json.dumps(query_logs(ids), indent=2)) + "\n")
    return "QueryLogs returns the same checkout order from OTLP and container stdout in the requested scope"


def check_kafka():
    order_id = json.loads((STATE / "order.json").read_text())["orderId"]
    for name in ["accounting", "fraud-detection"]:
        wait_for(name + " Kafka order consumption through QueryLogs",
                 lambda name=name: bool(order_log_sources([resources()[name]], order_id)), TELEMETRY_SECONDS)
    return "QueryLogs confirms both Kafka consumers recorded the checkout order"


def payment_counter():
    payment = resources()["payment"]
    response = observability_rpc("QueryMetrics", {
        "workspaceId": workspace_id(), "resourceIds": [payment], "environmentId": identity()["environmentId"],
        "metricName": "demo.payment.transactions", **time_range(), "intervalSeconds": 1, "aggregation": "max"})
    series = response.get("series", [])
    if any(s["resourceId"] != payment for s in series):
        raise RuntimeError("QueryMetrics returned a resource outside the requested scope")
    points = [point for item in series for point in item.get("points", [])]
    if any(not math.isfinite(float(point["value"])) or float(point["value"]) < 0 for point in points):
        raise RuntimeError("QueryMetrics returned an invalid payment counter value")
    latest = max(points, key=lambda point: dt.datetime.fromisoformat(point["timestamp"]), default=None)
    return float(latest["value"]) if latest else 0, response


def check_metrics():
    baseline = json.loads((STATE / "metric-baseline.json").read_text())["value"]
    def observed():
        nonlocal baseline
        value, response = payment_counter()
        if value <= baseline:
            baseline = value
            return False
        (OUT / "metrics-api.json").write_text(json.dumps({
            "baseline": baseline, "observed": value, "response": response,
        }, indent=2) + "\n")
        return True
    wait_for("payment counter increase through QueryMetrics", observed, TELEMETRY_SECONDS)
    return "QueryMetrics reports a payment counter increase after the checkout"


def connected_checkout_trace(spans, trace_id, ids):
    allowed = set(ids.values())
    expected_workspace = workspace_id()
    expected_environment = identity()["environmentId"]
    by_id = {}
    for span in spans:
        if (span.get("traceId") != trace_id or span.get("resourceId") not in allowed
                or span.get("workspaceId") != expected_workspace
                or span.get("environmentId") != expected_environment
                or span.get("resourceAttributes", {}).get("loco.region") != REGION):
            raise RuntimeError("GetTrace returned a span outside the requested trace or tenant scope")
        span_id = span.get("spanId", "")
        if not re.fullmatch(r"[0-9a-f]{16}", span_id) or span_id == "0" * 16:
            raise RuntimeError("GetTrace returned an invalid span ID")
        signature = (span.get("parentSpanId", ""), span["resourceId"])
        if span_id in by_id and by_id[span_id] != signature:
            raise RuntimeError("GetTrace returned conflicting duplicate span IDs")
        by_id[span_id] = signature
    required = {ids[name] for name in ["frontend", "checkout", "product-catalog", "payment", "shipping"]}
    neighbors = {span_id: set() for span_id in by_id}
    for span_id, (parent, _) in by_id.items():
        if parent in by_id:
            neighbors[span_id].add(parent)
            neighbors[parent].add(span_id)
    for anchor, (_, resource) in by_id.items():
        if resource != ids["checkout"]:
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
    ids = resources()
    trace_id = json.loads((STATE / "order.json").read_text())["traceId"]
    scope = {"workspaceId": workspace_id(), "resourceIds": list(ids.values()), "environmentId": identity()["environmentId"]}
    def observed():
        spans = observability_rpc("GetTrace", {"scope": scope, "traceId": trace_id}).get("spans", [])
        if not connected_checkout_trace(spans, trace_id, ids):
            return False
        (OUT / "traces-api.json").write_text(json.dumps({"traceId": trace_id, "spans": spans}, indent=2) + "\n")
        return True
    wait_for("connected checkout trace through GetTrace", observed, TELEMETRY_SECONDS)
    return "GetTrace links frontend, checkout, product-catalog, payment and shipping spans by parent IDs"


def check_observability_scope():
    other_workspace = api("loco.workspace.v1.WorkspaceService/CreateWorkspace", {
        "orgId": identity()["orgId"], "name": "telemetry-isolation-" + secrets.token_hex(4),
    })["workspaceId"]
    try:
        requests = {
            "QueryLogs": {"workspaceId": other_workspace, **time_range(1)},
            "QueryMetrics": {"workspaceId": other_workspace, **time_range(1), "metricName": "demo.payment.transactions",
                             "intervalSeconds": 60, "aggregation": "max"},
            "GetTrace": {"scope": {"workspaceId": other_workspace}, "traceId": secrets.token_hex(16)},
        }
        for method, request in requests.items():
            try:
                observability_rpc(method, request)
            except RPCError as exc:
                if exc.code != "permission_denied" or exc.status != 403:
                    raise RuntimeError(f"{method} did not enforce workspace authorization: {exc}") from exc
            else:
                raise RuntimeError(f"{method} allowed a token scoped to another workspace")
        return "The API rejects log, metric and trace queries for a workspace outside the token's scope"
    finally:
        api("loco.workspace.v1.WorkspaceService/DeleteWorkspace", {"workspaceId": other_workspace})


def verify():
    ids = resources()
    check_health(ids)
    record("observability-scope", check_observability_scope)
    if record("shop-checkout", traffic):
        record("logs-api", check_logs)
        record("metrics-api", check_metrics)
        record("traces-api", check_traces)
        record("kafka-consumers", check_kafka)
    else:
        for check in ["logs-api", "metrics-api", "traces-api", "kafka-consumers"]:
            RESULTS.append({"name": check, "status": "BLOCKED", "detail": "Checkout must pass first"})
    save_report()
    return all(r["status"] == "PASS" for r in RESULTS)


def diagnostics():
    if not owns_infrastructure():
        return
    if (STATE / "kubeconfig").exists():
        for args, name in [
            (["get", "pods", "-A", "-o", "wide"], "pods"),
            (["get", "events", "-A", "--sort-by=.lastTimestamp"], "events"),
            (["get", "applications", "-A", "-o", "yaml"], "applications"),
        ]:
            with contextlib.suppress(Exception):
                kube(*args, check=False, timeout=30, log=OUT / (name + ".log"))
        namespaces = [CONTROL_PLANE["kubernetes"]["namespace"], WORKER["kubernetes"]["namespace"]]
        if (STATE / "identity.json").exists():
            namespaces.append(app_namespace())
        for ns in namespaces:
            with contextlib.suppress(Exception):
                for pod in namespace_pods(ns):
                    name = pod["metadata"]["name"]
                    kube("-n", ns, "logs", name, "--all-containers", "--tail=100",
                         check=False, timeout=20, log=OUT / "pods" / (name + ".log"))
    with contextlib.suppress(Exception):
        compose("logs", "--no-color", "--tail=100", check=False, timeout=30, log=OUT / "fixtures.log")


def save_report():
    (OUT / "results.json").write_text(json.dumps(RESULTS, indent=2) + "\n")
    lines = ["# Loco demo E2E evidence", "", "Loco: " + LOCO_VERSION,
             "Astronomy Shop: " + VERSIONS["upstream"]["release"], ""]
    for result in RESULTS:
        lines.extend([f"## {result['status']}: {result['name']}", "", str(result["detail"]), ""])
    (OUT / "evidence.md").write_text("\n".join(lines))


def report():
    for failure in json.loads((OUT / "results.json").read_text()):
        if failure["status"] != "FAIL":
            continue
        marker = "[demo-e2e] " + failure["name"]
        existing = json.loads(run(["gh", "issue", "list", "-R", "team-loco/loco", "--state", "open",
                                   "--search", marker, "--json", "title,url"]))
        exact = [i for i in existing if i["title"] == marker]
        if exact:
            print(exact[0]["url"])
            continue
        body = OUT / "issue.md"
        body.write_text(f"""The Astronomy Shop E2E check `{failure['name']}` fails against Loco `{LOCO_VERSION}`.

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

Expected: locoadm installs Loco, all 16 apps deploy through Loco, checkout succeeds, and its trace, metrics and logs are queryable through the API.

Local evidence is in `artifacts/evidence.md` and the per-step logs. CI retains these artifacts. This report identifies the failing check; its cause may be a harness defect and needs triage before changing Loco.
""")
        print(run(["gh", "issue", "create", "-R", "team-loco/loco", "--title", marker, "--body-file", body]))


def owns_infrastructure():
    owner = STATE / "owner.yaml"
    return owner.exists() and yaml.safe_load(owner.read_text()) == {"root": str(ROOT)}


def down():
    if not owns_infrastructure():
        print("No infrastructure owned by this checkout; nothing to remove", flush=True)
        return
    compose("down", "--volumes", "--remove-orphans", timeout=120)
    run(["kind", "delete", "cluster", "--name", CLUSTER, "--kubeconfig", STATE / "kubeconfig"], timeout=120)
    if CLUSTER in run(["kind", "get", "clusters"]).splitlines():
        raise RuntimeError(f"kind still lists {CLUSTER}; run mise run down again")
    shutil.rmtree(STATE)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare", "validate", "up", "deploy", "verify", "e2e", "down", "report"])
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    STATE.mkdir(exist_ok=True)
    OUT.mkdir(exist_ok=True)
    if args.command == "e2e":
        success = False
        try:
            if record("install", up):
                deploy()
                verify()
            success = all(r["status"] == "PASS" for r in RESULTS)
        finally:
            diagnostics()
            save_report()
            if not args.keep and INFRA_STARTED:
                down()
        return 0 if success else 1
    if args.command == "deploy":
        return 0 if deploy() else 1
    if args.command == "verify":
        return 0 if verify() else 1
    globals()[args.command]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
