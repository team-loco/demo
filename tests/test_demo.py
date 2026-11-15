import importlib.util
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import datetime as dt
import hashlib
import http.server
import io
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch
import urllib.parse

import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo", ROOT / "scripts/demo.py")
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class DeploymentContractTests(unittest.TestCase):
    def test_exactly_half_the_apps_use_cluster_source_builds(self):
        modes = [app["mode"] for app in demo.APPS.values()]
        self.assertEqual(modes.count("source"), 8)
        self.assertEqual(modes.count("image"), 8)

    def test_every_app_has_its_own_partial_and_one_deployment_input(self):
        for name, app in demo.APPS.items():
            with self.subTest(name=name):
                doc = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
                self.assertEqual(doc["partial"], name)
                self.assertEqual(list(doc["services"]), [name])
                svc = doc["services"][name]
                self.assertEqual("image" in svc, app["mode"] == "image")
                self.assertEqual("dockerfile" in svc, app["mode"] == "source")
                if app["mode"] == "source":
                    self.assertTrue((ROOT / "upstream" / svc["dockerfile"]).is_file())
                    self.assertEqual(svc["context"], "source")
                else:
                    self.assertRegex(svc["image"], r":3\.1\.0-[a-z-]+@sha256:[a-f0-9]{64}$")

    def test_grpc_and_worker_apps_leave_health_omitted(self):
        for name, app in demo.APPS.items():
            if app["protocol"] in ("grpc", "worker"):
                config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
                self.assertNotIn("health", config["services"][name], name)
                self.assertEqual("port" in config["services"][name], app["protocol"] == "grpc", name)

    def test_no_app_exports_to_upstream_collector(self):
        for name in demo.APPS:
            config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
            env = config["services"][name]["env"]
            for value in env.values():
                self.assertNotIn("http://otel-collector:", value, name)
            self.assertEqual(env["OTEL_TRACES_SAMPLER"], "always_on")

    def test_infrastructure_disables_every_upstream_observability_backend(self):
        config = yaml.safe_load((ROOT / "config/demo-infrastructure.yaml").read_text())
        for name in ["opentelemetry-collector", "otel-ebpf-profiler", "jaeger", "prometheus", "grafana", "opensearch"]:
            self.assertIs(config[name]["enabled"], False)
        enabled = {name for name, value in config["components"].items() if value["enabled"]}
        self.assertEqual(enabled, {"flagd", "kafka", "valkey-cart", "astronomy-db"})

    def test_chart_render_guard_rejects_an_accidental_second_collector(self):
        docs = [{"kind": "Deployment", "metadata": {"name": n}}
                for n in ["flagd", "kafka", "valkey-cart", "astronomy-db"]]
        demo.assert_infrastructure_only(docs)
        docs.append({"kind": "DaemonSet", "metadata": {"name": "otel-collector"}})
        with self.assertRaisesRegex(RuntimeError, "Unexpected upstream workloads"):
            demo.assert_infrastructure_only(docs)

    def test_fixture_and_cluster_images_are_digest_pinned(self):
        fixtures = yaml.safe_load((ROOT / "config/fixtures.yaml").read_text())["services"]
        for image in [svc["image"] for svc in fixtures.values()] + [demo.VERSIONS["kindImage"]]:
            self.assertRegex(image, r":[^:@]+@sha256:[a-f0-9]{64}$")

    def test_app_requests_leave_cpu_for_builds_and_the_platform(self):
        millicores = 0
        for name in demo.APPS:
            config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
            millicores += int(config["services"][name]["regions"]["us-east-1"]["cpu"].removesuffix("m"))
        self.assertLessEqual(millicores, 2000)


class InstallationTests(unittest.TestCase):
    def test_installation_pins_the_cluster_release_and_build_egress(self):
        egress = [{"cidr": "172.18.0.5/32", "port": 5000}]
        doc = demo.installation("cluster-uid", egress)
        self.assertEqual(doc["release"], {"version": demo.LOCO_VERSION})
        self.assertEqual(doc["controlPlane"]["kubernetes"]["clusterUID"], "cluster-uid")
        self.assertEqual(doc["workers"][0]["kubernetes"]["clusterUID"], "cluster-uid")
        self.assertEqual(doc["workers"][0]["builds"]["privateEgress"], egress)
        self.assertEqual(demo.INSTALLATION["workers"][0]["builds"]["privateEgress"], [])

    def test_build_egress_allows_only_the_registry_and_storage_ports(self):
        egress = demo.private_egress({"registry": "172.18.0.5", "s3": "172.18.0.6"})
        self.assertEqual(egress, [{"cidr": "172.18.0.5/32", "port": 5000}, {"cidr": "172.18.0.6/32", "port": 19000}])

    def test_cluster_dns_covers_fixtures_bucket_hosts_and_the_gateway(self):
        addresses = {"postgres": "10.0.0.1", "registry": "10.0.0.2", "s3": "10.0.0.3", "dex": "10.0.0.4"}
        hosts = demo.cluster_hosts(addresses, "10.96.0.10")
        self.assertEqual(hosts["10.0.0.3"], ["s3.loco-demo.localhost", "loco-sources.s3.loco-demo.localhost",
                                             "loco-backups.s3.loco-demo.localhost"])
        self.assertEqual(hosts["10.96.0.10"], ["api.loco-demo.localhost", "console.loco-demo.localhost",
                                               "ingest.loco-demo.localhost"])
        self.assertEqual(hosts["10.0.0.4"], ["dex.loco-demo.localhost"])

    def test_installation_hostnames_resolve_through_the_routed_names(self):
        hosts = {name for names in demo.cluster_hosts({name: name + "-ip" for name in demo.FIXTURES}, "gw").values() for name in names}
        for url in [demo.CONTROL_PLANE["auth"]["issuer"], demo.CONTROL_PLANE["registry"]["endpoint"],
                    demo.CONTROL_PLANE["sourceBucket"]["endpoint"], demo.API_URL]:
            self.assertIn(urllib.parse.urlsplit(url).hostname, hosts)

    def test_first_apply_must_stop_at_the_issuer_checkpoint(self):
        applied = subprocess.CompletedProcess([], 0, "Control plane is running.")
        with patch.object(demo, "locoadm", side_effect=["", applied]), patch.object(demo, "kube") as kube:
            with self.assertRaisesRegex(RuntimeError, "certificate issuer checkpoint"):
                demo.install_loco("installation.yaml")
        kube.assert_not_called()

    def test_install_fails_unless_status_reports_ready_workers(self):
        checkpoint = subprocess.CompletedProcess([], 1, "configured certificate issuer is missing or not Ready")
        status = json.dumps({"phase": "workers-pending", "workers": [{"name": "local", "ready": False}]})
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "OUT", Path(tmp)), \
             patch.object(demo, "locoadm", side_effect=["", checkpoint, "", "", status]), \
             patch.object(demo, "kube"), patch.object(demo, "bootstrap"):
            with self.assertRaisesRegex(RuntimeError, "running installation"):
                demo.install_loco("installation.yaml")

    def test_cli_reports_the_created_id(self):
        self.assertEqual(demo.created_id('Workspace "shop" created successfully (ID: 0d3f6a3c-79e1-4a39-9a1b-6f3c2b1d9e8f)\n'),
                         "0d3f6a3c-79e1-4a39-9a1b-6f3c2b1d9e8f")
        with self.assertRaisesRegex(RuntimeError, "did not report"):
            demo.created_id("Workspace created")


class ReleaseBinaryTests(unittest.TestCase):
    def test_binaries_must_match_the_published_checksums(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "CACHE", Path(tmp)):
            for tool in ["loco", "locoadm"]:
                demo.binary(tool).parent.mkdir(parents=True, exist_ok=True)
                demo.binary(tool).write_bytes(tool.encode())
            checksums = {demo.binary(tool).name: hashlib.sha256(tool.encode()).hexdigest() for tool in ["loco", "locoadm"]}
            demo.download_release_binaries(checksums)
            checksums[demo.binary("locoadm").name] = "0" * 64
            with self.assertRaisesRegex(RuntimeError, "does not match the checksum"):
                demo.download_release_binaries(checksums)
            self.assertFalse(demo.binary("locoadm").exists())

    def test_a_release_without_this_platform_fails(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "CACHE", Path(tmp)):
            with self.assertRaisesRegex(RuntimeError, "publishes no"):
                demo.download_release_binaries({})


class FakeDex(http.server.BaseHTTPRequestHandler):
    submitted = []

    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path.startswith("/auth?"):
            self.send_response(302)
            self.send_header("Location", "/auth/local?state=abc")
            self.end_headers()
        else:
            body = (b'<form method="post" action="/auth/local/login?state=abc">'
                    b'<input name="login"><input type="password" name="password"></form>')
            if self.path.startswith("/callback"):
                body = b"Return to locoadm"
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        FakeDex.submitted.append(urllib.parse.parse_qs(self.rfile.read(length).decode()))
        self.send_response(303)
        self.send_header("Location", "/callback?code=issued&state=abc")
        self.end_headers()


class SignInTests(unittest.TestCase):
    def test_password_login_returns_to_the_bootstrap_callback(self):
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeDex)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_port}"
        control_plane = {**demo.CONTROL_PLANE, "auth": {**demo.CONTROL_PLANE["auth"], "bootstrapRedirectURI": base + "/callback"}}
        try:
            with patch.object(demo, "CONTROL_PLANE", control_plane):
                demo.sign_in(base + "/auth?client_id=loco")
                with self.assertRaisesRegex(RuntimeError, "password login form"):
                    demo.sign_in(base + "/callback")
            with self.assertRaisesRegex(RuntimeError, "did not return to locoadm"):
                demo.sign_in(base + "/auth?client_id=loco")
        finally:
            server.shutdown()
        self.assertEqual(FakeDex.submitted[0]["login"], [demo.DEMO_EMAIL])
        self.assertEqual(FakeDex.submitted[0]["password"], [demo.DEMO_PASSWORD])


class SessionTests(unittest.TestCase):
    def stored(self, directory, expires):
        path = directory / "home/.loco/credentials.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"host": demo.API_URL, "token": "current-access", "refreshToken": "current-refresh",
                                    "expiresAt": expires.isoformat()}))
        return path

    def test_fresh_session_is_used_as_stored(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), \
             patch.object(demo, "rpc") as rpc:
            self.stored(Path(tmp), dt.datetime.now(dt.UTC) + dt.timedelta(hours=1))
            self.assertEqual(demo.session_token(), "current-access")
        rpc.assert_not_called()

    def test_expiring_session_is_refreshed_into_the_cli_store(self):
        refreshed = {"tokens": {"accessToken": "new-access", "refreshToken": "new-refresh", "expiresIn": "3600"}}
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), \
             patch.object(demo, "rpc", return_value=refreshed) as rpc:
            path = self.stored(Path(tmp), dt.datetime.now(dt.UTC) + dt.timedelta(minutes=1))
            self.assertEqual(demo.session_token(), "new-access")
            stored = json.loads(path.read_text())
        rpc.assert_called_once_with("loco.auth.v1.AuthService/RefreshCLIToken", {"refreshToken": "current-refresh"})
        self.assertEqual(stored["refreshToken"], "new-refresh")
        self.assertGreater(dt.datetime.fromisoformat(stored["expiresAt"]), dt.datetime.now(dt.UTC) + dt.timedelta(minutes=45))


class HarnessTests(unittest.TestCase):
    def test_public_api_lists_follow_pagination(self):
        pages = [{"resources": [{"id": "one"}], "nextPageToken": "next"}, {"resources": [{"id": "two"}]}]
        with patch.object(demo, "api", side_effect=pages) as call:
            records = demo.list_api("ResourceService/ListWorkspaceResources", {"workspaceId": "ws"}, "resources")
        self.assertEqual([r["id"] for r in records], ["one", "two"])
        self.assertEqual(call.call_args_list[-1].args[1]["pageToken"], "next")

    def test_logs_are_scoped_and_reject_another_resource(self):
        with patch.object(demo, "workspace_id", return_value="ws"), \
             patch.object(demo, "identity", return_value={"environmentId": "env"}), \
             patch.object(demo, "observability_rpc", return_value={"entries": [{"resourceId": "other", "body": "order"}]}) as call:
            with self.assertRaisesRegex(RuntimeError, "outside the requested scope"):
                demo.order_log_sources(["payment"], "order")
        self.assertEqual(call.call_args.args[0], "QueryLogs")
        self.assertEqual(call.call_args.args[1]["resourceIds"], ["payment"])
        self.assertEqual(call.call_args.args[1]["environmentId"], "env")

    def test_checkout_logs_require_otlp_and_stdout_for_the_same_order(self):
        otlp = {"resourceId": "checkout-id", "body": "order-id"}
        stdout = {"resourceId": "email-id", "body": "order-id",
                  "logAttributes": {"log.file.path": "/logs/container.log", "log.iostream": "stdout"}}
        cases = [([otlp], False), ([stdout], False),
                 ([otlp, {**stdout, "body": "different-order"}], False),
                 ([otlp, {**stdout, "logAttributes": {"log.file.path": "/logs/container.log", "log.iostream": "stderr"}}], False),
                 ([otlp, stdout], True)]
        def check_once(name, predicate, seconds):
            if not predicate():
                raise RuntimeError("Required log sources missing")
        for entries, succeeds in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                (path / "resources.json").write_text(json.dumps({"checkout": "checkout-id", "email": "email-id"}))
                (path / "order.json").write_text(json.dumps({"orderId": "order-id"}))
                with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                     patch.object(demo, "query_logs", return_value=entries), \
                     patch.object(demo, "wait_for", side_effect=check_once):
                    if succeeds:
                        demo.check_logs()
                    else:
                        with self.assertRaisesRegex(RuntimeError, "Required log sources missing"):
                            demo.check_logs()
                self.assertEqual((path / "logs-api.json").exists(), succeeds)

    def test_stage_catalog_bakes_the_upstream_declarative_otel_config(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)):
            app = demo.stage_app("product-catalog")
            self.assertTrue((app / "source/otel-config.yml").exists())
            dockerfile = app / "source/src/product-catalog/Dockerfile"
            self.assertIn("COPY otel-config.yml /otel-config.yml", dockerfile.read_text())
            demo.stage_app("product-catalog")
            self.assertEqual(dockerfile.read_text().count("COPY otel-config.yml /otel-config.yml"), 1)

    def test_failures_are_recorded_and_secret_values_are_redacted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            state = path / "state"
            credentials = state / "home/.loco/credentials.json"
            credentials.parent.mkdir(parents=True)
            credentials.write_text(json.dumps({"token": "secret-session-value-must-not-leak", "refreshToken": "short"}))
            with patch.object(demo, "STATE", state), patch.object(demo, "OUT", path), patch.object(demo, "RESULTS", []):
                def fail():
                    raise RuntimeError("failed with secret-session-value-must-not-leak")
                self.assertFalse(demo.record("failure", fail))
                results = (path / "results.json").read_text()
                self.assertIn('"status": "FAIL"', results)
                self.assertNotIn("secret-session-value-must-not-leak", results)
                self.assertIn("[REDACTED]", results)

    def test_omitted_health_never_passes_when_resource_api_reports_http_checks(self):
        resource = {"spec": {"service": {"healthCheck": {"path": "/health"}}}}
        with patch.object(demo, "api", return_value={"resource": resource}), patch.object(demo, "wait_for") as wait:
            with self.assertRaisesRegex(RuntimeError, "omits health"):
                demo.check_resource("payment", "payment-id")
        wait.assert_not_called()

    def test_timeout_reports_the_last_failure(self):
        with patch.object(demo.time, "monotonic", side_effect=[0, 5]):
            def broken():
                raise RuntimeError("connection refused")
            with self.assertRaisesRegex(RuntimeError, "connection refused"):
                demo.wait_for("API", broken, seconds=1)

    def test_startup_connection_reset_is_retried_until_health_succeeds(self):
        checks = iter([ConnectionResetError("connection reset by peer"), True])
        def health():
            value = next(checks)
            if isinstance(value, Exception):
                raise value
            return value
        with patch.object(demo.time, "sleep"):
            demo.wait_for("API health", health, seconds=30)

    def test_failed_install_does_not_clean_up_an_existing_owned_cluster(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), patch.object(demo, "OUT", Path(tmp)), \
             patch.object(demo.sys, "argv", ["demo.py", "e2e"]), \
             patch.object(demo, "up", side_effect=RuntimeError("loco-demo already exists")), \
             patch.object(demo, "RESULTS", []), \
             patch.object(demo, "diagnostics"), patch.object(demo, "save_report"), \
             patch.object(demo, "down") as down:
            self.assertEqual(demo.main(), 1)
            down.assert_not_called()

    def test_down_leaves_infrastructure_it_does_not_own(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), \
             patch.object(demo, "run") as run, patch.object(demo, "compose") as compose:
            (Path(tmp) / "kubeconfig").write_text("existing")
            demo.down()
            run.assert_not_called()
            compose.assert_not_called()
            self.assertTrue((Path(tmp) / "kubeconfig").exists())

    def test_failed_checks_do_not_stop_later_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "resources.json").write_text(json.dumps({"payment": "payment-id"}))
            with patch.object(demo, "STATE", directory), patch.object(demo, "OUT", directory), \
                 patch.object(demo, "RESULTS", []), \
                 patch.object(demo, "check_resource", side_effect=RuntimeError("CreateContainerConfigError")), \
                 patch.object(demo, "check_builds", return_value="checked"), patch.object(demo, "check_observability_scope", return_value="checked"), \
                 patch.object(demo, "traffic", return_value="checked"), patch.object(demo, "check_logs", return_value="checked"), \
                 patch.object(demo, "check_metrics", side_effect=RuntimeError("token expired")), \
                 patch.object(demo, "check_traces", return_value="checked"), patch.object(demo, "check_kafka", return_value="checked"):
                self.assertFalse(demo.verify())
                failures = [result["name"] for result in demo.RESULTS if result["status"] == "FAIL"]
                self.assertEqual(failures, ["health:payment", "metrics-api"])
                self.assertEqual(demo.RESULTS[-1]["name"], "kafka-consumers")


class PublicTelemetryTests(unittest.TestCase):
    trace_id = "a" * 32
    resources = {name: name + "-id" for name in ["frontend", "checkout", "payment", "shipping", "product-catalog"]}

    def spans(self):
        parents = [None, 1, 2, 2, 2]
        return [{"traceId": self.trace_id, "spanId": f"{index:016x}",
                 "parentSpanId": f"{parent:016x}" if parent else "",
                 "resourceId": resource, "workspaceId": "workspace", "environmentId": "environment",
                 "resourceAttributes": {"loco.region": demo.REGION}}
                for index, (resource, parent) in enumerate(zip(self.resources.values(), parents), 1)]

    def test_observability_uses_the_workspace_scoped_token_and_region(self):
        request = {"workspaceId": "workspace", "resourceIds": ["payment-id"]}
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), \
             patch.object(demo, "api", return_value={}) as call:
            (Path(tmp) / "secrets.json").write_text(json.dumps({"telemetry": "api-issued-workspace-token"}))
            demo.observability_rpc("QueryLogs", request)
            call.assert_called_once_with("loco.observability.v1.ObservabilityService/QueryLogs",
                                         {**request, "regions": [demo.REGION]}, token="api-issued-workspace-token")
            demo.observability_rpc("QueryMetrics", {"workspaceId": "workspace"})
            self.assertEqual(call.call_args.args[1], {"workspaceId": "workspace", "region": demo.REGION})
            demo.observability_rpc("GetTrace", {"traceId": self.trace_id})
            self.assertEqual(call.call_args.args[1], {"traceId": self.trace_id})
        self.assertNotIn("regions", request)

    def test_logs_follow_cursors_and_fail_on_a_repeated_one(self):
        with patch.object(demo, "workspace_id", return_value="workspace"), \
             patch.object(demo, "identity", return_value={"environmentId": "environment"}):
            entry = {"resourceId": "checkout-id", "body": "order"}
            with patch.object(demo, "observability_rpc", side_effect=[{"entries": [entry], "nextCursor": "next"},
                                                                      {"entries": [entry]}]) as call:
                self.assertEqual(demo.query_logs(["checkout-id"]), [entry, entry])
            self.assertEqual(call.call_args.args[1]["cursor"], "next")
            with patch.object(demo, "observability_rpc", return_value={"nextCursor": "same"}):
                with self.assertRaisesRegex(RuntimeError, "repeated pagination cursor"):
                    demo.query_logs(["checkout-id"])

    def test_trace_requires_real_parent_connections_between_services(self):
        with patch.object(demo, "workspace_id", return_value="workspace"), \
             patch.object(demo, "identity", return_value={"environmentId": "environment"}):
            spans = self.spans()
            self.assertTrue(demo.connected_checkout_trace(spans, self.trace_id, self.resources))
            self.assertTrue(demo.connected_checkout_trace(spans + [spans[0]], self.trace_id, self.resources))
            for span in spans:
                span["parentSpanId"] = ""
            self.assertFalse(demo.connected_checkout_trace(spans, self.trace_id, self.resources))

    def test_trace_rejects_wrong_scope_and_conflicting_identity(self):
        with patch.object(demo, "workspace_id", return_value="workspace"), \
             patch.object(demo, "identity", return_value={"environmentId": "environment"}):
            for field, value in [("workspaceId", "other"), ("environmentId", "other"),
                                 ("resourceId", "other"), ("traceId", "b" * 32)]:
                with self.subTest(field=field):
                    spans = self.spans()
                    spans[0][field] = value
                    with self.assertRaisesRegex(RuntimeError, "outside the requested"):
                        demo.connected_checkout_trace(spans, self.trace_id, self.resources)
            spans = self.spans()
            duplicate = {**spans[0], "parentSpanId": "b" * 16}
            with self.assertRaisesRegex(RuntimeError, "conflicting duplicate"):
                demo.connected_checkout_trace(spans + [duplicate], self.trace_id, self.resources)

    def test_trace_verification_records_connected_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "resources.json").write_text(json.dumps(self.resources))
            (path / "order.json").write_text(json.dumps({"orderId": "order", "traceId": self.trace_id}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo, "workspace_id", return_value="workspace"), \
                 patch.object(demo, "identity", return_value={"environmentId": "environment"}), \
                 patch.object(demo, "observability_rpc", side_effect=[{}, {"spans": self.spans()}]) as call, \
                 patch.object(demo.time, "sleep"):
                demo.check_traces()
            self.assertEqual(call.call_count, 2)
            self.assertEqual(call.call_args.args, ("GetTrace", {
                "scope": {"workspaceId": "workspace", "resourceIds": list(self.resources.values()),
                          "environmentId": "environment"}, "traceId": self.trace_id}))
            self.assertEqual(json.loads((path / "traces-api.json").read_text())["traceId"], self.trace_id)

    def test_payment_counter_uses_latest_sample_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "resources.json").write_text(json.dumps({"payment": "payment-id"}))
            response = {"series": [{"resourceId": "payment-id", "points": [
                {"timestamp": "2026-10-16T06:05:00Z", "value": 89},
                {"timestamp": "2026-10-16T06:07:00Z", "value": 2},
                {"timestamp": "2026-10-16T06:09:00Z", "value": 6},
            ]}]}
            with patch.object(demo, "STATE", path), \
                 patch.object(demo, "workspace_id", return_value="workspace"), \
                 patch.object(demo, "identity", return_value={"environmentId": "environment"}), \
                 patch.object(demo, "observability_rpc", return_value=response) as call:
                self.assertEqual(demo.payment_counter()[0], 6)
            self.assertEqual(call.call_args.args[1]["metricName"], "demo.payment.transactions")

    def test_counter_reset_requires_a_subsequent_increase(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "metric-baseline.json").write_text(json.dumps({"value": 89}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo, "payment_counter", side_effect=[(2, {}), (2, {}), (3, {})]) as call, \
                 patch.object(demo.time, "sleep"):
                demo.check_metrics()
            self.assertEqual(call.call_count, 3)
            evidence = json.loads((path / "metrics-api.json").read_text())
            self.assertEqual(evidence["baseline"], 2)
            self.assertEqual(evidence["observed"], 3)

    def test_unchanged_counter_does_not_count_as_an_increase(self):
        for baseline, value in [(89, 2), (5, 5)]:
            with self.subTest(baseline=baseline), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                (path / "metric-baseline.json").write_text(json.dumps({"value": baseline}))
                with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                     patch.object(demo, "payment_counter", return_value=(value, {})), \
                     patch.object(demo.time, "sleep"), \
                     patch.object(demo.time, "monotonic", side_effect=[0, 0, demo.TELEMETRY_SECONDS + 1]):
                    with self.assertRaisesRegex(RuntimeError, "Timed out"):
                        demo.check_metrics()
                self.assertFalse((path / "metrics-api.json").exists())

    def test_scope_check_removes_only_its_workspace_on_success_and_failure(self):
        cases = [(demo.RPCError("query", 403, {"code": "permission_denied"}), True),
                 (None, False), (demo.RPCError("query", 503, {"code": "unavailable"}), False)]
        for error, succeeds in cases:
            with self.subTest(error=error), \
                 patch.object(demo, "identity", return_value={"orgId": "org"}), \
                 patch.object(demo, "api", side_effect=[{"workspaceId": "temporary-workspace"}, {}]) as api, \
                 patch.object(demo, "observability_rpc", side_effect=error, return_value={}) as query:
                if succeeds:
                    demo.check_observability_scope()
                    self.assertEqual([c.args[0] for c in query.call_args_list], ["QueryLogs", "QueryMetrics", "GetTrace"])
                else:
                    with self.assertRaises(RuntimeError):
                        demo.check_observability_scope()
                self.assertEqual(api.call_count, 2)
                self.assertEqual(api.call_args.args, ("loco.workspace.v1.WorkspaceService/DeleteWorkspace",
                                                     {"workspaceId": "temporary-workspace"}))


class LiveProgressTests(unittest.TestCase):
    secret = "issued-secret-value"

    def test_redacted_line_is_visible_before_process_exits(self):
        visible = threading.Event()
        class LiveOutput(io.StringIO):
            def write(self, text):
                result = super().write(text)
                if "READY [REDACTED]" in self.getvalue():
                    visible.set()
                return result
        output = LiveOutput()
        code = ("import sys,time; from pathlib import Path; "
                "sys.stdout.write('READY issued-'); sys.stdout.flush(); time.sleep(.05); "
                "print('secret-value',flush=True); "
                "exec('while not Path(sys.argv[1]).exists(): time.sleep(.01)')")
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "secrets.json").write_text(json.dumps({"telemetry": self.secret}))
            release = directory / "release"
            logfile = directory / "process.log"
            with patch.object(demo, "STATE", directory), redirect_stdout(output), ThreadPoolExecutor() as executor:
                future = executor.submit(demo.run, [sys.executable, "-u", "-c", code, str(release)],
                                         stream=True, timeout=5, log=logfile)
                try:
                    self.assertTrue(visible.wait(3), "subprocess output remained buffered")
                    self.assertFalse(future.done(), "child must still be running when output appears")
                finally:
                    release.touch()
                result = future.result(timeout=5)
            self.assertIn(self.secret, result)
            self.assertNotIn(self.secret, output.getvalue())
            self.assertIn("READY [REDACTED]", logfile.read_text())
            self.assertNotIn(self.secret, logfile.read_text())

    def test_timeout_keeps_redacted_live_output_and_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "secrets.json").write_text(json.dumps({"telemetry": self.secret}))
            logfile = directory / "timeout.log"
            output = io.StringIO()
            code = "import time; print('starting issued-secret-value',flush=True); time.sleep(10)"
            with patch.object(demo, "STATE", directory), redirect_stdout(output), \
                 patch.object(demo, "PROGRESS_SECONDS", .05):
                with self.assertRaises(subprocess.TimeoutExpired) as raised:
                    demo.run([sys.executable, "-u", "-c", code], stream=True, timeout=1, log=logfile)
            self.assertIn("RUN timeout (", output.getvalue())
            for text in [output.getvalue(), logfile.read_text(), raised.exception.output]:
                self.assertIn("starting [REDACTED]", text)
                self.assertNotIn(self.secret, text)

    def test_machine_readable_output_stays_buffered(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = demo.run([sys.executable, "-c", "print('{\"id\": 1}')"])
        self.assertEqual(json.loads(result), {"id": 1})
        self.assertEqual(output.getvalue(), "")

    def test_named_wait_reports_elapsed_time_and_redacted_error(self):
        attempts = iter([RuntimeError("not ready issued-secret-value"), False, True])
        def ready():
            result = next(attempts)
            if isinstance(result, Exception):
                raise result
            return result
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "secrets.json").write_text(json.dumps({"telemetry": self.secret}))
            output = io.StringIO()
            with patch.object(demo, "STATE", directory), redirect_stdout(output), \
                 patch.object(demo.time, "monotonic", side_effect=[0, 15, 31, 32]), \
                 patch.object(demo.time, "sleep"):
                demo.wait_for("worker", ready, seconds=60)
            text = output.getvalue()
            self.assertIn("WAIT worker (15s elapsed)", text)
            self.assertIn("WAIT worker (31s elapsed)", text)
            self.assertIn("READY worker (32s)", text)
            self.assertIn("latest error: not ready [REDACTED]", text)
            self.assertNotIn(self.secret, text)


if __name__ == "__main__":
    unittest.main()
