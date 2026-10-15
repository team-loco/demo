import importlib.util
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import io
import copy
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch

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
        self.assertIn("accounting", demo.APPS)
        self.assertIn("fraud-detection", demo.APPS)
        self.assertIn("load-generator", demo.APPS)

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

    def test_pin_file_contains_no_floating_image_references(self):
        images = list(demo.VERSIONS["loco"]["images"].values())
        images += list(demo.VERSIONS["apps"].values())
        images += list(demo.VERSIONS["infrastructureImages"].values())
        images.append(demo.VERSIONS["kindImage"])
        for image in images:
            self.assertRegex(image, r"@sha256:[a-f0-9]{64}$")
            self.assertNotIn(":latest", image)

    def test_kind_uses_a_separate_builder_worker(self):
        config = yaml.safe_load((ROOT / "config/kind.yaml").read_text())
        workers = [n for n in config["nodes"] if n["role"] == "worker"]
        self.assertEqual(workers[0]["labels"]["loco.build/demo-builder"], "true")
        operator = yaml.safe_load((ROOT / "config/operator.yaml").read_text())
        self.assertEqual(operator["builds"]["nodeSelector"]["loco.build/demo-builder"], "true")

    def test_app_requests_leave_cpu_for_the_builder_and_platform(self):
        millicores = 0
        for name in demo.APPS:
            config = yaml.safe_load((ROOT / "apps" / name / "loco.yaml").read_text())
            millicores += int(config["services"][name]["regions"]["us-east-1"]["cpu"].removesuffix("m"))
        self.assertLessEqual(millicores, 2000)


class HarnessTests(unittest.TestCase):
    def release_cache(self, directory):
        origin = directory / "origin"
        cache = directory / "cache"
        origin.mkdir()
        def git(*args, cwd=origin):
            result = subprocess.run(
                ["git", "-c", "user.name=Demo Test", "-c", "user.email=demo@example.test", *args],
                cwd=cwd, text=True, capture_output=True, check=True)
            return result.stdout.strip()
        git("init", "--initial-branch=main")
        (origin / "release.txt").write_text("old release")
        git("add", "release.txt")
        git("commit", "-m", "old release")
        git("tag", "v1")
        old = git("rev-parse", "HEAD")
        git("clone", "--branch", "v1", str(origin), str(cache))
        (origin / "release.txt").write_text("new release")
        git("commit", "-am", "new release")
        git("tag", "-a", "v2", "-m", "new release")
        return cache, old, git("rev-parse", "HEAD")

    def test_prepare_refreshes_an_old_release_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache, _, current = self.release_cache(Path(tmp))
            (cache / "local-note.txt").write_text("keep this")
            demo.checkout("unused/repository", "v2", current, cache)
            self.assertEqual((cache / "release.txt").read_text(), "new release")
            self.assertEqual((cache / "local-note.txt").read_text(), "keep this")
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cache, text=True).strip()
            self.assertEqual(head, current)
            demo.checkout("unused/repository", "v2", current, cache)

    def test_wrong_release_pin_leaves_the_cached_checkout_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache, old, _ = self.release_cache(Path(tmp))
            with self.assertRaisesRegex(RuntimeError, "expected .* got"):
                demo.checkout("unused/repository", "v2", "0" * 40, cache)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cache, text=True).strip()
            self.assertEqual(head, old)
            self.assertEqual((cache / "release.txt").read_text(), "old release")

    def test_refresh_preserves_conflicting_local_edits(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache, old, current = self.release_cache(Path(tmp))
            (cache / "release.txt").write_text("local changes")
            with self.assertRaises(RuntimeError):
                demo.checkout("unused/repository", "v2", current, cache)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=cache, text=True).strip()
            self.assertEqual(head, old)
            self.assertEqual((cache / "release.txt").read_text(), "local changes")

    def test_identity_and_cli_credentials_come_from_public_api_responses(self):
        responses = {
            "WhoAmI": {"user": {"id": "user-from-api"}},
            "UpdateUser": {}, "ApproveCLILogin": {"code": "approved-code"},
            "ExchangeCLICode": {"tokens": {"accessToken": "issued-access-token", "refreshToken": "issued-refresh-token", "expiresIn": "900"}},
            "ListUserOrgs": {"orgs": []}, "CreateOrg": {"orgId": "org-from-api"},
            "ListOrgWorkspaces": {"workspaces": []}, "CreateWorkspace": {"workspaceId": "workspace-from-api"},
            "ListEnvironments": {"environments": [{"name": "production", "id": "environment-from-api"}]},
            "CreateToken": {"token": "telemetry-token-issued-by-api"},
        }
        provider = MagicMock()
        provider.__enter__.return_value = io.BytesIO(b'{"id_token":"oidc-provider-token"}')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "cli").mkdir()
            (path / "endpoint.json").write_text(json.dumps({"cliApiUrl": "http://kind-node:30080"}))
            (path / "secrets.json").write_text(json.dumps({"database": "temporary-password"}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo.urllib.request, "urlopen", return_value=provider), \
                 patch.object(demo, "api", side_effect=lambda method, data, **kwargs: responses[method.rsplit("/", 1)[1]]) as calls:
                demo.create_identity()
            identity = json.loads((path / "identity.json").read_text())
            self.assertEqual(identity["workspaceId"], "workspace-from-api")
            credentials = json.loads((path / "cli/credentials.json").read_text())
            self.assertEqual(credentials["token"], "issued-access-token")
            self.assertEqual(credentials["refreshToken"], "issued-refresh-token")
            config = yaml.safe_load((path / "cli/config.yaml").read_text())
            self.assertEqual(config["scopes"]["default"]["workspace"]["id"], "workspace-from-api")
            self.assertEqual(config["locoHost"], "http://kind-node:30080")
            self.assertEqual(json.loads((path / "secrets.json").read_text())["telemetry"], "telemetry-token-issued-by-api")
            approval = next(c for c in calls.call_args_list if c.args[0].endswith("/ApproveCLILogin"))
            self.assertEqual(approval.kwargs["token"], "oidc-provider-token")
            self.assertEqual(len(approval.args[1]["codeChallenge"]), 43)

    def test_source_deploy_records_actual_builder_node_placement(self):
        responses = [
            {"items": [{"metadata": {"name": "build-pod"}, "spec": {"nodeName": "loco-demo-worker"}}]},
            {"metadata": {"labels": {"loco.build/demo-builder": "true"}}},
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "OUT", Path(tmp)), \
             patch.object(demo, "stage_app"), patch.object(demo, "cli", return_value="deployed"), \
             patch.object(demo, "list_builds", return_value=[{"id": "build-id", "status": "BUILD_STATUS_SUCCEEDED", "createdAt": "2026-10-12T00:00:00Z"}]), \
             patch.object(demo, "api", return_value={"build": {"status": "BUILD_STATUS_SUCCEEDED"}}), \
             patch.object(demo, "kube", side_effect=[json.dumps(r) for r in responses]):
            demo.deploy_one("payment", {"payment": "resource-id"})
            evidence = json.loads((Path(tmp) / "build-payment.json").read_text())
            self.assertEqual(evidence[0]["node"], "loco-demo-worker")

    def test_source_deploy_on_an_unlabelled_node_fails(self):
        responses = [
            {"items": [{"metadata": {"name": "build-pod"}, "spec": {"nodeName": "control-plane"}}]},
            {"metadata": {"labels": {}}},
        ]
        with patch.object(demo, "stage_app"), patch.object(demo, "cli", return_value="deployed"), \
             patch.object(demo, "list_builds", return_value=[{"id": "build-id", "status": "BUILD_STATUS_SUCCEEDED", "createdAt": "2026-10-12T00:00:00Z"}]), \
             patch.object(demo, "api", return_value={"build": {"status": "BUILD_STATUS_SUCCEEDED"}}), \
             patch.object(demo, "kube", side_effect=[json.dumps(r) for r in responses]):
            with self.assertRaisesRegex(RuntimeError, "not a builder worker"):
                demo.deploy_one("payment", {"payment": "resource-id"})

    def test_harness_has_no_direct_database_client_or_seed_path(self):
        source = (ROOT / "scripts/demo.py").read_text()
        for forbidden in ["psql", "clickhouse-client", "def sql(", "def ch(", "api/seed/", "INSERT INTO", "SELECT count"]:
            self.assertNotIn(forbidden, source)
        for required in ["BuildService/ListBuilds", "BuildService/GetBuild", "ResourceService/GetResource",
                         "WorkspaceService/CreateWorkspace", "OrgService/CreateOrg", "AuthService/ExchangeCLICode"]:
            self.assertIn(required, source)

    def test_old_release_contracts_fail_before_creating_infrastructure(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "CORE", Path(tmp)), \
             patch.object(demo, "prepare"), patch.object(demo, "run") as run:
            with self.assertRaises(demo.PublicAPIGap) as raised:
                demo.up()
            self.assertIn(demo.VERSIONS["loco"]["cli"], str(raised.exception))
            self.assertIn("not released artifacts", str(raised.exception))
            run.assert_not_called()

    def test_public_api_lists_follow_pagination(self):
        pages = [{"resources": [{"id": "one"}], "nextPageToken": "next"}, {"resources": [{"id": "two"}]}]
        with patch.object(demo, "api", side_effect=pages) as call:
            records = demo.list_api("ResourceService/ListWorkspaceResources", {"workspaceId": "ws"}, "resources")
        self.assertEqual([r["id"] for r in records], ["one", "two"])
        self.assertEqual(call.call_args_list[-1].args[1]["pageToken"], "next")

    def test_logs_use_public_proxy_api_and_reject_another_resource(self):
        with patch.object(demo, "workspace_id", return_value="ws"), \
             patch.object(demo, "identity", return_value={"environmentId": "env"}), \
             patch.object(demo, "observability_rpc", return_value={"entries": [{"resourceId": "other", "body": "order"}]}) as call:
            with self.assertRaisesRegex(RuntimeError, "outside the requested scope"):
                demo.order_in_logs(["payment"], "order")
        self.assertEqual(call.call_args.args[0], "QueryLogs")
        self.assertEqual(call.call_args.args[1]["resourceIds"], ["payment"])

    def test_metrics_use_public_proxy_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "resources.json").write_text(json.dumps({"payment": "payment-id"}))
            (path / "metric-baseline.json").write_text(json.dumps({"value": 0}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo, "workspace_id", return_value="ws"), \
                 patch.object(demo, "identity", return_value={"environmentId": "env"}), \
                 patch.object(demo, "observability_rpc", return_value={"series": [{"resourceId": "payment-id", "points": [{"value": 1}]}]}) as call:
                demo.check_metrics()
            self.assertEqual(call.call_args.args[0], "QueryMetrics")
            self.assertEqual(call.call_args.args[1]["metricName"], "demo.payment.transactions")
            self.assertTrue((path / "metrics-api.json").exists())

    def test_private_addresses_resolve_without_changing_listening_ports(self):
        env = {"CART_ADDR": "cart:7070", "SHIPPING_ADDR": "http://shipping:50050",
               "FRONTEND_HOST": "frontend", "FRONTEND_PORT": "8080",
               "PORT": "8080", "KAFKA_ADDR": "kafka:9092"}
        ids = {"cart": "cart-id", "shipping": "ship-id", "frontend": "front-id"}
        self.assertEqual(demo.resolve_env(env, ids), {
            "CART_ADDR": "resource-cart-id:80", "SHIPPING_ADDR": "http://resource-ship-id:80",
            "FRONTEND_HOST": "resource-front-id", "FRONTEND_PORT": "80",
            "PORT": "8080", "KAFKA_ADDR": "kafka:9092"})
        self.assertEqual(env["CART_ADDR"], "cart:7070")

    def test_service_host_ports_resolve_regardless_of_yaml_key_order(self):
        env = {"IMAGE_PROVIDER_PORT": "8081", "IMAGE_PROVIDER_HOST": "image-provider"}
        resolved = demo.resolve_env(env, {"image-provider": "image-id"})
        self.assertEqual(resolved["IMAGE_PROVIDER_PORT"], "80")

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
            state.mkdir()
            (state / "secrets.json").write_text(json.dumps({"session": "secret-session-value-must-not-leak"}))
            with patch.object(demo, "STATE", state), patch.object(demo, "OUT", path / "out"), patch.object(demo, "RESULTS", []):
                def fail():
                    raise RuntimeError("failed with secret-session-value-must-not-leak")
                self.assertFalse(demo.record("failure", fail))
                results = (path / "out/results.json").read_text()
                self.assertIn('"status": "FAIL"', results)
                self.assertNotIn("secret-session-value-must-not-leak", results)
                self.assertIn("[REDACTED]", results)

    def test_omitted_health_never_passes_when_resource_api_reports_http_checks(self):
        resource = {"spec": {"service": {"healthCheck": {"path": "/health"}}}}
        with patch.object(demo, "api", return_value={"resource": resource}):
            with self.assertRaises(demo.PublicAPIGap):
                demo.check_resource("payment", "payment-id")

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
            demo.wait_for("API health", health)


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

    def test_observability_uses_the_main_api_and_real_scoped_token(self):
        request = {"workspaceId": "workspace", "resourceIds": ["payment-id"]}
        with patch.object(demo, "credentials", return_value={"telemetry": "api-issued-workspace-token"}), \
             patch.object(demo, "api", return_value={}) as call:
            demo.observability_rpc("QueryLogs", request)
        call.assert_called_once_with("loco.observability.v1.ObservabilityProxyService/QueryLogs",
                                     {**request, "regions": [demo.REGION]}, token="api-issued-workspace-token")
        self.assertNotIn("regions", request)

    def test_metric_and_trace_requests_use_their_actual_region_contracts(self):
        with patch.object(demo, "api", return_value={}) as call:
            demo.observability_rpc("QueryMetrics", {"workspaceId": "workspace"}, token="issued-token")
            self.assertEqual(call.call_args.args[1], {"workspaceId": "workspace", "region": demo.REGION})
            demo.observability_rpc("QueryTraces", {"workspaceId": "workspace"}, token="issued-token")
            self.assertEqual(call.call_args.args[1], {"workspaceId": "workspace"})

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

    def test_traces_follow_cursors_and_preserve_scoping(self):
        responses = [{"spans": [{"spanId": "one"}], "nextCursor": "cursor"}, {"spans": [{"spanId": "two"}]}]
        with patch.object(demo, "workspace_id", return_value="workspace"), \
             patch.object(demo, "identity", return_value={"environmentId": "environment"}), \
             patch.object(demo, "observability_rpc", side_effect=responses) as call:
            spans = demo.query_trace(self.trace_id, list(self.resources.values()))
        self.assertEqual([span["spanId"] for span in spans], ["one", "two"])
        request = call.call_args.args[1]
        self.assertEqual(request["cursor"], "cursor")
        self.assertEqual(request["traceId"], self.trace_id)
        self.assertEqual(request["environmentId"], "environment")
        self.assertEqual(request["resourceIds"], list(self.resources.values()))

    def test_trace_repeated_cursor_fails_instead_of_looping(self):
        with patch.object(demo, "workspace_id", return_value="workspace"), \
             patch.object(demo, "identity", return_value={"environmentId": "environment"}), \
             patch.object(demo, "observability_rpc", return_value={"nextCursor": "same"}):
            with self.assertRaisesRegex(RuntimeError, "repeated pagination cursor"):
                demo.query_trace(self.trace_id, [])

    def test_trace_verification_records_connected_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "resources.json").write_text(json.dumps(self.resources))
            (path / "trace.json").write_text(json.dumps({"traceId": self.trace_id}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo, "workspace_id", return_value="workspace"), \
                 patch.object(demo, "identity", return_value={"environmentId": "environment"}), \
                 patch.object(demo, "query_trace", side_effect=[[], self.spans()]) as call, \
                 patch.object(demo.time, "sleep"):
                demo.check_traces()
            self.assertEqual(call.call_count, 2)
            self.assertEqual(json.loads((path / "traces-api.json").read_text())["traceId"], self.trace_id)

    def test_existing_counter_value_does_not_count_as_an_increase(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "metric-baseline.json").write_text(json.dumps({"value": 5}))
            with patch.object(demo, "STATE", path), patch.object(demo, "OUT", path), \
                 patch.object(demo, "payment_counter", return_value=(5, {})), \
                 patch.object(demo.time, "monotonic", side_effect=[0, demo.TELEMETRY_SECONDS + 1]):
                with self.assertRaisesRegex(RuntimeError, "Timed out"):
                    demo.check_metrics()
            self.assertFalse((path / "metrics-api.json").exists())

    def test_scope_checks_require_permission_denied_not_missing_api(self):
        with patch.object(demo, "identity", return_value={"orgId": "org"}), \
             patch.object(demo, "api", return_value={"workspaceId": "other-workspace"}):
            for status, code in [(404, "unimplemented"), (503, "unavailable")]:
                with self.subTest(code=code), patch.object(demo, "observability_rpc", side_effect=demo.RPCError("QueryLogs", status, {"code": code})):
                    with self.assertRaisesRegex(RuntimeError, "did not enforce workspace authorization"):
                        demo.check_observability_scope()
            with patch.object(demo, "observability_rpc", side_effect=demo.RPCError("query", 403, {"code": "permission_denied"})) as call:
                demo.check_observability_scope()
                self.assertEqual(call.call_count, 3)
            with patch.object(demo, "observability_rpc", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "allowed a token"):
                    demo.check_observability_scope()


class BootstrapContractTests(unittest.TestCase):
    def test_failed_bootstrap_does_not_clean_up_an_existing_owned_cluster(self):
        with patch.object(demo.sys, "argv", ["demo.py", "e2e"]), \
             patch.object(demo, "record", return_value=False), \
             patch.object(demo, "RESULTS", []), \
             patch.object(demo, "diagnostics"), patch.object(demo, "save_report"), \
             patch.object(demo, "down") as down:
            self.assertEqual(demo.main(), 1)
            down.assert_not_called()

    def test_failed_preflight_cleanup_preserves_existing_infrastructure(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "STATE", Path(tmp)), \
             patch.object(demo, "run") as run, patch.object(demo, "compose") as compose:
            (Path(tmp) / "compose.yaml").write_text("services: {}")
            (Path(tmp) / "kubeconfig").write_text("existing")
            demo.down()
            run.assert_not_called()
            compose.assert_not_called()
            self.assertTrue((Path(tmp) / "kubeconfig").exists())

    def test_control_plane_exposes_only_api_with_private_proxy_sidecar(self):
        docs = demo.control_plane_documents()
        deployment = next(doc for doc in docs if doc["kind"] == "Deployment")
        service = next(doc for doc in docs if doc["kind"] == "Service")
        containers = deployment["spec"]["template"]["spec"]["containers"]
        self.assertEqual([container["name"] for container in containers], ["api", "observability"])
        self.assertEqual(service["spec"]["ports"], [{"port": 8000, "targetPort": 8000, "nodePort": demo.API_NODE_PORT}])
        self.assertEqual(deployment["metadata"]["namespace"], demo.API_NAMESPACE)
        self.assertEqual(containers[1]["readinessProbe"]["httpGet"]["path"], "/readyz")
        self.assertFalse(deployment["spec"]["template"]["spec"]["automountServiceAccountToken"])
        for container in containers:
            self.assertRegex(container["image"], r"@sha256:[a-f0-9]{64}$")
        kind = yaml.safe_load((ROOT / "config/kind.yaml").read_text())
        mapping = kind["nodes"][0]["extraPortMappings"][0]
        self.assertEqual(mapping["containerPort"], demo.API_NODE_PORT)
        self.assertEqual(mapping["hostPort"], demo.CLI_PORT)
        self.assertEqual(mapping["listenAddress"], "127.0.0.1")

    def test_attachment_reads_stable_namespace_identity_and_waits_on_public_regions(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(demo, "OUT", Path(tmp)), \
             patch.object(demo, "kube", return_value=json.dumps({"metadata": {"uid": "cluster-uid"}})) as kube, \
             patch.object(demo, "api", side_effect=[{"regions": []}, {"regions": [{"region": demo.REGION, "healthStatus": "healthy"}]}]) as api, \
             patch.object(demo.time, "sleep"):
            demo.check_attachment()
            evidence = json.loads((Path(tmp) / "cluster-attachment.json").read_text())
        self.assertEqual(evidence["clusterKey"], "cluster-uid")
        self.assertEqual(api.call_args.args[0], "loco.resource.v1.ResourceService/ListRegions")
        kube.assert_called_once_with("get", "namespace", "kube-system", "-o", "json")


class FailureClassificationTests(unittest.TestCase):
    def test_runtime_failures_are_not_assigned_to_old_product_issues(self):
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
                failures = [result for result in demo.RESULTS if result["status"] == "FAIL"]
                self.assertEqual([result["name"] for result in failures], ["health:payment", "metrics-api"])
                self.assertTrue(all(result.get("issue") is None for result in failures))


class LocalCertificateTests(unittest.TestCase):
    def manifests(self):
        before = [
            {"kind": "ClusterIssuer", "metadata": {"name": "loco-demo-local"}, "spec": {"acme": {"server": "https://acme.example"}}},
            {"kind": "Certificate", "spec": {"issuerRef": {"name": "loco-demo-local", "kind": "ClusterIssuer"}}},
            {"kind": "Deployment", "spec": {"template": {"spec": {"containers": [{"image": "original"}]}}}},
        ]
        after = copy.deepcopy(before)
        after[0]["spec"] = {"selfSigned": {}}
        return before, after

    def test_local_certificate_issuer_has_no_acme_dependency(self):
        before, after = self.manifests()
        demo.assert_local_certificates(before, after)
        with self.assertRaisesRegex(RuntimeError, "self-signed"):
            demo.assert_local_certificates(before, before)

    def test_renderer_cannot_modify_controller_workloads(self):
        before, after = self.manifests()
        after[2]["spec"]["template"]["spec"]["containers"][0]["image"] = "modified"
        with self.assertRaisesRegex(RuntimeError, "change only the demo issuer"):
            demo.assert_local_certificates(before, after)

    def test_certificate_must_reference_the_local_issuer(self):
        before, after = self.manifests()
        before[1]["spec"]["issuerRef"]["name"] = "remote"
        after[1]["spec"]["issuerRef"]["name"] = "remote"
        with self.assertRaisesRegex(RuntimeError, "must reference"):
            demo.assert_local_certificates(before, after)


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
            (directory / "secrets.json").write_text(json.dumps({"session": self.secret}))
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
            (directory / "secrets.json").write_text(json.dumps({"session": self.secret}))
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
            (directory / "secrets.json").write_text(json.dumps({"session": self.secret}))
            output = io.StringIO()
            with patch.object(demo, "STATE", directory), redirect_stdout(output), \
                 patch.object(demo.time, "monotonic", side_effect=[0, 15, 31, 32]), \
                 patch.object(demo.time, "sleep"):
                demo.wait_for("cluster attachment", ready, seconds=60)
            text = output.getvalue()
            self.assertIn("WAIT cluster attachment (15s elapsed)", text)
            self.assertIn("WAIT cluster attachment (31s elapsed)", text)
            self.assertIn("READY cluster attachment (32s)", text)
            self.assertIn("latest error: not ready [REDACTED]", text)
            self.assertNotIn(self.secret, text)


if __name__ == "__main__":
    unittest.main()
