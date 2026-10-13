import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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


class HarnessTests(unittest.TestCase):
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

    def test_omitted_health_never_passes_when_loco_injects_http_probes(self):
        deployment = {"spec": {"template": {"spec": {"containers": [
            {"livenessProbe": {"httpGet": {"path": "/health", "port": 50051}},
             "readinessProbe": {"httpGet": {"path": "/health", "port": 50051}}}]}}}}
        with patch.object(demo, "wait_for"), patch.object(demo, "kube", return_value=json.dumps(deployment)):
            with self.assertRaisesRegex(RuntimeError, "omits health but Loco installed probes"):
                demo.check_pods("payment", "payment-id")

    def test_timeout_reports_the_last_failure(self):
        with patch.object(demo.time, "monotonic", side_effect=[0, 5]):
            def broken():
                raise RuntimeError("connection refused")
            with self.assertRaisesRegex(RuntimeError, "connection refused"):
                demo.wait_for("API", broken, seconds=1)


if __name__ == "__main__":
    unittest.main()
