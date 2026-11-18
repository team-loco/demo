import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import demo


class InstallationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state"
        self.state.mkdir()
        self.patch = patch.object(demo, "STATE", self.state)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_namespace_discovery_supports_release_and_environment_isolation(self):
        for name in ["loco-ws-shop", "loco-ws-production"]:
            with self.subTest(namespace=name):
                data = {"workspaceId": "shop", "environmentId": "production"}
                (self.state / "identity.json").write_text(json.dumps(data))
                namespaces = {"items": [
                    {"metadata": {"name": name, "labels": {"loco.io/environment-id": "production"}}},
                    {"metadata": {"name": "staging", "labels": {"loco.io/environment-id": "staging"}}},
                ]}
                with patch.object(demo, "kube", return_value=json.dumps(namespaces)):
                    self.assertTrue(demo.discover_app_namespace())
                self.assertEqual(demo.app_namespace(), name)

    def test_namespace_discovery_waits_for_controller(self):
        (self.state / "identity.json").write_text(json.dumps({"workspaceId": "shop", "environmentId": "production"}))
        with patch.object(demo, "kube", return_value='{"items": []}'):
            self.assertFalse(demo.discover_app_namespace())
        self.assertNotIn("namespace", demo.identity())

    def test_failed_deploy_preserves_original_failure(self):
        with patch.object(demo, "create_workspace"), patch.object(demo, "record", return_value=False), \
                patch.object(demo, "record_resources") as resources, patch.object(demo, "run") as run:
            self.assertFalse(demo.deploy())
        resources.assert_not_called()
        run.assert_not_called()

    def test_e2e_failed_deploy_skips_verification(self):
        with patch.object(sys, "argv", ["demo.py", "e2e", "--keep"]), \
                patch.object(demo, "OUT", self.state), patch.object(demo, "record", return_value=True), \
                patch.object(demo, "RESULTS", [{"status": "FAIL"}]), \
                patch.object(demo, "deploy", return_value=False), patch.object(demo, "verify") as verify, \
                patch.object(demo, "diagnostics"), patch.object(demo, "save_report"):
            self.assertEqual(demo.main(), 1)
        verify.assert_not_called()

    def test_new_cluster_clears_old_installation_state(self):
        (self.state / "identity.json").write_text('{"workspaceId": "deleted-workspace"}')
        (self.state / "home").mkdir()
        (self.state / "home/credentials.json").write_text("old-login")

        def create_cluster(args, **kwargs):
            if args[:3] == ["kind", "get", "clusters"] or args[:2] == ["docker", "ps"]:
                return ""
            self.assertFalse((self.state / "identity.json").exists())
            self.assertFalse((self.state / "home").exists())
            self.assertTrue((self.state / "owner.yaml").exists())
            return ""

        with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                patch.object(demo, "run", side_effect=create_cluster), patch.object(demo, "start_fixtures"), \
                patch.object(demo, "install_loco"), patch.object(demo, "INFRA_STARTED", False):
            demo.up()

    def test_existing_cluster_does_not_clear_state(self):
        identity = self.state / "identity.json"
        identity.write_text("existing-workspace")
        with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                patch.object(demo, "run", return_value=demo.CLUSTER):
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                demo.up()
        self.assertEqual(identity.read_text(), "existing-workspace")

    def test_up_recovers_owned_partial_teardown(self):
        for interrupted in (False, True):
            with self.subTest(interrupted=interrupted):
                self.state.mkdir(exist_ok=True)
                demo.write_yaml(self.state / "owner.yaml", {"root": str(demo.ROOT)})
                (self.state / "identity.json").write_text("stale-workspace")
                nodes = [f"{demo.CLUSTER}-worker"]
                if interrupted:
                    nodes.append(f"{demo.CLUSTER}-control-plane")

                def run(args, **kwargs):
                    match args[:3]:
                        case ["kind", "get", "clusters"]:
                            return demo.CLUSTER if nodes else ""
                        case ["kind", "get", "nodes"]:
                            return "\n".join(nodes)
                        case ["kind", "delete", "cluster"]:
                            if len(nodes) == 2:
                                nodes.remove(f"{demo.CLUSTER}-worker")
                                raise RuntimeError("kind deletion interrupted")
                            nodes.clear()
                        case ["kind", "create", "cluster"]:
                            self.assertFalse(nodes)
                            self.assertFalse((self.state / "identity.json").exists())
                            self.assertFalse(demo.teardown_pending())
                            nodes.append(f"{demo.CLUSTER}-control-plane")
                        case ["docker", "ps", "-a"]:
                            return ""
                        case _:
                            self.fail(f"Unexpected command {args}")

                with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                        patch.object(demo, "run", side_effect=run), patch.object(demo, "compose"), \
                        patch.object(demo, "start_fixtures"), patch.object(demo, "install_loco") as install, \
                        patch.object(demo, "INFRA_STARTED", False):
                    if interrupted:
                        with self.assertRaisesRegex(RuntimeError, "kind deletion interrupted"):
                            demo.down()
                        self.assertTrue(demo.teardown_pending())
                        self.assertTrue(demo.owns_infrastructure())
                    demo.up()
                install.assert_called_once()

    def test_deploy_checks_api_before_using_saved_credentials(self):
        demo.write_yaml(self.state / "owner.yaml", {"root": str(demo.ROOT)})
        (self.state / "identity.json").write_text("stale-workspace")
        with patch.object(demo, "OUT", self.state), patch.object(demo, "RESULTS", []), \
                patch.object(demo, "create_workspace") as workspace, patch.object(demo, "loco") as loco, \
                patch.object(demo.urllib.request, "urlopen", side_effect=ConnectionRefusedError):
            self.assertFalse(demo.deploy())
            self.assertIn("The demo API is unavailable. Run mise run up", demo.RESULTS[0]["detail"])
        workspace.assert_not_called()
        loco.assert_not_called()

    def test_checkout_recovers_when_failed_response_already_consumed_cart(self):
        carts, attempts = {}, []

        def frontend(request, timeout):
            path = urllib.parse.urlparse(request.full_url).path
            data = json.loads(request.data) if request.data else None
            if path in ["/api/products", "/api/recommendations"]:
                response = [{"id": "product"}]
            elif path == "/api/cart":
                carts[data["userId"]] = [data["item"]]
                response = {"items": carts[data["userId"]]}
            elif path == "/api/checkout":
                attempts.append(data["userId"])
                items = carts.pop(data["userId"], [])
                if not items:
                    raise urllib.error.HTTPError(request.full_url, 400, "empty cart", {}, io.BytesIO())
                if len(attempts) == 1:
                    raise urllib.error.HTTPError(request.full_url, 500, "connection dropped after order", {}, io.BytesIO())
                response = {"orderId": "verified-order"}
            else:
                self.fail(f"Unexpected frontend request: {path}")
            return io.BytesIO(json.dumps(response).encode())

        def wait(name, predicate, seconds):
            for _ in range(2):
                try:
                    if predicate():
                        return
                except demo.RETRYABLE as error:
                    if isinstance(error, urllib.error.HTTPError):
                        error.close()
            raise RuntimeError(f"{name} did not recover")

        with patch.object(demo, "OUT", self.state), patch.object(demo, "payment_counter", return_value=(0, None)), \
                patch.object(demo, "app_namespace", return_value="workloads"), patch.object(demo.subprocess, "Popen") as proc, \
                patch.object(demo.urllib.request, "urlopen", side_effect=frontend), patch.object(demo, "wait_for", side_effect=wait):
            self.assertIn("verified-order", demo.traffic())
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(*attempts)
        self.assertEqual(json.loads((self.state / "order.json").read_text())["orderId"], "verified-order")
        proc.return_value.terminate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
