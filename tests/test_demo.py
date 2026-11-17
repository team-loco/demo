import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
