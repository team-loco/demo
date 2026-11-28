import fcntl
import io
import json
from pathlib import Path
import signal
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

    def test_gitops_publication_keeps_platform_sources_and_publishes_complete_revisions(self):
        import shutil
        import yaml
        repo = self.state / "gitops"
        shutil.copytree(demo.GITOPS, repo)
        uploads = {}
        def upload(**request):
            uploads[request["Key"]] = request["Body"]
        def flux(*args, **kwargs):
            if args[:3] == ("create", "kustomization", "demo-config"):
                revision = args[args.index("--path") + 1].removeprefix("./").removesuffix("/clusters/local")
                self.assertIn(revision + "/clusters/local/kustomization.yaml", uploads)
                self.assertIn(revision + "/clusters/local/config/openbao-seal.yaml", uploads)
            return ""
        for registry in ["ghcr.io", demo.REGISTRY_ADDRESS]:
            with self.subTest(registry=registry):
                shutil.copytree(demo.GITOPS, repo, dirs_exist_ok=True)
                previous_uploads = dict(uploads)
                with patch.object(demo, "tune_flux_dependency_retries"), \
                        patch.object(demo, "storage_client") as storage, \
                        patch.object(demo, "flux", side_effect=flux), patch.object(demo, "kube"):
                    storage.return_value.put_object.side_effect = upload
                    demo.publish_gitops(registry + "/team-loco/loco/platform@sha256:" + "a" * 64, "./all")
                self.assertEqual({key: uploads[key] for key in previous_uploads}, previous_uploads)
                rendered = yaml.safe_load_all(demo.run(["kubectl", "kustomize", repo / "clusters/local",
                                                       "--load-restrictor=LoadRestrictionsNone"]))
                resources = {doc["metadata"]["name"]: doc for doc in rendered}
                for name in ["loco", "loco-postgres"]:
                    platform = resources[name]["spec"]
                    self.assertEqual(platform["sourceRef"], {"kind": "OCIRepository", "name": "platform-bundle"})
                    if registry == demo.REGISTRY_ADDRESS:
                        nested = json.loads(platform["patches"][0]["patch"])[0]["value"]
                        self.assertEqual(json.loads(nested[0]["patch"]), [
                            {"op": "add", "path": "/spec/insecure", "value": True}])
                config = resources["loco-cluster-config"]["spec"]
                self.assertEqual(config["sourceRef"], {"kind": "Bucket", "name": "demo-config"})
                self.assertIn(config["path"].removeprefix("./") + "/openbao-seal.yaml", uploads)
                self.assertIn({"name": "loco-postgres-operator"}, config["dependsOn"])

        with patch.object(demo, "tune_flux_dependency_retries"), \
                patch.object(demo, "storage_client") as storage, patch.object(demo, "flux") as publish:
            storage.return_value.put_object.side_effect = RuntimeError("upload interrupted")
            with self.assertRaisesRegex(RuntimeError, "upload interrupted"):
                demo.publish_gitops("ghcr.io/team-loco/loco/platform@sha256:" + "b" * 64, "./all")
        publish.assert_not_called()

    def test_create_workspace_uses_the_demo_users_starter_org(self):
        replies = [
            {"user": {"id": "demo-user"}},
            {"orgs": [{"id": "new-org", "createdBy": "demo-user"}], "nextPageToken": "next"},
            {"orgs": [{"id": "starter-org", "createdBy": "demo-user"},
                      {"id": "invited-org", "createdBy": "another-user"}]},
            {"workspaceId": "shop"},
            {"environments": [{"id": "production-id", "name": "production"}]},
            {"token": "demo-telemetry-token"},
        ]
        with patch.object(demo, "OUT", self.state), patch.object(demo, "api", side_effect=replies) as api, \
                patch.object(demo, "loco") as loco:
            demo.create_workspace()

        self.assertEqual(api.call_args_list[2].args[1]["pageToken"], "next")
        self.assertEqual(api.call_args_list[3].args, (
            "loco.workspace.v1.WorkspaceService/CreateWorkspace",
            {"orgId": "starter-org", "name": "astronomy-shop"},
        ))
        loco.assert_called_once_with("use", "starter-org/shop")
        self.assertEqual(demo.identity()["orgId"], "starter-org")

    def test_record_resources_uses_the_file_key_for_a_renamed_service(self):
        status = {"resource": {"id": "frontend-id", "slug": "frontend", "name": "Astronomy Storefront"}}
        (self.state / "identity.json").write_text(json.dumps({"orgId": "demo-org", "workspaceId": "demo-workspace"}))
        with patch.object(demo, "SERVICES", {"frontend": {}}), \
                patch.object(demo, "loco", return_value=json.dumps(status)) as loco:
            demo.record_resources()

        loco.assert_called_once_with("resource", "status", "frontend", "--output", "json", "--host", demo.API_URL,
                                     "--org", "demo-org", "--workspace", "demo-workspace")
        self.assertEqual(demo.resources(), {"frontend": "frontend-id"})

    def test_reload_rejects_edited_applied_migration_before_building(self):
        checkout = Path(self.temp.name) / "core"
        migrations = checkout / demo.MIGRATION_DIR
        migrations.mkdir(parents=True)
        applied = migrations / "00001_initial.sql"
        applied.write_text("CREATE TABLE session_tokens (id int);")
        (self.state / "database-migrations.json").write_text(
            json.dumps({"files": demo.migration_hashes(checkout)}))
        release = self.state / "gitops/releases/control-plane.yaml"
        release.parent.mkdir(parents=True)
        release.touch()
        applied.write_text("CREATE TABLE session_tokens (id int, kind text);")

        with patch.object(demo, "owns_infrastructure", return_value=True), \
                patch.object(demo, "loco_source", return_value={"source": str(checkout)}), \
                patch.object(demo, "kube", return_value="./all"), patch.object(demo, "prepare"), \
                patch.object(demo, "platform_bundle") as build:
            with self.assertRaisesRegex(RuntimeError, "00001_initial.sql.*mise run down"):
                demo.reload()
        build.assert_not_called()

    def test_reload_allows_new_migration_after_release_install(self):
        checkout = Path(self.temp.name) / "core"
        migrations = checkout / demo.MIGRATION_DIR
        migrations.mkdir(parents=True)
        initial = migrations / "00001_initial.sql"
        initial.write_text("CREATE TABLE session_tokens (id int);")
        demo.run(["git", "init", "--quiet"], cwd=checkout)
        demo.run(["git", "add", "."], cwd=checkout)
        demo.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.test",
                  "commit", "--quiet", "-m", "Initial schema"], cwd=checkout)
        commit = demo.run(["git", "rev-parse", "HEAD"], cwd=checkout).strip()
        (self.state / "database-migrations.json").write_text(json.dumps({"commit": commit}))
        added = migrations / "00002_new.sql"
        added.write_text("ALTER TABLE session_tokens ADD COLUMN kind text;")

        with patch.object(demo, "core_checkout", return_value=checkout):
            self.assertEqual(set(demo.check_reload_migrations()), {initial.name, added.name})

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

    def test_verify_stops_polling_after_services_cannot_start(self):
        failure = {"name": "health:frontend", "status": "FAIL", "detail": "Insufficient cpu"}
        with patch.object(demo, "RESULTS", [failure]), patch.object(demo, "resources", return_value={}), \
                patch.object(demo, "check_health"), patch.object(demo, "save_report") as report, \
                patch.object(demo, "record", side_effect=AssertionError("Polled after failed service health")):
            self.assertFalse(demo.verify())
        report.assert_called_once()

    def test_failed_deploy_preserves_original_failure(self):
        with patch.object(demo, "create_workspace"), patch.object(demo, "record", return_value=False), \
                patch.object(demo, "record_resources") as resources, patch.object(demo, "run") as run:
            self.assertFalse(demo.deploy())
        resources.assert_not_called()
        run.assert_not_called()

    def test_e2e_failed_deploy_skips_verification(self):
        with patch.object(sys, "argv", ["demo.py", "e2e", "--keep"]), \
                patch.object(demo, "CACHE", self.state), \
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
            self.assertTrue((self.state / "cluster-creating").exists())
            return ""

        with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                patch.object(demo, "run", side_effect=create_cluster), patch.object(demo, "start_fixtures"), \
                patch.object(demo, "install_loco"), patch.object(demo, "INFRA_STARTED", False):
            demo.up()
        self.assertFalse((self.state / "cluster-creating").exists())

    def test_existing_cluster_does_not_clear_state(self):
        identity = self.state / "identity.json"
        identity.write_text("existing-workspace")
        for owned in (False, True):
            with self.subTest(owned=owned):
                if owned:
                    demo.write_yaml(self.state / "owner.yaml", {"root": str(demo.ROOT)})
                    (self.state / "kubeconfig").touch()

                with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                        patch.object(demo, "cluster_exists", return_value=True), \
                        patch.object(demo, "run", return_value=f"{demo.CLUSTER}-control-plane"), \
                        patch.object(demo, "down") as down:
                    with self.assertRaisesRegex(RuntimeError, "already exists"):
                        demo.up()
                down.assert_not_called()
                self.assertEqual(identity.read_text(), "existing-workspace")

    def test_up_recovers_owned_partial_cluster(self):
        for phase in ("worker left behind", "interrupted deletion", "interrupted creation"):
            with self.subTest(phase=phase):
                self.state.mkdir(exist_ok=True)
                demo.write_yaml(self.state / "owner.yaml", {"root": str(demo.ROOT)})
                (self.state / "identity.json").write_text("stale-workspace")
                (self.state / "kubeconfig").touch()
                starting = phase == "interrupted creation"
                nodes = [] if starting else [f"{demo.CLUSTER}-worker"]
                if phase == "interrupted deletion":
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
                            nonlocal starting
                            self.assertFalse(nodes)
                            self.assertFalse((self.state / "identity.json").exists())
                            self.assertFalse(demo.teardown_pending())
                            nodes.append(f"{demo.CLUSTER}-control-plane")
                            args[-1].touch()
                            if starting:
                                starting = False
                                raise KeyboardInterrupt
                        case ["docker", "ps", "-a"]:
                            return ""
                        case _:
                            self.fail(f"Unexpected command {args}")

                with patch.object(demo, "prepare"), patch.object(demo, "validate"), \
                        patch.object(demo, "run", side_effect=run), \
                        patch.object(demo, "start_fixtures"), patch.object(demo, "install_loco") as install, \
                        patch.object(demo, "INFRA_STARTED", False):
                    if phase == "interrupted deletion":
                        with self.assertRaisesRegex(RuntimeError, "kind deletion interrupted"):
                            demo.down()
                        self.assertTrue(demo.teardown_pending())
                        self.assertTrue(demo.owns_infrastructure())
                    elif starting:
                        with self.assertRaises(KeyboardInterrupt):
                            demo.up()
                        self.assertTrue((self.state / "kubeconfig").is_file())
                        self.assertTrue((self.state / "cluster-creating").exists())
                    demo.up()
                    self.assertFalse((self.state / "cluster-creating").exists())
                install.assert_called_once()

    def test_down_removes_demo_without_local_ownership_and_preserves_state(self):
        for owner in (None, {"root": "/another/demo/checkout"}):
            with self.subTest(owner=owner):
                if owner:
                    demo.write_yaml(self.state / "owner.yaml", owner)
                (self.state / "identity.json").write_text("keep-local-state")
                commands = []

                def run(args, **kwargs):
                    commands.append(args)
                    if args[:3] == ["kind", "delete", "cluster"]:
                        self.assertEqual(args[3:5], ["--name", "loco-demo"])
                        self.assertNotEqual(args[-1], self.state / "kubeconfig")
                        self.assertTrue(args[-1].parent.is_dir())
                    return ""

                with patch.object(demo, "cluster_exists", side_effect=[True, False]), \
                        patch.object(demo, "run", side_effect=run):
                    demo.down()
                self.assertEqual(len(commands), 1)
                self.assertEqual((self.state / "identity.json").read_text(), "keep-local-state")
                self.assertFalse(demo.teardown_pending())
                if owner:
                    self.assertEqual((self.state / "owner.yaml").read_text(), "root: /another/demo/checkout\n")

    def test_down_reports_empty_context_only_after_discovery(self):
        output = io.StringIO()
        with patch.object(demo, "cluster_exists", return_value=False), patch("sys.stdout", output):
            demo.down()
        self.assertIn("No loco-demo cluster found", output.getvalue())

    def test_down_propagates_discovery_and_teardown_failures(self):
        cases = [
            (False, [RuntimeError("kind unavailable")], "kind unavailable"),
            (True, [True], "kind still lists loco-demo"),
        ]
        for owned, cluster, error in cases:
            with self.subTest(error=error):
                if owned:
                    demo.write_yaml(self.state / "owner.yaml", {"root": str(demo.ROOT)})
                with patch.object(demo, "run", return_value=""), \
                        patch.object(demo, "cluster_exists", side_effect=cluster):
                    with self.assertRaisesRegex(RuntimeError, error):
                        demo.down()
                self.assertTrue(self.state.exists())
                self.assertEqual(demo.teardown_pending(), owned)

    def test_lifecycle_lock_blocks_other_checkouts(self):
        lock_path = Path(self.temp.name) / "shared-lifecycle.lock"
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            for command in ("up", "down", "reload", "e2e"):
                with self.subTest(command=command), \
                        patch.object(demo, "LIFECYCLE_LOCK", lock_path), \
                        patch.object(demo, "CACHE", self.state / command), \
                        patch.object(sys, "argv", ["demo.py", command]), \
                        patch.object(demo, "run_command") as run_command:
                    self.assertEqual(demo.main(), 1)
                    run_command.assert_not_called()

    def test_streamed_release_logs_progress_and_keeps_manifest_separate(self):
        log = self.state / "platform-release.log"
        with (self.state / "manifest.json").open("w+t") as manifest:
            demo.run([sys.executable, "-c", "import sys; print('building', file=sys.stderr); print('{}')"],
                     log=log, stdout_file=manifest)
            manifest.seek(0)
            self.assertEqual(manifest.read(), "{}\n")
        self.assertEqual(log.read_text(), "building\n")

    def test_interrupt_kills_streamed_process_group(self):
        with patch.object(demo.subprocess, "Popen") as popen, patch.object(demo.os, "killpg") as kill:
            process = popen.return_value.__enter__.return_value
            process.stdout = ["started\n"]
            process.wait.side_effect = [KeyboardInterrupt, 0]
            with self.assertRaises(KeyboardInterrupt):
                demo.run(["kind"], stream=True, log=self.state / "kind.log")
        kill.assert_called_once_with(process.pid, signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 2)
        self.assertEqual((self.state / "kind.log").read_text(), "started\n")

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
