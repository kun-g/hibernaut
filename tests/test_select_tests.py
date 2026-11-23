from __future__ import annotations

import unittest

from scripts.select_tests import select_tests


class TestSelectTests(unittest.TestCase):
    def test_switchboard_change_selects_only_switchboard_and_bridge_tests(self) -> None:
        res = select_tests(["src/switchboard/relay_hub.py"])
        self.assertFalse(res.full_suite)
        self.assertIn("tests/test_switchboard_relay.py", res.tests)
        self.assertIn("tests/test_switchboard_core.py", res.tests)
        self.assertNotIn("tests/test_guidance_catalog.py", res.tests)
        self.assertNotIn("tests/test_pr_delivery.py", res.tests)

    def test_ccpeer_deploy_example_change_selects_its_test(self) -> None:
        res = select_tests(["deploy/ccpeer/com.hibernaut.ccpeer.plist.example"])
        self.assertFalse(res.full_suite)
        self.assertEqual(res.tests, ("tests/test_ccpeer.py",))

    def test_taskboard_schema_change_selects_schema_consuming_tests(self) -> None:
        res = select_tests(["schemas/taskboard/work_ready.metadata.schema.json"])
        self.assertFalse(res.full_suite)
        self.assertEqual(
            res.tests,
            (
                "tests/taskboard/test_m3_engine_and_switchboard.py",
                "tests/taskboard/test_m4_architecture_and_schemas.py",
                "tests/taskboard/test_m5_publish_replay.py",
                "tests/taskboard/test_m6_switchboard_contract.py",
                "tests/taskboard/test_m7_service.py",
                "tests/test_taskboard_dual_process_e2e.py",
                "tests/test_taskboard_hibernaut_loop.py",
            ),
        )

    def test_runner_schema_change_selects_schema_consuming_test(self) -> None:
        res = select_tests(["schemas/runner/v1/runner.run_request.schema.json"])
        self.assertFalse(res.full_suite)
        self.assertEqual(
            res.tests,
            ("tests/test_runner_pilot.py", "tests/test_runner_service.py", "tests/test_runner_stage1.py"),
        )

    def test_runner_service_program_and_deploy_example_select_the_tests_that_start_it(self) -> None:
        # The check program is run as a script; no test imports everything that starts it.
        program = select_tests(["src/runner/protocol_check.py"])
        self.assertIn("tests/test_runner_service.py", program.tests)
        self.assertIn("tests/test_runner_pilot.py", program.tests)
        self.assertEqual(select_tests(["deploy/runner/runner.example.toml"]).tests, ("tests/test_runner_service.py",))

    def test_taskboard_service_entry_and_example_config_select_service_tests(self) -> None:
        entry = select_tests(["src/taskboard/app/__main__.py"])
        self.assertEqual(
            entry.tests,
            (
                "tests/taskboard/test_m7_service.py",
                "tests/test_taskboard_dual_process_e2e.py",
                "tests/test_taskboard_triple_process_e2e.py",
            ),
        )
        example = select_tests(["taskboard.example.toml"])
        self.assertFalse(example.full_suite)
        self.assertEqual(example.tests, ("tests/taskboard/test_m7_service.py",))

    def test_switchboard_deploy_kit_change_selects_its_test(self) -> None:
        res = select_tests(["deploy/switchboard/Dockerfile"])
        self.assertFalse(res.full_suite)
        self.assertEqual(res.tests, ("tests/test_switchboard_deploy.py",))

    def test_taskboard_deploy_kit_and_prepare_script_select_the_deploy_test(self) -> None:
        for changed in (
            "deploy/taskboard/Dockerfile",
            "deploy/taskboard/docker-compose.yml",
            "deploy/taskboard/README.md",
            "scripts/prepare_taskboard_deploy.py",
        ):
            with self.subTest(changed):
                res = select_tests([changed])
                self.assertFalse(res.full_suite)
                self.assertEqual(res.tests, ("tests/test_taskboard_deploy.py",))

    def test_triple_process_e2e_runs_when_any_of_its_three_entrypoints_changes(self) -> None:
        for entry in (
            "src/switchboard/app.py",
            "src/taskboard/app/__main__.py",
            "src/agent_runtime/app/main.py",
            "tests/dual_process_hibernaut.py",
        ):
            with self.subTest(entry):
                self.assertIn("tests/test_taskboard_triple_process_e2e.py", select_tests([entry]).tests)

    def test_delivery_and_channel_changes_stay_scoped_to_affected_tests(self) -> None:
        delivery = select_tests(["src/agent_runtime/adapters/delivery/pr_delivery.py"])
        self.assertFalse(delivery.full_suite)
        self.assertIn("tests/test_pr_delivery.py", delivery.tests)
        self.assertIn("tests/test_sde_delivery.py", delivery.tests)
        self.assertIn("tests/test_verified_delivery.py", delivery.tests)
        self.assertIn("tests/test_architecture_boundaries.py", delivery.tests)
        self.assertNotIn("tests/test_switchboard_inbound.py", delivery.tests)
        self.assertNotIn("tests/test_gdrive_credential_proxy.py", delivery.tests)

        mcp = select_tests(["src/agent_runtime/adapters/channels/mcp_server.py"])
        self.assertFalse(mcp.full_suite)
        self.assertIn("tests/test_mcp_server.py", mcp.tests)
        self.assertIn("tests/test_architecture_boundaries.py", mcp.tests)
        self.assertNotIn("tests/test_guidance_catalog.py", mcp.tests)
        self.assertNotIn("tests/test_pr_delivery.py", mcp.tests)
        self.assertNotIn("tests/test_switchboard_relay.py", mcp.tests)

    def test_sibling_script_and_skill_data_fan_out_through_graph(self) -> None:
        gate = select_tests(["src/agent_runtime/adapters/executor_cli/claude_gate.py"])
        self.assertFalse(gate.full_suite)
        self.assertIn("tests/test_visible_roles.py", gate.tests)

        fake_gh = select_tests(["tests/fake_gh.py"])
        self.assertFalse(fake_gh.full_suite)
        self.assertIn("tests/test_pr_delivery.py", fake_gh.tests)
        self.assertIn("tests/test_sde_delivery.py", fake_gh.tests)

        skill = select_tests(["src/agent_runtime/adapters/executor_cli/skills/sde/SKILL.md"])
        self.assertFalse(skill.full_suite)
        self.assertIn("tests/test_role_skills_and_project_guidance.py", skill.tests)

    def test_run_updating_and_resource_rules_select_targeted_tests(self) -> None:
        updater = select_tests(["scripts/run_updating.py"])
        self.assertFalse(updater.full_suite)
        self.assertEqual(
            updater.tests,
            ("tests/test_auto_update.py", "tests/test_config_reload.py", "tests/test_graceful_stop.py",
             "tests/test_listener_handover.py"),
        )

        readme = select_tests(["README.md"])
        self.assertFalse(readme.full_suite)
        self.assertEqual(readme.tests, ("tests/test_sde_delivery.py",))

        pure_doc = select_tests(["docs/architecture.md", ".gitignore"])
        self.assertFalse(pure_doc.full_suite)
        self.assertEqual(pure_doc.tests, ())

    def test_global_unknown_or_deleted_code_falls_back_to_full_suite(self) -> None:
        ci = select_tests([".github/workflows/ci.yml"])
        self.assertTrue(ci.full_suite)
        self.assertEqual(ci.reason, "global trigger: .github/workflows/ci.yml")

        for path in ("pyproject.toml", ".importlinter", "src/agent_runtime/deleted_mod.py"):
            with self.subTest(path=path):
                res = select_tests([path])
                self.assertTrue(res.full_suite)
                self.assertGreater(len(res.tests), 30)

        deleted_test = select_tests(["tests/test_nonexistent_removed.py"])
        self.assertFalse(deleted_test.full_suite)
        self.assertEqual(deleted_test.tests, ())


if __name__ == "__main__":
    unittest.main()
