import unittest
from agent_runtime.app.config import parse_config


ROLES = {"sde": {"role": "sde"}, "coder-reviewer": {"role": "coder-reviewer"}}


class TestInteractiveConfig(unittest.TestCase):
    def test_interactive_is_a_role_setting_defaulting_to_true(self):
        cfg = parse_config({"executors": {"sde": {"role": "sde", "interactive": False},
                                          "coder-reviewer": {"role": "coder-reviewer"}}})
        self.assertFalse(cfg.roles["sde"].interactive)
        self.assertTrue(cfg.roles["coder-reviewer"].interactive)
        with self.assertRaisesRegex(ValueError, "executors.coder-reviewer: interactive must be true or false"):
            parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer", "interactive": "yes"}}})

    def test_legacy_visible_still_sets_interactive(self):
        for value in (False, True):
            cfg = parse_config({"executors": {"sde": {"role": "sde", "visible": value},
                                              "coder-reviewer": {"role": "coder-reviewer"}}})
            self.assertIs(cfg.roles["sde"].interactive, value)
        with self.assertRaisesRegex(ValueError, "executors.sde: visible must be true or false"):
            parse_config({"executors": {"sde": {"role": "sde", "visible": "no"},
                                        "coder-reviewer": {"role": "coder-reviewer"}}})

    def test_visible_and_interactive_together_are_rejected_even_when_equal(self):
        for old, new in ((True, True), (False, True), (True, False)):
            with self.assertRaisesRegex(ValueError, "executors.sde: both interactive and visible are set"):
                parse_config({"executors": {"sde": {"role": "sde", "visible": old, "interactive": new},
                                            "coder-reviewer": {"role": "coder-reviewer"}}})

    def test_terminal_backend_defaults_to_the_worktree_backend_only_for_orca(self):
        def project(**extra):
            cfg = parse_config({"executors": ROLES, "projects": {"p": {"repo": "o/p", "checkout": "/tmp/p", **extra}}})
            return cfg.projects["p"].terminal_backend

        self.assertEqual(project(), "orca")
        self.assertEqual(project(worktree_backend="orca"), "orca")
        self.assertEqual(project(worktree_backend="git"), "none")
        self.assertEqual(project(worktree_backend="git", terminal_backend="orca"), "orca")
        self.assertEqual(project(worktree_backend="orca", terminal_backend="none"), "none")
        for bad in ("tmux", "", 1):
            with self.assertRaisesRegex(ValueError, "Unsupported terminal_backend for 'p'"):
                project(terminal_backend=bad)


if __name__ == "__main__":
    unittest.main()
