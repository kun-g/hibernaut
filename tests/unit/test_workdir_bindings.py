import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.app.config import parse_config
from tests.support.workdir_bindings import OPS


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.ops = self.root / "ops"
        self.ops.mkdir()
        self.checkout = self.root / "repo"
        (self.checkout / "sub").mkdir(parents=True)

    def parse(self, *bindings, project=None, base_dir=None, steward=None):
        raw = {
            "runtime": {"inbox_bindings": list(bindings)},
            "executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
            "agents": {"ops": {"executor": "cli"}, "asker": {"executor": "cli"},
                       "steward": {"role": "steward", "executor": "claude-code", **(steward or {})},
                       "sde": {"role": "sde", "executor": "claude-code", "review": False},
                       "reviewer": {"role": "code-reviewer", "executor": "claude-code"}},
            "projects": {"p": {"repo": "o/r", "checkout": str(self.checkout), "worktree_backend": "git",
                               **(project or {})}},
        }
        return parse_config(raw, base_dir=base_dir)

    def binding(self, pattern=OPS, **extra):
        return {"pattern": pattern, "default_agent": "ops", "workdir": str(self.ops), **extra}

    def refused(self, message, *bindings, **kwargs):
        with self.assertRaisesRegex(ValueError, message):
            self.parse(*bindings, **kwargs)

    def test_a_binding_is_read_with_its_workdir(self):
        [bound] = self.parse(self.binding(agents=["ops", "asker"],
                                          rules=[{"tags": ["ask"], "agent": "asker"}])).runtime.inbox_bindings
        self.assertEqual((bound.workdir, bound.readonly, bound.binding.default_agent, bound.binding.agents),
                         (str(self.ops), False, "ops", ("ops", "asker")))
        self.assertEqual([rule.agent for rule in bound.binding.rules], ["asker"])
        self.assertTrue(bound.rule.matches(OPS))
        self.assertEqual(self.parse().runtime.inbox_bindings, ())

    def test_the_workdir_is_required(self):
        for raw in ({"pattern": OPS, "default_agent": "ops"}, self.binding(workdir=""), self.binding(workdir="  "),
                    self.binding(workdir=3)):
            with self.subTest(raw=raw):
                self.refused(r"runtime\.inbox_bindings\[0\]: workdir must be a non-empty string", raw)

    def test_home_and_relative_workdirs_are_resolved(self):
        with patch.dict(os.environ, {"HOME": str(self.root)}):
            cfg = self.parse(self.binding(workdir="~/ops"), self.binding("user:main", workdir="other"),
                             base_dir=self.root / "conf")
        self.assertEqual([b.workdir for b in cfg.runtime.inbox_bindings],
                         [str(self.ops), str(self.root / "conf" / "other")])
        self.assertFalse((self.root / "conf").exists())

    def test_two_bindings_may_not_write_one_workdir(self):
        link = self.root / "link"
        link.symlink_to(self.ops)
        for other in (self.ops, link, f"{self.ops}/../ops"):
            with self.subTest(other=str(other)):
                self.refused("same writable workdir", self.binding(), self.binding("user:main", workdir=str(other)))

    def test_a_workdir_that_does_not_exist_yet_is_compared_too(self):
        missing = str(self.root / "later")
        self.refused("same writable workdir", self.binding(workdir=missing), self.binding("user:main", workdir=missing))

    def test_a_missing_workdir_in_another_spelling_is_the_same_one(self):
        # Once created, both name one directory on a volume that ignores letter case (APFS by default).
        link = self.root / "link"
        link.symlink_to(self.ops)
        for one, other in ((self.ops / "Hb", self.ops / "hb"), (self.ops / "a" / "Hb", self.ops / "A" / "hB"),
                           (self.ops / "hb", link / "HB"), (self.ops / "caf\u00e9", self.ops / "cafe\u0301")):
            with self.subTest(one=str(one), other=str(other)):
                self.refused("same writable workdir", self.binding(workdir=str(one)),
                             self.binding("user:main", workdir=str(other)))
        self.parse(self.binding(workdir=str(self.ops / "Hb")),
                   self.binding("user:main", workdir=str(self.ops / "hb2")))
        self.assertEqual(list(self.ops.iterdir()), [])

    def test_a_missing_workdir_in_a_missing_checkout_is_found_in_another_spelling(self):
        self.refused("checkout of projects.p", self.binding(workdir=str(self.root / "Later" / "sub")),
                     project={"checkout": str(self.root / "later")})

    def test_readonly_bindings_may_share_a_workdir(self):
        cfg = self.parse(self.binding(checkout_access="readonly"),
                         self.binding("user:main", checkout_access="readonly"),
                         self.binding("sys:writer"))
        self.assertEqual([b.readonly for b in cfg.runtime.inbox_bindings], [True, True, False])
        self.refused("checkout_access must be", self.binding(checkout_access="write"))

    def test_a_pattern_of_many_inboxes_needs_a_readonly_workdir(self):
        self.refused("more than one inbox", self.binding("sys:ops(:.+)?"))
        self.parse(self.binding("sys:ops(:.+)?", checkout_access="readonly"))

    def test_a_writable_workdir_is_not_a_projects_checkout(self):
        for workdir in (self.checkout, self.checkout / "sub"):
            with self.subTest(workdir=str(workdir)):
                self.refused("checkout of projects.p", self.binding(workdir=str(workdir)))
                self.parse(self.binding(workdir=str(workdir), checkout_access="readonly"))
        self.parse(self.binding(workdir=str(self.root)))

    def test_a_writable_workdir_is_not_a_stewards(self):
        own = self.root / "steward"
        (own / "sub").mkdir(parents=True)
        for workdir in (own, own / "sub", self.root / "link" / "sub"):
            with self.subTest(workdir=str(workdir)):
                if not (self.root / "link").exists():
                    (self.root / "link").symlink_to(own)
                self.refused("workdir of agents.steward", self.binding(workdir=str(workdir)),
                             steward={"workdir": str(own)})
                self.parse(self.binding(workdir=str(workdir), checkout_access="readonly"),
                           steward={"workdir": str(own)})
        self.parse(self.binding(), steward={"workdir": str(own)})

    def test_a_writable_workdir_is_not_a_checkout_found_through_orca(self):
        with patch("agent_runtime.app.config.registered_checkout", return_value=str(self.checkout)):
            self.refused("checkout of projects.p", self.binding(workdir=str(self.checkout)), project={"checkout": ""})

    def test_a_binding_may_not_cover_a_projects_inbox(self):
        for pattern, inbox_id in (
            ("proj:p", None), ("proj:.+", None), (OPS, ["proj:p", "sys:.+"]), ("sys:a.*", ["proj:p", "sys:.*b"]),
            (OPS, OPS),
        ):
            with self.subTest(pattern=pattern, inbox_id=inbox_id):
                self.refused(r"runtime\.inbox_bindings", self.binding(pattern, checkout_access="readonly"),
                             project={"inbox_id": inbox_id} if inbox_id else None)
        self.parse(self.binding("sys:.+", checkout_access="readonly"), project={"inbox_id": ["proj:p", "proj:p:.+"]})

    def test_other_mistakes_are_rejected_like_in_project_bindings(self):
        for raw, message in (
            (self.binding(default_agent="ghost"), "default_agent 'ghost' is not declared"),
            (self.binding(agents=["asker"]), "default_agent 'ops' is not in agents"),
            (self.binding(colour="red"), "colour not supported"),
            (self.binding(pattern="sys:("), "invalid pattern"),
            (self.binding(pattern="sys/ops"), "sys/ops"),
            (self.binding(rules=[{"agent": "ops"}]), "needs source, event_type or tags"),
        ):
            with self.subTest(message=message):
                self.refused(message, raw)
        self.refused("listed twice", self.binding(), self.binding(workdir=str(self.root / "b")))

    def test_roles_work_in_a_project_and_are_refused(self):
        for agent in ("sde", "reviewer", "steward"):
            for extra in ({"default_agent": agent}, {"agents": ["ops", agent]},
                          {"rules": [{"tags": ["x"], "agent": agent}]}):
                with self.subTest(agent=agent, extra=extra):
                    self.refused(f"agent '{agent}'", self.binding(**extra))

    def test_project_bindings_take_no_workdir(self):
        self.refused("workdir not supported", project={"inbox_bindings": [
            {"pattern": "proj:p", "default_agent": "ops", "workdir": str(self.ops)}]})


if __name__ == "__main__":
    unittest.main()
