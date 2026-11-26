import fnmatch
import os
import tempfile
import tomllib
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.role_skills import BUILTIN_SKILLS_DIR, builtin_skill
from agent_runtime.app.config import load_config, parse_config
from tests.support.role_skills_and_project_guidance import write_skill


ROOT = Path(__file__).resolve().parents[2]


class TestRoleSkillConfig(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()

    def test_each_role_defaults_to_its_packaged_skill(self):
        cfg = parse_config({"executors": {"sde": {"role": "sde"}, "coder-reviewer": {"role": "coder-reviewer"}}})
        self.assertEqual(cfg.roles["sde"].profile.skills, (builtin_skill("sde"),))
        self.assertEqual(cfg.roles["coder-reviewer"].profile.skills, (builtin_skill("coder-reviewer"),))
        self.assertIn("failing test", builtin_skill("sde").read_text())
        self.assertIn("blocking", builtin_skill("coder-reviewer").read_text())
        # The packaged skills are declared as package data, so an installed wheel carries them.
        globs = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["setuptools"]["package-data"][
            "agent_runtime.adapters.executor_cli"
        ]
        package_dir = BUILTIN_SKILLS_DIR.parent
        for role in ("sde", "coder-reviewer"):
            rel = builtin_skill(role).relative_to(package_dir).as_posix()
            self.assertTrue(any(fnmatch.fnmatch(rel, g) for g in globs), rel)

    def test_ordered_skills_resolve_files_directories_and_paths_relative_to_the_config_file(self):
        conf_dir = self.root / "deploy"
        first = write_skill(conf_dir / "skills" / "team-tdd", "TEAM-TDD")
        second = write_skill(self.root / "elsewhere" / "security", "SECURITY")
        reviewer = write_skill(conf_dir / "skills" / "team-review", "TEAM-REVIEW")
        path = conf_dir / "hibernaut.toml"
        path.write_text(
            '[executors.sde]\nrole = "sde"\n'
            f'skills = ["skills/team-tdd", "{second}", "builtin:sde"]\n'
            '[executors.coder-reviewer]\nrole = "coder-reviewer"\nskills = ["./skills/team-review/SKILL.md"]\n'
        )
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            cfg = load_config(path)
        finally:
            os.chdir(cwd)
        self.assertEqual(cfg.roles["sde"].profile.skills, (first, second, builtin_skill("sde")))
        self.assertEqual(cfg.roles["coder-reviewer"].profile.skills, (reviewer,))

    def test_invalid_or_missing_skills_fail_with_the_executor_and_path(self):
        reviewer = {"coder-reviewer": {"role": "coder-reviewer"}}
        cases = {
            r"executors\.sde: skill 'nope/SKILL\.md' not found \(no file at .*nope/SKILL\.md\)": ["nope/SKILL.md"],
            r"executors\.sde: skill 'empty-dir' not found \(no file at .*empty-dir/SKILL\.md\)": ["empty-dir"],
            r"executors\.sde: 'builtin:coder-reviewer' is not available for role 'sde'": ["builtin:coder-reviewer"],
            r"executors\.sde: skills must name at least one skill": [],
            r"executors\.sde: skills must be a list": "builtin:sde",
        }
        (self.root / "empty-dir").mkdir()
        for message, skills in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"executors": {"sde": {"role": "sde", "skills": skills}, **reviewer}}, base_dir=self.root)

    def test_roles_keep_their_own_skills(self):
        sde_skill = write_skill(self.root / "a", "A")
        review_skill = write_skill(self.root / "b", "B")
        cfg = parse_config({"executors": {
            "sde": {"role": "sde", "skills": [str(sde_skill)]},
            "coder-reviewer": {"role": "coder-reviewer", "skills": [str(review_skill)]},
            "sde-default": {"role": "sde"},
        }})
        self.assertEqual(cfg.roles["sde"].profile.skills, (sde_skill,))
        self.assertEqual(cfg.roles["coder-reviewer"].profile.skills, (review_skill,))
        self.assertEqual(cfg.roles["sde-default"].profile.skills, (builtin_skill("sde"),))


if __name__ == "__main__":
    unittest.main()
