#!/usr/bin/env python3
"""Fixture tests for alias-tags.py, run by Docs / Build PR."""

import os
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alias-tags.py")

# A runner has no git identity, and annotated tags need one.
ENV = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
           GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
           GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")


class AliasTagsTest(unittest.TestCase):

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = tmp.name
        self.git("init", "-q", ".")
        self.git("commit", "-q", "--allow-empty", "-m", "one")
        self.first = self.git("rev-parse", "HEAD")
        self.git("commit", "-q", "--allow-empty", "-m", "two")
        self.second = self.git("rev-parse", "HEAD")
        self.git("tag", "4.19.2.1", self.first)
        self.git("tag", "-a", "-m", "release", "4.19.2.10", self.second)
        self.git("tag", "4.19.2.9", self.first)
        self.git("tag", "4.19.2.11-rc1", self.first)
        self.git("tag", "3.11.5.19", self.first)

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.repo, env=ENV, check=True,
                              capture_output=True, text=True).stdout.strip()

    def run_script(self, command, conf):
        with open(os.path.join(self.repo, "conf.py"), "w") as conf_py:
            conf_py.write(conf)
        return subprocess.run([sys.executable, SCRIPT, command], cwd=self.repo,
                              env=dict(ENV, CONF_PY="conf.py"), capture_output=True).returncode

    def aliases(self):
        return self.git("tag", "-l", "scylla-*")

    def alias_commit(self, alias):
        return self.git("rev-parse", "-q", "--verify", "refs/tags/%s^{commit}" % alias)

    def test_newest_patch_wins_by_version_order(self):
        self.assertEqual(0, self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x']"))
        self.assertEqual(self.second, self.alias_commit("scylla-4.19.2.x"))

    def test_an_annotated_release_tag_is_aliased_by_its_commit(self):
        self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x']")
        self.assertEqual("commit", self.git("cat-file", "-t", "refs/tags/scylla-4.19.2.x"))

    def test_release_candidates_are_ignored(self):
        self.git("tag", "-f", "4.19.2.11-rc1", self.second)
        self.git("tag", "-d", "4.19.2.10")
        self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x']")
        self.assertEqual(self.first, self.alias_commit("scylla-4.19.2.x"))

    def test_each_line_gets_its_own_alias(self):
        self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x', 'scylla-3.11.5.x']\n"
                                  "scylladb_markdown_recommonmark_versions = ['decoy']")
        self.assertEqual(self.second, self.alias_commit("scylla-4.19.2.x"))
        self.assertEqual(self.first, self.alias_commit("scylla-3.11.5.x"))

    def test_create_is_repeatable(self):
        self.assertEqual(0, self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x']"))
        self.assertEqual(0, self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x']"))

    def test_delete_removes_every_alias(self):
        conf = "RELEASE_LINES = ['scylla-4.19.2.x', 'scylla-3.11.5.x']"
        self.run_script("create", conf)
        self.assertEqual(0, self.run_script("delete", conf))
        self.assertEqual("", self.aliases())

    def test_a_line_with_no_release_fails(self):
        self.assertEqual(1, self.run_script("create", "RELEASE_LINES = ['scylla-4.20.0.x']"))

    def test_a_failed_create_leaves_no_alias(self):
        self.assertEqual(1, self.run_script("create", "RELEASE_LINES = ['scylla-4.19.2.x', 'scylla-4.20.0.x']"))
        self.assertEqual("", self.aliases())

    def test_delete_never_removes_a_release_tag(self):
        self.run_script("delete", "RELEASE_LINES = ['4.19.2.1']")
        self.assertEqual("4.19.2.1", self.git("tag", "-l", "4.19.2.1"))

    def test_a_malformed_entry_fails(self):
        self.assertEqual(1, self.run_script("create", "RELEASE_LINES = ['stable']"))

    def test_missing_release_lines_fails(self):
        self.assertEqual(1, self.run_script("create", "BRANCHES = []"))

    def test_empty_release_lines_is_a_no_op(self):
        self.assertEqual(0, self.run_script("create", "RELEASE_LINES = []"))
        self.assertEqual("", self.aliases())


if __name__ == "__main__":
    unittest.main()
