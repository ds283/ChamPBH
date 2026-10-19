# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""What ChamPBH pins and configures of RunRegistryKit (rrk-adoption prompt 01).

This module checks four things about the registry's adoption:

1. ``runregistrykit.toml`` at the repository root puts the runs under ``var/runs`` and the
   datastores under ``var/datastores``, and names the distributions every manifest records;
2. the file holds only keys the package knows, and the three it needs. RunRegistryKit refuses an
   unknown key as a ``ConfigError``; a ``[stores]`` table, which prompt 02 adds, is allowed;
3. ``.gitignore`` ignores both directories, so that no run record or datastore is committed;
4. the venv holds RunRegistryKit 0.2.0, installed from the tag's commit and not editable. This is
   the pin's check. It is the first test in this repository that asserts what the venv itself
   holds, and so it fails on a venv not reinstalled from ``requirements.txt``. When the pin moves,
   the literal moves with the line.

No test needs Ray or a store, and nothing is written. ``find_client_root`` and ``git
check-ignore`` only read.
"""

import subprocess
import tomllib
import unittest
from pathlib import Path

import runregistrykit

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "runregistrykit.toml"

# The keys RunRegistryKit v0.2.0 reads (its README, "The client's root").
KNOWN_KEYS = {"runs_root", "stores_root", "distributions", "stores"}
REQUIRED_KEYS = {"runs_root", "stores_root", "distributions"}

# The tag v0.2.0 of RunRegistryKit, in requirements.txt.
EXPECTED_PIN = {
    "version": "0.2.0",
    "url": "https://github.com/ds283/RunRegistryKit",
    "commit": "0bae3a3c501a8264ee4be5bc24f06422af38720f",
    "requested_revision": "v0.2.0",
    "editable": False,
}


class TestRegistryConfig(unittest.TestCase):
    def test_roots_and_distributions(self):
        """The package finds the repository root, and reads the roots and distributions."""
        found = runregistrykit.find_client_root(client_root=ROOT)
        self.assertEqual(Path(found.runs_root), ROOT / "var" / "runs")
        self.assertEqual(Path(found.stores_root), ROOT / "var" / "datastores")
        self.assertEqual(found.distributions, ("datastorekit", "raypoolkit"))

    def test_config_keys_are_known(self):
        """Every key is one the package knows, and the three it needs are present."""
        with open(CONFIG, "rb") as f:
            keys = set(tomllib.load(f))
        self.assertTrue(
            REQUIRED_KEYS <= keys, f"missing: {sorted(REQUIRED_KEYS - keys)}"
        )
        self.assertTrue(keys <= KNOWN_KEYS, f"unknown: {sorted(keys - KNOWN_KEYS)}")


class TestVarIsIgnored(unittest.TestCase):
    def _check_ignored(self, path):
        # git refuses -q with more than one path, so one call per path; 0 means ignored
        # and 1 means not.
        result = subprocess.run(
            ["git", "check-ignore", "-q", path], cwd=ROOT, capture_output=True
        )
        self.assertEqual(result.returncode, 0, f"{path} is not ignored")

    def test_runs_ignored(self):
        self._check_ignored("var/runs/x")

    def test_datastores_ignored(self):
        self._check_ignored("var/datastores/x.db")


class TestPin(unittest.TestCase):
    def test_runregistrykit_is_the_tag(self):
        """The venv holds v0.2.0 from its commit, not editable."""
        self.assertEqual(
            runregistrykit.package_provenance(("runregistrykit",))["runregistrykit"],
            EXPECTED_PIN,
        )


if __name__ == "__main__":
    unittest.main()
