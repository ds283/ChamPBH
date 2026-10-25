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

"""
Run provenance in the drivers' output (dk-and-rpk-migration prompt 01, item T1; README P5):
`utilities.installed_package_provenance()`, `utilities.format_package_provenance()`, and the one
line each driver prints.

**How a distribution is stood in.** Each test writes a real `<name>-<version>.dist-info`
directory (`METADATA`, and a `direct_url.json` where the case has one) into a `tempfile`
directory and puts that directory at the front of `sys.path`. The helper then finds it through
the real `importlib.metadata`, exactly as it would find an installed package; nothing in
`importlib.metadata` is patched, so a test cannot pass because a mock was aimed at a name the
helper does not call. The stand-ins' names (`champbh-standin-*`) are not on PyPI and are not
installed in the venv.

No test asserts what the venv itself holds: prompts 03 and 07 install the two packages, and the
helper's output in the venv is recorded in their logs, not pinned here.

The drivers are read with `ast`, never imported: each calls `ray.init` at import. No Ray, no
store, no PRyMordial solve. Under 1 s.
"""

import ast
import importlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import utilities
from utilities import format_package_provenance, installed_package_provenance

ROOT = Path(__file__).resolve().parents[2]
DRIVERS = ("main.py", "plot_by_beta.py", "plot_ScalarModel.py")
FIELDS = {"version", "url", "commit", "requested_revision", "editable"}

VCS_URL = "https://github.com/ds283/DatastoreKit"
VCS_COMMIT = "0de2b161ba09362adc36231a7b5b12d99dec8bb3"
VCS_TAG = "v0.2.3"


class _StandInCase(unittest.TestCase):
    """Stand-in distributions on a temporary `sys.path` entry."""

    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="champbh-provenance-test-"))
        sys.path.insert(0, str(self._dir))
        importlib.invalidate_caches()

    def tearDown(self):
        try:
            sys.path.remove(str(self._dir))
        except ValueError:
            pass
        importlib.invalidate_caches()
        shutil.rmtree(self._dir, ignore_errors=True)

    def stand_in(self, name, version, direct_url=None, raw_direct_url=None):
        """
        A `.dist-info` for `name` at `version`. `direct_url` (a JSON-serialisable value) is
        written as `direct_url.json`; `raw_direct_url` (bytes) is written verbatim instead;
        with neither there is no `direct_url.json`.
        """
        info = self._dir / f"{name.replace('-', '_')}-{version}.dist-info"
        info.mkdir()
        (info / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        )
        if direct_url is not None:
            (info / "direct_url.json").write_text(json.dumps(direct_url))
        elif raw_direct_url is not None:
            (info / "direct_url.json").write_bytes(raw_direct_url)
        return info


class TestInstalledPackageProvenance(_StandInCase):
    def test_not_installed(self):
        """A distribution that is not installed maps to None, and the line says so."""
        name = "champbh-standin-absent"
        provenance = installed_package_provenance((name,))
        self.assertEqual(provenance, {name: None})
        self.assertEqual(
            format_package_provenance(provenance),
            f">> packages: {name} not installed",
        )

    def test_vcs_install_at_a_tag(self):
        """A VCS install at a tag: url, commit and requested revision from direct_url.json."""
        name = "champbh-standin-vcs"
        self.stand_in(
            name,
            "0.2.3",
            direct_url={
                "url": VCS_URL,
                "vcs_info": {
                    "vcs": "git",
                    "commit_id": VCS_COMMIT,
                    "requested_revision": VCS_TAG,
                },
            },
        )
        provenance = installed_package_provenance((name,))
        self.assertEqual(set(provenance[name]), FIELDS)
        self.assertEqual(
            provenance[name],
            {
                "version": "0.2.3",
                "url": VCS_URL,
                "commit": VCS_COMMIT,
                "requested_revision": VCS_TAG,
                "editable": False,
            },
        )
        self.assertEqual(
            format_package_provenance(provenance),
            f">> packages: {name} 0.2.3 ({VCS_URL}@{VCS_COMMIT}, requested {VCS_TAG})",
        )

    def test_editable_install(self):
        """An editable install: editable True, no VCS fields, and the line says editable."""
        name = "champbh-standin-editable"
        url = "file:///Users/someone/Code/DatastoreKit"
        self.stand_in(
            name, "0.2.3", direct_url={"url": url, "dir_info": {"editable": True}}
        )
        provenance = installed_package_provenance((name,))
        self.assertEqual(
            provenance[name],
            {
                "version": "0.2.3",
                "url": url,
                "commit": None,
                "requested_revision": None,
                "editable": True,
            },
        )
        line = format_package_provenance(provenance)
        self.assertEqual(line, f">> packages: {name} 0.2.3 (editable, {url})")

    def test_a_local_directory_that_is_not_editable(self):
        """dir_info without editable, or with editable false, is not editable."""
        name = "champbh-standin-dir"
        url = "file:///Users/someone/Code/DatastoreKit"
        self.stand_in(name, "0.2.3", direct_url={"url": url, "dir_info": {}})
        provenance = installed_package_provenance((name,))
        self.assertIs(provenance[name]["editable"], False)
        self.assertEqual(
            format_package_provenance(provenance),
            f">> packages: {name} 0.2.3 ({url})",
        )

    def test_direct_url_absent(self):
        """No direct_url.json: None for the three VCS fields, editable False."""
        name = "champbh-standin-plain"
        self.stand_in(name, "1.0")
        provenance = installed_package_provenance((name,))
        self.assertEqual(
            provenance[name],
            {
                "version": "1.0",
                "url": None,
                "commit": None,
                "requested_revision": None,
                "editable": False,
            },
        )
        self.assertEqual(
            format_package_provenance(provenance),
            f">> packages: {name} 1.0 (no recorded origin)",
        )

    def test_direct_url_not_an_object_or_unreadable(self):
        """direct_url.json that is a JSON list, not JSON, not UTF-8, or a directory: no fields
        from it, editable False, and no exception."""
        cases = {
            "list": dict(direct_url=[VCS_URL, VCS_COMMIT]),
            "string": dict(direct_url="editable"),
            "not-json": dict(raw_direct_url=b'{"url": "https://example.org"'),
            "not-utf8": dict(raw_direct_url=b"\xff\xfe\x00{"),
        }
        for case, kwargs in cases.items():
            with self.subTest(case=case):
                name = f"champbh-standin-bad-{case}"
                self.stand_in(name, "0.1", **kwargs)
                provenance = installed_package_provenance((name,))
                self.assertEqual(
                    provenance[name],
                    {
                        "version": "0.1",
                        "url": None,
                        "commit": None,
                        "requested_revision": None,
                        "editable": False,
                    },
                )
                self.assertEqual(
                    format_package_provenance(provenance),
                    f">> packages: {name} 0.1 (no recorded origin)",
                )

        with self.subTest(case="directory"):
            name = "champbh-standin-bad-directory"
            info = self.stand_in(name, "0.1")
            (info / "direct_url.json").mkdir()
            provenance = installed_package_provenance((name,))
            self.assertEqual(provenance[name]["url"], None)
            self.assertIs(provenance[name]["editable"], False)

    def test_vcs_info_or_dir_info_not_an_object(self):
        """A vcs_info or dir_info that is not an object is treated as absent; url is kept."""
        name = "champbh-standin-odd"
        self.stand_in(
            name,
            "0.1",
            direct_url={"url": VCS_URL, "vcs_info": "git", "dir_info": ["editable"]},
        )
        provenance = installed_package_provenance((name,))
        self.assertEqual(
            provenance[name],
            {
                "version": "0.1",
                "url": VCS_URL,
                "commit": None,
                "requested_revision": None,
                "editable": False,
            },
        )

    def test_several_names_in_order_and_nothing_imported(self):
        """Several names give one entry each, in order, joined by '; '. The described package is
        never imported: its stand-in module raises on import."""
        absent = "champbh-standin-absent"
        name = "champbh-standin-vcs"
        self.stand_in(
            name,
            "0.2.0",
            direct_url={
                "url": VCS_URL,
                "vcs_info": {"vcs": "git", "commit_id": VCS_COMMIT},
            },
        )
        module = self._dir / "champbh_standin_vcs"
        module.mkdir()
        (module / "__init__.py").write_text(
            "raise RuntimeError('a provenance helper must not import this')\n"
        )
        provenance = installed_package_provenance((name, absent))
        self.assertEqual(list(provenance), [name, absent])
        self.assertNotIn("champbh_standin_vcs", sys.modules)
        self.assertEqual(
            format_package_provenance(provenance),
            f">> packages: {name} 0.2.0 ({VCS_URL}@{VCS_COMMIT}); {absent} not installed",
        )

    def test_default_names(self):
        """The default names, in this order: the two packages the dk-and-rpk-migration campaign
        adopts, then `ray`, `SQLAlchemy`, `numpy` and `scipy`, which ray-sqla-align prompt 01
        (README U3) added so that a driver's `>> packages:` line says which venv ran it.
        """
        names = ["datastorekit", "raypoolkit", "ray", "SQLAlchemy", "numpy", "scipy"]
        self.assertEqual(utilities.RECORDED_DISTRIBUTIONS, tuple(names))
        self.assertEqual(list(installed_package_provenance()), names)


def _is_provenance_print(node) -> bool:
    """`print(format_package_provenance(installed_package_provenance()), ...)`"""
    if not (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "print"
        and len(node.value.args) == 1
    ):
        return False
    inner = node.value.args[0]
    return (
        isinstance(inner, ast.Call)
        and isinstance(inner.func, ast.Name)
        and inner.func.id == "format_package_provenance"
        and len(inner.args) == 1
        and isinstance(inner.args[0], ast.Call)
        and isinstance(inner.args[0].func, ast.Name)
        and inner.args[0].func.id == "installed_package_provenance"
        and len(inner.args[0].args) == 0
    )


def _calls(tree, name):
    """Every call of `name` (a plain name or an attribute), anywhere in the tree."""
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if (isinstance(func, ast.Name) and func.id == name) or (
                isinstance(func, ast.Attribute) and func.attr == name
            ):
                found.append(node)
    return found


class TestDriversPrintProvenance(unittest.TestCase):
    def test_each_driver_prints_the_line_once_before_its_pool(self):
        """Each driver prints the provenance line exactly once, as a module-level statement,
        before `ray.init(` and before its `ShardedPool(` construction, and imports both names
        from utilities. Read with ast; no driver is imported."""
        for driver in DRIVERS:
            with self.subTest(driver=driver):
                tree = ast.parse((ROOT / driver).read_text(), filename=driver)

                helper_calls = _calls(tree, "installed_package_provenance")
                format_calls = _calls(tree, "format_package_provenance")
                self.assertEqual(len(helper_calls), 1, "installed_package_provenance")
                self.assertEqual(len(format_calls), 1, "format_package_provenance")

                prints = [node for node in tree.body if _is_provenance_print(node)]
                self.assertEqual(len(prints), 1, "one module-level print of the line")
                line = prints[0].lineno

                pools = _calls(tree, "ShardedPool")
                self.assertGreaterEqual(len(pools), 1, "a ShardedPool( construction")
                self.assertLess(line, min(call.lineno for call in pools))

                inits = [
                    call
                    for call in _calls(tree, "init")
                    if isinstance(call.func, ast.Attribute)
                    and isinstance(call.func.value, ast.Name)
                    and call.func.value.id == "ray"
                ]
                self.assertEqual(len(inits), 1, "one ray.init(")
                self.assertLess(line, inits[0].lineno)

                imported = {
                    alias.name
                    for node in tree.body
                    if isinstance(node, ast.ImportFrom) and node.module == "utilities"
                    for alias in node.names
                }
                self.assertLessEqual(
                    {"format_package_provenance", "installed_package_provenance"},
                    imported,
                )


if __name__ == "__main__":
    unittest.main()
