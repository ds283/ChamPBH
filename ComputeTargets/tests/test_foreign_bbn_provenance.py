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
The warning that a store serves BBN rows from another PRyMordial version or
network (bbn-tolerance prompt 02, README section 0.2 P6 extended by U4).

`BBNData` lookups are keyed on the version label and ignore `PRyM_version` and
`small_network`, so after a PRyMordial patch, or a change of network, a store
that was not refreshed serves the old rows with no sign that they are old.
`main.py` and `plot_by_beta.py` now print one warning naming each foreign
(`PRyM_version`, `small_network`) pair and its count. They never skip, filter
or recompute a row because of it.

- (b1) `foreign_bbn_provenance` on stand-in objects: current, foreign-version,
  foreign-network and failed rows, and a row not found in the store.
- (b2) the printer: once per foreign pair, nothing when nothing is foreign, and
  it returns None.
- (b3) the drivers call it, each with the name it uses for its network.
- (b4) the fields are readable on the objects the drivers hold: BBNData rows
  looked up from a temporary store, with `_do_not_populate`, as `main.py`
  (`failure=None`) and `plot_by_beta.py` (`failure=False`) look them up.

(b1), (b2) and (b4) fail before bbn-tolerance prompt 02 by ImportError; (b3)
fails there because neither driver calls the printer. No PRyMordial solve and
no Ray cluster. (b4) builds the undecorated `Datastore.__ray_actor_class__` on a
SQLite file in a `tempfile` directory, as `Datastore/tests` does, and reuses its
stand-ins; about 1 s.

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import ast
import unittest
from pathlib import Path

from Datastore.tests.test_version_keyed_lookups import (
    LABEL_A,
    _TempStoreCase,
    _insert,
    _insert_failed_bbn,
    _insert_failed_scalar_model,
    _model_proxy,
)

_ROOT = Path(__file__).resolve().parents[2]

CURRENT = "bf24c3d+ri02+sr01+bt02"
OLD = "bf24c3d+ri02+sr01"


def _selection():
    import pipeline_selection

    return pipeline_selection


class _Row:
    """Stands in for a BBNData lookup result. As BBNData does, PRyM_version and
    small_network raise on a failure row and on a row not found in the store."""

    def __init__(self, available=True, failure=False, version=CURRENT, small=True):
        self.available = available
        self.failure = failure if available else None
        self._version = version
        self._small = small

    def _check(self):
        if not self.available:
            raise RuntimeError("not populated")
        if self.failure:
            raise RuntimeError("an integration failure")

    @property
    def PRyM_version(self):
        self._check()
        return self._version

    @property
    def small_network(self):
        self._check()
        return self._small


def _mixed_rows():
    return (
        [_Row() for _ in range(5)]
        + [_Row(version=OLD, small=True) for _ in range(3)]
        + [_Row(version=OLD, small=False) for _ in range(2)]
        + [_Row(version=CURRENT, small=False)]
        + [_Row(failure=True) for _ in range(4)]
        + [_Row(available=False) for _ in range(6)]
    )


class TestForeignBBNProvenance(unittest.TestCase):
    def test_b1_counts(self):
        """(b1) Five current rows, three from the old version on the small
        network, two from the old version on the full network, one current
        version on the full network, four failures and six rows not found: the
        foreign pairs are counted by pair, the failures only as provenance not
        stored, and the rows not found not at all."""
        p = _selection().foreign_bbn_provenance(_mixed_rows(), CURRENT, True)
        self.assertEqual(
            p.foreign,
            {(OLD, True): 3, (OLD, False): 2, (CURRENT, False): 1},
        )
        self.assertEqual(p.not_stored, 4)
        self.assertEqual(p.current, 5)

        # from the full network's point of view the same rows divide differently
        p = _selection().foreign_bbn_provenance(_mixed_rows(), CURRENT, False)
        self.assertEqual(
            p.foreign, {(CURRENT, True): 5, (OLD, True): 3, (OLD, False): 2}
        )
        self.assertEqual(p.current, 1)
        self.assertEqual(p.not_stored, 4)

    def test_b2_printer(self):
        """(b2) The printer prints one line per foreign pair, with its count, the
        count of failure rows, and the refresh route; nothing when no row is
        foreign, even with failures present; and returns None."""
        sel = _selection()
        lines = []
        result = sel.warn_foreign_bbn_provenance(
            _mixed_rows(), CURRENT, True, emit=lines.append
        )
        self.assertIsNone(result)
        text = "\n".join(lines)
        for version, network, count in (
            (OLD, "small", 3),
            (OLD, "full", 2),
            (CURRENT, "full", 1),
        ):
            pair_lines = [
                line
                for line in lines
                if f"PRyM_version={version}," in line and f"{network} network" in line
            ]
            with self.subTest(version=version, network=network):
                self.assertEqual(len(pair_lines), 1, text)
                self.assertIn(f"{count} x", pair_lines[0])
        self.assertEqual(sum(1 for line in lines if " x PRyM_version=" in line), 3)
        self.assertEqual(sum(1 for line in lines if "4 failure row(s)" in line), 1)
        self.assertIn("--drop bbn-data", lines[-1])
        self.assertTrue(all(line.startswith("!! warning") for line in lines))

        quiet = []
        rows = [_Row() for _ in range(3)] + [_Row(failure=True), _Row(available=False)]
        self.assertIsNone(
            sel.warn_foreign_bbn_provenance(rows, CURRENT, True, emit=quiet.append)
        )
        self.assertEqual(quiet, [])

    def test_b3_the_drivers_call_it_with_their_network(self):
        """(b3) main.py and plot_by_beta.py each call warn_foreign_bbn_provenance
        once, with PRYM_VERSION and the same name they use for their network:
        main.py the name in its BBN payload, plot_by_beta.py the name it passes
        to compute_SM_baseline. Read from the source, since neither driver can
        be imported without side effects. Fails before bbn-tolerance prompt 02."""

        def calls_named(tree, name):
            return [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and (
                    (isinstance(node.func, ast.Name) and node.func.id == name)
                    or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
                )
            ]

        def argument(call, position, keyword):
            for k in call.keywords:
                if k.arg == keyword:
                    return k.value
            if len(call.args) > position:
                return call.args[position]
            return None

        def name_of(node):
            return node.id if isinstance(node, ast.Name) else None

        trees = {
            name: ast.parse((_ROOT / name).read_text(), filename=name)
            for name in ("main.py", "plot_by_beta.py")
        }

        # main.py: the name its BBN payload gives "small_network"
        payload_names = [
            name_of(value)
            for call in calls_named(trees["main.py"], "compute")
            for k in call.keywords
            if k.arg == "payload" and isinstance(k.value, ast.Dict)
            for key, value in zip(k.value.keys, k.value.values)
            if isinstance(key, ast.Constant) and key.value == "small_network"
        ]
        self.assertEqual(len(payload_names), 1)

        # plot_by_beta.py: the name it passes compute_SM_baseline
        baseline_calls = calls_named(trees["plot_by_beta.py"], "compute_SM_baseline")
        self.assertEqual(len(baseline_calls), 1)
        baseline_names = [name_of(argument(baseline_calls[0], 0, "small_network"))]

        for driver, network_names in (
            ("main.py", payload_names),
            ("plot_by_beta.py", baseline_names),
        ):
            with self.subTest(driver=driver):
                network_name = network_names[0]
                self.assertIsNotNone(
                    network_name, f"{driver} does not name its network"
                )
                warns = calls_named(trees[driver], "warn_foreign_bbn_provenance")
                self.assertEqual(len(warns), 1, f"{driver} does not warn")
                self.assertEqual(
                    name_of(argument(warns[0], 1, "prym_version")), "PRYM_VERSION"
                )
                self.assertEqual(
                    name_of(argument(warns[0], 2, "small_network")), network_name
                )


class TestProvenanceOnStoredRows(_TempStoreCase):
    def _lookup(self, store, failure):
        return store.object_get(
            "BBNData",
            model_proxy=_model_proxy(),
            tags=[],
            failure=failure,
            _do_not_populate=True,
        )

    def _store_with(self, success_version=None, small=None, failed=False):
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        if failed:
            _insert_failed_bbn(store, 1, "a failure")
        if success_version is not None:
            _insert(
                store,
                "BBNData",
                dict(
                    serial=2,
                    model_serial=_model_proxy().store_id,
                    failure=False,
                    Yp_BBN=0.247,
                    DOverH=2.46,
                    He3OverH=1.04,
                    Li7OverH=5.4,
                    small_network=small,
                    PRyM_version=success_version,
                    z_samples=0,
                    validated=True,
                ),
            )
        return store

    def test_b4_fields_are_readable_on_the_drivers_objects(self):
        """(b4) A successful row stored as (OLD, full network), looked up with
        _do_not_populate as main.py (failure=None) and as plot_by_beta.py
        (failure=False): PRyM_version and small_network are readable and the
        row is foreign to (CURRENT, small network); a failed row looked up as
        main.py does is counted as provenance not stored. No solve."""
        sel = _selection()
        store = self._store_with(success_version=OLD, small=False, failed=True)
        for failure in (None, False):
            with self.subTest(failure=failure):
                d = self._lookup(store, failure)
                self.assertTrue(d.available)
                self.assertFalse(d.failure)
                self.assertEqual(d.PRyM_version, OLD)
                self.assertIs(d.small_network, False)
                p = sel.foreign_bbn_provenance([d], CURRENT, True)
                self.assertEqual(p.foreign, {(OLD, False): 1})

    def test_b4_a_current_row_is_not_foreign(self):
        """(b4) A successful row stored as (CURRENT, small network), looked up
        as main.py does: it is counted as current, and nothing is foreign."""
        sel = _selection()
        d = self._lookup(self._store_with(success_version=CURRENT, small=True), None)
        self.assertIs(d.small_network, True)
        p = sel.foreign_bbn_provenance([d], CURRENT, True)
        self.assertEqual((p.foreign, p.current), ({}, 1))

    def test_b4_failed_rows_are_not_classified(self):
        """(b4) A model with only a failed row, looked up as main.py does
        (failure=None): it is counted as provenance not stored, not as foreign."""
        sel = _selection()
        d = self._lookup(self._store_with(failed=True), None)
        self.assertTrue(d.available)
        self.assertTrue(d.failure)
        p = sel.foreign_bbn_provenance([d], CURRENT, True)
        self.assertEqual((p.foreign, p.not_stored, p.current), ({}, 1, 0))


if __name__ == "__main__":
    unittest.main()
