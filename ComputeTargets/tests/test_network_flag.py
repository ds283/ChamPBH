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
The `small_network` switch reaches PRyMordial, selects the network it names,
and production passes the full network.

Written for production-readiness prompt 02 (item P2). Before that prompt
`_configure_PRyMordial` set `PRyM_init.small_network_flag`, which PRyMordial
never reads; it reads `smallnet_flag` when the solve runs (`PRyM_main.py`), so
every solve used the full network whatever the switch said.

**Test (b) runs PRyMordial twice, about 15 s.** Tests (a) and (c) run no
solve.

Test (b) bounds only the 7Li/H shift between the networks, which is what shows
that the flag selects one. The Yp and D/H shifts are printed, not bounded: how
far PRyMordial's small network sits from its full one in those is PRyMordial's
property, not a contract with this code (the user, 2026-09-30; log 02, addendum).
Nothing here needs a Ray cluster or a datastore. Run from the repository root,
since PRyMordial reads `PRyMrates/` from the working directory:

    PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
"""

import ast
import inspect
import time
import unittest
from pathlib import Path
from unittest import mock

from ComputeTargets.BBNData import (
    _configure_PRyMordial,
    compute_BBN_data,
    compute_SM_baseline,
)
from ComputeTargets.tests.prym_fixtures import (
    CONSTANT,
    RES_D_OVER_H_E5,
    RES_LI7_OVER_H_E10,
    RES_YP_BBN,
    SavedPRyMGlobals,
    run_prym,
)
from ComputeTargets.tests.test_prym_passenger import (
    CONST_HONLY_FULL_D_OVER_H_E5,
    CONST_HONLY_FULL_YP,
    REFERENCE_RTOL,
)

_ROOT = Path(__file__).resolve().parents[2]

# (b) small against full network, constant 0.08 rho_SM family. The review-
# remediation board measured 1 % in 7Li/H (47c50ae, smallnet_flag set directly);
# README section 6.2 sets the bound.
LI7_MIN_RELATIVE_SHIFT = 5e-3


def _relative(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


# Save and restore every PRyMordial module global these tests can touch:
# `_configure_PRyMordial`'s flags, NP_hubble_flag among them since
# science-readiness prompt 01, and PRyM_thermo's rho_NP. One helper, in
# prym_fixtures, since that prompt.
_SavedPRyMGlobals = SavedPRyMGlobals


def _calls_named(path: Path, name: str):
    """Every call in `path` whose callee is `name` or `<something>.name`."""
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if (isinstance(func, ast.Name) and func.id == name) or (
                isinstance(func, ast.Attribute) and func.attr == name
            ):
                yield node


_NETWORK_SOLVES = None


def _network_solves() -> dict:
    """
    The constant 0.08 rho_SM family through run_prym, small network then
    full, solved once per process for test (b). Returns the relative shifts
    small against full, and full against the pins.
    """
    global _NETWORK_SOLVES
    if _NETWORK_SOLVES is not None:
        return _NETWORK_SOLVES

    with _SavedPRyMGlobals():
        start = time.perf_counter()
        small = run_prym(CONSTANT.rho, small_network=True)
        wall_small = time.perf_counter() - start

        start = time.perf_counter()
        full = run_prym(CONSTANT.rho, small_network=False)
        wall_full = time.perf_counter() - start

    r = {
        "dLi7": _relative(small[RES_LI7_OVER_H_E10], full[RES_LI7_OVER_H_E10]),
        "dDoH": _relative(small[RES_D_OVER_H_E5], full[RES_D_OVER_H_E5]),
        "dYp": _relative(small[RES_YP_BBN], full[RES_YP_BBN]),
        "dYp_pin": _relative(full[RES_YP_BBN], CONST_HONLY_FULL_YP),
        "dDoH_pin": _relative(full[RES_D_OVER_H_E5], CONST_HONLY_FULL_D_OVER_H_E5),
    }
    print(
        f"\n[test_network_flag (b)] small: Yp {small[RES_YP_BBN]:.10g}, "
        f"D/H x1e5 {small[RES_D_OVER_H_E5]:.10g}, "
        f"7Li/H x1e10 {small[RES_LI7_OVER_H_E10]:.10g}, {wall_small:.1f} s; "
        f"full: Yp {full[RES_YP_BBN]:.10g}, D/H x1e5 {full[RES_D_OVER_H_E5]:.10g}, "
        f"7Li/H x1e10 {full[RES_LI7_OVER_H_E10]:.10g}, {wall_full:.1f} s\n"
        f"[test_network_flag (b)] small vs full: 7Li/H {r['dLi7']:.3e}, "
        f"D/H {r['dDoH']:.3e}, Yp {r['dYp']:.3e}; "
        f"full vs pins: Yp {r['dYp_pin']:.3e}, D/H {r['dDoH_pin']:.3e}"
    )
    _NETWORK_SOLVES = r
    return r


class _StubPRyMclass:
    """Stands in for PRyMclass so that compute_SM_baseline runs no solve."""

    def __init__(self, *args, **kwargs):
        pass

    def PRyMresults(self):
        return [float(i) for i in range(9)]


class TestNetworkFlag(unittest.TestCase):
    def test_a_flag_reaches_prymordial(self):
        """(a) _configure_PRyMordial(True) sets PRyM_init.smallnet_flag to True,
        and (False) to False. No solve. Fails before prompt 02, which set the
        unread small_network_flag instead."""
        import PRyM.PRyM_init as PRyMini

        with _SavedPRyMGlobals():
            _configure_PRyMordial(True)
            self.assertIs(PRyMini.smallnet_flag, True)
            _configure_PRyMordial(False)
            self.assertIs(PRyMini.smallnet_flag, False)

    def test_b_flag_selects_the_network(self):
        """(b) The constant 0.08 rho_SM family through run_prym with
        small_network=True and =False: 7Li/H moves by >= 5e-3 relative, and
        the full-network run reproduces the pinned Yp and D/H to the fixture's
        1e-5. The small network's Yp and D/H shifts are printed, not bounded.
        Since science-readiness prompt 01 the pins are the full-network
        Hubble-only reference (test_prym_passenger, CONST_HONLY_FULL_*).
        **Runs PRyMordial twice, about 15 s.**"""
        r = _network_solves()
        with self.subTest("7Li/H moves"):
            self.assertGreaterEqual(r["dLi7"], LI7_MIN_RELATIVE_SHIFT)
        with self.subTest("full network, pinned Yp"):
            self.assertLessEqual(r["dYp_pin"], REFERENCE_RTOL)
        with self.subTest("full network, pinned D/H"):
            self.assertLessEqual(r["dDoH_pin"], REFERENCE_RTOL)

    def test_c_production_defaults_are_the_full_network(self):
        """(c) The compute_BBN_data default is small_network=False, and so are
        compute_SM_baseline's result and PRyMordial's smallnet_flag when it is
        called as plot_by_beta.py calls it (PRyMclass stubbed: no solve). Also
        main.py's BBN payload and tools/bbn_baseline.py's default, read from
        their source, since neither can be imported without side effects."""
        default = (
            inspect.signature(compute_BBN_data._function)
            .parameters["small_network"]
            .default
        )
        self.assertIs(default, False)

        # plot_by_beta.py runs argparse and ray.init at import, so read its call
        calls = list(_calls_named(_ROOT / "plot_by_beta.py", "compute_SM_baseline"))
        self.assertEqual(len(calls), 1)
        args = [ast.literal_eval(a) for a in calls[0].args]
        kwargs = {k.arg: ast.literal_eval(k.value) for k in calls[0].keywords}

        import PRyM.PRyM_init as PRyMini
        import PRyM.PRyM_main as PRyMmain

        with _SavedPRyMGlobals():
            with mock.patch.object(PRyMmain, "PRyMclass", _StubPRyMclass):
                baseline = compute_SM_baseline(*args, **kwargs)
            self.assertIs(PRyMini.smallnet_flag, False)
        self.assertIs(baseline["small_network"], False)

        # main.py: the payload handed to BBNData.compute. Since science-readiness
        # prompt 01 it also carries the wall-clock limit, a name rather than a
        # literal, so the dict is read key by key and only small_network is
        # evaluated
        payloads = [
            {
                ast.literal_eval(key): value
                for key, value in zip(k.value.keys, k.value.values)
            }
            for c in _calls_named(_ROOT / "main.py", "compute")
            for k in c.keywords
            if k.arg == "payload" and isinstance(k.value, ast.Dict)
        ]
        bbn_payloads = [p for p in payloads if "small_network" in p]
        self.assertEqual(len(bbn_payloads), 1)
        self.assertIs(ast.literal_eval(bbn_payloads[0]["small_network"]), False)
        self.assertIn("wall_clock_limit", bbn_payloads[0])

        # tools/bbn_baseline.py: the --small-network argument's default
        flags = [
            c
            for c in _calls_named(_ROOT / "tools" / "bbn_baseline.py", "add_argument")
            if c.args and ast.literal_eval(c.args[0]) == "--small-network"
        ]
        self.assertEqual(len(flags), 1)
        defaults = [
            ast.literal_eval(k.value) for k in flags[0].keywords if k.arg == "default"
        ]
        self.assertEqual(defaults, [False])


if __name__ == "__main__":
    unittest.main()
