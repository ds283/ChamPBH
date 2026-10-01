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
The initial field as a run option, with a super-Planckian warning
(science-readiness prompt 04, README section 6.5).

(a) the check, with ExponentialCoupling and Planck_units; (b) the three drivers read
`args.phi_init_Mp` and keep no `5.0 * units.PlanckMass` literal; (c) the parser; (d) the
warning step returns the couplings it was given, unchanged, having printed one warning per
super-Planckian entry.

No Ray cluster, no datastore, no PRyMordial solve. Instant.
"""

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIVERS = ["main.py", "plot_by_beta.py", "plot_ScalarModel.py"]
T_INIT_GEV = 2.0e4
BETAS = [1.0, 6.0, 7.0, 25.0, 40.0]


def _setup(phi_Mp):
    from CosmologyConcepts import beta_value, temperature
    from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
        ExponentialCoupling,
    )
    from CosmologyConcepts.FieldValues import phi_value
    from Units import Planck_units

    units = Planck_units()
    couplings = [
        ExponentialCoupling(i, beta_value(i, b), units) for i, b in enumerate(BETAS)
    ]
    phi = phi_value(0, phi_Mp * units.PlanckMass)
    T = temperature(0, T_INIT_GEV * units.GeV)
    return units, couplings, phi, T


def _betas(couplings):
    return [c._beta_float for c in couplings]


class TestSuperPlanckianCheck(unittest.TestCase):
    def test_a_the_check(self):
        """
        (a) Omega(phi*) T* > M_P is beta phi*/M_P > ln(M_P/T*) = 32.43 for T* = 2e4 GeV
        (reduced M_P = 2.435e18 GeV). phi* = 5 selects beta > 6.49: {7, 25, 40} of
        {1, 6, 7, 25, 40}. phi* = 1 selects beta > 32.43: {40}.
        """
        from pipeline_selection import super_planckian_couplings

        units, couplings, phi, T = _setup(5.0)
        self.assertEqual(
            _betas(super_planckian_couplings(couplings, phi, T, units)),
            [7.0, 25.0, 40.0],
        )
        units, couplings, phi, T = _setup(1.0)
        self.assertEqual(
            _betas(super_planckian_couplings(couplings, phi, T, units)), [40.0]
        )


class TestNoLiteralLeft(unittest.TestCase):
    def test_b_the_drivers_read_the_option(self):
        """(b) No BinOp of the constant 5.0 with `units.PlanckMass` remains; each driver reads args.phi_init_Mp."""
        for name in DRIVERS:
            source = (ROOT / name).read_text()
            tree = ast.parse(source)
            literals = []
            for node in ast.walk(tree):
                if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult):
                    sides = [node.left, node.right]
                    has_five = any(
                        isinstance(s, ast.Constant) and s.value == 5.0 for s in sides
                    )
                    has_Mp = any(
                        isinstance(s, ast.Attribute) and s.attr == "PlanckMass"
                        for s in sides
                    )
                    if has_five and has_Mp:
                        literals.append(node.lineno)
            self.assertEqual(literals, [], f"{name} keeps a 5.0 * PlanckMass literal")
            reads = [
                n
                for n in ast.walk(tree)
                if isinstance(n, ast.Attribute)
                and n.attr == "phi_init_Mp"
                and isinstance(n.value, ast.Name)
                and n.value.id == "args"
            ]
            self.assertTrue(reads, f"{name} does not read args.phi_init_Mp")


class TestParser(unittest.TestCase):
    def test_c_the_option_parses(self):
        """(c) The default is 5.0 and "--phi-init-Mp 2" gives 2.0."""
        from config.argument_parser import create_argument_parser

        base = ["--database", "unused.db"]
        args = create_argument_parser().parse_args(base)
        self.assertEqual(args.phi_init_Mp, 5.0)
        args = create_argument_parser().parse_args(base + ["--phi-init-Mp", "2"])
        self.assertEqual(args.phi_init_Mp, 2.0)


class TestNothingIsDropped(unittest.TestCase):
    def test_d_the_warning_returns_the_list_unchanged(self):
        """(d) One warning per super-Planckian coupling, then the count; the same objects, same order."""
        from pipeline_selection import warn_super_planckian

        units, couplings, phi, T = _setup(5.0)
        original = list(couplings)
        lines = []
        returned = warn_super_planckian(couplings, phi, T, units, emit=lines.append)
        self.assertEqual(len(returned), len(original))
        self.assertTrue(all(a is b for a, b in zip(returned, original)))
        self.assertEqual(len(couplings), len(original))
        self.assertEqual(len([l for l in lines if "super-Planckian start" in l]), 3)
        self.assertEqual(len(lines), 4)
        for beta in ("7", "25", "40"):
            self.assertTrue(any(f"beta={beta}," in l for l in lines))

        lines = []
        units, couplings, phi, T = _setup(0.1)
        warn_super_planckian(couplings, phi, T, units, emit=lines.append)
        self.assertEqual(lines, [])


if __name__ == "__main__":
    unittest.main()
