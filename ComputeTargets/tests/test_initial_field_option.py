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

(a) the check, with ExponentialModel (at the seed's phi_ref) and Planck_units; (b) the three
drivers read `args.phi_init_Mp` and keep no `5.0 * units.PlanckMass` literal; (c) the parser; (d)
the warning step returns the models it was given, unchanged, having printed one warning per
super-Planckian entry; (e) a model the check flags at phi_ref = 0 and not at its seed's phi_ref;
(f) the text of the lines. (Until jordan-normalization prompt 07 the check took ExponentialCoupling
objects, which that prompt deleted. Since prompt 08 (P11) Omega is evaluated at the phi_ref of
the model's seed on the cosmology, so (a), (d), (e) and (f) build QCD_Cosmology on Planck2018,
main.py's, about 0.1 s.)

No Ray cluster, no datastore, no PRyMordial solve, no integration. About a second.
"""

import ast
import unittest
from math import exp, log
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIVERS = ["main.py", "plot_by_beta.py", "plot_ScalarHistory.py"]
T_INIT_GEV = 2.0e4
BETAS = [1.0, 6.0, 7.0, 25.0, 40.0]


def _cosmology(units):
    """main.py's cosmology: QCD_Cosmology on Planck2018 (it reads its EOS table from the root)."""
    from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
    from CosmologyModels.LambdaCDM import Planck2018

    return QCD_Cosmology(0, units, Planck2018())


def _setup(phi_Mp, M_Mp=1.0e-3, betas=BETAS):
    from CosmologyConcepts import M_value, beta_value, temperature
    from CosmologyConcepts.FieldValues import phi_value
    from CosmologyConcepts.Models import ExponentialModel
    from Units import Planck_units

    units = Planck_units()
    # one model per beta, at M = 1e-3 M_P and n = 1 by default (M enters through the seed's
    # phi_ref)
    couplings = [
        ExponentialModel(
            i, M_value(0, M_Mp * units.PlanckMass), 1, beta_value(i, b), units
        )
        for i, b in enumerate(betas)
    ]
    phi = phi_value(0, phi_Mp * units.PlanckMass)
    T = temperature(0, T_INIT_GEV * units.GeV)
    return units, couplings, phi, T, _cosmology(units)


def _betas(couplings):
    return [c._beta_float for c in couplings]


class TestSuperPlanckianCheck(unittest.TestCase):
    def test_a_the_check(self):
        """
        (a) Omega(phi*) T* > M_P is beta (phi* - phi_ref)/M_P > ln(M_P/T*) = 32.43 for
        T* = 2e4 GeV (reduced M_P = 2.435e18 GeV), with the seed's phi_ref (at M = 1e-3 M_P a
        tracking seed, phi_ref below 0.02 M_P). phi* = 5 selects beta > 6.506: {7, 25, 40} of
        {1, 6, 7, 25, 40}. phi* = 1 selects beta > 32.61: {40}.
        """
        from pipeline_selection import super_planckian_couplings

        units, couplings, phi, T, cosmology = _setup(5.0)
        self.assertEqual(
            _betas(super_planckian_couplings(couplings, phi, T, units, cosmology)),
            [7.0, 25.0, 40.0],
        )
        units, couplings, phi, T, cosmology = _setup(1.0)
        self.assertEqual(
            _betas(super_planckian_couplings(couplings, phi, T, units, cosmology)),
            [40.0],
        )

    def test_e_a_model_flagged_at_phi_ref_zero_and_not_at_its_seeds(self):
        """
        (e) beta = 7 at M = 0.5 M_P, phi* = 5 M_P: at phi_ref = 0, beta phi*/M_P = 35 > 32.43 and
        the start would be flagged; the seed is static with phi_ref = 0.3977 M_P, and
        beta (phi* - phi_ref)/M_P = 32.22 < 32.43, so it is not. beta = 7.2, whose seed's phi_ref
        is 0.3921 M_P (33.18), is flagged at both.
        """
        from pipeline_selection import super_planckian_couplings

        units, couplings, phi, T, cosmology = _setup(5.0, M_Mp=0.5, betas=[7.0, 7.2])
        seed = couplings[0].seed(cosmology)
        self.assertEqual(seed.kind, "static")
        self.assertAlmostEqual(seed.phi_ref / units.PlanckMass, 0.3977, delta=1e-4)
        ln_ratio = log(units.PlanckMass / T.value)
        self.assertGreater(couplings[0].realize(None, 0.0).log_Omega(phi), ln_ratio)
        self.assertLess(
            couplings[0].realize(None, seed.phi_ref).log_Omega(phi), ln_ratio
        )
        self.assertEqual(
            _betas(super_planckian_couplings(couplings, phi, T, units, cosmology)),
            [7.2],
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

        units, couplings, phi, T, cosmology = _setup(5.0)
        original = list(couplings)
        lines = []
        returned = warn_super_planckian(
            couplings, phi, T, units, cosmology, emit=lines.append
        )
        self.assertEqual(len(returned), len(original))
        self.assertTrue(all(a is b for a, b in zip(returned, original)))
        self.assertEqual(len(couplings), len(original))
        self.assertEqual(len([l for l in lines if "super-Planckian start" in l]), 3)
        self.assertEqual(len(lines), 4)
        for beta in ("7", "25", "40"):
            self.assertTrue(any(f"beta={beta}," in l for l in lines))

        lines = []
        units, couplings, phi, T, cosmology = _setup(0.1)
        warn_super_planckian(couplings, phi, T, units, cosmology, emit=lines.append)
        self.assertEqual(lines, [])

    def test_f_the_lines_name_the_model_and_the_seeds_phi_ref(self):
        """
        (f) Each line names beta, M, n, phi*, Omega(phi*) T*/M_P, the seed's phi_ref and kind,
        and says Omega is at the seed's phi_ref; the count line says the tuned phi_ref differs,
        by how much ln Omega(phi*) moves, and the measured sizes.
        """
        from pipeline_selection import warn_super_planckian

        units, couplings, phi, T, cosmology = _setup(5.0, betas=[7.0])
        lines = []
        warn_super_planckian(couplings, phi, T, units, cosmology, emit=lines.append)
        self.assertEqual(len(lines), 2)
        seed = couplings[0].seed(cosmology)
        Mp = units.PlanckMass
        ratio = exp(
            7.0 * (5.0 * Mp - seed.phi_ref) / Mp + log(T.value) - log(units.PlanckMass)
        )
        self.assertEqual(
            lines[0],
            f"!! warning: beta=7, M=0.001 M_P, n=1, phi*=5 M_P: Omega(phi*) T* = "
            f"{ratio:.5g} M_P (super-Planckian start), with Omega at the seed's phi_ref = "
            f"{seed.phi_ref / Mp:.5g} M_P (tracking seed)",
        )
        self.assertEqual(
            lines[1],
            "!! warning: 1 of 1 models start super-Planckian at the seed's phi_ref; they are "
            "computed like the others. The tuned phi_ref differs from the seed's, moving "
            "ln Omega(phi*) by -beta (phi_ref - phi_ref_seed)/M_P, in size up to 1.4e-3 at "
            "M = 1e-3 M_P with beta <= 5.5, 6.5e-3 at beta = 25, and up to 1.0 at M = 0.5 M_P "
            "from a static seed (jordan-normalization prompt 06's points)",
        )


if __name__ == "__main__":
    unittest.main()
