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

"""
The reduced Planck mass and the electronvolt, in the three unit systems.
(jordan-normalization prompt 05, K1-K3.)

`Units/base.py` holds one constant, `NEWTON_G_MEV_MINUS_2`, PRyMordial's G_N in MeV^-2, and
`REDUCED_PLANCK_MASS_EV = 1/sqrt(8 pi G_N)` in eV. `Planck_units.eV`, `Mpc_units.eV` and
`GeV_units.PlanckMass` are set from it. These tests say that

- the constant is PRyMordial's, compared as decimals on the text of `PRyM/PRyM_init.py`, which is
  read and never imported (PRyM_init reads files from the working directory at import), and that
  PRyMordial's `Mpl` is still `1/sqrt(GN)`, so that ChamPBH's M_P and PRyMordial's Mpl are one G_N;
- in each of the three systems, `PlanckMass / eV` is `REDUCED_PLANCK_MASS_EV`, and 1 K, 1/eV in
  metres and in seconds, and 1 kg agree with CODATA 2018's k_B = 8.617333262e-5 eV/K,
  hbar c = 1.973269804e-7 eV m, hbar = 6.582119569e-16 eV s and 1 kg = 5.609588604e35 eV, each to
  1e-6 relative.

The comparison with PRyM_init is decimal because PRyMordial's expression `6.70883*1.e-39*1.e-6`
evaluates to a float one ulp below the literal 6.70883e-45. No test here runs a solve.
"""

import re
import unittest
from decimal import Decimal
from math import pi, sqrt
from pathlib import Path

from Units import GeV_units, Mpc_units, Planck_units
from Units.base import NEWTON_G_MEV_MINUS_2, REDUCED_PLANCK_MASS_EV

PRYM_INIT = Path(__file__).resolve().parents[2] / "PRyM" / "PRyM_init.py"

# CODATA 2018
K_B_EV_PER_K = 8.617333262e-5
HBAR_C_EV_M = 1.973269804e-7
HBAR_EV_S = 6.582119569e-16
KILOGRAM_EV = 5.609588604e35

TOLERANCE = 1.0e-6

SYSTEMS = (
    ("Planck_units", Planck_units),
    ("Mpc_units", Mpc_units),
    ("GeV_units", GeV_units),
)


def _relative(value, reference):
    return abs(value / reference - 1.0)


def _prym_lines():
    return PRYM_INIT.read_text().splitlines()


def _prym_G_N():
    """PRyM_init's `GN`, as the Decimal product of the factors of its expression. Exactly one line defines it."""
    pattern = re.compile(r"^GN\s*=\s*(.+?)\s*#")
    matches = [m for m in map(pattern.match, _prym_lines()) if m is not None]
    assert len(matches) == 1, f"{len(matches)} lines define GN in {PRYM_INIT}"

    product = Decimal(1)
    for factor in matches[0].group(1).split("*"):
        product *= Decimal(factor.strip())
    return product


class TestNewtonConstantIsPRyMordials(unittest.TestCase):
    def test_a_G_N_equals_PRyM_init_as_decimals(self):
        """`NEWTON_G_MEV_MINUS_2` is the product of the factors of PRyM_init's `GN`, to the last digit."""
        self.assertEqual(_prym_G_N(), Decimal(repr(NEWTON_G_MEV_MINUS_2)))

    def test_b_PRyMordials_Planck_mass_is_one_over_sqrt_G_N(self):
        """`Mpl = 1./np.sqrt(GN)` is present verbatim, so a change in PRyMordial's `Mpl` fails here."""
        self.assertIn("Mpl = 1./np.sqrt(GN) # Planck mass in [MeV], PDG", _prym_lines())

    def test_c_the_reduced_Planck_mass_is_from_the_same_G_N(self):
        """sqrt(8 pi) times M_P equals PRyMordial's Mpl, 1e6 eV / sqrt(G_N), to 1e-15."""
        G = float(_prym_G_N())

        self.assertLessEqual(
            _relative(REDUCED_PLANCK_MASS_EV * sqrt(8.0 * pi), 1.0e6 / sqrt(G)), 1.0e-15
        )


class TestTheUnitSystemsAreConsistent(unittest.TestCase):
    def test_a_Planck_mass_over_eV_is_the_constant(self):
        """In each system `PlanckMass / eV` is `REDUCED_PLANCK_MASS_EV`, to 1e-15."""
        for name, cls in SYSTEMS:
            with self.subTest(system=name):
                units = cls()
                self.assertLessEqual(
                    _relative(units.PlanckMass / units.eV, REDUCED_PLANCK_MASS_EV),
                    1.0e-15,
                )

    def test_b_kelvin_is_k_B(self):
        """1 K is k_B = 8.617333262e-5 eV (CODATA 2018), to 1e-6."""
        for name, cls in SYSTEMS:
            with self.subTest(system=name):
                units = cls()
                self.assertLessEqual(
                    _relative(units.Kelvin / units.eV, K_B_EV_PER_K), TOLERANCE
                )

    def test_c_inverse_eV_is_hbar_c_in_metres(self):
        """1/eV is hbar c = 1.973269804e-7 m (CODATA 2018), to 1e-6."""
        for name, cls in SYSTEMS:
            with self.subTest(system=name):
                units = cls()
                self.assertLessEqual(
                    _relative((1.0 / units.eV) / units.Metre, HBAR_C_EV_M), TOLERANCE
                )

    def test_d_inverse_eV_is_hbar_in_seconds(self):
        """1/eV is hbar = 6.582119569e-16 s (CODATA 2018), to 1e-6."""
        for name, cls in SYSTEMS:
            with self.subTest(system=name):
                units = cls()
                self.assertLessEqual(
                    _relative((1.0 / units.eV) / units.Second, HBAR_EV_S), TOLERANCE
                )

    def test_e_kilogram_is_5p6096e35_eV(self):
        """1 kg is 5.609588604e35 eV (CODATA 2018), to 1e-6."""
        for name, cls in SYSTEMS:
            with self.subTest(system=name):
                units = cls()
                self.assertLessEqual(
                    _relative(units.Kilogram / units.eV, KILOGRAM_EV), TOLERANCE
                )


if __name__ == "__main__":
    unittest.main()
