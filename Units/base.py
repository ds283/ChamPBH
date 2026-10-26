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

from abc import ABC, abstractmethod
from math import pi, sqrt

# Newton's constant G_N in MeV^-2, as PRyMordial has it: `GN = 6.70883*1.e-39*1.e-6` in
# PRyM/PRyM_init.py, the PDG value 6.70883e-39 GeV^-2 expressed in MeV^-2. It is CODATA 2018's
# G = 6.67430e-11 m^3 kg^-1 s^-2, which is the G from which the Planck length, mass, time and
# temperature literals of the three unit systems (Planck_units, Mpc_units, GeV_units) are derived.
# It is written out here and not imported: PRyM_init reads files from the working directory at
# import.
NEWTON_G_MEV_MINUS_2 = 6.70883e-45

# The reduced Planck mass in eV: 1/sqrt(8 pi G_N), converted from MeV to eV. PRyMordial's
# `Mpl = 1./np.sqrt(GN)` is the full Planck mass from the same G_N, sqrt(8 pi) times this, so that
# ChamPBH's M_P and PRyMordial's Mpl are one G_N. The three unit systems set the electronvolt (or,
# in GeV_units, the Planck mass) from this number.
REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2)


class UnitsLike(ABC):
    def __init__(self, name: str):
        self._name = name
        self._name_hash = hash(name)

    def __eq__(self, other):
        return self._name_hash == other._name_hash

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def system_name(self):
        return self._name

    @property
    @abstractmethod
    def Metre(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def Kilometre(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def Kilogram(self):
        raise NotImplementedError

    @property
    def Gram(self):
        return self.Kilogram / 1e3

    @property
    @abstractmethod
    def Second(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def Kelvin(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def PlanckMass(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def eV(self):
        raise NotImplementedError

    @property
    def keV(self):
        return 1e3 * self.eV

    @property
    def MeV(self):
        return 1e6 * self.eV

    @property
    def GeV(self):
        return 1e9 * self.eV

    @property
    @abstractmethod
    def c(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def Mpc(self):
        raise NotImplementedError


def check_units(A, B):
    """
    Check that objects A and B are defined with the same units.
    Assumes they both provide a .units property that returns a UnitsLike object
    :param A:
    :param B:
    :return:
    """
    if A.units != B.units:
        raise RuntimeError("Units used for wavenumber k and cosmology are not equal")
