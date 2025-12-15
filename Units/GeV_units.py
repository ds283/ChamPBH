from math import pi, sqrt

from Units.base import UnitsLike


class GeV_units(UnitsLike):
    def __init__(self):
        UnitsLike.__init__(self, "GeV units")

    # numerical values obtained from https://en.wikipedia.org/wiki/Planck_units,
    # assuming c = hbar = k_B = 1
    # That leaves a single dimensionful unit in which we measure mass, length, energy, time
    # We can choose this to be whatever we like; often it is GeV, but here we are choosing it
    # to be Mpc instead.

    GeV = 1.0
    eV = GeV / 1e3

    PlanckMass = 2.436e27 * eV

    PlanckMass = 1.0
    sqrt_NewtonG = sqrt(1.0 / (8.0 * pi)) / PlanckMass

    Metre = sqrt_NewtonG / 1.616255e-35
    Kilometre = 1000 * Metre
    Mpc = 3.08567758e22 * Metre

    Kilogram = 1.0 / (2.176434e-8 * sqrt_NewtonG)
    Second = sqrt_NewtonG / 5.391247e-44
    Kelvin = 1.0 / (1.416784e32 * sqrt_NewtonG)

    # c should be unity for consistency, since we have assumed c = hbar = k_B = 1 in writing some of the equations above
    c = 299792458 * Metre / Second
