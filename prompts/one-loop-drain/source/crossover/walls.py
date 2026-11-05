"""
crossover side-campaign (one-loop-drain README §8.9): the wall and its classical bounce, and Bogoliubov
coefficients of a massive test field on a prescribed background.

Units: M = 1 and omega_t = 1, as production_energy_ratio.tex §"Exact wall: numerical method". The potential is
measured from its floor V0, U(phi) = V - V0 = c (e^{1/phi} - 1), with c fixed by V''(phi_t) = 1 at the classical
turning point phi_t = 1/L:

    c = e^{-L} / (L^4 + 2 L^3),     E = U(phi_t) = c (e^L - 1).

For L >~ 40 the floor is negligible and this is the note's V = E e^{1/phi - L}. For small L it keeps the floor,
which the cascade's late bounces see (ln(E/V0) = ln(e^L - 1)). V'' = c e^{1/phi} (1/phi^4 + 2/phi^3) is the
same with or without the floor.

The mode equation is the note's eq:mode, x'' + (k^2 + V''(phi(t))) x = 0. Each mode starts in the first-order
adiabatic (WKB) positive-frequency state at a time where its adiabaticity parameter |omega'|/omega^2 is below
EPS, and is integrated to the mirror time on the outgoing leg, where it is projected on the first-order adiabatic
negative-frequency state. The background is time-symmetric about the turning point, so only the incoming leg is
integrated, once, with dense output.
"""

from math import exp, sqrt

import numpy as np
from scipy.integrate import solve_ivp

EPS = 1e-5  # the adiabaticity parameter at which a mode is started and projected
RTOL, ATOL = 1e-11, 1e-14


class Wall:
    def __init__(self, L: float):
        self.L = L
        self.c = exp(-L) / (L**4 + 2.0 * L**3)
        self.E = self.c * (exp(L) - 1.0)
        self.phi_t = 1.0 / L

    def U(self, p):
        return self.c * np.expm1(1.0 / p)

    def dU(self, p):
        return -self.c * np.exp(1.0 / p) / p**2

    def V2(self, p):
        return self.c * np.exp(1.0 / p) * (1.0 / p**4 + 2.0 / p**3)

    def V3(self, p):
        return -self.c * np.exp(1.0 / p) * (1.0 / p**6 + 6.0 / p**5 + 6.0 / p**4)

    def phi_dot(self, p):
        """|phi'| on the classical trajectory at phi (energy conservation)."""
        return np.sqrt(np.maximum(2.0 * (self.E - self.U(p)), 0.0))

    def adiabaticity(self, k, p):
        """|omega'|/omega^2 for mode k with the field at phi on the classical trajectory."""
        w2 = k * k + self.V2(p)
        return abs(self.V3(p) * self.phi_dot(p)) / (2.0 * w2**1.5)

    def start_phi(self, k, eps=EPS):
        """The field value, outwards from the turning point, beyond which mode k is adiabatic to eps."""
        p = 2.0 * self.phi_t
        while self.adiabaticity(k, p) > eps:
            p *= 1.5
            if p > 1e12:
                raise RuntimeError(f"no adiabatic start for k={k} at L={self.L}")
        return p


class WallN(Wall):
    """The wall of V = V0 exp((M/phi)^n), for any n (added 2026-11-05 for U15; Wall is n = 1 and unchanged).

    In units M = 1, omega_t = 1, with y = phi^{-n} and U = c (e^y - 1): the classical turning point is
    phi_t = L^{-1/n}, where L = ln(1 + E/V0) = y_t (so L = M/phi_t only at n = 1), and c is fixed by
    V''(phi_t) = 1. With d1..d4 the derivatives of y,
        d1 = -n y/phi, d2 = n(n+1) y/phi^2, d3 = -n(n+1)(n+2) y/phi^3, d4 = n(n+1)(n+2)(n+3) y/phi^4,
    the derivatives of U are c e^y times d1; d1^2 + d2; d1^3 + 3 d1 d2 + d3; and
    d1^4 + 6 d1^2 d2 + 3 d2^2 + 4 d1 d3 + d4.
    """

    def __init__(self, L: float, n: int):
        self.L = L
        self.n = n
        self.phi_t = L ** (-1.0 / n)
        d1, d2, _, _ = self._dy(self.phi_t)
        self.c = exp(-L) / (d1 * d1 + d2)
        self.E = self.c * (exp(L) - 1.0)

    def _dy(self, p):
        n = self.n
        y = p ** (-n)
        return (
            -n * y / p,
            n * (n + 1) * y / p**2,
            -n * (n + 1) * (n + 2) * y / p**3,
            n * (n + 1) * (n + 2) * (n + 3) * y / p**4,
        )

    def U(self, p):
        return self.c * np.expm1(p ** (-self.n))

    def dU(self, p):
        return self.c * np.exp(p ** (-self.n)) * self._dy(p)[0]

    def V2(self, p):
        d1, d2, _, _ = self._dy(p)
        return self.c * np.exp(p ** (-self.n)) * (d1 * d1 + d2)

    def V3(self, p):
        d1, d2, d3, _ = self._dy(p)
        return self.c * np.exp(p ** (-self.n)) * (d1**3 + 3.0 * d1 * d2 + d3)

    def V4(self, p):
        d1, d2, d3, d4 = self._dy(p)
        return (
            self.c
            * np.exp(p ** (-self.n))
            * (d1**4 + 6.0 * d1 * d1 * d2 + 3.0 * d2 * d2 + 4.0 * d1 * d3 + d4)
        )


class Bounce:
    """The classical bounce: the incoming leg from phi_start to the turning point, with dense output."""

    def __init__(self, wall: Wall, phi_start: float):
        self.wall = wall
        w = wall

        def rhs(t, y):
            return [y[1], -w.dU(y[0])]

        def turn(t, y):
            return y[1]

        turn.terminal = True
        turn.direction = 1  # phi' rises through zero at the turning point
        v0 = -float(w.phi_dot(phi_start))
        # an upper bound on the time to the turning point: the slowest speed is near it, so integrate long
        # and let the event stop it
        T = 10.0 * phi_start / abs(v0) + 100.0
        sol = solve_ivp(
            rhs,
            (0.0, T),
            [phi_start, v0],
            method="DOP853",
            rtol=RTOL,
            atol=ATOL,
            dense_output=True,
            events=turn,
        )
        if sol.status != 1:
            raise RuntimeError(f"no turning point at L={w.L}: {sol.message}")
        self.t_turn = float(sol.t_events[0][0])
        self.phi_turn = float(sol.y_events[0][0][0])
        self.sol = sol

    def phi(self, t):
        """phi at time t on the whole bounce (mirror the incoming leg about the turning point)."""
        s = np.where(t <= self.t_turn, t, 2.0 * self.t_turn - t)
        return self.sol.sol(s)[0]

    def time_at(self, p):
        """The time on the incoming leg at which the field is at phi = p (p >= phi_turn)."""
        from scipy.optimize import brentq

        return brentq(lambda t: self.sol.sol(t)[0] - p, 0.0, self.t_turn, xtol=1e-13)


def wkb_state(w, wdot, sign):
    """First-order adiabatic mode (x, x') of unit normalization; sign -1 positive frequency, +1 negative."""
    x = 1.0 / np.sqrt(2.0 * w)
    xd = (sign * 1j * w - 0.5 * wdot / w) * x
    return x, xd


def beta_squared(bounce: Bounce, V2, V3, k: float, phi_start: float) -> float:
    """|beta_k|^2 for a mode of momentum k on the bounce, started adiabatic at phi_start on the incoming leg."""
    t0 = bounce.time_at(phi_start) if phi_start < bounce.sol.y[0][0] else 0.0
    t1 = 2.0 * bounce.t_turn - t0

    def omega(t):
        p = bounce.phi(t)
        sgn = np.where(t <= bounce.t_turn, 1.0, -1.0)
        pd = (
            sgn
            * bounce.sol.sol(np.where(t <= bounce.t_turn, t, 2.0 * bounce.t_turn - t))[
                1
            ]
        )
        w2 = k * k + V2(p)
        w = np.sqrt(w2)
        wdot = V3(p) * pd / (2.0 * w)
        return w, wdot

    w0, wd0 = omega(np.array(t0))
    x0, xd0 = wkb_state(float(w0), float(wd0), -1.0)

    def rhs(t, y):
        p = bounce.phi(np.array(t))
        m2 = k * k + V2(p)
        return [y[2], y[3], -m2 * y[0], -m2 * y[1]]

    sol = solve_ivp(
        rhs,
        (t0, t1),
        [x0.real, x0.imag, xd0.real, xd0.imag],
        method="DOP853",
        rtol=RTOL,
        atol=ATOL,
    )
    x = sol.y[0, -1] + 1j * sol.y[1, -1]
    xd = sol.y[2, -1] + 1j * sol.y[3, -1]
    w1, wd1 = omega(np.array(t1))
    w1, wd1 = float(w1), float(wd1)
    # x = alpha u_+ + beta u_-, with u_+- the first-order adiabatic modes at t1; the Wronskian projection
    up, upd = wkb_state(w1, wd1, -1.0)
    um, umd = wkb_state(w1, wd1, +1.0)
    # beta = i (u_+ xd - u_+' x) for this normalization (Wronskian of u_+ with u_- is -i... checked numerically)
    wr = up * umd - upd * um
    beta = (up * xd - upd * x) / wr
    return float(abs(beta) ** 2)
