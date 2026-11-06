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
The spectrum model of the quanta a bounce produces, its moments and its kernels (one-loop-drain prompt 05 §1.1, §1.2;
README §6.1 U4, §8.13 U16 and U17, P41). Pure: plain floats and numpy arrays in, the same out. No I/O, nothing of the
integrator or the store, no Ray. tools/quanta_measures.py reads it; a later measurement of the shape changes this module
alone, and SPECTRUM_MODEL with it.

All momenta are comoving. A generation g has scale Q_g = k_ex,g a(N_g) and energy Delta K_g; x = q/Q_g, and its
occupation is

    n_g(q) = A_g s(q/Q_g),    A_g = 2 pi^2 Delta K_g / (Q_g^4 I),    I = int_0^inf x^3 s(x) dx,

so that (1/2 pi^2) int q^3 n_g dq = Delta K_g.

**The shape** (U4 as ruled, 2026-11-02, for every generation whatever its estimate, U16; with the associated piece's upper
end of U17, ruled by the user 2026-11-06):

    s(x) = s_dir(x) + c_a s_assoc(x),
    s_dir(x)   = x^2                                   for x <= x_p,   x_p^2 e^{-kappa (x - x_p)}           above,
    s_assoc(x) = 49 x_*^3 / x                          for x <= x_p,   49 x_*^3 / x * e^{-kappa (x - x_p)}  above,

with c_a the associated piece's scale (default 1; P46). s is continuous at x_p, and s(x_*) = 50 s_dir(x_*) at c_a = 1
(x_* < x_p). U17's question was where the 1/x piece ends: as written by U4, x^3 s_assoc = 49 x_*^3 x^2 makes I diverge.
The user chose "1/x to x_p, then e^{-kappa}" of three options (orchestrator note 05, ruling 1).

**The kernels**, each at a given c_a:

    G(y)     = int x^3 s(x) [sqrt(1 + y^2/x^2) - 1] dx / I,   y = m a / Q_g, the matter-like excess of a generation,
    Omega(u) = int x^3 s(x) s(x e^{-u}) dx / I,             u = ln(Q_g/Q_j), the occupation overlap per unit A_g,

so that generation g contributes A_g Omega(ln(Q_g/Q_j)) to bounce j's overlap n_bar_j.

They are tabulated once per c_a, on first use, as ln G on a uniform grid in ln y and ln Omega on a uniform grid in u >= 0,
and interpolated by a not-a-knot cubic spline; the nodes are computed by composite Gauss-Legendre quadrature split at x_p
(and, for Omega, at x_p e^u). Beyond the grids they are given by closed-form asymptotes carrying every term needed for
1e-7 relative at the grid's end (orchestrator note 05, ruling 3). With M_k = int x^k s dx and C = c_a 49 x_*^3:

  G, y -> 0 (y < G_Y_MIN):  G I = M_1 y^2/2 - C y^3/3 + B_4 y^4 + O(y^6 ln y), where
        B_4 = -(1/8) int s_dir/x dx + C/(8 x_p) - (C/8) int_{x_p}^inf e^{-kappa(x - x_p)} x^{-2} dx.
     For x > y, sqrt(x^2 + y^2) - x = y^2/2x - y^4/8x^3 + ...; the y^2 term is M_1 y^2/2. The 1/x piece on [0, x_p] is
     integrated exactly: C int_0^X x (sqrt(x^2 + y^2) - x) dx = C [((X^2 + y^2)^{3/2} - y^3 - X^3)/3]
     = C [X y^2/2 + y^4/(8X) - y^6/(48 X^3) + ... - y^3/3], which gives the non-analytic -C y^3/3 (the region x <~ y of
     the 1/x piece) and the C/(8 x_p) of B_4. The direct piece on [0, x_p], x^4 (sqrt(x^2 + y^2) - x), gives
     X^4 y^2/8 - X^2 y^4/16 + (y^6/16) ln(X/y) + O(y^6): no y^3 or y^5 term. The tails above x_p are analytic in y^2,
     and their y^4 terms are -(1/8) x_p^2 e^{kappa x_p} E_1(kappa x_p) (direct) and -(C/8) e^{kappa x_p} E_2(kappa x_p)/x_p
     (associated). The terms left out are O(y^6 ln y) against M_1 y^2/2: below 1e-11 relative at G_Y_MIN = 1e-3.
     At c_a = 0, C = 0: the y^3 term vanishes and B_4 is the direct piece's alone.
  G, y -> inf (y > G_Y_MAX):  G I = M_2 y - M_3 + M_4/(2y) - M_6/(8y^3) + M_8/(16 y^5) + R, from
        sqrt(x^2 + y^2) - x = y - x + x^2/2y - x^4/8y^3 + x^6/16y^5 - 5x^8/128y^7 + ... for x < y; M_3 = I, so the -1 is
     exact. |R| <= 5 M_10/(128 y^7) plus the region x > y, of order e^{-kappa y}; at G_Y_MAX = 50 both are below 1e-15
     relative. The form is the same at c_a = 0, with the moments of s_dir.
  Omega, u -> +inf (u > OMEGA_U_MAX): for x e^{-u} <= x_p, s(x e^{-u}) = x^2 e^{-2u} + C e^u / x exactly, so
        Omega I = e^{-2u} M_5 + C e^u M_2 - R,
     with 0 <= R <= int_{x_p e^u}^inf x^3 s(x) (x^2 e^{-2u} + C e^u / x) dx, of order e^{-kappa x_p e^u} (s(z) is below its
     small-z form for z > x_p). At OMEGA_U_MAX = 4, kappa x_p e^4 = 134: R is below 1e-40 relative. At c_a = 0 the
     form is e^{-2u} M_5 / I.
  Omega, u < 0: exactly Omega(u) = e^{4u} Omega(-u) (substitute x = w e^u in the integral), so the table and the
     asymptote serve both signs, and as u -> -inf, Omega I = e^{6u} M_5 + C e^{3u} M_2 - e^{4u} R(-u).

**Moments**, in closed form: with T_k = int_0^inf (x_p + t)^k e^{-kappa t} dt = sum_j binom(k, j) x_p^{k-j} j!/kappa^{j+1},
    M_k = x_p^{k+3}/(k+3) + x_p^2 T_k + C (x_p^k/k + T_{k-1})    (k >= 1),
    I = M_3,  J = M_2 / M_3.

**Direct-quadrature references** (`*_quad`) of every kernel and moment, by scipy's adaptive quad split at x_p (and at y,
or x_p e^u, and at the tail's scales), slow and used only by the tests and the Derive gate's script. Each returns its value
and its stated error (quad's error estimates, summed, relative to the value).

**The peak-collapsed spectrum** (PEAK_SPECTRUM), s(x) = delta(x - 1), is for the Derive gate's comparison with the scoping
probe only (prompt §2.2; orchestrator note 05, ruling 4): I = J = 1 and G(y) = y^2/(sqrt(1 + y^2) + 1), the
cancellation-free form of sqrt(1 + y^2) - 1. It has no Omega. Nothing on the tool's command line selects it (U16).
"""

from functools import lru_cache
from math import comb, exp, factorial, isfinite, log, log1p, pi, sqrt
from typing import Tuple

import numpy as np
from scipy.integrate import quad
from scipy.interpolate import CubicSpline
from scipy.special import exp1, expn

# ---------------------------------------------------------------------------------------------------------------------
# The constants, with their sources (prompt §1.1; P4). Each number is the one taken from the note named.
# ---------------------------------------------------------------------------------------------------------------------

# the version of the shape and its constants, named in every output of tools/quanta_measures.py. It changes whenever the
# shape, a constant or the associated piece's end changes. "U4-U17-v1": U4's shape, with U17's upper end of the
# associated piece, and the constants below
SPECTRUM_MODEL = "U4-U17-v1"

# x_p, the direct piece's peak in units of k_ex: production_energy_ratio.tex, section "Results", "Below the peak
# |beta_k|^2 propto k^2 (fitted slope 1.99)", and Table "One reflection off V_0 e^{M/phi}" (tab:beta), k_peak/omega_t =
# 0.31 (L = 40, 70); with k_ex = omega_t/2, x_p = 0.62 (U4, README §6.1)
X_PEAK = 0.62

# kappa, the direct piece's exponential slope above the peak, fitted to the same note's statements (orchestrator note 05,
# ruling 2): section "Results", "Above the peak the spectrum falls exponentially, to about 10^{-20} at k = 5 omega_t", and
# the peak values of Table tab:beta. With 5 omega_t = 10 k_ex (x = 10) and s(10)/s(x_p) = 1e-20/|beta|^2_max,
#     kappa_L = ln(|beta|^2_max,L / 1e-20) / (10 - x_p)
# at each of the Table's five rows; the module takes the median row, L = 90 (|beta|^2_max = 1.43e-4). The five rows give
# KAPPA_BY_L below, the fit's spread. BriefingNotes/scripts/ does not hold the spectrum's script (its section "Provenance"
# names beta_k_reflection.py, which is not in the repository), so the fit is to the Table and the statement
BETA2_MAX_BY_L = {40: 6.89e-4, 70: 2.33e-4, 90: 1.43e-4, 110: 9.62e-5, 134: 6.52e-5}
BETA2_AT_5_OMEGA_T = 1.0e-20
X_AT_5_OMEGA_T = 10.0
KAPPA_FIT_L = 90


def kappa_fit(beta2_max: float) -> float:
    """kappa from one row of the Table: ln(|beta|^2_max / 1e-20) / (10 - x_p) (ruling 2)."""
    return log(beta2_max / BETA2_AT_5_OMEGA_T) / (X_AT_5_OMEGA_T - X_PEAK)


KAPPA_BY_L = {L: kappa_fit(b) for L, b in BETA2_MAX_BY_L.items()}
KAPPA = KAPPA_BY_L[KAPPA_FIT_L]

# x_* and the associated piece's coefficient: kb_partB_report.tex, section "The soft occupation": "the 2PI's n_q at the
# smallest probe, q = 0.0029 omega_t, is 0.012: fifty times the test field's |beta_q|^2". With k_ex = omega_t/2,
# x_* = 0.0058; the associated piece is 49 x_*^3/x, so that s_dir(x_*) + s_assoc(x_*) = x_*^2 + 49 x_*^2 = 50 s_dir(x_*)
# (U4: "the toy's 50x at q = 0.0029 omega_t"). Its 1/x form is the report's n_q^inh = c_N R omega_t/q
X_STAR = 0.0058
ASSOC_RATIO_AT_X_STAR = 50.0
ASSOC_COEFFICIENT = ASSOC_RATIO_AT_X_STAR - 1.0  # 49

# the associated piece's upper end (U17, the user, 2026-11-06): the 1/x piece to x_p, then e^{-kappa (x - x_p)}, the
# direct piece's kappa. "1/x to x_p, then e^{-kappa} (Recommended)", of three options put
ASSOC_END = "1/x to x_p, then e^{-kappa (x - x_p)} (U17)"

# the associated piece's default scale (P46; U16: in the model of record at scale 1)
DEFAULT_ASSOC_SCALE = 1.0

# ---------------------------------------------------------------------------------------------------------------------
# The grids (ruling 3). The ends are where the asymptotes' left-out terms are below 1e-7 relative (the module docstring
# gives each bound); the steps are where the spline's error is below 1e-7 relative (measured by the tests and
# .documents/one-loop-drain/derive-05/kernels_05.py)
# ---------------------------------------------------------------------------------------------------------------------

G_Y_MIN = 1.0e-3
G_Y_MAX = 50.0
G_LOG_Y_STEP = 0.01
OMEGA_U_MAX = 4.0
OMEGA_U_STEP = 0.005

# the tabulation's composite Gauss-Legendre rule: GL_POINTS per panel; below x_p in t = ln x with panels of width
# GL_LOG_PANEL from ln x_p - GL_LOG_DEPTH; above x_p in x with panels of width at most GL_PANEL, to GL_TAIL past the last
# break (e^{-kappa GL_TAIL} = e^{-159})
GL_POINTS = 20
GL_LOG_PANEL = 0.5
GL_LOG_DEPTH = 46.0
GL_PANEL = 0.25
GL_TAIL = 40.0

_GL_X, _GL_W = np.polynomial.legendre.leggauss(GL_POINTS)


def _check_scale(assoc_scale: float) -> float:
    c = float(assoc_scale)
    if not isfinite(c) or c < 0.0:
        raise ValueError(
            f"quanta_spectrum: the associated piece's scale must be finite and non-negative, not {assoc_scale!r}"
        )
    return c


def assoc_coefficient(assoc_scale: float) -> float:
    """C = c_a 49 x_*^3, the associated piece's coefficient at scale c_a."""
    return _check_scale(assoc_scale) * ASSOC_COEFFICIENT * X_STAR**3


# ---------------------------------------------------------------------------------------------------------------------
# The shape
# ---------------------------------------------------------------------------------------------------------------------


def s_dir(x):
    """The direct piece: x^2 for x <= x_p, x_p^2 e^{-kappa (x - x_p)} above."""
    x = np.asarray(x, dtype=float)
    return np.where(x <= X_PEAK, x * x, X_PEAK * X_PEAK * np.exp(-KAPPA * (x - X_PEAK)))


def s_assoc(x):
    """The associated piece at scale 1: 49 x_*^3/x for x <= x_p, and that times e^{-kappa (x - x_p)} above (U17)."""
    x = np.asarray(x, dtype=float)
    return (
        ASSOC_COEFFICIENT
        * X_STAR**3
        / x
        * np.where(x <= X_PEAK, 1.0, np.exp(-KAPPA * (x - X_PEAK)))
    )


def shape(x, assoc_scale: float = DEFAULT_ASSOC_SCALE):
    """s(x) = s_dir(x) + c_a s_assoc(x)."""
    c = _check_scale(assoc_scale)
    return s_dir(x) + c * s_assoc(x)


def _x3s(x, C: float):
    """x^3 s(x) at coefficient C, written so that x -> 0 is finite."""
    x = np.asarray(x, dtype=float)
    below = x <= X_PEAK
    tail = np.exp(-KAPPA * (np.maximum(x, X_PEAK) - X_PEAK))
    return np.where(
        below,
        x**5 + C * x * x,
        (X_PEAK * X_PEAK * x**3 + C * x * x) * tail,
    )


def _s_at(z, C: float):
    """s(z) at coefficient C (z > 0)."""
    z = np.asarray(z, dtype=float)
    below = z <= X_PEAK
    tail = np.exp(-KAPPA * (np.maximum(z, X_PEAK) - X_PEAK))
    return np.where(below, z * z + C / z, (X_PEAK * X_PEAK + C / z) * tail)


# ---------------------------------------------------------------------------------------------------------------------
# The moments, in closed form
# ---------------------------------------------------------------------------------------------------------------------


def _tail_moment(k: int) -> float:
    """T_k = int_0^inf (x_p + t)^k e^{-kappa t} dt."""
    return sum(
        comb(k, j) * X_PEAK ** (k - j) * factorial(j) / KAPPA ** (j + 1)
        for j in range(k + 1)
    )


def moment(k: int, assoc_scale: float = DEFAULT_ASSOC_SCALE) -> float:
    """M_k = int_0^inf x^k s(x) dx, k >= 1 (M_0 diverges where c_a > 0)."""
    if k < 1:
        raise ValueError("quanta_spectrum.moment: k >= 1")
    C = assoc_coefficient(assoc_scale)
    direct = X_PEAK ** (k + 3) / (k + 3) + X_PEAK * X_PEAK * _tail_moment(k)
    assoc = X_PEAK**k / k + _tail_moment(k - 1)
    return direct + C * assoc


# ---------------------------------------------------------------------------------------------------------------------
# Composite Gauss-Legendre (the tabulation)
# ---------------------------------------------------------------------------------------------------------------------


def _panels(a: float, b: float, width: float) -> np.ndarray:
    n = max(1, int(np.ceil((b - a) / width - 1e-12)))
    return np.linspace(a, b, n + 1)


def _gl_nodes(edges: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """The nodes and weights of the composite rule on the panels with these edges."""
    lo, hi = edges[:-1], edges[1:]
    half = 0.5 * (hi - lo)
    mid = 0.5 * (hi + lo)
    nodes = (mid[:, None] + half[:, None] * _GL_X[None, :]).ravel()
    weights = (half[:, None] * _GL_W[None, :]).ravel()
    return nodes, weights


def _gl_below_peak():
    """Nodes x and weights w (in x) on (0, x_p], by panels in t = ln x."""
    t, wt = _gl_nodes(_panels(log(X_PEAK) - GL_LOG_DEPTH, log(X_PEAK), GL_LOG_PANEL))
    x = np.exp(t)
    return x, wt * x


def _gl_linear(a: float, b: float):
    if b <= a:
        return np.empty(0), np.empty(0)
    return _gl_nodes(_panels(a, b, GL_PANEL))


# ---------------------------------------------------------------------------------------------------------------------
# The spectrum object (orchestrator note 05, ruling 4)
# ---------------------------------------------------------------------------------------------------------------------


class QuantaSpectrum:
    """
    U4's spectrum at one scale c_a of the associated piece: the shape, its moments I and J, the kernels G and Omega, the
    version string and the scale. The tables are built on first use of a kernel. Use spectrum(c_a), which caches one
    object per scale.
    """

    def __init__(self, assoc_scale: float = DEFAULT_ASSOC_SCALE):
        self.assoc_scale: float = _check_scale(assoc_scale)
        self.model: str = SPECTRUM_MODEL
        self.C: float = assoc_coefficient(self.assoc_scale)
        self._M = {k: moment(k, self.assoc_scale) for k in range(1, 11)}
        self.I: float = self._M[3]
        self.J: float = self._M[2] / self._M[3]
        self.log_I: float = log(self.I)
        self.log_J: float = log(self.J)
        # ln s(x_p), the occupation at the direct peak per unit A_g
        self.log_s_peak: float = log(X_PEAK * X_PEAK + self.C / X_PEAK)
        # the small-y series' y^4 coefficient B_4 (times I)
        z = KAPPA * X_PEAK
        int_sdir_over_x = X_PEAK * X_PEAK / 2.0 + X_PEAK * X_PEAK * exp(z) * exp1(z)
        int_tail_over_x2 = exp(z) * expn(2, z) / X_PEAK
        self._B4: float = (
            -int_sdir_over_x / 8.0
            + self.C / (8.0 * X_PEAK)
            - self.C * int_tail_over_x2 / 8.0
        )
        self._G_spline = None
        self._Omega_spline = None

    def __repr__(self) -> str:
        return f"QuantaSpectrum(model={self.model!r}, assoc_scale={self.assoc_scale!r})"

    # ---- the shape ----

    def s(self, x):
        """s(x) at this scale."""
        return _s_at(x, self.C)

    def moment(self, k: int) -> float:
        """M_k (k = 1 ... 10, closed form)."""
        return self._M[k]

    # ---- G ----

    def G_node(self, log_y: np.ndarray) -> np.ndarray:
        """G at ln y by the tabulation's composite Gauss-Legendre rule (no table, no asymptote)."""
        xb, wb = _gl_below_peak()
        xt, wt = _gl_linear(X_PEAK, X_PEAK + GL_TAIL)
        x = np.concatenate([xb, xt])
        w = np.concatenate([wb, wt])
        f = _x3s(x, self.C) / x  # x^2 s(x)
        out = np.empty(len(log_y))
        for i, t in enumerate(np.asarray(log_y, dtype=float)):
            y = exp(t)
            h = y * y / (np.sqrt(x * x + y * y) + x)
            out[i] = np.dot(w, f * h)
        return out / self.I

    def _G_table(self) -> CubicSpline:
        if self._G_spline is None:
            lo, hi = log(G_Y_MIN), log(G_Y_MAX)
            n = int(round((hi - lo) / G_LOG_Y_STEP))
            t = np.linspace(lo, hi, n + 1)
            self._G_spline = CubicSpline(t, np.log(self.G_node(t)))
        return self._G_spline

    def G_small(self, y):
        """G's small-y asymptote: (M_1 y^2/2 - C y^3/3 + B_4 y^4)/I."""
        y = np.asarray(y, dtype=float)
        return (
            y * y * (self._M[1] / 2.0 - self.C * y / 3.0 + self._B4 * y * y)
        ) / self.I

    def G_large(self, y):
        """G's large-y asymptote: M_2 y/I - 1 + (M_4/(2y) - M_6/(8y^3) + M_8/(16y^5))/I."""
        y = np.asarray(y, dtype=float)
        r = 1.0 / y
        r2 = r * r
        return (
            self._M[2] * y
            + r * (self._M[4] / 2.0 - r2 * (self._M[6] / 8.0 - r2 * self._M[8] / 16.0))
        ) / self.I - 1.0

    def G_of_log_y(self, log_y):
        """G at ln y (array): the table inside [G_Y_MIN, G_Y_MAX], the asymptotes beyond it."""
        t = np.asarray(log_y, dtype=float)
        scalar = t.ndim == 0
        t = np.atleast_1d(t)
        out = np.empty_like(t)
        lo = t < log(G_Y_MIN)
        hi = t > log(G_Y_MAX)
        mid = ~(lo | hi)
        if mid.any():
            out[mid] = np.exp(self._G_table()(t[mid]))
        if lo.any():
            out[lo] = self.G_small(np.exp(t[lo]))
        if hi.any():
            out[hi] = self.G_large(np.exp(t[hi]))
        return out[0] if scalar else out

    def G(self, y):
        """G(y), y > 0."""
        return self.G_of_log_y(np.log(np.asarray(y, dtype=float)))

    # ---- Omega ----

    def Omega_node(self, u_values: np.ndarray) -> np.ndarray:
        """Omega at u >= 0 by the tabulation's composite Gauss-Legendre rule, split at x_p and x_p e^u."""
        xb, wb = _gl_below_peak()
        out = np.empty(len(u_values))
        for i, u in enumerate(np.asarray(u_values, dtype=float)):
            if u < 0.0:
                raise ValueError("Omega_node: u >= 0")
            b = X_PEAK * exp(u)
            xm, wm = _gl_linear(X_PEAK, b)
            xt, wt = _gl_linear(b, b + GL_TAIL)
            x = np.concatenate([xb, xm, xt])
            w = np.concatenate([wb, wm, wt])
            out[i] = np.dot(w, _x3s(x, self.C) * _s_at(x * exp(-u), self.C))
        return out / self.I

    def _Omega_table(self) -> CubicSpline:
        if self._Omega_spline is None:
            n = int(round(OMEGA_U_MAX / OMEGA_U_STEP))
            u = np.linspace(0.0, OMEGA_U_MAX, n + 1)
            self._Omega_spline = CubicSpline(u, np.log(self.Omega_node(u)))
        return self._Omega_spline

    def log_Omega_large(self, u):
        """ln Omega's asymptote as u -> +inf: ln[(e^{-2u} M_5 + C e^u M_2)/I]."""
        u = np.asarray(u, dtype=float)
        if self.C > 0.0:
            return (
                log(self.C * self._M[2])
                + u
                + np.log1p(np.exp(-3.0 * u) * self._M[5] / (self.C * self._M[2]))
                - self.log_I
            )
        return log(self._M[5]) - 2.0 * u - self.log_I

    def log_Omega(self, u):
        """ln Omega(u), any real u: the table on [0, OMEGA_U_MAX], the asymptote above, and e^{4u} Omega(-u) below 0."""
        u = np.asarray(u, dtype=float)
        scalar = u.ndim == 0
        u = np.atleast_1d(u)
        a = np.abs(u)
        out = np.empty_like(u)
        mid = a <= OMEGA_U_MAX
        if mid.any():
            out[mid] = self._Omega_table()(a[mid])
        if (~mid).any():
            out[~mid] = self.log_Omega_large(a[~mid])
        neg = u < 0.0
        out[neg] += 4.0 * u[neg]
        return out[0] if scalar else out

    def Omega(self, u):
        """Omega(u)."""
        return np.exp(self.log_Omega(u))

    # ---- the direct-quadrature references (slow; tests and the gate only) ----

    def moment_quad(self, k: int) -> Tuple[float, float]:
        """(M_k, stated relative error) by adaptive quadrature split at x_p."""
        return _quad_pieces(
            lambda x: x ** (k - 3) * _x3s(x, self.C),
            _breaks_tail([0.0, X_PEAK], X_PEAK),
        )

    def I_quad(self) -> Tuple[float, float]:
        return self.moment_quad(3)

    def J_quad(self) -> Tuple[float, float]:
        m2, e2 = self.moment_quad(2)
        m3, e3 = self.moment_quad(3)
        return m2 / m3, e2 + e3

    def G_quad(self, y: float) -> Tuple[float, float]:
        """(G(y), stated relative error) by adaptive quadrature of x^2 s(x) y^2/(sqrt(x^2 + y^2) + x), split at x_p and
        y, over the full spectrum (no table, no asymptote)."""
        y = float(y)
        f = lambda x: _x3s(x, self.C) / x * y * y / (sqrt(x * x + y * y) + x)
        breaks = [0.0, X_PEAK]
        for b in (y / 10.0, y, 10.0 * y):
            if 0.0 < b < X_PEAK:
                breaks.append(b)
        breaks = _breaks_tail(breaks, X_PEAK)
        for b in (y / 2.0, y, 2.0 * y):
            if b > X_PEAK and b < breaks[-1]:
                breaks.append(b)
        value, err = _quad_pieces(f, sorted(set(breaks)))
        return value / self.I, err

    def Omega_quad(self, u: float) -> Tuple[float, float]:
        """(Omega(u), stated relative error) by adaptive quadrature of x^3 s(x) s(x e^{-u}), split at x_p and x_p e^u and
        at the scales of both factors' tails, any real u (no table, no symmetry, no asymptote).
        """
        u = float(u)
        b = X_PEAK * exp(u)
        f = lambda x: float(_x3s(x, self.C)) * float(_s_at(x * exp(-u), self.C))
        lo, hi = min(X_PEAK, b), max(X_PEAK, b)
        breaks = [0.0, lo, hi]
        # the faster factor's decay scale past each break: e^{-kappa x} past x_p, e^{-kappa x e^{-u}} past x_p e^u
        for start, scale in ((X_PEAK, 1.0 / KAPPA), (b, exp(u) / KAPPA)):
            for k in (1.0, 4.0, 16.0, 64.0, 160.0):
                breaks.append(start + k * scale)
        for k in (0.25, 0.5):
            breaks.append(lo * k)
        # beyond max(x_p, x_p e^u) + 160/kappa the integrand is below e^{-160} of its peak: past x_p the first factor
        # falls as e^{-kappa x}, and below x_p e^u (u > 0) the second is its small-z form, so the faster fall bounds both
        end = hi + 160.0 / KAPPA
        breaks = sorted(set(x for x in breaks if 0.0 <= x < end)) + [end]
        value, err = _quad_pieces(f, breaks)
        return value / self.I, err


def _breaks_tail(breaks, start: float):
    """Breaks past start at the tail's scale 1/kappa, to start + GL_TAIL."""
    out = list(breaks)
    for k in (1.0, 4.0, 10.0, 25.0, 60.0, 160.0):
        b = start + k / KAPPA
        if b < start + GL_TAIL:
            out.append(b)
    out.append(start + GL_TAIL)
    return sorted(set(out))


def _quad_pieces(f, breaks) -> Tuple[float, float]:
    """
    sum of quad(f) over the consecutive intervals of breaks, with the summed error estimate relative to the sum. A first
    pass at 1e-8 gives the sum's scale; the second asks each piece for 1e-13 relative or 1e-16 of that scale absolute,
    so that a piece that carries nothing (a far tail) is not asked for digits it does not have.
    """
    pieces = [(a, b) for a, b in zip(breaks[:-1], breaks[1:]) if b > a]
    g = lambda x: float(f(x))
    scale = sum(abs(quad(g, a, b, epsrel=1e-8, limit=200)[0]) for a, b in pieces)
    total, err = 0.0, 0.0
    for a, b in pieces:
        v, e = quad(g, a, b, epsabs=1e-16 * scale, epsrel=1e-13, limit=400)
        total += v
        err += e
    return total, (err / abs(total) if total != 0.0 else float("inf"))


@lru_cache(maxsize=None)
def spectrum(assoc_scale: float = DEFAULT_ASSOC_SCALE) -> QuantaSpectrum:
    """The spectrum at scale c_a of the associated piece, one cached object per scale (its tables are built once)."""
    return QuantaSpectrum(_check_scale(assoc_scale))


class PeakSpectrum:
    """
    The peak-collapsed spectrum s(x) = delta(x - 1), for the Derive gate's comparison with the scoping probe only (prompt
    §2.2; ruling 4): I = J = 1, G(y) = y^2/(sqrt(1 + y^2) + 1). It has no Omega and no peak occupation.
    """

    model = "peak-collapsed (Derive gate only)"
    assoc_scale = 0.0
    I = 1.0
    J = 1.0
    log_I = 0.0
    log_J = 0.0

    def G_of_log_y(self, log_y):
        t = np.asarray(log_y, dtype=float)
        y = np.exp(np.minimum(t, 20.0))
        small = y * y / (np.sqrt(1.0 + y * y) + 1.0)
        big = np.exp(t)
        # above y = e^20, sqrt(1 + y^2) - 1 = y - 1 + 1/(2y) to 1e-26 relative
        return np.where(t > 20.0, big - 1.0 + 0.5 / big, small)

    def G(self, y):
        return self.G_of_log_y(np.log(np.asarray(y, dtype=float)))

    def log_Omega(self, u):
        raise NotImplementedError("the peak-collapsed spectrum has no Omega")


PEAK_SPECTRUM = PeakSpectrum()

# A_g's prefactor: A_g = 2 pi^2 Delta K_g / (Q_g^4 I)
LOG_TWO_PI_SQUARED = log(2.0 * pi * pi)
