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
README §6.1 U4, §8.13 U16 and U17, P41; the measured shape by wall since power-law-wall prompt 03, README §6.5 R8, §6.6
R9, R10, P19-P21). Pure: plain floats and numpy arrays in, the same out. No I/O, nothing of the integrator or the store,
no Ray. tools/quanta_measures.py reads it; a later measurement of the shape changes this module alone, and the wall's
SPECTRUM_MODELS string with it.

All momenta are comoving. A generation g has scale Q_g = k_ex,g a(N_g) and energy Delta K_g; x = q/Q_g, and its
occupation is

    n_g(q) = A_g s(q/Q_g),    A_g = 2 pi^2 Delta K_g / (Q_g^4 I),    I = int_0^inf x^3 s(x) dx,

so that (1/2 pi^2) int q^3 n_g dq = Delta K_g. Every result is independent of s's overall normalization.

**The walls** (P19). spectrum(wall, assoc_scale) takes the wall, with no default; an unknown wall is a ValueError naming
the known ones (WALLS):
  - WALL_EXPONENTIAL, "exponential": the wall of V_0 e^{(M/phi)^n}, its table measured at L = 90 at n = 1 (P21), serving
    the exponential family at every n, as the one shape of U4-U17-v1 did;
  - WALL_POWER_N1, WALL_POWER_N2, "power-n1" and "power-n2": the wall of V_0 [1 + (M/phi)^n] at n = 1 and 2, at the
    scale-free reference E/V_0 -> infinity (R9, U8 (a)).
Each wall's spectrum is named by its own string, SPECTRUM_MODELS[wall] (spectrum_model(wall), QuantaSpectrum.model),
which every output of tools/quanta_measures.py carries in its spectrum_model column; no column is added.

**The shape** (R8, R10), for each wall:

    s(x) = s_dir(x) + c_a s_assoc(x),

with c_a the associated piece's scale (default 1; P46) and
  - the direct piece, the test field's |beta_k|^2 measured on the wall (prompts/power-law-wall/source/wall/
    spectrum_table.py, at walls.py's strict setting), normalized so that s_dir = x^2 below the table:
      s_dir(x) = x^2                                         x <= x_lo,
               = exp(P(ln x))                                x_lo < x < x_hi,
               = s_dir(x_hi) e^{-kappa_hi (x - x_hi)}         x >= x_hi;
    P the not-a-knot cubic spline through the table's (ln x, ln s) nodes, uniform in ln x from x_lo (k/omega_t = 1e-3,
    R8) to x_hi; kappa_hi = -d ln s_dir/dx at x_hi from P's end derivative, so that s_dir and its slope are continuous
    there (R8: "matched in value and slope"); the k^2 law below is matched in value at x_lo (R8; at n = 1 the
    measured power there is 1.66, tending to 2 as k^2 ln^2(1/k) would, and the logarithm is neglected by the user's
    ruling);
  - the associated piece (R10, U9 as proposed): 1/x up to the table's peak x_p, then the direct piece's own fall,
      s_assoc(x) = C_1/x                                     x <= x_p,
                 = (C_1/x) s_dir(x)/s_dir(x_p)               x > x_p,
    which is U17's form exactly where s_dir is U4's, with C_1 = 49 x_* s_dir(x_*), so that s(x_*) = 50 s_dir(x_*) at
    c_a = 1 as U4 put it, and x_* = 0.005615853347600149 on every wall (X_STAR, power-law-wall prompt 07c, README §6.14
    P104: the 2PI run's q = 0.0029 omega_t over the exponential reference's k_ex/omega_t = 0.5163952511757223; 0.0058
    before 07c, over k_ex = omega_t/2; on the power wall carried over in x from the exponential's 2PI run and unmeasured
    there, U1-U7 and R10 as ruled). C = c_a C_1.
x_p is the maximum of P (a root of its derivative). s is continuous everywhere; s_assoc is continuous at x_p and, x_p
being P's maximum, so is its slope.

x = k/k_ex, k_ex the production's, E^{1/2}|V'''/V''|/2 at phi_t (power-law-wall prompt 07c; README §6.12 R13-R15, the
one k_ex of every use; exact at the reference): on the exponential at L = 90, k_ex = 0.5163952511757223 omega_t (0.494535
omega_t in the old k_ex, -d ln V/dphi E^{1/2}/2, before 07c; U4-U17-v1 took omega_t/2); on the power wall
k_ex = (n + 2)/(2 sqrt(n(n + 1))) omega_t at every E/V_0, 1.0606601717798214 omega_t at n = 1 and 0.816496580927726 at
n = 2. Since the bounce in k/omega_t on the power wall does not depend on E/V_0 and k_ex/omega_t no longer does either,
the table holds at every E/V_0 there: R9's shift of a generation near the floor (by s_t, in the old k_ex) is gone, to
3.4e-7 in ln |beta|^2 down to the laboratory R = 1 (07b's measurement).

**The kernels**, each at a given c_a:

    G(y)     = int x^3 s(x) [sqrt(1 + y^2/x^2) - 1] dx / I,   y = m a / Q_g, the matter-like excess of a generation,
    Omega(u) = int x^3 s(x) s(x e^{-u}) dx / I,             u = ln(Q_g/Q_j), the occupation overlap per unit A_g,

so that generation g contributes A_g Omega(ln(Q_g/Q_j)) to bounce j's overlap n_bar_j.

They are tabulated once per (wall, c_a), on first use, as ln G on a uniform grid in ln y and ln Omega on a uniform grid in
u >= 0, and interpolated by a not-a-knot cubic spline. The nodes, and the moments, are computed by composite
Gauss-Legendre quadrature (_nodes): below x_lo in panels of ln x; above it on the pieces between consecutive breaks
(every table node, x_p and x_hi; for Omega also each of them times e^u), each divided so that no panel is wider than
GL_DT in ln x or GL_DX in x, to x_hi + GL_TAIL/kappa_hi (the tail is e^{-GL_TAIL} there). Beyond the grids they are
given by asymptotes carrying every term needed for 1e-7 relative at the grid's end. With M_k = int x^k s dx:

  G, y -> 0 (y < G_Y_MIN):  G I = M_1 y^2/2 - C y^3/3 + B_4 y^4 + O(y^6 ln y), where
        B_4 = -(1/8) int_0^inf s_dir/x dx + C/(8 x_p) - (C/8) int_{x_p}^inf s_dir(x)/(s_dir(x_p) x^2) dx.
     For x > y, sqrt(x^2 + y^2) - x = y^2/2x - y^4/8x^3 + ...; the y^2 term is M_1 y^2/2. The 1/x piece on [0, x_p] is
     integrated exactly: C int_0^X x (sqrt(x^2 + y^2) - x) dx = C [((X^2 + y^2)^{3/2} - y^3 - X^3)/3]
     = C [X y^2/2 + y^4/(8X) - y^6/(48 X^3) + ... - y^3/3], which gives the non-analytic -C y^3/3 and the C/(8 x_p) of
     B_4. The direct piece is x^2 on [0, x_lo], and x^4 (sqrt(x^2 + y^2) - x) there integrates to X^4 y^2/8 - X^2 y^4/16
     + (y^6/16) ln(X/y) + O(y^6) at X = x_lo (y < G_Y_MIN < x_lo on every wall): no y^3 or y^5 term. Above x_lo both
     pieces are smooth and the expansion is in y^2, with the y^4 terms of B_4. The terms left out are y^6 times
     (1/16) ln(x_lo/y) + (1/16) int_{x_lo}^inf s_dir/x^3 dx + C/(48 x_p^3) + ..., against M_1 y^2/2: at G_Y_MIN = 5e-4,
     y^4 = 6.25e-14 times that factor (below 0.43 on every wall: int s_dir/x^3 is 5.4, 2.0 and 3.9) over M_1/2 (M_1 is
     0.086, 5.2e-5 and 1.5e-3 on the exponential and the power wall at n = 1 and 2, in these units), below 4e-10
     relative, at power-n1 (the tests measure it against quadrature: 4.3e-10). G_Y_MIN is 5e-4 since power-law-wall
     prompt 07c (README §6.14 P104), 1e-3 before: power-n1's x_lo in the one k_ex, 9.43e-4, fell below 1e-3, and the
     derivation needs y < x_lo. At c_a = 0, C = 0: the y^3 term vanishes and B_4 is the direct piece's alone.
  G, y -> inf (y > G_Y_MAX):  G I = M_2 y - M_3 + M_4/(2y) - M_6/(8y^3) + M_8/(16 y^5) + R, from
        sqrt(x^2 + y^2) - x = y - x + x^2/2y - x^4/8y^3 + x^6/16y^5 - 5x^8/128y^7 + ... for x < y; M_3 = I, so the -1 is
     exact. |R| <= 5 M_10/(128 y^7) plus the region x > y, where s is the tail, of order e^{-kappa_hi y}; at G_Y_MAX = 50
     and kappa_hi >= 4.34 both are below 1e-11 relative on every wall (8.5e-14 on the exponential, the largest; the
     tests measure them). The form is the same at c_a = 0, with the moments of s_dir.
  Omega, u -> +inf (u > OMEGA_U_MAX): for x e^{-u} <= x_lo (< x_p), s(x e^{-u}) = x^2 e^{-2u} + C e^u / x exactly, so
        Omega I = e^{-2u} M_5 + C e^u M_2 - R,
     with 0 <= R <= int_{x_lo e^u}^inf x^3 s(x) (x^2 e^{-2u} + C e^u / x) dx, since s(z) <= z^2 + C/z for z >= x_lo (s_dir
     <= z^2 there on every wall, and s_dir(z)/s_dir(x_p) <= 1 above x_p; the tests check both). The bound falls with
     u as the weight of x^5 s and x^2 s above x_lo e^u; OMEGA_U_MAX = 9 puts x_lo e^u >= 7.64 on every wall (power-n1's;
     15.7 and 9.9 on the others), where they are below 8e-16 of M_5 and 6e-19 of M_2 (the tests measure them; at u = 8
     they are up to 3.5e-4). Re-derived by power-law-wall prompt 07c on the tables in the one k_ex: x_lo is lower than
     before (16.4 was the old tables' x_lo e^9), but the tails fall faster (kappa_hi >= 4.34), so 9 still meets the
     bound. At c_a = 0 the form is e^{-2u} M_5 / I.
  Omega, u < 0: exactly Omega(u) = e^{4u} Omega(-u) (substitute x = w e^u in the integral), so the table and the
     asymptote serve both signs, and as u -> -inf, Omega I = e^{6u} M_5 + C e^{3u} M_2 - e^{4u} R(-u).

**Moments**, by the same quadrature: M_k for k = 1 ... 10 (M_0 diverges where c_a > 0), I = M_3, J = M_2 / M_3.

**Direct-quadrature references** (`*_quad`) of every kernel and moment, by scipy's adaptive quad on the pieces between
the same breaks (and y, or x_p e^u ..., and the tail's scales), slow and used only by the tests and the measurement
scripts. Each returns its value and its stated error (quad's error estimates, summed, relative to the value).

**The peak-collapsed spectrum** (PEAK_SPECTRUM), s(x) = delta(x - 1), is for the Derive gate's comparison with the scoping
probe only (one-loop-drain prompt 05 §2.2; orchestrator note 05, ruling 4): I = J = 1 and G(y) = y^2/(sqrt(1 + y^2) + 1),
the cancellation-free form of sqrt(1 + y^2) - 1. It has no Omega and no wall. Nothing on the tool's command line selects
it (U16).
"""

from bisect import bisect_right
from collections import namedtuple
from functools import lru_cache
from math import exp, isfinite, log, pi, sqrt
from typing import Tuple

import numpy as np
from scipy.integrate import quad
from scipy.interpolate import CubicSpline

# ---------------------------------------------------------------------------------------------------------------------
# The walls and their names (P19)
# ---------------------------------------------------------------------------------------------------------------------

WALL_EXPONENTIAL = "exponential"
WALL_POWER_N1 = "power-n1"
WALL_POWER_N2 = "power-n2"
WALLS = (WALL_EXPONENTIAL, WALL_POWER_N1, WALL_POWER_N2)

# the version of each wall's spectrum, named in every output of tools/quanta_measures.py. It changes whenever the shape,
# a table, a constant or the associated piece's form changes. "R8-<wall>-v1": R8's three regions on the wall's table,
# with R10's associated piece (power-law-wall prompt 03); before it every wall would have been "U4-U17-v1", U4's analytic
# shape with U17's end of the associated piece, the one shape of one-loop-drain prompt 05. "R8-<wall>-v2" since
# power-law-wall prompt 07c (README §6.14 P104): the tables re-measured in x = k/k_ex with the one k_ex,
# E^{1/2}|V'''/V''|/2, and x_* with them
SPECTRUM_MODELS = {
    WALL_EXPONENTIAL: "R8-exponential-v2",
    WALL_POWER_N1: "R8-power-n1-v2",
    WALL_POWER_N2: "R8-power-n2-v2",
}


def _check_wall(wall) -> str:
    if wall not in SPECTRUM_MODELS:
        raise ValueError(
            f"quanta_spectrum: the wall must be one of {WALLS}, not {wall!r}"
        )
    return wall


def spectrum_model(wall: str) -> str:
    """The wall's spectrum model, SPECTRUM_MODELS[wall]; an unknown wall is a ValueError naming the known ones."""
    return SPECTRUM_MODELS[_check_wall(wall)]


# ---------------------------------------------------------------------------------------------------------------------
# The associated piece's constants, with their sources
# ---------------------------------------------------------------------------------------------------------------------

# x_* and the associated piece's coefficient: kb_partB_report.tex, section "The soft occupation": "the 2PI's n_q at the
# smallest probe, q = 0.0029 omega_t, is 0.012: fifty times the test field's |beta_q|^2" (U4: "the toy's 50x at
# q = 0.0029 omega_t"). Its 1/x form is the report's n_q^inh = c_N R omega_t/q. Since power-law-wall prompt 03 (R10, U9 as
# proposed) the coefficient is C_1 = 49 x_* s_dir(x_*), so that s_dir(x_*) + s_assoc(x_*) = 50 s_dir(x_*) on the
# measured shape, and x_* is one value in x on every wall: carried over from the exponential's 2PI run, and unmeasured
# on the power wall (U1-U7 as ruled). Since power-law-wall prompt 07c (README §6.12 P82, §6.14 P104) x_* =
# 0.0029/0.5163952511757223 = 0.005615853347600149, the 2PI probe over the exponential reference's k_ex/omega_t in the
# one k_ex (07b's spectrum_kex.py); before it, 0.0058, over k_ex = omega_t/2. In k/omega_t it is 0.002734 on the power
# wall at n = 1 and 0.003552 at n = 2 (R10 carries x, not k/omega_t). Flagged, as before: unmeasured off the exponential
X_STAR = 0.005615853347600149
ASSOC_RATIO_AT_X_STAR = 50.0
ASSOC_COEFFICIENT = ASSOC_RATIO_AT_X_STAR - 1.0  # 49

# the associated piece's form (R10, U9: "U9 accept as proposed"): 1/x to the table's x_p, then the direct piece's own
# fall, which is U17's "1/x to x_p, then e^{-kappa}" where the direct piece is U4's
ASSOC_END = "1/x to x_p, then (C_1/x) s_dir(x)/s_dir(x_p) (R10)"

# the associated piece's default scale (P46; U16: in the model of record at scale 1)
DEFAULT_ASSOC_SCALE = 1.0

# ---------------------------------------------------------------------------------------------------------------------
# The grids. The ends are where the asymptotes' left-out terms are below 1e-7 relative (the module docstring gives each
# bound); the steps are where the spline's error is below 1e-7 relative (measured by the tests)
# ---------------------------------------------------------------------------------------------------------------------

G_Y_MIN = 5.0e-4
G_Y_MAX = 50.0
G_LOG_Y_STEP = 0.01
OMEGA_U_MAX = 9.0
OMEGA_U_STEP = 0.01

# the composite Gauss-Legendre rule (_nodes): below x_lo, GL_LOG_POINTS per panel of width GL_LOG_PANEL in ln x from
# ln x_lo - GL_LOG_DEPTH; above it GL_POINTS per panel, panels at most GL_DT wide in ln x and GL_DX in x, to
# x_hi + GL_TAIL/kappa_hi
GL_LOG_POINTS = 20
GL_LOG_PANEL = 0.5
GL_LOG_DEPTH = 46.0
GL_POINTS = 10
GL_DT = 0.05
GL_DX = 0.25
GL_TAIL = 160.0

_GL_LOG_X, _GL_LOG_W = np.polynomial.legendre.leggauss(GL_LOG_POINTS)
_GL_X, _GL_W = np.polynomial.legendre.leggauss(GL_POINTS)

# ---------------------------------------------------------------------------------------------------------------------
# The tables (R8; P19, P21; R9). Each is ln |beta_k|^2 of the test field (walls.Bounce and walls.beta_squared at walls.py's
# strict setting, rtol 1e-13, atol 1e-17, EPS 1e-8), at N nodes uniform in t = ln x from ln X_LO to ln X_HI
# (np.linspace), measured by spectrum_table.py's method in x = k/k_ex with the one k_ex, E^{1/2}|V'''/V''|/2 at phi_t
# (power-law-wall prompt 07b, prompts/power-law-wall/source/kex/spectrum_kex.py `table`; set here by prompt 07c, README
# §6.14 P104, exactly as .documents/power-law-wall/kex-07b/spectrum_tables.txt writes them):
#   - the exponential: WallN(90, 1) in walls.py's units (M = 1, omega_t = 1), k_ex/omega_t = 0.5163952511757223;
#   - the power wall: PowerWall(1e40, n) in units of phi_t, k_ex/omega_t = (n + 2)/(2 sqrt(n(n + 1))) =
#     1.0606601717798214 (n = 1) and 0.816496580927726 (n = 2), at every E/V_0.
# X_LO is 1e-3 omega_t over k_ex (R8). X_HI is the smallest of 3, 3.5, 4, ... at which the matched tail carries at most
# 1e-3 of x^3 s_dir (orchestrator note 03, ruling 5): 7.4e-4, 8.0e-6 and 1.4e-4 of it. x_p = 0.619424, 0.155250 and
# 0.253372; kappa_hi = 4.342684, 6.545990 and 5.467311 (07b's spectrum_tables.txt; before 07c, in the old k_ex, the
# tables had 153, 156 and 156 nodes, x_hi 4.0, 6.5 and 5.5, and the spline was within 2.8e-6 of direct evaluations)
# ---------------------------------------------------------------------------------------------------------------------

WallTable = namedtuple(
    "WallTable", ["wall", "reference", "k_ex_over_omega_t", "x_lo", "x_hi", "ln_beta2"]
)

_LN_BETA2_EXPONENTIAL = (
    -19.28526022448748,
    -19.185266353261547,
    -19.085272579139062,
    -18.985278919035782,
    -18.88528548753666,
    -18.78529211507394,
    -18.68529904621082,
    -18.585306224167088,
    -18.485313490584915,
    -18.38532122738158,
    -18.28532912877096,
    -18.185337357670672,
    -18.085346105103106,
    -17.98535514283755,
    -17.88536462129535,
    -17.785374589072966,
    -17.685385088917325,
    -17.585396218732996,
    -17.48540798762036,
    -17.385420536880176,
    -17.285433835437082,
    -17.1854480551957,
    -17.085463233843033,
    -16.98547950967116,
    -16.885497004858333,
    -16.785515829256333,
    -16.685536104961248,
    -16.585558018136545,
    -16.48558171128188,
    -16.38560743235425,
    -16.285635272790202,
    -16.185665550271313,
    -16.0856985347661,
    -15.985734453942106,
    -15.88577364714798,
    -15.785816443384073,
    -15.685863236460374,
    -15.585914441293276,
    -15.485970514458831,
    -15.386031975591656,
    -15.286099387452856,
    -15.18617337953512,
    -15.086254659440495,
    -14.986343929503391,
    -14.886442097822572,
    -14.786550096042783,
    -14.686668912300865,
    -14.586799710503335,
    -14.48694376956936,
    -14.38710243578022,
    -14.287277286020066,
    -14.18747000588087,
    -14.087682476726119,
    -13.987916771896291,
    -13.888175172946946,
    -13.788460227747324,
    -13.688774731944566,
    -13.589121773809252,
    -13.48950477426842,
    -13.389927507945009,
    -13.29039414102101,
    -13.190909287518336,
    -13.091478033894631,
    -12.992106003717655,
    -12.892799408893428,
    -12.793565108547252,
    -12.694410680833805,
    -12.59534449440141,
    -12.49637579099298,
    -12.39751477575666,
    -12.29877271696184,
    -12.200162055469201,
    -12.101696525517013,
    -12.003391286835935,
    -11.905263070755984,
    -11.807330340844612,
    -11.70961346900895,
    -11.612134928003536,
    -11.514919505071502,
    -11.417994533157325,
    -11.32139014626464,
    -11.225139557984688,
    -11.12927936584322,
    -11.033849884191943,
    -10.938895506544386,
    -10.844465100517162,
    -10.750612436782896,
    -10.657396654814166,
    -10.564882767500512,
    -10.473142206132268,
    -10.382253407993177,
    -10.292302448603147,
    -10.203383719377818,
    -10.115600651608732,
    -10.029066488047384,
    -9.94390510039884,
    -9.860251853065204,
    -9.778254509925695,
    -9.698074181930389,
    -9.619886310292483,
    -9.543881679423073,
    -9.470267452297646,
    -9.399268218505025,
    -9.331127043495353,
    -9.266106506031885,
    -9.204489708475085,
    -9.146581242527759,
    -9.092708091873082,
    -9.043220451490182,
    -8.99849244251808,
    -8.9589227014163,
    -8.924934822538974,
    -8.896977634570277,
    -8.8755252938522,
    -8.861077180857793,
    -8.854157590940263,
    -8.855315216268076,
    -8.865122422591226,
    -8.884174332521862,
    -8.91308773522937,
    -8.952499851440205,
    -9.003066991415585,
    -9.065463151822675,
    -9.140378604972112,
    -9.228518539926192,
    -9.330601819109397,
    -9.447359916272063,
    -9.579536101192245,
    -9.72788493345653,
    -9.893172122239816,
    -10.07617480068968,
    -10.277682253689926,
    -10.498497125536286,
    -10.739437121328283,
    -11.001337202596796,
    -11.285052264228776,
    -11.591460268309346,
    -11.921465799711823,
    -12.276004000299368,
    -12.656044833241747,
    -13.06259762610715,
    -13.496715841623857,
    -13.959502027508893,
    -14.452112901793706,
    -14.975764536432175,
    -15.53173761032199,
    -16.121382710132245,
    -16.74612566679698,
    -17.40747292227134,
    -18.107016931153787,
    -18.84644160285761,
)

_LN_BETA2_POWER_N1 = (
    -8.840911832200176,
    -8.758095527186217,
    -8.675425832616872,
    -8.592905560422226,
    -8.510537655566665,
    -8.428324861031106,
    -8.346270053373996,
    -8.264376521430785,
    -8.18264724414606,
    -8.101085599011896,
    -8.019694835227288,
    -7.938478334077697,
    -7.857439835099113,
    -7.7765827948759965,
    -7.695911130678545,
    -7.6154286333159416,
    -7.535139376679805,
    -7.4550474373269875,
    -7.375157138965076,
    -7.295472806550311,
    -7.215999113704821,
    -7.13674063771414,
    -7.0577022775759195,
    -6.978889033263983,
    -6.900306094293978,
    -6.821958798948564,
    -6.7438526819033635,
    -6.6659934437105886,
    -6.588387035189918,
    -6.5110395414981,
    -6.433957299071657,
    -6.357146878850076,
    -6.280615020383167,
    -6.204368782532708,
    -6.128415404932869,
    -6.05276242311473,
    -5.977417653903633,
    -5.902389149829566,
    -5.827685307244484,
    -5.753314801161662,
    -5.6792866194581855,
    -5.605610137084797,
    -5.532294984428381,
    -5.459351244472732,
    -5.386789327941439,
    -5.3146200447459035,
    -5.242854643496903,
    -5.1715047711491176,
    -5.100582536573007,
    -5.030100521986742,
    -4.960071792770054,
    -4.890509925321773,
    -4.821429023687607,
    -4.752843756310931,
    -4.684769383809678,
    -4.617221744495904,
    -4.550217336070814,
    -4.4837733167518605,
    -4.417907530988972,
    -4.352638550140227,
    -4.287985704146098,
    -4.223969115920387,
    -4.160609739097574,
    -4.097929379192806,
    -4.035950769346304,
    -3.9746975800630904,
    -3.9141944677948457,
    -3.8544671312620995,
    -3.795542356296235,
    -3.7374480638972583,
    -3.680213363790149,
    -3.623868610623523,
    -3.5684454626226256,
    -3.513976944322649,
    -3.4604975057634753,
    -3.408043097546448,
    -3.356651240247057,
    -3.3063610961029952,
    -3.257213548998136,
    -3.209251287701169,
    -3.1625188925997127,
    -3.1170629267832024,
    -3.0729320317753057,
    -3.030177028961963,
    -2.988851024933477,
    -2.9490095261666673,
    -2.910710549768266,
    -2.874014751876461,
    -2.838985556293911,
    -2.8056892906592754,
    -2.7741953295661297,
    -2.7445762455442533,
    -2.7169079693719396,
    -2.6912699566665723,
    -2.6677453606702968,
    -2.6464212217039567,
    -2.6273886603160816,
    -2.6107430809281635,
    -2.5965843887577504,
    -2.585017215410644,
    -2.5761511586536163,
    -2.570101036378837,
    -2.5669871461371807,
    -2.5669355462212637,
    -2.57007834960125,
    -2.5765540306549775,
    -2.586507749334052,
    -2.6000916979293334,
    -2.6174654506990143,
    -2.638796352072806,
    -2.6642599127762217,
    -2.694040221950332,
    -2.7283303998834647,
    -2.7673330511892495,
    -2.8112607609714013,
    -2.860336605807164,
    -2.914794695497137,
    -2.9748807369637817,
    -3.0408526473253508,
    -3.1129811646718215,
    -3.191550528028742,
    -3.276859161251165,
    -3.369220418403439,
    -3.468963346210846,
    -3.576433499580406,
    -3.691993795513714,
    -3.816025410373075,
    -3.948928725255993,
    -4.0911243155009105,
    -4.243053999622731,
    -4.405181939305629,
    -4.577995783602668,
    -4.762007894272779,
    -4.957756618995498,
    -5.165807632508507,
    -5.386755348742007,
    -5.621224406907362,
    -5.869871225576082,
    -6.133385650683486,
    -6.412492665368712,
    -6.707954218849887,
    -7.020571123467909,
    -7.351185052793189,
    -7.70068064638363,
    -8.069987734401288,
    -8.460083650138351,
    -8.871995659406858,
    -9.306803563563232,
    -9.765642375595894,
    -10.249705172128975,
    -10.760246015052248,
    -11.298583140650592,
    -11.866102317897166,
    -12.464260224031518,
    -13.094587926658027,
    -13.758695047063936,
    -14.458273607575231,
    -15.195102114963765,
    -15.971050223019335,
    -16.788083413123246,
    -17.648267474879393,
    -18.55377355821954,
    -19.50688700366622,
)

_LN_BETA2_POWER_N2 = (
    -11.827477081643654,
    -11.728763625871457,
    -11.630084953393983,
    -11.531442965628031,
    -11.432839378187015,
    -11.33427648495004,
    -11.235756093049071,
    -11.137280592799623,
    -11.038851923468593,
    -10.940472517137733,
    -10.84214533801706,
    -10.743871536097389,
    -10.645654483938056,
    -10.547497162085989,
    -10.44940252231648,
    -10.35137377649102,
    -10.253414111413806,
    -10.155526883989651,
    -10.057715542333762,
    -9.959983803854385,
    -9.862335551537473,
    -9.764774959876119,
    -9.667306355157057,
    -9.569933921168124,
    -9.472663056317069,
    -9.37549838226356,
    -9.278444933311212,
    -9.181508040254982,
    -9.084693386662046,
    -8.988006964599734,
    -8.891455057913689,
    -8.795044198249549,
    -8.698781157369863,
    -8.602672997109673,
    -8.506727155355492,
    -8.410951453570592,
    -8.315354090996788,
    -8.219943612321082,
    -8.124728792510128,
    -8.029719192460318,
    -7.934924546201635,
    -7.840354982546622,
    -7.746021136379088,
    -7.651934205828602,
    -7.558105923211364,
    -7.464548495599261,
    -7.371274630562898,
    -7.27829763154077,
    -7.1856314472787615,
    -7.093290646180344,
    -7.001290454281444,
    -6.909646569913415,
    -6.818375726644576,
    -6.72749526905976,
    -6.637023216882249,
    -6.5469783986820165,
    -6.4573805559510555,
    -6.36825030344249,
    -6.27960908818454,
    -6.191479293919128,
    -6.103884338249035,
    -6.016848637197862,
    -5.930397632249977,
    -5.844557906948658,
    -5.75935716816043,
    -5.67482436676685,
    -5.5909896661829155,
    -5.507884471498551,
    -5.425541630795643,
    -5.3439953944277105,
    -5.263281410537609,
    -5.183436904762025,
    -5.10450070676774,
    -5.0265132574056475,
    -4.949516767735294,
    -4.873555231794363,
    -4.79867451762359,
    -4.7249224646562835,
    -4.652348943744677,
    -4.581005926700119,
    -4.510947680235155,
    -4.4422306964021985,
    -4.374913935402498,
    -4.309058875919984,
    -4.244729596808566,
    -4.181992965627741,
    -4.120918679114543,
    -4.061579463413012,
    -4.004051149105586,
    -3.9484128592105683,
    -3.894747122201612,
    -3.843140057269489,
    -3.7936814928739366,
    -3.746465226119296,
    -3.701589085923431,
    -3.6591552388903246,
    -3.6192702992129298,
    -3.582045613860875,
    -3.5475974239439863,
    -3.516047151441324,
    -3.4875216116703,
    -3.4621532893107525,
    -3.4400806172303304,
    -3.421448257184245,
    -3.4064074100218718,
    -3.3951161424826406,
    -3.3877397097253996,
    -3.384450954967051,
    -3.385430626126699,
    -3.3908678281174645,
    -3.40096042091487,
    -3.4159154539002183,
    -3.435949646573269,
    -3.461289877362902,
    -3.492173698320975,
    -3.5288498822029637,
    -3.571578998926324,
    -3.6206340221016164,
    -3.6763009714881854,
    -3.7388795691745917,
    -3.8086839780818513,
    -3.8860435299383114,
    -3.971303509820045,
    -4.064825990511252,
    -4.16699070551871,
    -4.278195966413646,
    -4.398859629248707,
    -4.529420112450045,
    -4.670337468250869,
    -4.8220945091249305,
    -4.985197992047841,
    -5.160179870194479,
    -5.347598611882057,
    -5.54804055357943,
    -5.762121391660721,
    -5.990487683831224,
    -6.233818464234325,
    -6.492826937397384,
    -6.768262249176415,
    -7.060911336851843,
    -7.371600930738258,
    -7.701199598413521,
    -8.050619899190435,
    -8.42082069433605,
    -8.812809534787228,
    -9.227645161755312,
    -9.666440149559955,
    -10.13036386144291,
    -10.620644898055488,
    -11.138574916767343,
    -11.685511012412064,
    -12.262879998537118,
    -12.872181048922714,
    -13.514990180565764,
    -14.19296394780815,
    -14.907843482373597,
    -15.661458951644489,
    -16.45573408230639,
)

_TABLES = {
    WALL_EXPONENTIAL: WallTable(
        wall=WALL_EXPONENTIAL,
        reference="WallN(90, 1): V_0 e^{M/phi} at L = ln(1 + E/V_0) = 90 (P21)",
        k_ex_over_omega_t=0.5163952511757223,
        x_lo=0.0019365011543448797,
        x_hi=3.5,
        ln_beta2=_LN_BETA2_EXPONENTIAL,
    ),
    WALL_POWER_N1: WallTable(
        wall=WALL_POWER_N1,
        reference="PowerWall(1e40, 1): V_0 [1 + M/phi] at E/V_0 = 1e40, the scale-free reference (R9)",
        k_ex_over_omega_t=1.0606601717798214,
        x_lo=0.0009428090415820634,
        x_hi=3.0,
        ln_beta2=_LN_BETA2_POWER_N1,
    ),
    WALL_POWER_N2: WallTable(
        wall=WALL_POWER_N2,
        reference="PowerWall(1e40, 2): V_0 [1 + (M/phi)^2] at E/V_0 = 1e40, the scale-free reference (R9)",
        k_ex_over_omega_t=0.816496580927726,
        x_lo=0.001224744871391589,
        x_hi=3.0,
        ln_beta2=_LN_BETA2_POWER_N2,
    ),
}


# ---------------------------------------------------------------------------------------------------------------------
# The direct piece of one wall
# ---------------------------------------------------------------------------------------------------------------------


class _Direct:
    """The direct piece of one wall: x^2 below x_lo, the spline P on the table, the matched tail above x_hi."""

    def __init__(self, table: WallTable):
        self.table = table
        ln_b = np.array(table.ln_beta2, dtype=float)
        self.x_lo = float(table.x_lo)
        self.x_hi = float(table.x_hi)
        self.t_lo = log(self.x_lo)
        self.t_hi = log(self.x_hi)
        self.t = np.linspace(self.t_lo, self.t_hi, len(ln_b))
        # normalized so that s_dir = x^2 below the table (every result is independent of the normalization)
        self.ln_s = ln_b - ln_b[0] + 2.0 * self.t_lo
        self.spline = CubicSpline(self.t, self.ln_s)
        self._c = (
            self.spline.c
        )  # (4, nodes - 1): the cubic on each interval, in powers of (t - t_i)
        self._t_list = [float(v) for v in self.t]
        self._c_list = [[float(v) for v in row] for row in self._c]
        self.s_hi = exp(float(self.ln_s[-1]))
        self.kappa_hi = -float(self.spline(self.t_hi, 1)) / self.x_hi
        # x_p: the spline's maximum, a root of its derivative beside the largest node
        i = int(np.argmax(self.ln_s))
        roots = self.spline.derivative().roots(extrapolate=False)
        roots = roots[
            (roots > self.t[max(i - 1, 0)])
            & (roots < self.t[min(i + 1, len(self.t) - 1)])
        ]
        t_p = float(roots[np.argmax(self.spline(roots))])
        self.x_p = exp(t_p)
        self.s_p = float(self.s(self.x_p))
        self.x_end = self.x_hi + GL_TAIL / self.kappa_hi
        # C_1 = 49 x_* s_dir(x_*), the associated piece's coefficient at scale 1 (R10)
        self.C1 = ASSOC_COEFFICIENT * X_STAR * float(self.s(X_STAR))
        # the breaks of the shape: the table's nodes and x_p
        self.breaks = np.unique(np.concatenate([np.exp(self.t), [self.x_p]]))

    def s(self, x):
        """s_dir(x), x > 0 (array or scalar)."""
        x = np.asarray(x, dtype=float)
        scalar = x.ndim == 0
        x = np.atleast_1d(x)
        out = np.empty_like(x)
        lo = x <= self.x_lo
        hi = x >= self.x_hi
        mid = ~(lo | hi)
        out[lo] = x[lo] * x[lo]
        out[hi] = self.s_hi * np.exp(-self.kappa_hi * (x[hi] - self.x_hi))
        if mid.any():
            t = np.log(x[mid])
            i = np.clip(
                np.searchsorted(self.t, t, side="right") - 1, 0, len(self.t) - 2
            )
            d = t - self.t[i]
            c = self._c
            out[mid] = np.exp(((c[0, i] * d + c[1, i]) * d + c[2, i]) * d + c[3, i])
        return out[0] if scalar else out

    def s_scalar(self, x: float) -> float:
        """s_dir(x) for one float, by the same expressions (for the adaptive references)."""
        if x <= self.x_lo:
            return x * x
        if x >= self.x_hi:
            return self.s_hi * exp(-self.kappa_hi * (x - self.x_hi))
        t = log(x)
        i = min(max(bisect_right(self._t_list, t) - 1, 0), len(self._t_list) - 2)
        d = t - self._t_list[i]
        c = self._c_list
        return exp(((c[0][i] * d + c[1][i]) * d + c[2][i]) * d + c[3][i])


_DIRECT = {wall: _Direct(table) for wall, table in _TABLES.items()}


def _direct(wall) -> _Direct:
    return _DIRECT[_check_wall(wall)]


def _check_scale(assoc_scale: float) -> float:
    c = float(assoc_scale)
    if not isfinite(c) or c < 0.0:
        raise ValueError(
            f"quanta_spectrum: the associated piece's scale must be finite and non-negative, not {assoc_scale!r}"
        )
    return c


def assoc_coefficient(wall: str, assoc_scale: float) -> float:
    """C = c_a C_1, C_1 = 49 x_* s_dir(x_*), the associated piece's coefficient on the wall at scale c_a (R10)."""
    return _check_scale(assoc_scale) * _direct(wall).C1


# ---------------------------------------------------------------------------------------------------------------------
# The shape
# ---------------------------------------------------------------------------------------------------------------------


def x_peak(wall: str) -> float:
    """x_p, the table's peak (the maximum of its spline): the associated piece's end, ln s(x_p)'s argument."""
    return _direct(wall).x_p


def s_dir(wall: str, x):
    """The direct piece of the wall: x^2 below x_lo, the table's spline, the matched tail above x_hi."""
    return _direct(wall).s(x)


def s_assoc(wall: str, x):
    """The associated piece at scale 1 (R10): C_1/x for x <= x_p, (C_1/x) s_dir(x)/s_dir(x_p) above."""
    d = _direct(wall)
    C1 = assoc_coefficient(wall, 1.0)
    x = np.asarray(x, dtype=float)
    return C1 / x * np.where(x <= d.x_p, 1.0, d.s(np.maximum(x, d.x_p)) / d.s_p)


def shape(wall: str, x, assoc_scale: float = DEFAULT_ASSOC_SCALE):
    """s(x) = s_dir(x) + c_a s_assoc(x) on the wall (a float for a float x, by the scalar expressions)."""
    c = _check_scale(assoc_scale)
    if np.ndim(x) == 0:
        d = _direct(wall)
        x = float(x)
        sd = d.s_scalar(x)
        return sd + c * d.C1 / x * (1.0 if x <= d.x_p else sd / d.s_p)
    return s_dir(wall, x) + c * s_assoc(wall, x)


# ---------------------------------------------------------------------------------------------------------------------
# Composite Gauss-Legendre (the moments and the tabulation)
# ---------------------------------------------------------------------------------------------------------------------


def _gl(
    edges: np.ndarray, gx: np.ndarray, gw: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    lo, hi = edges[:-1], edges[1:]
    half = 0.5 * (hi - lo)
    mid = 0.5 * (hi + lo)
    nodes = (mid[:, None] + half[:, None] * gx[None, :]).ravel()
    weights = (half[:, None] * gw[None, :]).ravel()
    return nodes, weights


def _nodes(d: _Direct, breaks: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Nodes x and weights w (in x) for int_0^{x_end} f dx: below x_lo in panels of ln x; above it on each piece between
    consecutive breaks (which must include x_lo and lie in [x_lo, x_end)), divided geometrically into panels at most
    GL_DT wide in ln x and GL_DX in x, with GL_POINTS in ln x each.
    """
    n_log = int(np.ceil(GL_LOG_DEPTH / GL_LOG_PANEL))
    t_below = np.linspace(d.t_lo - GL_LOG_DEPTH, d.t_lo, n_log + 1)
    tb, wb = _gl(t_below, _GL_LOG_X, _GL_LOG_W)
    xs, ws = [np.exp(tb)], [wb * np.exp(tb)]
    b = np.unique(np.concatenate([breaks, [d.x_end]]))
    b = b[(b >= d.x_lo) & (b <= d.x_end)]
    edges = [log(float(b[0]))]
    for a, c in zip(b[:-1], b[1:]):
        dt = log(c / a)
        n = max(1, int(np.ceil(dt / GL_DT - 1e-9)), int(np.ceil(c * dt / GL_DX - 1e-9)))
        edges.extend(list(np.linspace(log(a), log(c), n + 1)[1:]))
    t, w = _gl(np.array(edges), _GL_X, _GL_W)
    xs.append(np.exp(t))
    ws.append(w * np.exp(t))
    return np.concatenate(xs), np.concatenate(ws)


# ---------------------------------------------------------------------------------------------------------------------
# The spectrum object
# ---------------------------------------------------------------------------------------------------------------------


class QuantaSpectrum:
    """
    One wall's spectrum at one scale c_a of the associated piece: the shape, its moments I and J, the kernels G and
    Omega, the version string, the wall and the scale. The tables are built on first use of a kernel. Use
    spectrum(wall, c_a), which caches one object per (wall, scale).
    """

    def __init__(self, wall: str, assoc_scale: float = DEFAULT_ASSOC_SCALE):
        self.wall: str = _check_wall(wall)
        self.model: str = SPECTRUM_MODELS[wall]
        self.assoc_scale: float = _check_scale(assoc_scale)
        self._d: _Direct = _DIRECT[wall]
        d = self._d
        self.x_lo: float = d.x_lo
        self.x_hi: float = d.x_hi
        self.x_p: float = d.x_p
        self.kappa_hi: float = d.kappa_hi
        self.C: float = assoc_coefficient(wall, self.assoc_scale)
        x, w = _nodes(d, d.breaks)
        self._x, self._w = x, w
        sx = self.s(x)
        self._M = {k: float(np.dot(w, x**k * sx)) for k in range(1, 11)}
        self.I: float = self._M[3]
        self.J: float = self._M[2] / self._M[3]
        self.log_I: float = log(self.I)
        self.log_J: float = log(self.J)
        # ln s(x_p), the occupation at the direct peak per unit A_g
        self.log_s_peak: float = log(d.s_p + self.C / d.x_p)
        # the small-y series' y^4 coefficient B_4 (times I)
        sd = d.s(x)
        int_sdir_over_x = float(np.dot(w, sd / x))
        above = x > d.x_p
        int_r_over_x2 = float(np.dot(w[above], sd[above] / x[above] ** 2)) / d.s_p
        self._B4: float = (
            -int_sdir_over_x / 8.0
            + self.C / (8.0 * d.x_p)
            - self.C * int_r_over_x2 / 8.0
        )
        self._G_spline = None
        self._Omega_spline = None

    def __repr__(self) -> str:
        return f"QuantaSpectrum(wall={self.wall!r}, model={self.model!r}, assoc_scale={self.assoc_scale!r})"

    # ---- the shape ----

    def s(self, x):
        """s(x) at this scale."""
        d = self._d
        x = np.asarray(x, dtype=float)
        sd = d.s(x)
        r = np.where(x <= d.x_p, 1.0, sd / d.s_p)
        return sd + self.C / x * r

    def s_scalar(self, x: float) -> float:
        """s(x) for one float (the adaptive references)."""
        d = self._d
        sd = d.s_scalar(x)
        return sd + self.C / x * (1.0 if x <= d.x_p else sd / d.s_p)

    def moment(self, k: int) -> float:
        """M_k (k = 1 ... 10, by the composite rule)."""
        return self._M[k]

    # ---- G ----

    def G_node(self, log_y: np.ndarray) -> np.ndarray:
        """G at ln y by the composite rule (no table, no asymptote)."""
        x, w = self._x, self._w
        f = x * x * self.s(x)
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
        """Omega at u >= 0 by the composite rule, on pieces between the shape's breaks and those breaks times e^u."""
        d = self._d
        out = np.empty(len(u_values))
        for i, u in enumerate(np.asarray(u_values, dtype=float)):
            if u < 0.0:
                raise ValueError("Omega_node: u >= 0")
            eu = exp(u)
            x, w = _nodes(d, np.concatenate([d.breaks, d.breaks * eu]))
            out[i] = np.dot(w, x**3 * self.s(x) * self.s(x / eu))
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

    # ---- the direct-quadrature references (slow; tests and the measurement scripts only) ----

    def _breaks(self, extra=()):
        """The pieces' breaks for the references: 0, the shape's breaks, the tail's scales and its end, and extra."""
        d = self._d
        b = [0.0] + list(d.breaks) + list(extra)
        for k in (1.0, 4.0, 10.0, 25.0, 60.0):
            b.append(d.x_hi + k / d.kappa_hi)
        b.append(d.x_end)
        return sorted(set(v for v in b if 0.0 <= v <= d.x_end))

    def moment_quad(self, k: int) -> Tuple[float, float]:
        """(M_k, stated relative error) by adaptive quadrature on the pieces between the breaks."""
        return _quad_pieces(lambda x: x**k * self.s_scalar(x), self._breaks())

    def I_quad(self) -> Tuple[float, float]:
        return self.moment_quad(3)

    def J_quad(self) -> Tuple[float, float]:
        m2, e2 = self.moment_quad(2)
        m3, e3 = self.moment_quad(3)
        return m2 / m3, e2 + e3

    def G_quad(self, y: float) -> Tuple[float, float]:
        """(G(y), stated relative error) by adaptive quadrature of x^2 s(x) y^2/(sqrt(x^2 + y^2) + x) on the pieces
        between the breaks and y/10, y, 10 y, over the full spectrum (no table, no asymptote).
        """
        y = float(y)
        f = lambda x: x * x * self.s_scalar(x) * y * y / (sqrt(x * x + y * y) + x)
        extra = [b for b in (y / 10.0, y / 2.0, y, 2.0 * y, 10.0 * y)]
        value, err = _quad_pieces(f, self._breaks(extra))
        return value / self.I, err

    def Omega_quad(self, u: float) -> Tuple[float, float]:
        """(Omega(u), stated relative error) by adaptive quadrature of x^3 s(x) s(x e^{-u}) on the pieces between the
        breaks of both factors and their tails' scales, any real u (no table, no symmetry, no asymptote).
        """
        u = float(u)
        d = self._d
        eu = exp(u)
        f = lambda x: x**3 * self.s_scalar(x) * self.s_scalar(x / eu)
        extra = list(d.breaks * eu)
        for k in (1.0, 4.0, 16.0, 64.0):
            extra.append((d.x_hi + k / d.kappa_hi) * eu)
        # past x_end the first factor is below e^{-GL_TAIL} of its value at x_hi, and the second is at most its
        # small-z form there (u > 0) or falls faster still (u < 0)
        value, err = _quad_pieces(f, self._breaks(extra))
        return value / self.I, err


def _quad_pieces(f, breaks) -> Tuple[float, float]:
    """
    sum of quad(f) over the consecutive intervals of breaks, with the summed error estimate relative to the sum. A first
    pass at 1e-8 gives the sum's scale; the second asks each piece for 1e-13 relative or 1e-16 of that scale absolute,
    so that a piece that carries nothing (a far tail) is not asked for digits it does not have.
    """
    pieces = [(a, b) for a, b in zip(breaks[:-1], breaks[1:]) if b > a]
    scale = sum(abs(quad(f, a, b, epsrel=1e-8, limit=200)[0]) for a, b in pieces)
    total, err = 0.0, 0.0
    for a, b in pieces:
        v, e = quad(f, a, b, epsabs=1e-16 * scale, epsrel=1e-13, limit=400)
        total += v
        err += e
    return total, (err / abs(total) if total != 0.0 else float("inf"))


@lru_cache(maxsize=None)
def _spectrum(wall: str, assoc_scale: float) -> QuantaSpectrum:
    return QuantaSpectrum(wall, assoc_scale)


def spectrum(wall: str, assoc_scale: float = DEFAULT_ASSOC_SCALE) -> QuantaSpectrum:
    """
    The wall's spectrum at scale c_a of the associated piece, one cached object per (wall, scale) (its tables are built
    once). The wall is required (P19): WALL_EXPONENTIAL, WALL_POWER_N1 or WALL_POWER_N2; any other is a ValueError naming
    them.
    """
    return _spectrum(_check_wall(wall), _check_scale(assoc_scale))


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
