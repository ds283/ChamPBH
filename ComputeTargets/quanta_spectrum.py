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
    c_a = 1 as U4 put it, and x_* = 0.0058 on every wall (X_STAR; on the power wall carried over from the exponential's
    2PI run and unmeasured there, U1-U7 as ruled). C = c_a C_1.
x_p is the maximum of P (a root of its derivative). s is continuous everywhere; s_assoc is continuous at x_p and, x_p
being P's maximum, so is its slope.

x = k/k_ex, k_ex the production's (lambda_t sqrt(E)/2 at phi_t, exact at the reference): on the exponential at L = 90,
k_ex = 0.494535 omega_t (U4-U17-v1 took omega_t/2, so its x_p = 0.62 was k_peak/(omega_t/2); the table's x_p is
k_peak/(0.494535 omega_t)); on the power wall k_ex = (s_t/2) sqrt(n/(n + 1)) omega_t, with s_t = 1 at the reference.
On the power wall the bounce in k/omega_t does not depend on E/V_0 and only k_ex moves, by s_t = (E/V_0)/(1 + E/V_0):
a generation near the floor has its spectrum at x/s_t in the table's x. Measured, not acted on (R9; spectrum_table.py's
`shape`).

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
     (1/16) ln(x_lo/y) + (1/16) int_{x_lo}^inf s_dir/x^3 dx + C/(48 x_p^3) + ..., against M_1 y^2/2: at G_Y_MIN = 1e-3,
     y^4 = 1e-12 times a factor below 30 on every wall (int s_dir/x^3 <~ ln(x_p/x_lo) + 1 since s_dir <= x^2, and
     M_1 >~ 0.06), below 3e-11 relative. At c_a = 0, C = 0: the y^3 term vanishes and B_4 is the direct piece's alone.
  G, y -> inf (y > G_Y_MAX):  G I = M_2 y - M_3 + M_4/(2y) - M_6/(8y^3) + M_8/(16 y^5) + R, from
        sqrt(x^2 + y^2) - x = y - x + x^2/2y - x^4/8y^3 + x^6/16y^5 - 5x^8/128y^7 + ... for x < y; M_3 = I, so the -1 is
     exact. |R| <= 5 M_10/(128 y^7) plus the region x > y, where s is the tail, of order e^{-kappa_hi y}; at G_Y_MAX = 50
     and kappa_hi >= 2.15 both are below 1e-11 relative on every wall (7.4e-12 on the power wall at n = 1; the tests
     measure them). The form is the same at
     c_a = 0, with the moments of s_dir.
  Omega, u -> +inf (u > OMEGA_U_MAX): for x e^{-u} <= x_lo (< x_p), s(x e^{-u}) = x^2 e^{-2u} + C e^u / x exactly, so
        Omega I = e^{-2u} M_5 + C e^u M_2 - R,
     with 0 <= R <= int_{x_lo e^u}^inf x^3 s(x) (x^2 e^{-2u} + C e^u / x) dx, since s(z) <= z^2 + C/z for z >= x_lo (s_dir
     <= z^2 there on every wall, and s_dir(z)/s_dir(x_p) <= 1 above x_p; the tests check both). The bound falls with
     u as the weight of x^5 s and x^2 s above x_lo e^u; OMEGA_U_MAX = 9 puts x_lo e^u >= 16.4 on every wall, where
     they are below 1.2e-15 of M_5 and 1e-18 of M_2 (the tests measure them; at u = 8 they are up to 3.6e-4). At c_a = 0 the form is e^{-2u} M_5 / I.
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
# shape with U17's end of the associated piece, the one shape of one-loop-drain prompt 05
SPECTRUM_MODELS = {
    WALL_EXPONENTIAL: "R8-exponential-v1",
    WALL_POWER_N1: "R8-power-n1-v1",
    WALL_POWER_N2: "R8-power-n2-v1",
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
# smallest probe, q = 0.0029 omega_t, is 0.012: fifty times the test field's |beta_q|^2". With k_ex = omega_t/2,
# x_* = 0.0058 (U4: "the toy's 50x at q = 0.0029 omega_t"). Its 1/x form is the report's n_q^inh = c_N R omega_t/q. Since
# power-law-wall prompt 03 (R10, U9 as proposed) the coefficient is C_1 = 49 x_* s_dir(x_*), so that
# s_dir(x_*) + s_assoc(x_*) = 50 s_dir(x_*) on the measured shape, and x_* = 0.0058 in x on every wall: carried over from
# the exponential's 2PI run, and unmeasured on the power wall (U1-U7 as ruled)
X_STAR = 0.0058
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

G_Y_MIN = 1.0e-3
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
# (np.linspace), measured by prompts/power-law-wall/source/wall/spectrum_table.py `table` at power-law-wall prompt 03's
# commit (.documents/power-law-wall/law-03/spectrum_tables.txt):
#   - the exponential: WallN(90, 1) in walls.py's units (M = 1, omega_t = 1), k_ex/omega_t = 0.4945353550468402;
#   - the power wall: PowerWall(1e40, n) in units of phi_t (s_t = 1 to double precision), k_ex/omega_t =
#     0.3535533905932738 (n = 1) and 0.408248290463863 (n = 2).
# X_LO is 1e-3 omega_t over k_ex (R8). X_HI is the smallest of 3, 3.5, 4, ... at which the matched tail carries at most
# 1e-3 of x^3 s_dir (orchestrator note 03, ruling 5): 2.2e-4, 7.4e-4 and 4.5e-4 of it. The not-a-knot spline through
# the nodes is within 2.8e-6 relative of direct evaluations at every midpoint, and monotone on either side of the peak
# ---------------------------------------------------------------------------------------------------------------------

WallTable = namedtuple(
    "WallTable", ["wall", "reference", "k_ex_over_omega_t", "x_lo", "x_hi", "ln_beta2"]
)

_LN_BETA2_EXPONENTIAL = (
    -19.28526035673611,
    -19.18539439341281,
    -19.085528220138183,
    -18.985662392779343,
    -18.8857969648226,
    -18.78593132795566,
    -18.68606616655616,
    -18.58620116640382,
    -18.486336524941766,
    -18.38647197014926,
    -18.28660774931186,
    -18.18674387640216,
    -18.086880425149484,
    -17.987017236422915,
    -17.88715459410782,
    -17.7872924602838,
    -17.687430776250146,
    -17.58756976323203,
    -17.487709359699156,
    -17.387849726166113,
    -17.287990855261942,
    -17.188132884834495,
    -17.088275920264273,
    -16.988420016092146,
    -16.888565310194355,
    -16.788711916521173,
    -16.688860003542928,
    -16.58900967888211,
    -16.48916116271966,
    -16.389314607031082,
    -16.289470215574635,
    -16.18962824158034,
    -16.089788932593585,
    -15.989952557483907,
    -15.890119407160386,
    -15.790289860454124,
    -15.690464276457588,
    -15.590643057381836,
    -15.490826688571055,
    -15.391015658795387,
    -15.291210540178152,
    -15.191411948945255,
    -15.091620568853344,
    -14.991837161088144,
    -14.892062554107703,
    -14.792297685269247,
    -14.692543570665688,
    -14.592801326217407,
    -14.493072215202105,
    -14.393357611420209,
    -14.293659038540532,
    -14.193978172881806,
    -14.094316878224173,
    -13.994677205304166,
    -13.895061426550086,
    -13.795472043676378,
    -13.695911829815943,
    -13.596383835871375,
    -13.496891450370699,
    -13.397438403263367,
    -13.298028818798729,
    -13.198667250088379,
    -13.099358732334807,
    -13.000108821104806,
    -12.900923654213116,
    -12.801810011960438,
    -12.702775381045711,
    -12.603828028509678,
    -12.504977083362459,
    -12.406232625483767,
    -12.307605783007688,
    -12.209108839800733,
    -12.110755356990092,
    -12.012560300024415,
    -11.914540184509443,
    -11.81671323392041,
    -11.719099552617832,
    -11.621721316945159,
    -11.524602983191222,
    -11.427771517191106,
    -11.331256645537787,
    -11.235091128807012,
    -11.139311062708398,
    -11.043956204735428,
    -10.949070331642337,
    -10.854701627917695,
    -10.760903108701829,
    -10.667733078319824,
    -10.575255627279278,
    -10.483541169375226,
    -10.392667020603493,
    -10.302718022391579,
    -10.213787209613924,
    -10.125976524669904,
    -10.039397578545103,
    -9.954172457889348,
    -9.870434577534018,
    -9.78832957685482,
    -9.708016256284072,
    -9.629667550272588,
    -9.553471530554205,
    -9.479632432416146,
    -9.408371695157749,
    -9.339929005177606,
    -9.274563329179975,
    -9.212553922405007,
    -9.154201295257819,
    -9.099828119619774,
    -9.04978005532585,
    -9.004426475924571,
    -8.964161072438895,
    -8.929402314581603,
    -8.900593749617263,
    -8.878204121744249,
    -8.862727297736507,
    -8.854681989481463,
    -8.854611269213652,
    -8.86308188022613,
    -8.88068335334841,
    -8.908026947671855,
    -8.945744442815382,
    -8.994486818820276,
    -9.054922867946523,
    -9.12773779034957,
    -9.21363183169531,
    -9.313319025444894,
    -9.42752610471726,
    -9.55699164884958,
    -9.702465527175836,
    -9.86470869723906,
    -10.044493407431938,
    -10.242603843835324,
    -10.45983725002404,
    -10.697005535253997,
    -10.954937374156536,
    -11.234480786892188,
    -11.536506177868512,
    -11.861909799321436,
    -12.211617598661878,
    -12.586589402013635,
    -12.987823383440755,
    -13.416360768793503,
    -13.873290725556881,
    -14.35975539462065,
    -14.876955025476846,
    -15.42615318511195,
    -16.008682017201973,
    -16.625947539037924,
    -17.279434968271083,
    -17.970714082702425,
    -18.701444620075737,
    -19.473381729524473,
    -20.28838149674855,
)

_LN_BETA2_POWER_N1 = (
    -8.840911813017335,
    -8.757848053108798,
    -8.674931880633698,
    -8.592165953606255,
    -8.509553362062924,
    -8.427096696502689,
    -8.34479921707468,
    -8.262663963878664,
    -8.180694022712851,
    -8.098892781766684,
    -8.01726355349398,
    -7.935809777323725,
    -7.854535105742728,
    -7.773443144555725,
    -7.692537833347605,
    -7.611822972267145,
    -7.531302699755927,
    -7.450981124261046,
    -7.370862612641503,
    -7.290951565170425,
    -7.211252676088392,
    -7.131770560750433,
    -7.052510220007519,
    -6.97347666973261,
    -6.894675170521918,
    -6.8161111090211755,
    -6.73779010421976,
    -6.659717915181158,
    -6.581900551129599,
    -6.504344177583741,
    -6.427055212934569,
    -6.350040292406136,
    -6.27330625533105,
    -6.196860232077273,
    -6.120709560868181,
    -6.044861876843101,
    -5.969325080385223,
    -5.894107340624968,
    -5.819217155159301,
    -5.744663317921702,
    -5.670454935765899,
    -5.596601496671506,
    -5.5231127828788,
    -5.449998990937236,
    -5.377270695143269,
    -5.304938844048222,
    -5.233014845711199,
    -5.161510517332881,
    -5.090438136044818,
    -5.01981046686714,
    -4.949640766836442,
    -4.879942812092909,
    -4.810730925010908,
    -4.7420199796335165,
    -4.673825477447557,
    -4.606163502666018,
    -4.539050801779782,
    -4.472504802888883,
    -4.4065436352355265,
    -4.341186165242361,
    -4.276452033776219,
    -4.212361690391868,
    -4.148936431282551,
    -4.086198429656728,
    -4.0241707881614435,
    -3.9628775828478666,
    -3.902343892230612,
    -3.84259585519362,
    -3.783660721289286,
    -3.7255669007353918,
    -3.6683440182302234,
    -3.612022970043404,
    -3.5566359843091644,
    -3.502216684091807,
    -3.4488001568026,
    -3.3964230136510483,
    -3.345123475033306,
    -3.294941441579757,
    -3.245918574602573,
    -3.198098381444419,
    -3.151526304707361,
    -3.1062498160821272,
    -3.0623185152462686,
    -3.019784233663487,
    -2.978701142103447,
    -2.939125865466641,
    -2.9011176061326727,
    -2.8647382672762887,
    -2.8300525875996456,
    -2.797128281469158,
    -2.766036186755829,
    -2.736850420996175,
    -2.7096485438341786,
    -2.6845117270522696,
    -2.6615249391593907,
    -2.6407771360414367,
    -2.622361456887155,
    -2.606375438815813,
    -2.5929212377971287,
    -2.5821058635756384,
    -2.5740414264262275,
    -2.568845391993392,
    -2.5666408573879824,
    -2.567556839057346,
    -2.5717285713400617,
    -2.5792978278485448,
    -2.590413254454086,
    -2.6052307203050886,
    -2.6239136942732824,
    -2.646633630454948,
    -2.673570376746395,
    -2.704912617322322,
    -2.740858315849166,
    -2.7816152048697216,
    -2.827401284075867,
    -2.8784453527571285,
    -2.9349875680577093,
    -2.997280038232761,
    -3.065587427302925,
    -3.1401876247816096,
    -3.2213724128207675,
    -3.3094482019715374,
    -3.4047367771148087,
    -3.5075761021263454,
    -3.618321157793326,
    -3.7373448235812927,
    -3.86503880637042,
    -4.001814618080527,
    -4.148104600228245,
    -4.304363012796113,
    -4.471067154707524,
    -4.648718575043709,
    -4.83784432286,
    -5.038998268137149,
    -5.252762492077867,
    -5.479748748194887,
    -5.720600002684019,
    -5.97599204296177,
    -6.246635183204697,
    -6.5332760378537635,
    -6.836699420404539,
    -7.157730297412481,
    -7.497235868901491,
    -7.856127745280674,
    -8.235364259726953,
    -8.635952855594345,
    -9.058952626898252,
    -9.50547697869704,
    -9.976696443603567,
    -10.473841615435953,
    -10.998206214314168,
    -11.551150318015322,
    -12.134103985685305,
    -12.748570497645764,
    -13.396130491422886,
    -14.078445529771196,
)

_LN_BETA2_POWER_N2 = (
    -11.827477141149659,
    -11.728604795739544,
    -11.629767433352972,
    -11.53096672542716,
    -11.432204860156943,
    -11.33348357973801,
    -11.234805115799315,
    -11.136171493208257,
    -11.037585069250904,
    -10.939048043069441,
    -10.840563486607774,
    -10.742132434869559,
    -10.643758416461505,
    -10.545444322915559,
    -10.447193246204556,
    -10.349008341044645,
    -10.250892802896384,
    -10.152850010236907,
    -10.054883470593316,
    -9.956996871001012,
    -9.859194188916574,
    -9.76147956201064,
    -9.663857375197713,
    -9.566331833740504,
    -9.468908376163245,
    -9.371591623958599,
    -9.274386677909032,
    -9.177298900870943,
    -9.080334005343255,
    -8.983498031226551,
    -8.886797306015355,
    -8.790238403794294,
    -8.69382814405178,
    -8.597573637536875,
    -8.501482380413933,
    -8.40556224945142,
    -8.309821510772531,
    -8.214268750074082,
    -8.118912842010472,
    -8.023763402078377,
    -7.928830232749812,
    -7.834123543253778,
    -7.7396540531795575,
    -7.645433053084833,
    -7.551472366649237,
    -7.45778429400395,
    -7.364381641857369,
    -7.271277825744424,
    -7.178486910123102,
    -7.08602357987015,
    -6.99390318040837,
    -6.9021415522735925,
    -6.810755571037852,
    -6.719762718530621,
    -6.629181164402112,
    -6.5390299047345986,
    -6.4493288570924205,
    -6.360098810437245,
    -6.271361394792275,
    -6.183139195982178,
    -6.095455842655731,
    -6.008335963129651,
    -5.9218052294848444,
    -5.835890469248846,
    -5.750619635870525,
    -5.666021957549841,
    -5.5821278610589635,
    -5.4989690508024704,
    -5.416578689586191,
    -5.334991341614226,
    -5.254242993160632,
    -5.17437123316837,
    -5.095415258162042,
    -5.017415901830681,
    -4.940415790250936,
    -4.864459343980938,
    -4.789592889257066,
    -4.715864742175422,
    -4.643325259758694,
    -4.572026957528584,
    -4.502024642992466,
    -4.433375383066614,
    -4.366138755239391,
    -4.300376865123997,
    -4.236154468957132,
    -4.173539133181537,
    -4.112601295742496,
    -4.053414457743881,
    -3.9960552669945746,
    -3.940603701828914,
    -3.8871431945495796,
    -3.835760808912504,
    -3.7865473715866163,
    -3.7395977277014656,
    -3.6950108049080455,
    -3.652889933342455,
    -3.6133429436749656,
    -3.5764824678115037,
    -3.542426099769347,
    -3.511296680384345,
    -3.483222527497027,
    -3.4583377014684844,
    -3.4367822967798753,
    -3.418702728178056,
    -3.4042520422294738,
    -3.3935902509964317,
    -3.386884660034367,
    -3.3843102737356836,
    -3.386050127337569,
    -3.392295723849511,
    -3.4032474642157173,
    -3.4191150783368163,
    -3.440118109799211,
    -3.466486419032743,
    -3.498460707358477,
    -3.5362930721981556,
    -3.5802475935216753,
    -3.630600951439557,
    -3.687643078245956,
    -3.7516778378841904,
    -3.8230237518424257,
    -3.9020147673910572,
    -3.989001046728231,
    -4.084349813220328,
    -4.188446241871897,
    -4.301694394595703,
    -4.424518204541349,
    -4.557362512470859,
    -4.700694157161863,
    -4.855003122563934,
    -5.020803748078792,
    -5.198636002456188,
    -5.389066814948947,
    -5.592691480690431,
    -5.810135152042355,
    -6.042054387720133,
    -6.289138793668298,
    -6.552112740986417,
    -6.831737165914633,
    -7.128811495486076,
    -7.444175654427274,
    -7.778712115962172,
    -8.13334819710665,
    -8.50905830036285,
    -8.906866416620547,
    -9.327848653329914,
    -9.773136072994376,
    -10.243917155796888,
    -10.741441315815667,
    -11.267021635669197,
    -11.822038292861716,
    -12.407941941407444,
    -13.026257695962844,
    -13.67858840795567,
    -14.366619019429988,
    -15.092120771667975,
)

_TABLES = {
    WALL_EXPONENTIAL: WallTable(
        wall=WALL_EXPONENTIAL,
        reference="WallN(90, 1): V_0 e^{M/phi} at L = ln(1 + E/V_0) = 90 (P21)",
        k_ex_over_omega_t=0.4945353550468402,
        x_lo=0.0020221001184137478,
        x_hi=4.0,
        ln_beta2=_LN_BETA2_EXPONENTIAL,
    ),
    WALL_POWER_N1: WallTable(
        wall=WALL_POWER_N1,
        reference="PowerWall(1e40, 1): V_0 [1 + M/phi] at E/V_0 = 1e40, the scale-free reference (R9)",
        k_ex_over_omega_t=0.3535533905932738,
        x_lo=0.0028284271247461914,
        x_hi=6.5,
        ln_beta2=_LN_BETA2_POWER_N1,
    ),
    WALL_POWER_N2: WallTable(
        wall=WALL_POWER_N2,
        reference="PowerWall(1e40, 2): V_0 [1 + (M/phi)^2] at E/V_0 = 1e40, the scale-free reference (R9)",
        k_ex_over_omega_t=0.408248290463863,
        x_lo=0.0024494897427831783,
        x_hi=5.5,
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
