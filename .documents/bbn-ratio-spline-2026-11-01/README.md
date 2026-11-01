# BBN's ratio spline at the ratio's jumps — measured 2026-11-01

A measurement made at the user's request on 2026-11-01, outside any campaign, on branch
`inelastic-reflection` at `4fefdf8` (whose code is `inelastic-reflection` prompt 10's, `8c12afa`, with
two comments corrected). It answers two open issues, which the user then closed as measured
("please record the measurements and close the issues as measured. i won't plan a fix at the moment"):

- `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]` (`science-readiness` board §3; opened by its
  prompt 05, 2026-10-02). `inelastic-reflection` prompt 11's verification document §3 and that campaign's
  board name it as the `jordan-normalization` board's; it is `science-readiness`'s.
- `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]` (`inelastic-reflection` board
  §3; opened by its prompt 11, 2026-11-01).

Both ask the same thing. BBN reads the ratio r = ρ_NP/ρ_R,J through a cubic spline in ln T_J over the
stored samples (`build_rho_NP_callback`, `ComputeTargets/BBNData.py:94`; the spline at `:170`). Where r
jumps between two samples, does the spline ring, and does that, or anything else about the jump, move Yp and
D/H?

## 1. Method

[`probe_ringing.py`](probe_ringing.py), run from the repository root once per history. For each history:

1. **The history.** `tune_history` on `main.py`'s axis, 250 samples per decade of 1 + z to 300 decades,
   with `main.py`'s initial data (T_init = 2×10⁴ GeV, φ* = 5 M_P, π = 0) and the cosmology `QCD_Cosmology`
   with Planck2018. That is what a build stores.
2. **The same history, sampled finer.** `integrate_history` once more at the tuned normalization and
   φ_ref, on the production axis plus a fine axis (FINE points per decade, 25 000 unless stated) inside
   BBN's window, 0.2 keV ≤ T_J ≤ 100 MeV, padded by two production steps. `integrate_history` integrates
   first and then reads the dense output at each point of the axis (`ComputeTargets/ScalarHistory.py`,
   the loop after `truncate_to_history`), so the axis does not change the integration. The probe checks
   this: in every run the production points of the second integration equal the tuned run's samples
   bitwise (5003 to 5437 of them per run; none different).
3. **r at each sample**, as `compute_BBN_data` forms it: ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m) from the
   point H_J, and r = ρ_NP/ρ_R,J.
4. **The interpolants' error.** Three interpolants of the production samples in ln(T_J/MeV): the cubic
   spline the code uses (`make_interp_spline`, k = 3), linear, and PCHIP (a monotone cubic, which cannot
   overshoot). Each is evaluated at the fine samples and compared with r there. A **jump** is a step in r
   larger than 10⁻³ between two consecutive fine samples; a production interval holding one is "in jump",
   and the error is also reported one, two and three or more intervals away. (At FINE = 2500 the 10⁻³
   threshold also catches steep drift, so that run counts more jumps.)
5. **The yields.** PRyMordial, small network, through `_run_PRyMordial` (`ComputeTargets/BBNData.py`, the
   code's route), on five callbacks: the production cubic (`build_rho_NP_callback` itself, which is
   what a build runs), the production linear and PCHIP, and the fine cubic and fine linear. Each callback
   returns r(T) ρ_SM(T) with `thermodynamic_rho_SM`, as the code's does.

The histories: dissipation mode at M = 4×10⁻³¹ M_P (β = 1.2 at α = 0.1 and 0.03, β = 2 at α = 0.03, the
three cases of the second issue whose rms_step was above natural mode's), natural mode at M = 10⁻⁷ M_P
(β = 1.2 and 2, prompt 11's comparison), and natural mode at β = 1.6, M = 10⁻⁵ M_P, the first issue's own
case. β = 1.2, α = 0.1 was also run at FINE = 2500 and 100 000, to see the fine result converge.

The outputs are under [`out/`](out/), one file per run, with `uptime` at its start (one-minute load
6.6–28.3, from the machine's own daemons). The committed copy of the probe was formatted with `black` and
its usage line corrected after the runs; nothing else in it changed.

## 2. What the jumps are

At each resolved reflection π flips sign, and H_J, which carries the factor (1 + (ln A)′π), jumps. Between
two production samples (0.0092 e-folds apart) r moves by about 1: for example from −0.47 to +0.63 as T_J
falls through 30.7 MeV (natural, β = 1.2, M = 10⁻⁷), so that H_J² goes from about 0.5 to 1.6 times the
Standard Model's. The fine samples show each as one step: at 25 000 per decade one fine step in the
interval exceeds 10⁻³ (two, in two intervals of natural β 1.6), and r on the fine samples there goes at
most 0.02 beyond the two production values.

| History | jumps in the window | 10–100 keV | 0.1–1 MeV | 10–100 MeV | largest production step |
|---|---|---|---|---|---|
| dissipation β 1.2, α 0.1 | 6 | 2 | 3 | 1 | 1.06 at 36.7 MeV |
| dissipation β 1.2, α 0.03 | 4 | 1 | 2 | 1 | 1.09 at 32.3 MeV |
| dissipation β 2, α 0.03 | 18 | 4 | 10 | 4 | 1.28 at 79.8 MeV |
| natural β 1.2, M 10⁻⁷ | 3 | 0 | 2 | 1 | 1.10 at 30.7 MeV |
| natural β 2, M 10⁻⁷ | 29 | 5 | 16 | 3 | 1.32 at 71.5 MeV |
| natural β 1.6, M 10⁻⁵ | 40 | 8 | 28 | 2 | 1.36 at 78.2 MeV |

(Natural β 2 also has two jumps at 0.3–1 keV, one at 1–3 keV and two outside the probe's windows; natural
β 1.6 one at 1–3 keV and one at 1–10 MeV.) Both modes have them. The dissipation histories' extra rms_step in 10–100 keV that prompt 11
measured comes from the reflections they still make there; a drain itself is a much smaller step.

## 3. Ringing: real in r, negligible in the yields

The largest |interpolant − fine r| (FINE = 25 000), by distance from a jump interval:

| History | cubic: next / 2 away / ≥ 3 away | linear: next to a jump | PCHIP: next to a jump |
|---|---|---|---|
| dissipation β 1.2, α 0.1 | 0.159 / 0.042 / 0.011 | 1.3×10⁻⁵ | 1.3×10⁻³ |
| dissipation β 1.2, α 0.03 | 0.168 / 0.045 / 0.012 | 1.5×10⁻⁵ | 1.2×10⁻³ |
| dissipation β 2, α 0.03 | 0.238 / 0.063 / 0.017 | 3.6×10⁻⁵ | 3.6×10⁻³ |
| natural β 1.2, M 10⁻⁷ | 0.218 / 0.058 / 0.016 | 1.6×10⁻⁵ | 1.2×10⁻³ |
| natural β 2, M 10⁻⁷ | 0.241 / 0.064 / 0.017 | 3.6×10⁻⁵ | 3.5×10⁻³ |
| natural β 1.6, M 10⁻⁵ | 0.275 / 0.073 / 0.019 | 2.3×10⁻⁵ | 2.1×10⁻³ |

The cubic spline overshoots next to every jump by 15–20 % of the jump, and the overshoot falls by about 3.7
per interval, as a cubic interpolating spline's does across a step. In a window with no jump the cubic
error is at most 4×10⁻⁸ (10⁻¹⁴ on a parked history's flat ratio), except natural β 1.6, M 10⁻⁵ at
0.3–10 keV, where it reaches 1.5×10⁻³: smaller steps there fall below the probe's 10⁻³ threshold, and
they were not looked at further. Inside a jump interval every
interpolant is wrong by up to the jump itself, 0.7–1.0.

The yields do not see the ringing. On the same production samples, cubic, linear and PCHIP give:

| History | Yp: cubic / linear / PCHIP | D/H (×10⁻⁵): cubic / linear / PCHIP |
|---|---|---|
| dissipation β 1.2, α 0.1 | 0.2494882175 / 0.2494966134 / 0.2494877141 | 2.494698477 / 2.494962353 / 2.494944230 |
| dissipation β 1.2, α 0.03 | 0.2538513362 / 0.2538582974 / 0.2538530522 | 2.518832563 / 2.519092294 / 2.519032123 |
| dissipation β 2, α 0.03 | 0.2463607581 / 0.2463691913 / 0.2463623682 | 2.473403046 / 2.473325627 / 2.473390744 |
| natural β 1.2, M 10⁻⁷ | 0.2567128067 / 0.2567222027 / 0.2567207296 | 2.599078048 / 2.599015230 / 2.598984973 |
| natural β 2, M 10⁻⁷ | 0.2465028909 / 0.2465081106 / 0.2465058200 | 2.461537733 / 2.461685920 / 2.461512276 |
| natural β 1.6, M 10⁻⁵ | 0.2458467913 / 0.2458406772 / 0.2458415726 | 2.432023092 / 2.431945650 / 2.432087747 |

The three agree in Yp to at most 9.4×10⁻⁶ (3.7×10⁻⁵ relative), and in D/H to at most 1.1×10⁻⁴ relative,
which is PRyMordial's own D/H scatter at this setting (`numerical-strategies.md` §7.7.4: median 4.4×10⁻⁵,
maximum 1.51×10⁻⁴). The overshoot alternates in sign and cancels in BBN's integrals over time.

## 4. Where the jump falls: the larger effect

Inside a jump interval every interpolant draws a ramp where r has a step, so the step's position inside
the 0.0092 e-fold interval is lost. Sampling finer locates it. Production cubic (what a build stores) against
the fine linear interpolant (FINE = 25 000):

| History | Yp: production → fine | relative | D/H: production → fine | relative |
|---|---|---|---|---|
| dissipation β 1.2, α 0.1 | 0.2494882 → 0.2495459 | +2.3×10⁻⁴ | 2.494698 → 2.495027 | +1.3×10⁻⁴ |
| dissipation β 1.2, α 0.03 | 0.2538513 → 0.2539144 | +2.5×10⁻⁴ | 2.518833 → 2.518095 | −2.9×10⁻⁴ |
| dissipation β 2, α 0.03 | 0.2463608 → 0.2462419 | −4.8×10⁻⁴ | 2.473403 → 2.471864 | −6.2×10⁻⁴ |
| natural β 1.2, M 10⁻⁷ | 0.2567128 → 0.2566784 | −1.3×10⁻⁴ | 2.599078 → 2.598829 | −9.6×10⁻⁵ |
| natural β 2, M 10⁻⁷ | 0.2465029 → 0.2465551 | +2.1×10⁻⁴ | 2.461538 → 2.460941 | −2.4×10⁻⁴ |
| natural β 1.6, M 10⁻⁵ | 0.2458468 → 0.2458463 | −1.9×10⁻⁶ | 2.432023 → 2.432181 | +6.5×10⁻⁵ |

The sign varies from history to history, as the jumps' positions do. On the fine samples, cubic and linear
agree in Yp to 1.0×10⁻⁵ at most (natural β 2) and mostly to about 10⁻⁶.

**It converges with the spacing.** Dissipation β = 1.2, α = 0.1, Yp from the fine linear interpolant: 0.2495502
at 2 500 per decade, 0.2495459 at 25 000, 0.2495452 at 100 000. The production value's error against
100 000 is 5.70×10⁻⁵; at 2 500 it is 5.0×10⁻⁶ and at 25 000 7×10⁻⁷, so about tenfold per tenfold in density.
The fine D/H values scatter by about 1.5×10⁻⁴ (2.495045, 2.495027 and 2.494905 for linear), PRyMordial's
own scatter.

**Its size**, for comparison (figures from `numerical-strategies.md` §7.7.4–§7.7.5):
- up to 4.8×10⁻⁴ in Yp and 6.2×10⁻⁴ in D/H, relative, on these six histories;
- PRyMordial's own Yp scatter at the production setting is at most 4.3×10⁻⁵, so the Yp effect is up to
  about ten times it; the D/H effect is up to about four times PRyMordial's D/H scatter;
- the small network's D/H offset from the full network is −2.4 to −3.6×10⁻⁴, and the error of PRyMordial's
  default low-T tolerance in D/H was up to 1.8×10⁻³; the effect here is of the same order;
- the observational uncertainties on Yp and D/H are of order 1 %, about twenty times the largest effect here.

## 5. What this settles

- **`[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`.** Measured: the spline rings at every
  resolved jump, by 15–20 % of the jump in the next interval, and the ringing moves Yp by at most
  3.7×10⁻⁵ relative and D/H within PRyMordial's scatter (§3). The issue's impact statement, that a
  PRyMordial failure on point input could not be attributed to the true H rather than to the
  interpolation, is answered for these histories: all 40 solves of the probe succeeded, and an interpolant
  that cannot overshoot (PCHIP) gives the same yields as the cubic.
- **`[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]`.** Measured: the steps that
  move the yields are the reflections' jumps, which natural mode has as well. Locating them on a 100×
  finer axis moves Yp by up to 4.8×10⁻⁴ and D/H by up to 6.2×10⁻⁴, relative (§4). Yp at β = 1.2 is not
  monotonic in α in prompt 11's grid (0.25386, 0.24949, 0.24952, 0.25411 at α = 0.03, 0.1, 0.3, 0.9).
  These effects are 10⁻⁴ of Yp, too small to explain it, so the non-monotonicity does not come from the
  ratio's interpolation.
- **Both closed as measured** by the user on 2026-11-01. No fix is planned. If one is wanted, the
  dominant term goes away if BBN is given each reflection's and injection's position: the values just
  before and just after each one as samples. That changes stored values, and would need its own campaign.
- Nothing here was measured on the full network, at other M, or on histories that fail BBN.
