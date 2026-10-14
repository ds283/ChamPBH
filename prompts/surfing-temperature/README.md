# Campaign — surfing-temperature

**Written:** 2026-10-14 on `main` at `f41b104`, by Claude Opus 5.5, at the user's request of the
same day: compute the Jordan-frame surfing temperature as a function of β. Two parts: the hot and
cold roots of the reachability condition, and the temperatures a surfing trajectory actually
reaches, measured on integrated histories and plotted against β. **Ruled by the user on
2026-10-14: U1–U4 (§6.1), and P1–P10 accepted as proposed (§6.2).** Prompts 01–05 written the same
day, on branch `surfing-temperature` after the planning commit `8adb806` (§2.1). **U5** (the μ/π
shoulder is a fourth kick; §0.3, §6.1) was ruled the same day, before 01 was dispatched.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- the paper draft, `Paper1.tex` (in the paper repository): §`sec:surfing-solutions`, which gives
  the reachability condition `eq:surfing-equation`, the capture oscillator `eq:capture-oscillator`
  and the threshold table;
- the integrator audit, `.documents/integrator-audit-2026-09-30/README.md` §8 F1, which measured
  T_J rising 12–13 % during the surfing overshoot;
- `prompts/jordan-normalization/source/brief_jordan_normalization.md` (untracked), for what that
  campaign will and will not change here (§0.4).

## 0. Why this campaign exists

### 0.1 What is asked

For each β from about 1.1 to 5:
1. **The two roots of the reachability condition** Σ(T)/(1 + Σ(T)/2) = 1/(3β²), on each kick the β
   can reach. The hot root is met while Σ is still rising as the plasma cools, and it is the
   attractor. The cold root is met on the falling flank, and it is a saddle (`eq:capture-oscillator`:
   stable if and only if d ln Σ/d ln T_J < 0).
2. **The temperatures a surfing trajectory reaches**, from an integrated history at φ* = 5 M_P and
   M = 10⁻³ M_P:
   - the capture temperature;
   - the largest temperature reached during the oscillation;
   - the smallest temperature reached during the oscillation;
   - the delivery temperature.
3. **One figure** of all of these against β.

### 0.2 Capture, and the turning points of T_J

The temperature law (`ComputeTargets/ScalarModel.py:441`) is

    d ln T_J / dN = −(1 + β π/M_P) / (1 + (d g_s/d ln T) / (3 g_s)),

and its denominator is positive. **So T_J has a turning point exactly where β π/M_P = −1.** For the
measurement this means:
- **Capture is the first T_J minimum.** Capture is the field's first arrival at the kinematic
  velocity, the first downward crossing of β π/M_P through −1. That crossing is the paper's
  undershoot.
- **Every later turning point is another crossing**, alternately a maximum (upward) and a minimum
  (downward).
- **The largest temperature is always a crossing.** At the first bounce π = 0, so T_J is falling
  there, and the largest T_J before delivery is at an upward crossing.
- **The oscillation decays**, as e^(−N/2). So the first dip is the deepest, and "capture" and
  "smallest" defined over the whole oscillation would be the same number. The user ruled (U1) that
  "smallest" is the *second* dip.
- **With φ* = 5, QCD captures reach the wall within about one period** (the paper, after
  `eq:capture-oscillator`). So the second dip is often absent, and a blank there is a result, not
  a failure.

### 0.3 What exists at `f41b104` (read, nothing run)

- **`kick_threshold_curve`** (`extract_common.py`) gives β_th(T) = √((2 + Σ)/(6Σ)) from
  `cosmology.w(T)`, the same w the integrator reads. `plot_by_beta.py` overlays it on `T_deliver.pdf`
  over 0.05–50 GeV, with both flanks drawn. It is the inverse of what is wanted, and it does not
  separate the roots or the kicks.
- **The EOS** (`CosmologyModels/GenericEOS/Xav_EOS_spline.py`): a cubic spline in ln T of 189 rows,
  from 10 keV to 25.1 TeV, with w = 1/3 outside. The spline's Σ peaks are:

  | Kick | Peak Σ | At |
  |---|---|---|
  | electroweak | 0.037436 | 53.25 GeV |
  | QCD | 0.31453 | 0.1819 GeV |
  | μ/π shoulder | 0.085365 | 43.16 MeV |
  | e⁺e⁻ | 0.10073 | 0.1605 MeV |

  The paper's thresholds come from three of these: 3.01, 1.107 and 1.86. T_init = 2×10⁴ GeV is above
  the electroweak kick and inside the table.

  *Added 2026-10-14, before 01 was dispatched (U5).* The orchestrator's probe of the spline found
  the **μ/π shoulder**, a fourth local maximum above 01's floor of 1e-4. It is in the table's rows
  (Σ = 0.0852 at 44.7 MeV, with a trough of 0.0746 at 71 MeV), not only in the spline, and it lies
  where muons and pions annihilate. Its threshold is β = 2.018, which is not in the paper's table.
  The trough between it and QCD (Σ = 0.074496 at 72.99 MeV) is crossed at β = 2.154. So:
  - for 2.018 < β < 2.154 the shoulder is a separate reachable interval at about 35–60 MeV;
  - above β = 2.154 it merges with QCD, and the QCD cold root falls from about 86 MeV to about
    31 MeV.

  The other troughs do not merge in the campaign's range: EW–QCD (Σ = 0.0047411 at 9.026 GeV)
  merges at β = 8.39, and shoulder–e⁺e⁻ (Σ = 0.00025952 at 7.544 MeV) at β = 35.8. The five
  other local maxima are ripples of 2e-9 to 2e-5 between 10 and 28 keV, below the floor.
- **`first_bounce`** (`ScalarModel.py:788`) walks the accepted steps' interpolants and finds the first
  turning point of φ by `brentq`, or the first elastic reflection. Its `log_T_Jordan` is the existing
  T_deliver, which the user ruled (U2) is the delivery temperature.
- **`compute_scalar_model` does not return the dense output.** `tools/history_and_bbn.py:300-326`
  captures the `IntegrationResult` by wrapping `integrate_scalar_history` for the duration of the
  call. The new driver can do the same, so no production code path changes.
- **`tools/history_and_bbn.py` has main.py's initial data:** π* = 0, T* = 2×10⁴ GeV, Λ = 1 meV,
  n = 1, `QCD_Cosmology` on `Planck2018`, the default tolerances and step control. It also has the
  check that the run is from the repository root.
- **The suites at `f41b104`** (`CLAUDE.md`): CosmologyModels 18, ComputeTargets 152 (61 + 91),
  ObjectFactories 69, tools 34.

### 0.4 Independence from jordan-normalization

The science rebuild is held until `jordan-normalization` lands. This campaign builds no store, so
it is not held. What that campaign would change here:
- **The roots: nothing.** Σ is a function of T_J alone, and the brief (§2.2) records that T_J/MeV is
  unchanged by the normalization. Its R4 resets the Planck-mass constant, which relabels MeV by at
  most 2.8×10⁻⁴.
- **The capture: nothing.** The surfing system (φ′, T_J) depends only on d ln A/dφ = β/M_P, and
  φ_ref does not enter it (brief §2.1).
- **The largest temperature, the second dip and delivery: possibly something.** These are set by
  when the field reaches the wall, and that depends on Λ, which the normalization makes an output
  (2.13 meV against 1 meV in the brief's test at β = 1.5). A re-run after that campaign is one
  command (§7).

## 1. Scope

**In scope:**
- a function for the reachability roots, per kick, and its tests (01);
- a function for the turning points of T_J on the dense output, and a store-free, Ray-free driver
  that integrates one history per β and writes one CSV row each, with tests and a handful of β run
  to measure the cost (02);
- a plot script that reads the CSV (03);
- the full scan and the figure, run by a prompt under the user's exception U4 (04);
- the documents and the close (05).

**Out of scope:**
- **Storing any of this.** No `ScalarModel` column, no `VERSION_LABEL` bump and no change to
  `compute_scalar_model`'s payload (§5 rule 9). Per-history surfing columns would ride on the
  science rebuild after `jordan-normalization`, and are for a later campaign if the user wants
  them.
- **Other φ* and M.** The driver takes both as options, but the campaign runs φ* = 5 M_P and
  M = 10⁻³ M_P only (the user's ruling).
- **Later surfing episodes.** After the first bounce the measurement stops (U3).
- **`plot_by_beta.py`'s threshold overlay**, which stays as it is.
- **DatastoreKit, RayPoolKit and the sibling repositories:** not touched.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-reachability-roots.md` | `reachability_roots(cosmology, beta)` in `extract_common.py` (P1): the kick windows located on the spline, the hot and cold roots of each kick by `brentq`, with the stability sign. Tests on a stub cosmology with analytic Σ, and on the production EOS against the peak values of §0.3 | Sonnet |
| 02 | `02-surfing-turning-points-and-driver.md` | `surfing_turning_points(result, coupling)` beside `first_bounce` in `ScalarModel.py`, not called by `compute_scalar_model`. A new driver `tools/surfing_scan.py` (P2) whose CSV row per β is defined in §4. Tests on stub interpolants and on one short real history. Five β run (P6), with the wall-clock time per history | Opus |
| 03 | `03-plot-surfing-temperature.md` | `tools/plot_surfing_temperature.py`, reading the CSV and writing the figure of §4. The roots on a fine β grid, Δβ = 0.005 (P4). Tests on a synthetic CSV | Sonnet |
| 04 | `04-run-the-scan.md` | **The scan (U4).** 79 histories, β = 1.10–5.00 in steps of 0.05, resumable, in chunks under the Bash cap (P7). The CSV and figure in `.documents/surfing-temperature/out/` (P8). `.documents/surfing-temperature-verification.md` created | Opus |
| 05 | `05-close.md` | `architecture-summary.md` (the new functions and tools); the handover, including the re-run after `jordan-normalization`; the board closed; `prompts/INDEX.md` | Sonnet |

### 2.1 When each prompt file is written

01–05 are written once the user has ruled on §6.2. 01 and 02 are independent of each other. 03
needs 01, and the CSV header from 02. 04 needs 01–03. *Written 2026-10-14, after the user's
rulings.* Orchestrator notes are written one prompt at a time, in `orchestrator/`, just before each
dispatch.

### 2.2 The charters

**01 — Reachability roots** (Sonnet).
- **What it adds.**
  1. **`reachability_roots(cosmology, beta)`** returns one record per kick with fields:
     - `kick` (`"EW"`, `"QCD"`, `"mu-pi"` or `"ee"`; U5);
     - `T_hot`, `T_cold` (dimensionful), or None where the kick's peak Σ_eff < 1/(3β²);
     - `Sigma_peak`, `T_peak`;
     - `slope_hot`, `slope_cold`, the values of d ln Σ/d ln T at each root.
  2. **The kick windows** are bounded by the minima of Σ between the peaks, and found on the same
     spline the integrator reads (P5). They are computed once per cosmology and returned, so that
     02 can label a capture by the same windows.
  3. **The roots** are the sign changes of Σ_eff − 1/(3β²) over the whole range, refined by
     `brentq` and paired, hot then cold, into reachable intervals. Each root is assigned to the kick
     window it lies in. An interval that spans a trough between two kicks is reported as merged,
     with no root invented at the window edge. A window with more than one interval, from a
     wiggle of the spline, is reported, not hidden: the outermost pair is returned, with a flag.
     (Prompt 01 §1.1 R2; written 2026-10-14.)
- **Acceptance.**
  1. **A stub cosmology** with Σ a known function of ln T (two Gaussian kicks): roots equal to the
     analytic ones to 1e-10 relative; None below threshold; slope signs negative at the hot root and
     positive at the cold one.
  2. **The production EOS:**
     - the peaks equal §0.3's to the digits given;
     - the thresholds √((2 + Σ_peak)/(6 Σ_peak)) agree with the paper's table to its digits (the
       shoulder's, which the paper does not give, with §0.3's 2.018; U5);
     - at a β just above each threshold the two roots straddle `T_peak`.
  3. **Consistency with `kick_threshold_curve`**: at each root, β_th(T) = β to 1e-9.
  4. Deliberate breakages: the two flanks swapped; Σ used in place of Σ_eff; a window edge moved
     onto the peak.

**02 — Surfing turning points and the driver** (Opus).
- **What it adds.**
  1. **`surfing_turning_points(result, coupling)`.** It walks the accepted steps' interpolants, up to
     and including the first bounce as `first_bounce` defines it, and finds every root of
     1 + (d ln A/dφ) π by `brentq` (xtol as `first_bounce`'s). For each root it returns N, ln T_J, φ
     and the direction of the crossing. It is general in the coupling (d ln A/dφ, not β), and it is
     not called by `compute_scalar_model`.
  2. **`tools/surfing_scan.py`**, built from `tools/history_and_bbn.py`'s setup:
     - it takes `--beta-min`, `--beta-max`, `--beta-step` (or `--beta-values`), `--phi-init-Mp`
       (default 5), `--M` (default 1e-3), `--T-stop-GeV` (default 1e-3, P3) and `--out`;
     - it captures the `IntegrationResult` as `history_and_bbn.py` does;
     - it skips BBN;
     - it appends one CSV row per β as each history ends (§4), flushed, and skips a β already in the
       file, so a run can be resumed or split;
     - it refuses a run not made from the repository root, and starts no Ray.
  3. **From the turning points, per β:**
     - **capture** is the first downward crossing;
     - **largest** is the largest T_J at an upward crossing before the first bounce;
     - **second dip** is the first downward crossing after the first upward one;
     - **delivery** is `first_bounce`'s T_J;
     - each is blank where it does not exist, and the CSV row says why: not captured, or no second
       dip before the wall.
     - The capture is labelled with its kick by 01's windows (U3). Until 01 lands, 02's tests use
       their own stub windows.
- **Acceptance.**
  1. **Stub interpolants** with π chosen so that the crossings are known: every crossing found, in
     order, with the right direction, and none after the first bounce. Also:
     - a reflection before the first turning point ends the walk there, as in `first_bounce`;
     - a history that never crosses gives "not captured".
  2. **One real history** (a QCD capture at β = 2, T_stop a few MeV): at every crossing,
     |1 + β π/M_P| < 1e-8. The ln T_J on the interpolant at N ± 10⁻³ lies on the side the crossing's
     direction says. Delivery equals `first_bounce`'s T_J exactly. (A history without PRyMordial;
     its time is given in the test's docstring.)
  3. **The driver on five β** (P6), each history's wall-clock time recorded in the log, together
     with the projected scan time. Every first bounce lies above T_stop, and that is a stop if not.
  4. The suites at §0.3's counts plus the new tests; `black` clean.
  5. Deliberate breakages: the direction of a crossing inverted; the walk not stopped at the first
     bounce; the "second dip" taken as the first.

**03 — The plot** (Sonnet). The figure is §4's. A test on a synthetic CSV checks:
- the script runs with no Ray and no store;
- it draws every row;
- blanks are left as gaps, not drawn as zeros;
- the fine root grid is drawn from 01's function.

**04 — Run the scan** (Opus; U4). `tools/surfing_scan.py` over β = 1.10, 1.15, …, 5.00 (79 values),
run in chunks with a resumable CSV (P7). Then:
- `tools/plot_surfing_temperature.py`;
- the CSV and the figure committed under `.documents/surfing-temperature/out/` (P8);
- `.documents/surfing-temperature-verification.md` created, recording:
  - the per-history wall-clock times;
  - any failure, with its reason;
  - the dynamical thresholds (the smallest β captured by each kick), against 01's analytic ones;
  - the measured ratios T/T_hot, as measurements, not tested against any bound (§5 rule 10).

**05 — Close** (Sonnet). `architecture-summary.md`; the handover (§7) as a dated addendum to
`.documents/review-remediation-verification.md`; the board closed; `prompts/INDEX.md`. Final suites.

## 3. Stores

None. No prompt opens a datastore, and no test opens one outside a `tempfile` directory. The
science store, the pilot store in the repository root and the rehearsal stores are not opened.

## 4. The interfaces between prompts

- **01 ships** `reachability_roots` and the kick windows. 02 labels captures with them, and 03 draws
  the root curves from them.
- **02 ships** the CSV, one row per β, with these columns (P9):
  - **inputs:** `beta`, `phi_init_Mp`, `M_Mp`, `Lambda_eV`, `T_stop_GeV`;
  - **status:** `status` (`ok` or `failure`), `failure_reason`, `captured`, `capture_kick`;
  - **measured temperatures, in GeV:** `T_capture_GeV`, `N_capture`, `T_max_GeV`, `N_max`,
    `T_second_dip_GeV`, `N_second_dip`, `T_deliver_GeV`, `N_deliver`, `deliver_reflected`;
  - **counts:** `n_crossings` (before the first bounce), `reflections`, `accepted_steps`,
    `wall_s`;
  - **the roots for the capture kick:** `T_hot_GeV`, `T_cold_GeV`.

  The figure reads only this file.
- **03 ships the figure**, two panels sharing the β axis:
  - **Upper panel**, T_J in GeV on a log axis:
    - for each kick, the hot root (solid) and the cold root (dashed), with the band between them
      lightly shaded (the reachable window);
    - the e⁺e⁻ roots drawn and labelled "not surfed" when no history is captured there;
    - the measured capture, largest, second-dip and delivery temperatures as markers, one style
      each;
    - an uncaptured β drawn as a gap.
  - **Lower panel:** each measured temperature divided by the hot root of its capture kick, on a
    linear axis with a line at 1. This is where the undershoot, the overshoot and the cooling at
    delivery can be read.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", and `dk-and-rpk-migration`'s rule 8: each
prompt names the deliberate breakages its tests must catch, and the log records each as a diff that
can be replayed with `git apply`; they are never committed. And:

9. **No stored science value changes.** `compute_scalar_model`, its payload, `VERSION_LABEL` and the
   store schema are untouched. 02 adds a function that production does not call.
10. **Measurements, not bounds.** The scan's temperatures, ratios and dynamical thresholds are
    recorded. They are never tested against expected values from the paper or the audit, and a
    difference from either is reported, not opened as an issue unless it is a defect of the code.
11. **No Ray.** No prompt starts a cluster and no driver here calls `ray.init`. The histories run
    through `compute_scalar_model._function`, as `tools/history_and_bbn.py` does.
12. **The scan (04) is a one-off exception** to `CLAUDE.md`'s "nothing a prompt runs should take
    longer than a few PRyMordial solves", ruled by the user (U4). It runs no PRyMordial. If 02's
    projection exceeds the budget P7 sets, 04 stops before it starts.
13. **Existing tests are not modified.**

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 Ruled by the user (U1–U4), 2026-10-14

| | Ruling |
|---|---|
| **U1** — the measured set | **Capture** is the first downward crossing of β π/M_P = −1, which is the first T_J minimum. **Largest** is the largest T_J between capture and delivery. **Smallest** is the minimum after the first maximum, blank if the wall arrives first. The CSV records how many crossings there were |
| **U2** — delivery | **T_J at the first bounce**: `first_bounce`'s dense-output turning point of φ, or the reflection; the existing T_deliver |
| **U3** — which episode | **The first surfing episode, whichever kick captures it**, labelled EW or QCD. For β ≳ 3 that is expected to be the electroweak kick |
| **U4** — process | **A campaign, in which a prompt runs the full scan** (04), as an exception to `CLAUDE.md`'s run-length rule |
| **U5** — the μ/π shoulder (ruled 2026-10-14, before 01 was dispatched; §0.3) | **A fourth kick**, labelled `"mu-pi"` (shown as "μ/π"), between QCD and e⁺e⁻. The production EOS has four kicks, and anything else is 01's stop. A merged interval is reported by the existing machinery. Each kick's record carries the interval's own hot and cold roots, and `merged` names the other kick. So for a QCD capture above β = 2.154, 02's `T_cold_GeV` is the merged interval's cold edge |

### 6.2 Proposed by the planner (P1–P10)

**Accepted as proposed by the user, 2026-10-14.**

- **P1 — where the code goes.**
  - `reachability_roots` goes in `extract_common.py`, beside `kick_threshold_curve`, which shares
    its formula.
  - `surfing_turning_points` goes in `ComputeTargets/ScalarModel.py`, beside `first_bounce`, so
    that a later campaign could store its output without moving it.
  - The tests go in `ComputeTargets/tests/` for both functions, and in `tools/tests/` for the
    driver and the plot script.
- **P2 — the driver** is a new `tools/surfing_scan.py`, not an extension of `history_and_bbn.py`,
  whose output lines other prompts' logs quote.
- **P3 — T_stop = 1 MeV** by default. It is below every QCD delivery, above the e⁺e⁻ kick, and it
  avoids the late history, which is the expensive part. 02 checks that every first bounce lies above
  it.
- **P4 — the β grids.** The measured β grid is the user's: 1.10–5.00, Δβ = 0.05, 79 values. The roots,
  which cost nothing, are drawn on Δβ = 0.005 so that the curves and the thresholds are sharp. 1.10
  is below the QCD threshold (1.107), so the first point is expected to show no root and no capture.
- **P5 — the kick windows** are bounded by the minima of Σ between the peaks, and above and below by
  the table's ends.
- **P6 — 02's five β:** 1.15, 1.5, 2.5, 3.5 and 5.0. These cover the QCD capture near threshold,
  mid-range QCD, the region below the electroweak threshold and the electroweak capture at the top
  of the range.
- **P7 — the scan's budget.** If 02's projection exceeds 60 minutes, 04 stops and asks. Otherwise
  it runs in chunks of β, each under the Bash cap, into one resumable CSV.
- **P8 — outputs committed.** The CSV and the figure (PDF, and a PNG for viewing) go in
  `.documents/surfing-temperature/out/`. Both are small.
- **P9 — the CSV columns** are §4's, in GeV, with the roots repeated for the capture kick so that
  the file can be read alone.
- **P10 — the branch and models.** The branch is `surfing-temperature`, cut from `main` at
  `f41b104` with the planning commit, and merged into `main` at the close. The models are §2's:
  Sonnet for 01, 03 and 05, Opus for 02 and 04.

## 7. What this campaign hands to the user

- The figure and the CSV, at φ* = 5 M_P and M = 10⁻³ M_P.
- The one command that re-runs the scan at another φ* or M, or after `jordan-normalization` lands
  (§0.4: the roots and the capture will not move; the largest temperature, the second dip and
  delivery may).
- If the user wants per-history surfing values stored, that is a later campaign that adds
  `ScalarModel` columns on the rebuild.
