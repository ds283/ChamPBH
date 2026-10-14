# Prompt 02 — surfing turning points and the driver

**Campaign:** [`README.md`](README.md) · **Board items:** **T1**–**T5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, after the user's rulings (README §6: U1–U4; P1–P10 as proposed). Measured
at `8adb806`; your own measurement governs. Its orchestrator note, written after 01's review, says
whether 01 has landed. It governs where it differs from this file.
**Gate:**
- on branch `surfing-temperature`, no tracked change in `git status` (the untracked set is 01's
  gate list, and none is yours);
- 01 is not required. If it has landed, use its `find_kicks` and `reachability_roots` (T3). If it has
  not, see T3's fallback.
- You start no Ray, and open no store.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. A walk over the dense output that must be exact, a driver that
must resume cleanly, and the cost measurement 04 depends on.

**Read first:**

1. [`README.md`](README.md): §0.2 (why capture is the first T_J minimum), §0.3, §2.2 (02's charter),
   §4 (the CSV columns), §5, §6.1 (U1–U3), §6.2 (P1–P3, P6, P7, P9).
2. `CLAUDE.md`: "Repository mechanics", in particular the test rules, run from the root, and
   "Long-running jobs".
3. `ComputeTargets/ScalarModel.py`:
   - the temperature law at `:441-443`;
   - `IntegrationResult` (`:131-152`) and `Reflection` (`:120-129`);
   - `first_bounce` and `FirstBounce` (`:777-871`): the walk you will mirror;
   - `T_Jordan_crossing` (`:873-922`);
   - `compute_scalar_model`'s payload (`:1191-1229`).
4. `tools/history_and_bbn.py`, in full. Its setup (`:80-148`, `:238-330`) is main.py's initial data,
   and `:300-326` capture the `IntegrationResult` by wrapping `integrate_scalar_history`.
5. `ComputeTargets/tests/test_first_bounce.py` and the helpers in `test_kinematic_cap_loop.py`
   (`build`, `integrate`, the probe states P1–P3 at `:44-79`): how the existing tests drive the
   integrator without Ray.
6. If 01 has landed: its log's "State handed to the next prompt", and `find_kicks` and
   `reachability_roots` in `extract_common.py`.

---

## 1. What is wanted

For each β, one integrated history and one CSV row holding the four temperatures of U1–U2, with
enough around them to read the row alone.

### 1.1 The items

- **T1 — `surfing_turning_points(result, coupling)`** in `ComputeTargets/ScalarModel.py`, after
  `first_bounce` (P1).
  - **What it finds.** Let g(N) = 1 + (d ln A/dφ)(φ(N)) · π(N), read on the interpolants. For the
    exponential coupling this is 1 + β π/M_P. g is the numerator of the temperature law (with its
    sign), so T_J is stationary exactly where g = 0 (README §0.2).
  - **The walk.** It walks the accepted steps in order, as `first_bounce` does, and stops at the
    first bounce: `first_bounce(result).N`, or the history's end if there is none.
  - **Within each step.** Look for sign changes of g at the step's ends **and at interior sample
    points**. A step is at most 0.1 e-fold under the global cap, so two crossings inside one step are
    not expected, but the interior samples make that a measurement rather than an assumption. Record
    in the log how many interior points you chose and why.
  - **The roots.** Refine each by `brentq` with `first_bounce`'s xtol (1e-15).
  - **What it returns.** A list of `SurfingCrossing(N, phi_Einstein, pi_Einstein, log_T_Jordan,
    direction)`:
    - `direction` is `"min"` where g goes from positive to negative (β π/M_P falls through −1,
      and T_J has a minimum);
    - `direction` is `"max"` where g goes the other way.
  - **Edge cases:**
    - a crossing exactly at a step boundary is counted once;
    - a reflection is a step boundary, as in `first_bounce`, and the walk never crosses one, because
      the first reflection is at or after the first bounce.
  - It is not called by `compute_scalar_model`, and nothing in the payload, the store or
    `VERSION_LABEL` changes (README §5 rule 9).
- **T2 — `tools/surfing_scan.py`** (P2), a new script in the form of `tools/history_and_bbn.py`.
  - **The setup.** It shares that script's setup: `QCD_Cosmology` on `Planck2018`, Λ = 1 meV, n = 1,
    π* = 0, T* = 2×10⁴ GeV, the default tolerances and step control, and the same z grid. Import the
    constants from `tools/history_and_bbn.py`, or restate them with a comment naming it. Say which in
    the log. Do not change `history_and_bbn.py`.
  - **The options:**
    - `--beta-min`, `--beta-max`, `--beta-step`, or `--beta-values`. The grid is built by integer
      index, β = β_min + k Δβ rounded to 10 decimal places, so that 1.10 + 0.05k does not drift.
    - `--phi-init-Mp` (default 5) and `--M` (in M_P, default 1e-3).
    - `--T-stop-GeV` (default 1e-3, P3).
    - `--out` (the CSV path; required).
  - **The run.** It refuses a run not made from the repository root (as `history_and_bbn.py`
    does), and starts no Ray. It does no BBN. Each history runs through
    `compute_scalar_model._function`, with the `IntegrationResult` captured as
    `history_and_bbn.py:300-326` do.
  - **Writing the CSV.** One row per β is appended as each history ends, and flushed. The columns
    are README §4's, in that order.
  - **The sidecar.** A JSON file `<out>.meta.json` holds the git `HEAD` and whether any tracked file
    differs from it (`git status --porcelain --untracked-files=no`; the user's untracked files are
    not a dirty tree), `VERSION_LABEL`, the options, the start time and the Python and SciPy versions. It is
    written at the start, and added to on resume.
  - **Resuming.** If `--out` exists, the script reads it. It refuses (exit 2) if the header differs
    or if any row's φ*, M, Λ or T_stop differs from the options. It skips every β already in the
    file (matched to 1e-9). A failed history is a row (`status=failure`, the payload's reason), so
    it is not retried on resume.
  - **The console.** One line per β, giving β, the status, the four temperatures in GeV, the
    crossing count and the wall time. `compute_scalar_model`'s own printing may be left as it is or
    sent to a log file; say which.
- **T3 — the summary per history**, a pure function in `tools/surfing_scan.py` (for example
  `summarise(crossings, bounce, kicks, roots, units)`) that turns T1's list into the row:
  - **capture**: the first `"min"` crossing;
  - **largest**: the largest T_J among the `"max"` crossings;
  - **second dip**: the first `"min"` crossing after the first `"max"`;
  - **delivery**: `first_bounce`'s T_J, with `deliver_reflected`;
  - **`captured`**: true if there is at least one crossing;
  - **`capture_kick`**: the label of 01's window that contains T_capture;
  - **`T_hot_GeV`, `T_cold_GeV`**: that kick's roots at this β;
  - **`n_crossings`**: the number of crossings before the first bounce.
  - **Blanks.** A field that does not exist is blank, and `failure_reason` stays empty for a
    successful history. Whether the blank means "not captured" or "no second dip before the wall"
    can be read from `captured` and `n_crossings`. A captured history with no bounce above T_stop
    has a blank delivery and `status=ok`; the console line flags it.
  - **Fallback.** If 01 has not landed, `capture_kick`, `T_hot_GeV` and `T_cold_GeV` are blank, and
    the function takes `kicks=None`. Say so in the log. 03 and 04 run after 01, so 04's rows have
    them.
- **T4 — the tests.**
  1. **`ComputeTargets/tests/test_surfing_turning_points.py`**, on **stub interpolants**: an object
     with `ts` and `interpolants`, as `first_bounce` reads them, and a stub coupling with constant
     `d_logOmega_dphi`. π is chosen so that g has known zeros: a damped oscillation of β π/M_P about
     −1, then a rise to π > 0 at a known turning point of φ.
     - every crossing is found, in order, with its direction, to 1e-12 in N;
     - none is reported after the first bounce;
     - two zeros of g placed inside one step are both found (this is what the interior samples are
       for);
     - a history whose g never changes sign returns an empty list;
     - a reflection before any crossing ends the walk there, with an empty list.
  2. **`tools/tests/test_surfing_scan.py`** (import as `import tools.surfing_scan as ss`, as
     `test_compare_stores` imports its tool):
     - `summarise` on hand-built crossing lists: none (not captured); min only; min-max with the
       wall before a second dip; min-max-min-max (second dip present, largest the larger max);
     - the β grid: 1.10 to 5.00 by 0.05 gives 79 values with no drift;
     - **resume, in a `tempfile` directory**:
       - a CSV with two rows is read back, and its β are skipped;
       - a CSV whose φ* differs is refused, exit 2;
       - the tool is run through `main(argv)` on a monkeypatched history function, so that no
         integration runs;
     - **one real history**: β = 2, φ* = 5, M = 1e-3, from T*, to a T_stop just below its first
       bounce, through the tool's own history function. The test checks that:
       - there is at least one crossing;
       - at every crossing |1 + β π/M_P| < 1e-8 on the interpolant;
       - ln T_J at N ± 10⁻³ is above the crossing's value for a `"min"` and below it for a
         `"max"`;
       - delivery equals `first_bounce(result).log_T_Jordan` exactly.

       The docstring gives the wall time. No PRyMordial runs. If the history takes more than about
       a minute, end it at a higher T_stop that still lies below the first bounce, and say so.
- **T5 — the cost, five β** (P6): 1.15, 1.5, 2.5, 3.5 and 5.0.
  - **The run.** Run the tool once, into a CSV in your scratch directory (not committed), from the
    repository root, in the foreground, with the load beside it.
  - **Per β, record in the log:**
    - the wall time and the accepted steps;
    - the four temperatures and the crossing count;
    - the capture kick;
    - whether the first bounce lies above T_stop.
  - **The projection.** Interpolate the wall time over β ∈ [1.10, 5.00] and sum over the 79-point
    grid; P7's budget is 60 minutes. Also project the time of the longest single history, which sets
    how 04 chunks the scan under the ten-minute Bash cap.

---

## 2. Verification

1. **Before any edit:** the four roots at the counts on the board (18 / 152 or more / 69 / 34),
   serially, in the foreground, with the load.
2. **After:**
   - the four roots, with the new tests in their roots: ComputeTargets in its two halves (the new
     module falls in `test_[g-z]*.py`), and tools;
   - `black --check` on every touched Python file. `ScalarModel.py` is black-clean at `8adb806`.
3. **`compute_scalar_model` is unchanged**: `git diff` of `ScalarModel.py` adds T1's function (and
   its namedtuple) and nothing else. Show it in the log.
4. **T5's table** in the log.
5. **Deliberate breakages** (each a diff, never committed):
   - the direction of a crossing inverted;
   - the walk not stopped at the first bounce;
   - the interior samples removed;
   - `summarise` taking the first `"min"` as the second dip;
   - resume ignoring a φ* mismatch.

   Each must fail at least one new test, named in the log.

## 3. Acceptance

1. T1–T5 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. Suites: CosmologyModels 18, ComputeTargets as left by 01 + the new tests, ObjectFactories 69,
   tools 34 + the new tests.
4. **One commit**, with the log `logs/02-surfing-turning-points-and-driver.md` in the template of
   `dk-and-rpk-migration` README §5.1. It updates the board: row 02, T1–T5, the gate "Turning
   points", and a new `### 5.3 After prompt 02 (<date>)` with T5's projection. Write "this commit"
   where the SHA goes. T5's scratch CSV is not committed.

## 4. Stop conditions: stop and ask

- **T1 cannot be written without changing `compute_scalar_model`** or the payload.
- **The interior samples find two crossings inside one accepted step** on a real history (T4 or
  T5). It means the step cap is coarser than the oscillation. Report it; do not tighten the step
  control.
- **A history in T5 fails**, or has a captured first episode with no bounce above T_stop. P3's
  T_stop would then be wrong for 04.
- **T5 projects the scan above P7's 60 minutes.**
- An existing test fails, or anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `ComputeTargets/ScalarModel.py` (T1's function and namedtuple only);
  - the new `tools/surfing_scan.py`;
  - the two new test modules;
  - this board, and the log.
- It does not change `compute_scalar_model`, `first_bounce`, `tools/history_and_bbn.py`,
  `extract_common.py` or any stored value.
- It runs at most the five histories of T5 and the one of T4. It runs no PRyMordial, starts no Ray,
  and opens no store.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
