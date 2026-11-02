# Prompt 01 — measure the fixes

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-02, after the plan was committed (`a566748`) and U1–U5 ruled by the user the same
day. It is written from:
- README §0–§6 as ruled (U2: "F1 through F4"; P1, P2, P4, P9 standing);
- the diagnosis, `.documents/post-reflection-step-2026-11-02/README.md`, its three scripts and `out/`;
- `inelastic-reflection` prompt 11 §1.1 (R29, R30): the detached batch and the module attribute set at call
  time, which this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `reflection-restart`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/datastores/science-2026.8.0.db` and `science-2026.8.0-reflections.db` exist, each with its 16 shards;
- neither `var/reflection-restart/` nor `.documents/reflection-restart/` nor
  `.documents/reflection-restart-verification.md` exists.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus**. A change inside the step loop that must be bitwise at its default, four
variants of it, and an account of SciPy's error estimate.

**Read first:**

1. [`README.md`](README.md), whole: §0.1 (the defect and what is open), §0.2 (the store), §0.3 (F1–F4),
   §5 (the rules), §6.1 as ruled, §6.2 P1, P2, P4, P7, P9.
2. `.documents/post-reflection-step-2026-11-02/README.md`, whole, and its scripts `trace_step.py` and
   `scan_first_steps.py`: how a history is instrumented, how the bad step was re-integrated resolved, and
   what the scan counted. Their outputs in `out/` are the numbers the Signature gate reproduces.
3. `ComputeTargets/ScalarHistory.py`:
   - `StepControl` and its comment (:202–:235);
   - `integrate_scalar_history`, its docstring and its step loop: `new_solver` (:1635), the start (:1646),
     the release (`resume_after_release`, :2451–:2465), the floor rule and the reflection restart
     (:2496–:2552), the kinematic cap (:2553–:2567), the step and its exceptions (:2569–:2588), and the drain
     restart (:2700);
   - `observe_turning_points` (:1907) and where the gate reads the step's interpolant: the code that raises
     the three failures;
   - `PARKING_EPSILON` and the comment above it (:113–:127), on reading it at call time: the precedent for the probe
     switch (§1.2).
4. `tools/history_and_bbn.py`: `main`, its options (`--alpha`, `--fixed-normalization NORM PHI_REF_MP`,
   `--T-stop-GeV`), and how it calls `tune_history` and `integrate_history`.
5. SciPy 1.17.0's `scipy/integrate/_ivp/radau.py` (in `venv/`): `Radau.__init__` (`first_step`, the
   Jacobian at the start), `_step_impl` (the Newton solve, the error estimate and its acceptance,
   `MAX_FACTOR`, `MIN_FACTOR`), `predict_factor`.
6. `.documents/post-reflection-step-2026-11-02/failures.py`: the read-only route into a store (sqlite
   `mode=ro` on each shard) and the joins from `ScalarHistory` to M, β and α.

---

## 0. Where this prompt sits

01 measures; it fixes nothing. Its record is what the user rules the fix from (U6), beside 02's audit. It
also builds the scanner that 02 runs over the store, so 02 can start once 01 lands.

What is not known, and this prompt must find out (README §0.1):
- **whether each of F1–F4 removes the bad step**, and at what cost, at every M of the run;
- **why Radau accepts the bad step.** If its error estimate under-reads at h·ω ≫ 1, a fix that bounds only
  the first step after a restart may leave a later, grown step just as bad. That decides between F3 alone
  and F3 with a guard;
- **whether bad steps hide in successes.** The diagnosis scanned first steps only; this prompt scans every
  step that begins outward.

## 1. What is wanted

### 1.1 The scanner (P1, P2)

`tools/outward_step_scan.py`, with tests in `tools/tests/test_outward_step_scan.py`.

- **One history.** It runs a history as `tools/history_and_bbn.py` does, with no BBN: either tuned
  (`tune_history`, as `main.py`) or at a given normalization and φ_ref (`integrate_history`, as
  `--fixed-normalization`). It takes β, M, α and φ* as the tool does.
- **What it records**, for every accepted Radau step that begins with π > 0, in every Radau instance of the
  integration (or integrations, when tuned; say which integration each belongs to):
  - N at the start, h, h·ω with ω = √|J[1,0]| from the Jacobian Radau holds at the step's start;
  - π_end/π_start, whether π changes sign on the step's interpolant, and the least φ of the interpolant
    (at least 50 points, as the diagnosis), in units of M and relative to φ_start;
  - whether it is the first step of its Radau instance, and what started that instance (start, reflection,
    drain, release);
  - the step's cap (`max_step`), the solver's `h_abs` before the step, and −π′ at its start.
- **The flag (P1).** A step is flagged if it begins with π > 0 and either π_end/π_start < ½ with no sign
  change of π on its interpolant, or its interpolant falls below (1 − f)·φ_start, f = `cap_fraction`.
  h·ω is recorded, not flagged on. State the distribution of π_end/π_start and of h·ω you find, so the
  threshold can be seen not to be delicate.
- **Per history**, a summary record: the outcome (success, or the failure's reason), accepted steps, RHS
  evaluations, wall-clock, reflections, injections, parking N and T, release N and T, the dark-radiation
  values the history computes (ρ_χ/ρ_R at 1 MeV, 70 keV and today; ΔN_eff at 1 MeV and T_CMB), the count of
  outward steps, of first outward steps, and of flagged ones, the largest h·ω over all accepted steps
  (information only), and the flagged steps' rows in full.
- **How it observes.** By a patch of SciPy's `Radau.step` confined to the tool's own process, as the
  diagnosis did, or by an observer hook in `integrate_scalar_history` with a default of None that is
  bitwise. Choose, and say why. A hook is a change to production code and needs its own test that the
  default changes nothing; a patch must not leak into any other module's import.
- **From a store.** `--from-store PATH` with filters (M, β, α, block, success only) selects histories from a
  store read-only (sqlite `mode=ro` on each shard, as `failures.py`) and runs each at its stored
  normalization and φ_ref and α. For each it also compares the re-integrated samples with the stored
  `ScalarHistoryValue` rows, field by field, and records whether they are **bitwise** equal, and if not the
  largest relative difference per field and the first differing sample. This is P3's check, which 02 runs
  at scale; build it here.
- **Many histories.** A batch mode runs a list of histories (or a store selection) in a pool of worker
  processes (`--jobs N`), one JSON line per history appended to an output file under `var/`, flushed per
  history. A history whose line exists is skipped, so a killed batch resumes. A history that raises
  something other than a `ComputationFailureError` is recorded with its traceback and the batch goes on.
  It writes its pid to a file beside its output and `** scan done` as its last log line.
- **No Ray, no store written.** The tool never opens a store for writing; with `--from-store` it refuses a
  path it cannot open read-only.

**Tests** (`tools/tests/`, no Ray, no production store): the flag on synthetic step rows (each condition
alone, neither, a sign change); the per-history summary and the resume on a stub integration; the batch's
handling of a raising history; the store comparison on a temporary store built by `_TempStoreCase`
(`ObjectFactories/tests/test_version_keyed_lookups.py`) if that is practical, or otherwise on synthetic rows
with the gap stated in the log. Each new test is shown to catch a breakage of what it tests (P9 of earlier
campaigns: break it, see it fail, restore).

### 1.2 The four fixes, behind a probe switch (U2, README §5)

A module attribute of `ComputeTargets/ScalarHistory.py`, read by `integrate_scalar_history` **at call
time**, as `PARKING_EPSILON` is:

    _PROBE_OUTWARD_FIX: Optional[str] = None

with a comment saying it exists for `reflection-restart` 01's measurement only, is not an option of any
driver, and is removed by prompt 03 (README §5). `None` is today's behaviour and must be **bitwise**: both
golden histories, the tuned golden history and the golden fingerprint pass unchanged. Any other value not
listed below raises `ValueError` at the start of the integration. The values:

- **`"F1"` — the symmetric cap.** In the kinematic cap, `cap = min(cap, f·φ/π)` also when π > 0. Nothing else
  changes. It applies at every step, not only after restarts.
- **`"F2"` — the stiffness cap.** `cap = min(cap, c/ω)`, ω = √|J[1,0]| of the Jacobian the solver holds
  (`solver.J`; say how stale it can be). c is the module attribute `_PROBE_STIFFNESS_C`, default 1.0. Applies
  at every step.
- **`"F3"` — a first step at the restart.** At each of the restarts with π > 0 (after a reflection, a drain,
  a release; P7), the new Radau is built with `first_step = min(global_max_step, max(h_floor, f·φ/|π|))`,
  φ and π those of the restarted state. Nothing else changes; the start of the history (π* = 0) is
  untouched.
- **`"F4"` — verify and retake.** After an accepted step that began with π > 0, before the step is used for
  anything (dense output, turning points, the gate's samples, the drain, the step count, the budget), test it
  by the flag of §1.1. If flagged, discard it: rebuild Radau from the step's start state and N, and cap the
  retaken step at half the discarded h; repeat until a step passes, or until the cap would fall below
  h_floor, which is a `ComputationFailureError` naming this rule. Count the retakes. A discarded step must
  leave no trace but the count. On a history with no flagged step, F4 is bitwise today's: show it.
- **`"F3+F4"`** — both.

Measure F2 at c = 1 everywhere, and also at c = 0.3 and 3 on the three failures and on the six clean
histories. If the measurement shows another combination is needed to make a fix work, add it, say why,
and record it as an `IMPLEMENTATION CHOICE`.

**Tests** (`ComputeTargets/tests/`, P8 in its probe form): for each value, from the reflected state of the
diagnosis's §3 step 3 (β = 1.5, M = 10⁻¹², α = 0.05; take it from `trace_step.py`'s route or record it as
float.hex), a window of the integration under the fix has no flagged step and π keeps the sign the resolved
re-integration gives; `None` reproduces the bad step there; an unknown value raises. State each test's
runtime in its docstring.

### 1.3 The measurement

Scripts under `.documents/reflection-restart/measure/`, each with a docstring saying what it does, how it
is run and that it writes only under `var/reflection-restart/01/`. Run the batch detached (`nohup`, pid
recorded), verify once that it started, and wait for it to end (`CLAUDE.md`, "Do not babysit").

The histories, each under `None`, `F1`, `F2`, `F3`, `F4` and `F3+F4`:

1. **The three failures**, tuned: (β, M, α) = (1.5, 10⁻¹², 0.05), (1.14, 10⁻¹⁵, 0.01), (1.09, 10⁻¹⁵, 0.03).
2. **The six clean histories** of the diagnosis's scan, tuned: (1.5, 10⁻¹², 0.02), (1.5, 10⁻¹², 0.15),
   (1.09, 10⁻¹², 0.08), (1.2, 10⁻¹⁵, 0.03), (1.5, 4×10⁻³¹, 0.08), (1.2, 4×10⁻³¹, 0.08).
3. **The golden histories**, as `ComputeTargets/tests/golden_history.py` and `golden_tuned_history.py` run
   them: bitwise against their records, or the fields that moved and by how much.
4. **The reflection store's 42 histories** (`var/datastores/science-2026.8.0-reflections`: β = 1.2, 2.0, 2.9
   at 4×10⁻³¹, 10⁻¹⁵ and 10⁻¹² for α = 0.001, 0.01, 0.08, 0.15, and at 10⁻³ and 0.5 for α = 0), each at its
   stored normalization through `--from-store`. Under `None` each must reproduce its stored samples
   **bitwise**; that is the first evidence for 02. Under each fix: Δ accepted steps, wall-clock ratio,
   flagged steps, and the change in the per-history values of §1.1 against the stored ones.
5. **Blocks B's masses**: at 10⁻³³ and 10⁻³⁰, from the main store, the three β of its grid nearest 1.2, 2.0
   and 2.9 at α = 0.01 and 0.08, at stored normalization.

**Signature first.** Before the variants run, the scanner under `None` must reproduce the diagnosis: the
three bad steps' h, h·ω and π_end/π_start to the digits `out/scan_first_steps.txt` prints, and its count of
845 first outward steps over the nine histories, counted as the diagnosis counted them (every Radau
instance in the process). If it does not, stop and say what differs.

### 1.4 Why Radau accepts the bad step (P4)

From the bad step's start state at β = 1.5, M = 10⁻¹², α = 0.05 (and one of the 10⁻¹⁵ failures), take
single Radau steps at h from the good range (≤ 4×10⁻⁶) up to the bad (6.4×10⁻⁴), on the same right-hand
side. For each: SciPy's `error_norm` as `_step_impl` computes it (reproduce it from SciPy's private
functions or by instrumenting `_step_impl`; say which), whether the step is accepted, and the true error of
its endpoint against the resolved re-integration (`max_step` = h/20000, as `trace_step.py`). Then say
whether the under-reading grows with h·ω, at what h·ω acceptance starts to be wrong, and whether Radau,
started at a good h, could grow to a bad one by `MAX_FACTOR` steps while each step passes its error test.
The last is what decides whether F3 alone suffices; if it can, show it by a run or by the numbers.

Bounded: a few hours of work. If it is not settled, record what was measured and open an issue.

### 1.5 The record

`.documents/reflection-restart-verification.md`, a new document, §1 for this prompt:
- the scanner and its flag, with the distributions of §1.1;
- the Signature gate's reproduction;
- one table per variant over §1.3's histories: outcome, flagged steps (first steps and later ones apart),
  goldens bitwise or moved, cost (accepted steps and wall-clock against `None`, per M), and the largest
  change in each per-history value against `None` and against the store;
- §1.4's account;
- the planner's question restated with the answer: which fixes remove the bad step, which leave which
  histories bitwise, and what each costs. **No recommendation is required, and none is binding**; if you
  give one, say what it rests on. The user rules U6.

Committed outputs go to `.documents/reflection-restart/out/` (summaries and tables, not the raw JSON lines
if they are large; say where the rest is under `var/`).

## 2. What is not wanted

- **No fix is chosen or made permanent.** The switch stays a module attribute that defaults to today's
  behaviour; prompt 03 removes it.
- **No store is written.** The run's stores are read-only. No Ray.
- No change to the cap's parameters, h_floor, the parking cut, the gate's sampling or the drain rule.
- No figure, no `science-runs/` change, no `CLAUDE.md` change but the test counts if they change.
- The 2026.8.0 audit at scale is 02's. Here the store is read only for §1.3's items 4 and 5.

## 3. Verification

- The suites, before you change anything and after, per `CLAUDE.md` (ComputeTargets in two halves; tools
  alone): record both sets of counts. A count that falls is a stop.
- The golden histories and the golden fingerprint pass bitwise at `_PROBE_OUTWARD_FIX = None`.
- The Signature gate (§1.3).
- The Measure gate (README §2.1): every variant measured on every history of §1.3, and §1.4 answered or
  opened as an issue.
- `black` on the files you changed.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- the tool and its tests; the switch, its four values and their tests;
- `.documents/reflection-restart/measure/` and `out/`; the verification document §1;
- the log `logs/01-measure-the-fixes.md` (template: `dk-and-rpk-migration` README §5.1), with every
  deviation classified, "Observations not acted on", and §8 "State handed to the next prompt": the tool's
  command for 02, its throughput per M, and anything 02 must know about the bitwise check;
- the board: 01's row, its items, the gates Signature and Measure, §3/§4 for any issue opened, with
  `.documents/OPEN_ISSUES.md` in the same commit;
- `CLAUDE.md`'s test counts, if they changed.

The commit message follows `CLAUDE.md` invariant 5.
