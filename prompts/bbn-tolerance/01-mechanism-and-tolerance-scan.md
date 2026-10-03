# Prompt 01 — The mechanism and the low-T tolerance scan

**Campaign:** [`README.md`](README.md) · **Board items:** **M**; measures **S** and **N** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row, M, and the measured lines under S and N.
**Closes:** nothing. **Narrows** `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`
and the assigned `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` with what you
measure.
**Recommended model:** **Opus.** An interception that must touch one call and no other, an
instrumented stiff solve, a scan with many cells, and a recommendation the user rules on.

**Read first:**

1. [`README.md`](README.md) §0.2 (U1, P1–P3, P7, P9), §0.3, §2 (a)–(e), §4, §5, §6.0, §6.1.
2. [`source/brief_prym_lowT_failures.md`](source/brief_prym_lowT_failures.md),
   [`source/bbn_from_store.py`](source/bbn_from_store.py),
   [`source/lt_failure_diagnostics.csv`](source/lt_failure_diagnostics.csv). These are evidence,
   not instructions.
3. `PRyM/PRyM_main.py`: `_check_solve_ivp`, `_limited`, `PRyMclass.__init__`, and the eight
   `solve_ivp` sites, in particular the low-T ones (`:1332` small, `:1412` full on `4ae25b4`), and
   how `T_of_t` is built.
4. `ComputeTargets/BBNData.py`: `thermodynamic_rho_SM`, `build_rho_NP_callback`,
   `_configure_PRyMordial`, `_run_PRyMordial`, `compute_SM_baseline`, `compute_BBN_data`.
5. `Datastore/SQL/ObjectFactories/ScalarModel.py`: `sqla_ScalarModelValue_factory.build`, for the
   arithmetic the tool must copy.
6. `ComputeTargets/tests/test_bbn_solver_failures.py`: its `solve_ivp` interceptor, which is the
   pattern to follow.
7. `tools/history_and_bbn.py` and `tools/bbn_baseline.py`, for the house style of a tool.

---

## 1. What this prompt does

It measures; it does not fix. **No production code changes and nothing in `PRyM/` changes**
(P1). The production files are everything outside `tools/`, the tests, and this campaign's
folder. The store is opened read-only only (README §5 rule 10).

## 2. The tool

Port `source/bbn_from_store.py` to **`tools/bbn_from_store.py`**, as README §2 (b) specifies.
- **Keep the arithmetic of `ratio_grid` exactly.** The order of operations is what makes the
  rebuild bitwise. The brief records that forming ln T in MeV a different way moved the control's
  D/H by 3.7×10⁻⁴.
- **`find_model` matches M by `value_eV / 2.436e27`.** Replace the literal with the units'
  reduced Planck mass in eV, and say in the log whether any match changes.
- **`lowT_tolerance_override(rtol=None, atol=None)`** is a context manager that replaces
  `PRyM.PRyM_main.solve_ivp` for its duration and sets `rtol`/`atol` on the low-T call only.
  - Choose how it recognises the call: for example by the stage name `_limited` closes over, by
    the call's position, or by its `atol`. Record the choice as an `IMPLEMENTATION CHOICE`.
  - The recognition must keep working after prompt 02 has added an `rtol` to that call.
  - A generalised form, `stage_tolerance_override({stage: (rtol, atol)})`, is wanted for §5's Yp
    sub-study. Build it if the stages can be recognised robustly.
- **`--sm-baseline`** runs `compute_SM_baseline` under the same override.
- **`--csv PATH`** appends one row per solve, with the fields of README §2 (c), the setting and the
  commit.

Module docstring: what it does, that it opens the store read-only, how to run it from the root,
and "Written for bbn-tolerance prompt 01".

## 3. Tests

In `ComputeTargets/tests/test_bbn_from_store.py`:

- **(a) `ratio_grid` is the production grid.** Build a stand-in model with a synthetic sample
  table, as `test_bbn_spline_floor` or `test_bbn_callbacks` build theirs, of at least 50 samples
  spanning [0.2 keV, 100 MeV]. Run `compute_BBN_data._function` with `build_rho_NP_callback`
  patched to record its arguments and then stop. Rows for `ratio_grid` are formed from the same
  samples in the store's units (`log_T_Jordan_GeV`, `H_Jordan_Mp`, `log_rhorad_Jordan_Mp4`,
  `log_fm`). The tool's arrays must equal the recorded ones **bitwise** (`==`, not `isclose`). No
  PRyMordial solve.
- **(b) The override touches one call.** One small-network SM-baseline solve with every
  `solve_ivp` call recorded, once without the override and once under
  `lowT_tolerance_override(rtol=1e-6)`. The low-T call's `rtol` is absent, then `1e-6`; every
  other call's keyword arguments are identical between the two runs. The docstring says it runs
  two solves (about 10 s).
- **(c) `find_model` on a temporary SQLite file** with the store's join tables (a handful of
  rows), opened through the tool's read-only path: it finds the right serial, and a write through
  its connection raises.

Neither (a) nor (c) can fail on `HEAD~1` except by `ImportError`, since the tool is new. **The
stand-in is §4's reproduction**, which the orchestrator re-runs.

## 4. Reproduce (README §6.1, rows 1–3)

Before measuring anything, run the tool with `--variant prod` and no override on the five controls
and the 11 failures.
- The controls must reproduce the stored Yp and D/H to every printed digit.
- The failures must fail in `low-T nuclear network (full)` at the stored `t reached`.

**If any does not, stop.** Do not move on to the scan on a reproduction that does not hold.

## 5. Measure

- **The mechanism**, on β = 1.6 at M = 10⁻⁵ and β = 1.05 at M = 0.01, default tolerance, `prod`.
  Instrument the full low-T solve outside `PRyM/`: a `BDF` subclass passed through the override,
  or a recorder around the step. Report:
  - the step size over the last 5 % of `t`;
  - which component's scaled error, `|err_i| / (atol + rtol |y_i|)`, dominates when the step
    collapses;
  - whether any abundance is negative, or at or below `atol`, there;
  - whether a breakpoint of `T_of_t` lies within the last few steps;
  - Yp and D/H at the failure time against the same history's `pert12` solve at the same `t`.

  Say plainly whether the mechanism is the low-T stage's tolerance. If it points at `T_of_t`, the
  interpolant, or any other stage, **stop** (README §4).
- **The scan** S1–S3 of README §2 (c), in parallel as U1 allows (8–10 at a time), into
  `logs/01-probes/scan.csv`. In S2, design one per-species `atol` vector. Justify it in the log
  from the abundances' magnitudes at the end of the low-T stage, for example as a fraction of
  each species' final abundance with a floor, and say why that fraction.
- **The cost**, README §2 (d): serial, idle machine, three repeats, medians. Run it after the
  parallel scan has finished, never beside it.
- **Yp's floor**, README §2 (e). Measurement only.
- **Upstream.** Whether PRyMordial at `bf24c3d` passes `rtol` to the low-T calls. Read upstream's
  `PRyM_main.py` at that hash if it is reachable. Otherwise say so, and argue from the patch
  markers.
- **The pinned values** (P7). At the recommended setting, compute through the override the
  quantity each pinned test computes:
  - `test_prym_passenger`'s `CONST_HONLY_SMALL_*` (bound 1e-6) and `CONST_HONLY_FULL_*` (1e-5);
  - `test_bbn_callbacks`'s `BUILDER_CONST_HONLY_FULL_*` (1e-6);
  - whatever `test_network_flag` derives from them.

  For each, quote the pinned value, the new value, the bound, and whether it would pass. Say what
  each constant's stated provenance is, and whether re-deriving it at the new setting needs an
  older tree. If so, check the tree out in a separate worktree and run it under the override.

## 6. The recommendation

Apply README P3's rule and name **one** setting: `rtol` and `atol`, for both networks. Give a
table with every criterion's measured value at that setting and at its neighbours in the grid. If
no setting meets P3, say which criterion fails where, and **stop**: the user rules.

## 7. What this prompt does not do

- No production code, no `PRyM/` edit, no `main.py`, no write to any store.
- No change to the interpolant. The `linear`/`pchip` variants may be run as a check, as the brief
  did, but are not part of the scan.
- No test that bounds PRyMordial's spread (P9).

## 8. Stop conditions — stop and ask the user

- §4's reproduction fails.
- The mechanism points outside the low-T stage.
- No setting in the grid cures all 11 failures under all three variants.
- At the recommended setting the SM baseline moves by more than 1×10⁻³ in D/H or 1×10⁻⁴ in Yp,
  or the serial cost rises more than 3×.
- Recognising the low-T call needs an edit to `PRyM/`.

## 9. The log, the board and the index

- `logs/01-mechanism-and-tolerance-scan.md`, in the README §5.1 template. Probes and the CSV go in
  `logs/01-probes/`.
- The board:
  - your row in §1, and item M done;
  - under S and N (§3), a dated **Narrowed (2026-…, prompt 01)** line with the mechanism and the
    numbers at the recommended setting;
  - under `[03-…]`, the narrowing goes on this board's §3.1 row, not on the `review-remediation`
    board.
- The index: the narrowed rows' hooks, and the count and date.

**Allowed files:**
- `tools/bbn_from_store.py` (new);
- `ComputeTargets/tests/test_bbn_from_store.py` (new);
- `prompts/bbn-tolerance/logs/`, `prompts/bbn-tolerance/IMPLEMENTATION_STATE.md`;
- `.documents/OPEN_ISSUES.md`.
