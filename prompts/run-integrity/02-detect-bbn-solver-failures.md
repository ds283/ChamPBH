# Prompt 02 — Detect BBN solver failures; bump the version

**Campaign:** [`README.md`](README.md) · **Board item:** **F** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and F.
**Closes:**
- `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]` on the
  `review-remediation` board;
- `[00-a-nan-new-physics-sample-hangs-prymordial]` on this board.

**Recommended model:** **Opus**. There are three small changes, a vendored patch and the
campaign's one version bump. The judgement is in putting the boundary exactly at the PRyMordial
call, and in showing that no successful solve moved.

**Read first:**

1. [`README.md`](README.md) §0.2, §0.4, §2 (b), §2 (c), §5, §6.2.
2. `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3, the entry for the first issue above,
   and this campaign's `IMPLEMENTATION_STATE.md` §3, the entry for the second.
3. [`planning-probes/prymordial_solver_probe.py`](planning-probes/prymordial_solver_probe.py).
   Run it once (about 80 s). It is the pattern for your tests: it replaces the name
   `PRyM.PRyM_main.solve_ivp`, which PRyMordial looks up at call time, and forces one call to
   fail.
4. `PRyM/PRyM_main.py`:
   - the eight `solve_ivp` calls, at `:199, 239, 425, 585, 1022, 1097, 1189, 1252`;
   - the lines after each that read the result.

   Confirm for yourself which run under production's flags; the README §2 (b) table says five.
5. The earlier patch's marker at `PRyM/PRyM_main.py:140` (review-remediation prompt 03). Copy
   its style.
6. `ComputeTargets/BBNData.py`:
   - `:37–49`, `PRYM_VERSION` and `_failure_payload`;
   - `:92–236`, `build_NP_callbacks`;
   - `:239–290`, `_configure_PRyMordial` and `compute_SM_baseline`;
   - `:293–470`, `compute_BBN_data`, and in particular the PRyMordial call at `:436–446`.
7. `config/version.py` (prompt 01's), and `main.py`'s import of it.
8. `ComputeTargets/tests/prym_fixtures.py` (`run_prym`, `:170–`), and
   `ComputeTargets/tests/test_network_flag.py`, for how the existing tests save and restore
   PRyMordial's module globals.

---

## 1. The changes

**F1 — PRyMordial checks its solves.** This is a patch to the vendored `PRyM/`.

- **The check.** After **each of the eight** `solve_ivp` calls, if `not sol.success`, raise one
  exception class defined in `PRyM/`. Its message names:
  - the stage (README §2 (b) table);
  - `sol.status` and `sol.message`;
  - the t reached, `sol.t[-1]`, against the target.
- **Keep the call site.** The call still goes through the module-level name `solve_ivp`. Do not
  re-import or rename it; the tests intercept that name.
- **Mark it.** Each patched site gets a comment naming `run-integrity` prompt 02.
- **Leave the rest alone.** Not a tolerance, a method, an argument, or the `julia_flag` branches.
- **List every patched line in the log**, so that the patch can be re-applied on an upgrade.

**F2 — the version string.** `PRYM_VERSION` becomes `"bf24c3d+cham03+ri02"`. Add a sentence to
the comment above it saying what `ri02` is.

**F3 — the PRyMordial boundary** (README §0.2).

- **The helper.** Factor the call at `BBNData.py:436–446` into one pure helper, which takes the
  three callbacks and `small_network` and does the `_configure_PRyMordial` call and the solve.
- **What it returns.** The abundances. Or, for **any `Exception`** raised inside the call,
  `_failure_payload(f"PRyMordial: {type(e).__name__}: {e}")`.
  - That covers the new class, a `ComputationFailureError` from ChamPBH's callbacks, which
    PRyMordial calls, and anything else.
  - Not `BaseException`: a `KeyboardInterrupt` must still stop the run.
- **Where it is used.** `compute_BBN_data` uses it, and returns its failure payload unchanged.
- **Where it is not.**
  - No other `except` is added to `compute_BBN_data`.
  - `compute_SM_baseline` does not use the helper. It keeps raising, and says so in a comment.

**F4 — the finiteness guard** (README §2 (c)). In `build_NP_callbacks`:

- if any sample of `log_T_MeV`, `density_ratio` or `pressure_ratio` is not finite, raise
  `ComputationFailureError` naming the array, the first index and its T;
- in each of the three callbacks, a non-finite `T_in_MeV` raises `ComputationFailureError`
  before the negative-T guard.

For finite input no value changes.

**F5 — the version bump.** `VERSION_LABEL` goes from `"2026.3.0"` to **`"2026.4.0"`** in
`config/version.py`. Add a dated sentence to its comment: from 2026.4.0 a failed PRyMordial solve
is stored as a failure with its reason, not as a success, and `PRyM_version` is
`"bf24c3d+cham03+ri02"`.

**F6 — documents, additively.** Add a dated note to `.documents/numerical-strategies.md` §7.4
(defensive behaviour) and §7.5 (running PRyMordial), saying:

- what is now checked;
- where the boundary is;
- what becomes a failure row.

Do not rewrite what is there (CLAUDE.md rule 6).

---

## 2. Tests — `ComputeTargets/tests/test_bbn_solver_failures.py`

**Docstring: this module runs partial PRyMordial solves, about 40 s in all.**

- **Save and restore everything you touch.** That is every PRyMordial module global, and the
  `PRyM.PRyM_main.solve_ivp` name.
- **How to force a failure.** Integrate the first 1 % of the span, then mark the result failed,
  as the probe does. Select the call to fail **by its order**, not by line number.

- **(a) Every production stage is checked.** Run the SM callbacks (all three zero) with
  `small_network=False`. For each k = 1 … 5, force the k-th `solve_ivp` call to fail. Then:
  - PRyMordial raises the new class;
  - its message names a stage;
  - the five stage names are distinct.

  **Must fail on `HEAD~1`**, where PRyMordial returns abundances. Quote in the log the
  abundances `HEAD~1` returns for k = 5.
  - **It must fail for that reason, not on an import.** Write it so that `HEAD~1` fails because
    nothing names a stage. Either import the new class inside the test body, or catch
    `Exception` and assert on its type name and message. A module-level import of the new class
    would fail on `HEAD~1` with an `ImportError`, which is not the check (orchestrator
    README).
- **(b) The small-network stages are checked.** The same, with `small_network=True`, for the two
  calls that differ from the full network. **Must fail on `HEAD~1`.**
- **(c) The boundary.** Through the F3 helper:
  - a forced failure (k = 1 is quickest) gives a failure payload whose reason begins
    `"PRyMordial: "` and names the class;
  - a callback that raises `RuntimeError` inside PRyMordial gives a failure payload naming
    `RuntimeError`.

  The helper is new, so `HEAD~1` meets it only as an import error. The breakage record for (c)
  is the `except` clause at `HEAD~1`'s `BBNData.py:445`, quoted in the log, together with (a).
- **(d) The finiteness guard.** No solve.
  - `build_NP_callbacks` with one NaN in `density_ratio` raises `ComputationFailureError`, and
    the message names the index.
  - The same holds for an infinite value in `pressure_ratio`.
  - A callback built from finite samples, called with `float("nan")`, raises.
  - **Must fail on `HEAD~1`**, where nothing raises.
  - **Do not run PRyMordial in (d)**: on `HEAD~1` it would hang.
- **(e) The version string.** `PRYM_VERSION == "bf24c3d+cham03+ri02"`.

**Every pinned abundance must pass unchanged** in the existing suite: they are all successful
solves. `ComputeTargets/tests` rises by the methods you add; the other two suites are unchanged.

---

## 3. What this prompt does not do

- No `PRyM/` change beyond F1 (README §0.4).
- No change to `main.py`'s lookups or to any factory; those are prompt 03's.
- No Ray timeout. If you find a hang route other than NaN ρ_NP, record it under Observations and
  open a §3 issue; do not fix it.
- No change to the callbacks' values, the spline domain or the sampling.

## 4. Acceptance

1. README §6.2, every row, with measured values in the log.
2. `git diff HEAD~1 HEAD -- PRyM/` shows only the checks, their exception class and their marker
   comments. It is not black-formatted.
3. All three suites pass with every pin unchanged. `black --check` is clean on the changed files
   outside `PRyM/`.
4. `grep -n VERSION_LABEL config/version.py` shows `"2026.4.0"`. `grep -rn "VERSION_LABEL ="`
   still finds one definition.
5. The board and the index:
   - F done;
   - both issues closed:
     - their rows deleted from `.documents/OPEN_ISSUES.md`;
     - a dated **Resolved** line on the `review-remediation` entry;
     - the planning issue moved to this board's §4;
   - count and date corrected.

## 5. Stop conditions — stop and ask the user

- **A pinned abundance moves.** Then some successful solve changed, and the patch did more than
  check.
- **A production stage cannot be forced to fail through `solve_ivp`.** For example, if it turns
  out to go through the Julia path.
- The boundary cannot be drawn without catching exceptions from ChamPBH's own code.

## 6. The log and the board

`logs/02-detect-bbn-solver-failures.md`, in the README §5.1 template. Beyond the template:

- every patched line of `PRyM/PRyM_main.py`, with its stage name;
- the five production stage names and the two small-network ones, as the tests printed them;
- the `HEAD~1` abundances for (a) k = 5, and how each of (a), (b) and (d) was shown to fail;
- `VERSION_LABEL` and `PRYM_VERSION` before and after, and the new comment sentences verbatim;
- the consequence, on the board's header and on F's row: **every store made before 2026.4.0 is
  invalid.**
