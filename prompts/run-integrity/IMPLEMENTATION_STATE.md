# Run integrity campaign — implementation state

**Last updated:** 2026-09-30 · **Status: PLANNED — 0 of 4 prompts landed.**
`VERSION_LABEL` is `"2026.3.0"`; prompt 02 will make it `"2026.4.0"`.

The campaign was planned on 2026-09-30, against `production-readiness` at `27a32bc`. It fixes three
issues on the closed [`review-remediation`](../review-remediation/IMPLEMENTATION_STATE.md) board,
which the user chose as the ones to clear before a science run, and two the planner opened while
reading the code they touch (§3):

- lookups that ignore the version label (V);
- PRyMordial solves that fail silently, exceptions that escape, and a NaN that hangs (F);
- failed BBN rows recomputed on every run, and lookup results paired with the wrong models (R).

The three `review-remediation` issues stay on that board, each with an **Assigned** line naming
this campaign. When a prompt closes one, it adds a **Resolved** line there (README §5 rule 4). The
two opened here are in §3 below, and move to §4 when closed.

Target branch `run-integrity` from `27a32bc`.

**Campaign:** [`README.md`](README.md) ·
**Code:**
- `Datastore/SQL/Datastore.py`, and the `ScalarModel`, `AdiabaticHistory` and `BBNData` factories;
- `config/version.py` (new), `config/argument_parser.py`;
- `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` (the label import only);
- `pipeline_selection.py` (new);
- `ComputeTargets/BBNData.py`, `PRyM/PRyM_main.py` (the `solve_ivp` checks only);
- `Datastore/tests/` (new), `ComputeTargets/tests/`;
- `CLAUDE.md` (one test command).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.4 (assigned), §1.5
(opened here)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The same holds when a prompt closes one of the three assigned issues on the
> `review-remediation` board. The index is an index: one line per issue, pointing at the board
> that holds it. Where the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-09-30, the user: scope.** The two issues the user named, `[03-bbn-solver-failures-…]`
  and `[03-main-recomputes-failed-bbn-rows-…]`, plus `[00-datastore-lookups-ignore-the-version-column]`
  and the pairing defect the planner found (§3). The planner's argument: caching a failure is safe
  only if a new label retries it. The user accepted it.
- **2026-09-30, the user: the exception boundary is the PRyMordial call.** Anything raised inside
  it becomes a failure row, with its type and message. Exceptions from ChamPBH's own code
  propagate. The alternative offered was to catch every `Exception` in `compute_BBN_data`; it
  was declined because it would cache bugs as physics failures.
- **2026-09-30, the user: retry.** A failed BBN row is final within a label. A new label retries
  it, and so does `--retry-failed-bbn`. The alternatives offered were a new label only, and
  never automatically.
- **2026-09-30, the user: one bump, to `VERSION_LABEL = "2026.4.0"`, in prompt 02.** Prompt 03
  lands under the same label. **Every store made before 2026.4.0 is invalid.**
- **2026-09-30, the planner: one label for all three scripts.** Once lookups are keyed on the
  label, `plot_ScalarModel.py`'s stale `"2026.1.1"` would read none of the pipeline's rows. The
  label moves to `config/version.py` (README §2 (a)). This is part of V, not a separate issue.
- **2026-09-30, the planner: the NaN guard is on ChamPBH's side.** The hang is in PRyMordial's
  LSODA, on a `t_span` of `[nan, 0.745]`. The fix is to refuse non-finite samples before
  PRyMordial is called, rather than to add a timeout. A Ray task timeout is out of scope (README
  §0.4).

None pending. Decisions the prompts may surface, each a stop-and-ask in its prompt:

- a design for V that needs a `build()` signature change across every factory, or an edit to
  `base.py`;
- a production PRyMordial stage that cannot be forced to fail through `solve_ivp`;
- a change to `RayWorkPool` to make a stored failure count as done.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Key the compute-target lookups on the version](01-version-keyed-lookups.md) | **V** | Opus | ✍️ 2026-09-30 | — | — | — |
| 02 | [Detect BBN solver failures; bump the version](02-detect-bbn-solver-failures.md) | **F**, version bump | Opus | ✍️ 2026-09-30 | — | — | — |
| 03 | [Stop recomputing failed BBN rows; pair lookups correctly](03-failure-caching-and-pairing.md) | **R** | Opus | ✍️ 2026-09-30 | — | — | — |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-09-30 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| V | **DEFECT, high** | No `build()` filters on the `version` column that every compute-target row carries, so a store opened under a new label returns old rows: store_id 1 under `2026.4.0` in the planning probe. Three scripts carry three labels, one of them `"2026.1.1"`. Closes `[00-datastore-lookups-ignore-the-version-column]`. | 01 | not started |
| F | **DEFECT, high** | No `solve_ivp` result in PRyMordial is checked. A last solve that gives up at 1 % of its span returns D/H 5.0e-3 off, with no exception. Only three exception types become failure rows. A NaN in ρ_NP hangs PRyMordial for more than 60 s. Closes `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]` and `[00-a-nan-new-physics-sample-hangs-prymordial]`. | 02 | not started |
| R | **DEFECT, medium** | `main.py` looks up BBN successes only, so a failure is recomputed on every run; `failure=None` raises once two failed rows exist. Both stages zip lookup results against the unfiltered bin, so after a failed `ScalarModel` they schedule the wrong models: {V1, V3} for {V2, V4}. Closes `[03-main-recomputes-failed-bbn-rows-on-every-run]` and `[00-main-pairs-lookup-results-against-the-unfiltered-bin]`. | 03 | not started |

---

## 3. Active and unresolved issues

Two opened by the planner on 2026-09-30, while reading the code of the three assigned issues. Both
are assigned to this campaign's prompts. Issues opened by later prompts go here too, with an index
row under §1.5 of `.documents/OPEN_ISSUES.md`.

- **[00-main-pairs-lookup-results-against-the-unfiltered-bin]** *(the planner, 2026-09-30;
  reasoned from `main.py` on `27a32bc`, reproduced by `planning-probes/pairing_probe.py`; not run
  in the pipeline)*.
  - **What.**
    - **Two lengths.** In both `build_adiabatic_batch` and `build_bbn_data_batch`, the query
      payload is built only from `ScalarModel`s that did not fail (`main.py:389`, `:635`). The
      results are then zipped against `binned_batch[key]`, which still holds every pair
      (`:413–425`, `:659–671`).
    - **The shift.** After a failed model, every result is paired with the model before it,
      and `zip` drops the last.
  - **Measured on the probe's bin.** Five models; model 1's `ScalarModel` failed; models 0 and 3
    already have BBN rows.
    - The BBN stage schedules {V1, V3}. The right set is {V2, V4}.
    - V1's `compute_BBN_data` reads `model.values` on a failed model. That raises `RuntimeError`
      (`ComputeTargets/ScalarModel.py:1156`), outside every `except`, and the run stops.
  - **Impact.** A single failed `ScalarModel` that is not last in its shard bin:
    - misdirects the adiabatic and BBN stages;
    - duplicates work for models already done;
    - skips models that are not done;
    - and in the BBN stage, stops the run.
  - **Assigned (2026-09-30):** to this campaign, prompt 03 (R), by the user's decision to include
    it.

- **[00-a-nan-new-physics-sample-hangs-prymordial]** *(the planner, 2026-09-30;
  `planning-probes/prymordial_solver_probe.py nan` on `27a32bc`)*.
  - **What.** With ρ_NP = NaN below 0.1 MeV, PRyMordial's first two solves succeed (lines 199 and
    425). The high-T solve at line 585 is then given `t_span = [nan, 0.745]`, and it did not
    return within the probe's 60 s alarm.
  - **Why nothing catches it.** `build_NP_callbacks` checks only that the T grid decreases
    (`ComputeTargets/BBNData.py:128–134`), never that the ratios are finite. Both of its per-call
    guards, `T < 0` and `_check_domain`, are False for a NaN T.
  - **Impact.** In a Ray task, a hang stalls the queue with no failure row and no message.
    Whether a stored history can produce a NaN ratio is not known. The guard is cheap either way.
  - **Assigned (2026-09-30):** to this campaign, prompt 02 (F4), as part of
    `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`.

---

## 4. Resolved issues

None yet.
