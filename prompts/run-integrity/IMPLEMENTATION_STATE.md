# Run integrity campaign — implementation state

**Last updated:** 2026-09-30 · **Status: IN PROGRESS — 2 of 4 prompts landed (01, 02).**
`VERSION_LABEL` is `"2026.4.0"` since prompt 02, defined once in `config/version.py` since
prompt 01. `PRyM_version` is `"bf24c3d+cham03+ri02"`.
**Every store made before 2026.4.0 is invalid.** Since prompt 01, a lookup returns only rows made
under the current label; opening an old store recomputes every compute target beside the old
rows.

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
- **2026-09-30, the user: `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]` is not
  folded into a later prompt.**
  - **Why.** Prompt 01 did not cause it, and prompt 03 does not widen it. Prompt 03 changes which
    failure rows `main.py`'s lookup returns, not which success rows it returns.
  - **The alternative, declined.** Adding `validated == True` to prompt 03. It changes which rows a
    lookup returns, so it would be a revert unit of its own and need a new §6.3 row.
  - **What happens instead.** Prompt 04 re-checks the orchestrator's narrowed reading on the final
    tree and records it on the entry (§3). The issue stays open and unassigned.
- **2026-09-30, the user: log 02's Deviation 5 is accepted as it stands.**
  - **What it says.** README §2 (c) and the "Now" column of §6.2 are wrong about a NaN sample on
    `f0de762`. `build_NP_callbacks` did not return and let PRyMordial hang. `make_interp_spline`
    raised `ValueError`, which escaped `compute_BBN_data` with no failure row. The measured hang
    comes from a callback called with T = NaN.
  - **The review.** The orchestrator reproduced it: test (d) on `HEAD~1` errors with
    `ValueError: Array must not contain infs or nans.` for the NaN and inf samples. Prompt 02's
    guards close both routes, and the §6.2 target is met as written.
  - **Accepted untagged.** The log does not give it one of the three tags, and the user
    accepted it that way. The README is not amended; log 02 is the record.
- **2026-09-30, the user: prompt 04 is told about the probe's line number.**
  `planning-probes/prymordial_solver_probe.py` chooses the solve to truncate by line
  (`truncate_line = 1252`, `:103`). Prompt 02's patch moved that call to `:1291`, so on `cc34de4`
  the probe as committed truncates nothing and returns the SM baseline unchanged. The orchestrator
  ran a scratch copy outside the repository, with only that number changed to 1291. It raised
  `PRyMSolverFailureError` in stage `'low-T nuclear network (full)'`. The probe is left as it is,
  as a record of `27a32bc`. Prompt 04 §1 and orchestrator prompt 04 check 3 carry the note.

None pending. Decisions the prompts may surface, each a stop-and-ask in its prompt:

- a design for V that needs a `build()` signature change across every factory, or an edit to
  `base.py`;
- a production PRyMordial stage that cannot be forced to fail through `solve_ivp`;
- a change to `RayWorkPool` to make a stored failure count as done.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Key the compute-target lookups on the version](01-version-keyed-lookups.md) | **V** | Opus | ✍️ 2026-09-30 | ✅ 2026-09-30 | see `git log` ("Key the compute-target lookups on the version label") | [`logs/01-version-keyed-lookups.md`](logs/01-version-keyed-lookups.md) |
| 02 | [Detect BBN solver failures; bump the version](02-detect-bbn-solver-failures.md) | **F**, version bump | Opus | ✍️ 2026-09-30 | ✅ 2026-09-30 | see `git log` ("Detect PRyMordial solver failures and bump to 2026.4.0") | [`logs/02-detect-bbn-solver-failures.md`](logs/02-detect-bbn-solver-failures.md) |
| 03 | [Stop recomputing failed BBN rows; pair lookups correctly](03-failure-caching-and-pairing.md) | **R** | Opus | ✍️ 2026-09-30 | — | — | — |
| 04 | [Close-out verification and handover](04-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-09-30 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| V | **DEFECT, high** | No `build()` filters on the `version` column that every compute-target row carries, so a store opened under a new label returns old rows: store_id 1 under `2026.4.0` in the planning probe. Three scripts carry three labels, one of them `"2026.1.1"`. Closes `[00-datastore-lookups-ignore-the-version-column]`. | 01 | **done 2026-09-30** (log 01). `ScalarModel`, `AdiabaticHistory` and `BBNData` register `"key_on_version": True`; `Datastore.object_get` hands their `build()` a copy of each payload with the current serial under `"_version_serial"`, and `build()` filters `version == serial`, raising if the key is absent. The label is defined once, in `config/version.py`, and imported by `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py`; still `"2026.3.0"`. The planning probe's store_id 1 under `2026.4.0` is now `available=False`. Schema byte-identical. Suites 18 / 30 / 11, all OK |
| F | **DEFECT, high** | No `solve_ivp` result in PRyMordial is checked. A last solve that gives up at 1 % of its span returns D/H 5.0e-3 off, with no exception. Only three exception types become failure rows. A NaN in ρ_NP hangs PRyMordial for more than 60 s. Closes `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]` and `[00-a-nan-new-physics-sample-hangs-prymordial]`. | 02 | **done 2026-09-30** (log 02). Each of the eight `solve_ivp` calls in `PRyM/PRyM_main.py` is followed by a marked `_check_solve_ivp`, which raises `PRyMSolverFailureError` naming the stage, status, message and t reached; all five production stages and both small-network ones raise when forced to fail (on `HEAD~1`, k = 5 returned D/H x1e5 2.474578712). `_run_PRyMordial(callbacks, small_network)` turns any `Exception` inside the PRyMordial call into `"PRyMordial: <Type>: <message>"`; nothing else gained an `except`, and `compute_SM_baseline` still raises. `build_NP_callbacks` refuses non-finite samples, and each callback a non-finite T, with `ComputationFailureError`. `PRyM_version` `"bf24c3d+cham03+ri02"`; **`VERSION_LABEL` `"2026.4.0"`: every store made before 2026.4.0 is invalid.** Every pin unchanged. Suites 18 / 35 / 11, all OK |
| R | **DEFECT, medium** | `main.py` looks up BBN successes only, so a failure is recomputed on every run; `failure=None` raises once two failed rows exist. Both stages zip lookup results against the unfiltered bin, so after a failed `ScalarModel` they schedule the wrong models: {V1, V3} for {V2, V4}. Closes `[03-main-recomputes-failed-bbn-rows-on-every-run]` and `[00-main-pairs-lookup-results-against-the-unfiltered-bin]`. | 03 | not started |

---

## 3. Active and unresolved issues

Two opened by the planner on 2026-09-30, while reading the code of the three assigned issues. Both
are assigned to this campaign's prompts. Issues opened by later prompts go here too, with an index
row under §1.5 of `.documents/OPEN_ISSUES.md`. Prompt 01 opened two on 2026-09-30, neither
assigned (the two `01-` entries). Prompt 02 closed
`[00-a-nan-new-physics-sample-hangs-prymordial]` (§4) and opened two, neither assigned (the two
`02-` entries).

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

- **[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]** *(log 01, observation 1;
  reasoned from the code on `90b2c86` + prompt 01; not run)*.
  - **What.** `ScalarModel.build` filters `validated == True`
    (`Datastore/SQL/ObjectFactories/ScalarModel.py:253`). `AdiabaticHistory.build` and
    `BBNData.build` do not: they filter only on `model_serial` and, since prompt 01, `version`.
    Rows are inserted with `validated=False` and validated after the store.
  - **Impact.** A run interrupted between store and validation leaves an unvalidated row. Unless
    the next run passes `--prune-unvalidated`, that row is served as found.
    - For `AdiabaticHistory` with a partial value set, `build()` then raises "Fewer z-samples than
      expected".
    - For `BBNData` looked up with `_do_not_populate` (as `main.py` does), the sample count is not
      checked, so the row counts as done.
  - **Next step.** Add `validated == True` to both lookups, as `ScalarModel.build` has. It changes
    which rows a lookup returns, so it needs its own prompt. **Not in prompt 01's scope** ("nothing
    else in the query changes").
  - **Decision (2026-09-30, the user):** not folded into prompts 02–03. It stays open and
    unassigned. Prompt 04 re-checks the orchestrator's narrowed reading on the final tree and
    records it here (prompt 04 §3). The reading, from the code on `bacddd8`, is that an interrupted
    run cannot leave a partial row, so the served case is only a row that failed its count check.
- **[01-plot-by-beta-profile-label-names-plot-scalarmodel]** *(log 01, observation 2)*.
  - **What.** `plot_by_beta.py:87` builds its `ProfileAgent` label as
    `f'{VERSION_LABEL}--plot_ScalarModel-primarydb-...'`, a copy of `plot_ScalarModel.py:86`.
  - **Impact.** Cosmetic: only the name of a profiling run, and only with `--profile-db`.
  - **Next step.** Say `plot_by_beta` there, in any commit that owns that line. Prompt 01 kept the
    labels' format, as it was told to.
- **[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]** *(log 02, observation 1; a scratch
  command on `f0de762`, quoted in log 02's Verification)*.
  - **What.** `build_NP_callbacks` raises `IndexError` for an empty sample grid and `ValueError`
    (from `make_interp_spline`) for 1–3 samples. Both are outside `compute_BBN_data`'s
    `except ComputationFailureError` (`ComputeTargets/BBNData.py:510`), and outside the
    PRyMordial boundary, so they propagate.
  - **Impact.** A model with fewer than four samples in [1e-4 keV, 100 MeV] ends its Ray task with
    an exception and no failure row. Whether a stored history can do so is not known; the
    `T_Jordan_stop` pre-check does not exclude it.
  - **Next step.** A length check in `build_NP_callbacks` that raises `ComputationFailureError`, so
    the case becomes a `"BBN callbacks: ..."` failure row. It changes which inputs become failure
    rows, so it needs its own prompt. **Not in prompt 02's scope.**
- **[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]** *(log 02, observation 2;
  reasoned from the code on `f0de762` + prompt 02; not run)*.
  - **What.** Since prompt 02 the samples and T are checked for finiteness, but not the callbacks'
    return values. For a finite T in the domain, `rho_NP`, `P_NP` or `drho_NP_dT` can still be
    NaN if `rho_SM_MeV4` or `drho_SM_dT_MeV3` is, that is if the EOS's `G_rho` or
    `dG_rho_dlogT` is non-finite there.
  - **Impact.** Such a NaN would reach PRyMordial, which on the planning probe's evidence can
    hang on one. No measurement shows the EOS returning a non-finite value. This is the only NaN
    route found other than those prompt 02 closes.
  - **Next step.** Raise `ComputationFailureError` on a non-finite return value. Prompt 02 was
    told not to change the callbacks' values, and did not add it.

---

## 4. Resolved issues

Two assigned issues, closed by prompts 01 and 02 on 2026-09-30. Their entries stay on the
`review-remediation` board, with a **Resolved** line; they are listed here as the record. One
opened here, closed by prompt 02 and moved from §3.

- **[00-datastore-lookups-ignore-the-version-column]** — resolved by prompt 01 (log 01). The three
  compute-target lookups are keyed on the current label's serial, delivered by
  `Datastore.object_get` and required by each `build()`. The parameter tables stay unkeyed. The
  label is defined once, in `config/version.py`. Tests (a)–(d) and (f) in
  `Datastore/tests/test_version_keyed_lookups.py` fail on `90b2c86`.
- **[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]** — resolved by prompt 02
  (log 02). Every `solve_ivp` result in `PRyM/PRyM_main.py` is checked, in a marked patch, and a
  failed solve raises `PRyMSolverFailureError` naming its stage. Any `Exception` inside the
  PRyMordial call becomes a failure payload, through `_run_PRyMordial`. Tests (a) and (b) in
  `ComputeTargets/tests/test_bbn_solver_failures.py` fail on `f0de762`; (c)'s breakage record is
  the three-type `except` quoted in log 02.
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
  - **Resolved (2026-09-30):** by prompt 02 (log 02). `build_NP_callbacks` refuses a non-finite
    sample of `log_T_MeV`, `density_ratio` or `pressure_ratio`, and each callback a non-finite T,
    with `ComputationFailureError`, before PRyMordial is called. Test (d) fails on `f0de762`.
    **Measured differently from the entry above:** on `f0de762` a NaN *sample* did not reach
    PRyMordial. `make_interp_spline` raised `ValueError`, which escaped `compute_BBN_data` with no
    failure row. The hang's route was a callback *returning* NaN, as for a NaN T. That route is
    closed too; a NaN from the EOS is not (`[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]`).
