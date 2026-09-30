# Log 04 — Close-out verification and handover

**Prompt:** prompts/run-integrity/04-close-out-verification.md
**Commit:** the commit that adds this file ("Close the run-integrity campaign with a verification addendum"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-09-30
**Result:** COMPLETE

Every README §6.1–§6.3 row is at target on `6fd9017`. No stop condition of prompt §6 was met. One
of the three claims about `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]` does not
hold as stated, so the entry is **not** narrowed (prompt §3, "if any claim fails"; Verification,
below).

## What shipped

No production file and no test changed. `VERSION_LABEL` is `"2026.4.0"` and `PRYM_VERSION` is
`"bf24c3d+cham03+ri02"`, before and after this prompt.

- **`.documents/review-remediation-verification.md`**: a new §4.7, "Addendum 2026-09-30 — the
  `run-integrity` campaign", inserted after §4.6 and before `---` / §5. 129 lines added, none
  removed (`git diff --numstat`). It states README §7's five points with evidence, supersedes by
  statement §4.6 point 1's "Still nothing stops an old store being reused" and every earlier
  `"2026.3.0"` statement, and carries the §6.1–§6.3 verification table and the one-line
  reproduction command.
- **`prompts/run-integrity/IMPLEMENTATION_STATE.md`**: the header now says COMPLETE and states the
  rule (every store made before 2026.4.0 is invalid; a lookup no longer returns its rows); row 04
  of §1 is filled. §3 has one new entry, `[04-adiabatichistory-lookup-ignores-do-not-populate]`
  (Observations); §2 and §4 are unchanged.
- **`prompts/INDEX.md`**: `run-integrity` marked complete; header "0 planned, 0 live, 3 closed";
  the open-issue column updated.
- **`.documents/OPEN_ISSUES.md`**: header "(in progress)" became "(closed 2026-09-30)" for
  `run-integrity`; the open count went from 15 to 16 and `run-integrity`'s from 5 to 6, for the
  new row in §1.5. None of the five issues the campaign closed is in it (§1.4 says "None open";
  §1.5 lists only the unassigned ones).
- This log.

## Deviations from the prompt

### The scope check used the board and the logs, not each prompt's §1 and §3 — STRUCTURALLY REQUIRED

- **What the prompt assumed.** Prompt §1 says to check every file in `git diff --stat
  27a32bc..HEAD` against each prompt's §1 and §3.
- **What was there.** My instructions from the caller said not to read the other prompts in the
  campaign.
- **What I did.** I checked each file against the board's "Code" list and against the "What
  shipped" section of logs 01–03. Every file is accounted for (Verification, "Scope check").
  Whether each prompt's own §1/§3 allowed the two `.documents/` additions, the `CLAUDE.md` edit and
  the `Datastore/SQL/Datastore.py` edit is taken from the logs, which record them. A reader who
  wants the stricter check should compare the list against the three prompt files.

## Verification performed

All commands from the repository root with `venv/bin/python`, on `6fd9017` (`git rev-parse HEAD`
printed `6fd9017d8b1fef375e548de620fd57861e3132ad`), with a clean tree before my edits.

### Suites

| Suite | `27a32bc` | Final tree | Wall-clock |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | **18**, OK | 80.0 s |
| `ComputeTargets/tests` | 30 | **41**, OK | 89.4 s |
| `Datastore/tests` | 0 (none) | **17**, OK | 1.2 s |

The counts match log 03's "After" (18 / 41 / 17). The pinned-abundance lines the suite prints match
log 02's to every digit (for example `test_network_flag (b)`: small Yp 0.2540780344, D/H x1e5
2.670892604, ⁷Li/H x1e10 5.1424297; full Yp 0.2540937879, D/H x1e5 2.671499971, ⁷Li/H x1e10
5.091224307).

### README §6 rows

Every row is in the addendum's table (§4.7) with its final value and witness. **No row differs
from the value its prompt's log quotes.** No row is below target. The rows shown to fail on
`HEAD~1` were not re-shown here; the logs did that, and the addendum says so.

### Greps (I ran them)

```
$ grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/"
config/version.py:33:VERSION_LABEL = "2026.4.0"

$ grep -n PRYM_VERSION ComputeTargets/BBNData.py
42:PRYM_VERSION = "bf24c3d+cham03+ri02"
367:        "PRyM_version": PRYM_VERSION,
543:        "PRyM_version": PRYM_VERSION,  # PRyMordial seems not to have a proper versioning scheme
```

Also: `grep -c "= solve_ivp(" PRyM/PRyM_main.py` = 8 and `grep -c "^ *_check_solve_ivp(sol_"` = 8;
the `PRyM/` diff has ten marked insertions, none deleted; `main.py:632` passes `"failure": None`;
`config/argument_parser.py:246` defines `--retry-failed-bbn`; the three scripts import the label
(`main.py:65`, `plot_by_beta.py:49`, `plot_ScalarModel.py:57`); the only `except` added in
`ComputeTargets/BBNData.py` is `:336`, inside `_run_PRyMordial`.

### Scope check

`git diff --stat 27a32bc..HEAD`: 41 files, 4965 insertions, 91 deletions. Against the board's
"Code" list and logs 01–03:

- **Production:** `Datastore/SQL/Datastore.py` (+34), the three factories (`ScalarModel`,
  `AdiabaticHistory`, `BBNData`), `config/version.py` and `config/argument_parser.py`, `main.py`,
  `plot_by_beta.py` and `plot_ScalarModel.py` (one line each: the import), `pipeline_selection.py`,
  `ComputeTargets/BBNData.py`, `PRyM/PRyM_main.py` (+41, no deletions). All on the list.
- **Tests:** `Datastore/tests/` (three files), `ComputeTargets/tests/test_bbn_solver_failures.py`
  and `test_pipeline_selection.py`. No pre-existing test file is in the diff.
- **Documents, additive only (no deletions):** `.documents/architecture-summary.md` (+32),
  `numerical-strategies.md` (+50), `OPEN_ISSUES.md`, `prompts/INDEX.md`,
  `prompts/review-remediation/IMPLEMENTATION_STATE.md` (+20, no deletions: the three **Resolved**
  lines and three **Assigned** lines), `CLAUDE.md` (one test command and the SQLite allowance).
- **Campaign material:** the README, prompts, board, logs, orchestrator prompts and planning probes
  under `prompts/run-integrity/`.

Not in the diff: `thirdparty/`, any `register()` column, `base.py`,
`CosmologyModels/`, `Xav_EOS_data.csv`. **Every file is accounted for. None is outside the lists.**

### The three planning probes, re-run

1. **`datastore_version_probe.py`** (read its output, not its captions):
   ```
   [1] ScalarModel under the label that stored it: available=True, store_id=1
   [2] version serials: 2026.3.0 -> 1, 2026.4.0 -> 2
   [3] ScalarModel under a NEW label: available=False, store_id=None
   [4] BBNData, default lookup (failure=False, what main.py uses): available=False
   [5] BBNData, failure=True under the NEW label: available=False, reason=None
   [6] BBNData, failure=None, two failed rows: available=False
   ```
   [3] and [5] are "not returned", and [6] does not raise. [6] is invisible only because the probe
   stores under 2026.3.0 and looks up under 2026.4.0 (prompt 01's keying), so it no longer
   witnesses the `failure=None` defect; test (a) of `Datastore/tests/test_bbn_failure_lookup.py`
   does, as log 03 says.
2. **`prymordial_solver_probe.py truncated`, as committed** (it truncates line 1252, which the
   patch moved):
   ```
   == SM baseline, last solve fails after 1 % of its span (8.2 s): returned Yp=0.2468872958, D/H x1e5=2.462251065, 7Li/H x1e10=5.423441017
   ```
   Every solve, including `PRyM_main.py:1291`, reports `status=0 success=True`. Nothing is
   truncated; it is the SM baseline.
3. **The same probe, from a scratch copy** in the session scratchpad, outside the repository, with
   only `truncate_line` changed. `grep -n "sol_at_LT = solve_ivp(" PRyM/PRyM_main.py` finds
   `:1226` and `:1291`, so the number is 1291. The `diff` between the probe and the copy:
   ```
   103c103
   <     truncate_line = 1252
   ---
   >     truncate_line = 1291
   ```
   It printed:
   ```
   == SM baseline, last solve fails after 1 % of its span (7.6 s): raised PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)': status=-1, message='forced failure (probe)
      PRyM_main.py:1291 BDF: status=-1 success=False t reached 1.328e+04 of 1.316e+06
   ```
   That is the new class, naming `'low-T nuclear network (full)'`. The probe itself is unedited.
   (A first attempt to make the copy, with GNU `sed -i`, failed on macOS and left the copy
   unchanged; its output was the baseline. The copy was redone with `sed -i ''` and the `diff`
   above is of the working copy.)
4. **`prymordial_solver_probe.py nan`** (unchanged; 60 s): `PRyM_main.py:616` entered with
   `t_span=[array(nan), array(0.74499227)]`, `NO RETURN within 60 s`. It calls PRyMordial with its
   own callback, bypassing `build_NP_callbacks`, so the guard is not in its path. It measures
   PRyMordial, not ChamPBH. Only the target of §6.2's NaN rows is re-measured, by the suite, as
   log 02's Deviation 5 and the board's Decisions say.
5. **`pairing_probe.py`** (unchanged by design): `main.py schedules BBN for: ['V1', 'V3']`; the
   correct set `['V2', 'V4']`. The new helper's {V2, V4} is test (d) of `test_pipeline_selection`.

### `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]`: the three claims, on `6fd9017`

I read each claim against the code. Line numbers are from `6fd9017`; the prompt's are from
`bacddd8`.

1. **Holds.** `Datastore.object_store` opens one `self._engine.begin()` (`Datastore.py:606`),
   calls each factory's `store` inside it (the row, then the tags and the values, for `BBNData`:
   `ObjectFactories/BBNData.py:305–345`), and calls `conn.commit()` once (`:636`).
   `object_validate` opens its own transaction (`:719`, `conn.commit()` at `:741`). An interrupted
   run therefore leaves no row, or a complete row with `validated=False`.
2. **Holds.** `BBNData.store` writes `z_samples = None` for a failure
   (`ObjectFactories/BBNData.py:305`). `validate()` sets `validated = True` for a failure
   (`:357–359`). `validate_on_startup` lists only `failure == False` rows (`:403`). Line numbers
   have moved from the prompt's `:349–351` and `:387–400`; the content is the same.
3. **Does not hold as stated.** Its first sentence holds: an unvalidated row with values missing
   arises only if `validate()`'s count failed after the store, and `validate()` prints
   `!! WARNING: ... did not validate` (`:377`). The three sub-claims:
   - **Startup warning and prune: holds.** `Datastore._validate_on_startup` (`Datastore.py:441–466`)
     prints the `INTEGRITY WARNING` and lists such rows; `--prune-unvalidated` deletes them
     (`BBNData.py:430–`, `AdiabaticHistory.py:362–`).
   - **"`main.py`'s adiabatic and BBN lookups pass `_do_not_populate`, so there such a row counts
     as done without a word": holds for BBN only.** Both payloads pass the key (`main.py:378`,
     `:633`; the first-pass lookup at `:239` passes it as a keyword). `BBNData.build` honours it
     (`:203`, `:204`), so the BBN count check at `:259–262` is skipped. **`AdiabaticHistory.build`
     never reads the key** (`grep -n "populate" Datastore/SQL/ObjectFactories/AdiabaticHistory.py`
     finds only a comment at `:171`). It always loads the values and raises `RuntimeError`
     "Fewer z-samples than expected" at `:226–229` (`z_samples` is `nullable=False`, `:119`). So
     for `AdiabaticHistory`, an unvalidated row with a short count does not "count as done without
     a word": it stops `main.py`'s lookup with that exception.
   - **"A populated read raises ... That includes `plot_by_beta.py`'s reads": holds for
     `AdiabaticHistory` only.** `plot_by_beta.py`'s `BBNData` lookups pass `_do_not_populate`
     (`:614`, `:689`, `:719`, `:746`), so they are not populated and do not raise. Its
     `AdiabaticHistory` lookup (`:689`, key ignored) does.
   - **What the code shows instead.** A short-count row is served silently only by the `BBNData`
     lookups that pass `_do_not_populate` (`main.py`, `plot_by_beta.py`). The `AdiabaticHistory`
     lookup raises on it, in every caller. The issue's existing **Impact** says the same ("For
     `AdiabaticHistory` with a partial value set, `build()` then raises"); it is the orchestrator's
     "there such a row counts as done" for adiabatic that is wrong.
   - **Consequence.** Per prompt §3, the **Narrowed** line was not added, and the entry and its
     index hook are unchanged. The issue stays open and unassigned; the count is unchanged. Claims
     1 and 2 and the first sentence of claim 3 are a sound narrowing of "*partial* row", and the
     board may want that recorded when claim 3 is restated. **That is for the user.**

### Formatting

No Python file changed, so `black` had nothing to format.

### What was reasoned, not run

The claims above were read from the code, not exercised. No pipeline run, no Ray, no datastore
beyond the suites' temporary SQLite files.

## Observations not acted on

- **`AdiabaticHistory.build` ignores `_do_not_populate`, though `main.py` and `plot_by_beta.py`
  pass it** (`main.py:378`, `plot_by_beta.py:689`; the key is read only by `BBNData.build`, `:203`).
  Every adiabatic lookup loads every value row, which costs a full read per model per run, and the
  key reads as if it saved that. No wrong result. Opened as
  `[04-adiabatichistory-lookup-ignores-do-not-populate]` on the board's §3, with an index row in
  `.documents/OPEN_ISSUES.md` §1.5.
- **The narrowed reading's claim 3**, above, for the user's decision.

## State handed to the next prompt

There is no next prompt: this closes the campaign.

- **The addendum's heading** is `### 4.7 Addendum 2026-09-30 — the \`run-integrity\` campaign` in
  `.documents/review-remediation-verification.md`, after §4.6 and before §5.
- **The final tree** is `6fd9017` for the production code; the close-out commit adds documents only.
- **The one command that reproduces the whole verification** (about 4 minutes), from the repository
  root:
  ```bash
  PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . && PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . && PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/datastore_version_probe.py && PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/pairing_probe.py && grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/" && grep -n PRYM_VERSION ComputeTargets/BBNData.py && git diff --stat 27a32bc..HEAD
  ```
  The solver probe needs the scratch copy described above (`truncate_line = 1291`), so it is not
  in the one-liner.
- **Suites:** 18 / 41 / 17, all OK.
- **Open, unassigned, on this board (§3):** the five issues listed in `.documents/OPEN_ISSUES.md`
  §1.5. `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]` is **not** narrowed, for the
  reason under "Verification".
- **Every store made before 2026.4.0 is invalid, and a lookup no longer returns its rows.**
