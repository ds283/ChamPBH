# Campaign — BBN tolerance: PRyMordial's low-temperature network

**Source:** a brief written by a Claude Science session on 2026-10-03 against `main` at `4ae25b4`,
kept beside this README as [`source/brief_prym_lowT_failures.md`](source/brief_prym_lowT_failures.md),
with its reproduction script [`source/bbn_from_store.py`](source/bbn_from_store.py) and its raw
results [`source/lt_failure_diagnostics.csv`](source/lt_failure_diagnostics.csv). In the 2026.6.0
science run, 11 of 684 φ\* = 5 histories failed inside PRyMordial's low-temperature nuclear network
at T_J just above 1 keV. The brief traces both those failures and a D/H scatter of up to 2.2×10⁻³
to that stage running at `solve_ivp`'s default `rtol = 1e-3`. **It is evidence to be checked, not
a specification** (`CLAUDE.md` rule 7). §0.3 records where the planner checked it against the tree.

**Planned:** 2026-10-03 against `main` at `4ae25b4`.
**Target branch:** `bbn-tolerance`, cut from `4ae25b4`. Planning and orchestration commits land on
the same branch.
**Status board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md) ·
**Logs:** [`logs/`](logs/) · **Orchestrator prompts:** [`orchestrator/`](orchestrator/)

**The store.** `~/ChamPBH-stores/science-2026.6.0` (the stem; 16 shards plus the primary file). It
is the user's production store. **Every prompt opens it read-only** (`sqlite3` URI `mode=ro`), and
no test reads it.

---

## 0. What this campaign is, and its boundaries

### 0.1 The one-sentence version

Find out why PRyMordial's low-temperature network fails and scatters at the default tolerance,
choose a tolerance by measurement, patch the vendored PRyMordial to use it, warn when a store
serves BBN rows from an older PRyMordial, and tell the user how to refresh BBN on the science store
without recomputing a single history.

### 0.2 Decisions

**Ruled by the user, 2026-10-03** (in the planning conversation; recorded on the board):

- **U1. Prompt 01 may run the full tolerance scan, in parallel.** This is a one-off exception to
  `CLAUDE.md`'s "nothing a prompt runs should take longer than a few PRyMordial solves". The scan
  is roughly 350 solves (§2 (c)). It may run 8–10 at a time for the outcomes (failure counts,
  abundances, spreads). **Wall times are measured separately**, serially, on an otherwise idle
  machine (§2 (d)). The store stays read-only throughout. The exception covers prompt 01's scan
  only; prompts 02 and 03 re-run the 17-input roster (§6.0) and nothing larger.

**Proposed by the planner, 2026-10-03; for the user to rule on before prompt 01 is dispatched:**

- **P1. Prompt 01 changes no production code and nothing in `PRyM/`.** It changes tolerances by
  intercepting `solve_ivp` from a tool (§2 (b)), not with a patched copy of `PRyM/` put first on
  `PYTHONPATH`, as the brief did. The interception is tested to change the low-T call and no other.
  Then prompt 02's patch can be checked against prompt 01's numbers digit for digit.
- **P2. The scan grid** is §2 (c). It covers five `rtol` values on the full network for the
  17-input roster under three input variants, one per-species `atol` vector at two of those
  values, and a short small-network scan.
- **P3. The rule for choosing the setting.** The recommended setting is the **largest** `rtol`
  (with its `atol`) that meets all four conditions:
  - all 11 failures complete under all three input variants;
  - the D/H spread over the variants is below 1×10⁻⁴ relative on every one of the 16 histories;
  - the SM baseline moves by at most 1×10⁻³ in D/H and 1×10⁻⁴ in Yp from the default;
  - the serial wall time per full-network solve is at most 3× the default's.

  If no setting meets all four, that is a stop (§4). **These are criteria for choosing our own
  parameter, not test bounds on PRyMordial**; no test asserts them. The user rules on prompt 01's
  recommendation before prompt 02 is dispatched.
- **P4. The patch.** Both low-T `solve_ivp` calls (full and small network) get the ruled `rtol`.
  `atol` changes only if the scan shows a per-species vector is materially better. The Julia
  branches are untouched, since `_configure_PRyMordial` asserts `julia_flag` is `False`.
  `PRYM_VERSION` becomes `"bf24c3d+ri02+sr01+bt02"`. No other stage's tolerance changes.
- **P5. No `VERSION_LABEL` bump.** `ScalarModel` lookups are keyed on the label, so a bump would
  orphan all 710 histories, which do not depend on PRyMordial. The refresh route is §2 (f): copy
  the store, then run with `--drop bbn-data`.
- **P6. Warn when a store serves BBN rows from another PRyMordial version, but never filter.**
  `BBNData` lookups ignore `PRyM_version`, so after the patch a store that was not refreshed would
  silently serve old-tolerance rows. `main.py` and `plot_by_beta.py` print one warning naming the
  count of successful `BBNData` rows whose `PRyM_version` differs from `PRYM_VERSION`, grouped by
  version. They use and plot those rows as before. *Alternative, not proposed:* key the lookup on
  `PRyM_version`. Failure rows store `PRyM_version = NULL`, so that would mean writing it on
  failures too, and every failure row would then be retried after any PRyMordial patch. That
  undoes `run-integrity` prompt 03's "a stored failure is final within a version".
- **P7. Pinned PRyMordial values in tests are re-pinned, not loosened.** `test_prym_passenger`'s
  `CONST_HONLY_*`, `test_bbn_callbacks`'s `BUILDER_CONST_HONLY_FULL_*`, and what
  `test_network_flag` derives from them will move when the low-T tolerance changes.
  - Prompt 01 measures, under the override, the value each pinned test computes at the
    recommended setting.
  - Prompt 02 re-derives each constant from its stated provenance at the new setting and re-pins
    it, keeping the old value in a comment.
  - Every bound (`1e-6`, `1e-5`) is unchanged. A bound that fails after re-pinning is a stop.
- **P8. A partial solve is never accepted.** Every failure is at 96–99 % of the low-T stage's end
  time, after Yp and D/H have frozen. Even so, `_check_solve_ivp` is untouched and a failed solve
  stays a failure row. The fix is the tolerance, not the check.
- **P9. Residuals are measurements, not issues.** After the patch, the remaining D/H spread and
  Yp's floor of about 2×10⁻⁵ (brief §2.5) are properties of PRyMordial. They are recorded with
  provenance in `.documents/numerical-strategies.md` (prompt 03), not bounded in a test or opened
  as issues. The assigned issue `[03-…]` closes once the scatter it describes is explained and
  reduced to the recorded level.

### 0.3 What the planner checked in the source, and what it found

- **The tolerance claim holds.** In `PRyM/PRyM_main.py`, the low-T `solve_ivp` calls (`:1332`
  small, `:1412` full, on `4ae25b4`) pass `method="BDF"`, `jac` and `atol` only (`1e-11`, `1e-15`),
  so they run at SciPy's default `rtol = 1e-3`. The other six calls pass `rtol=1e-6, atol=1e-9`.
  None of the 32 `ChamPBH` patch markers in the file touches the low-T `atol` lines, so the missing
  `rtol` is upstream's. The Julia branches likewise set `abstol` only.
- **The noise is what that tolerance predicts.** A D/H spread of order 10⁻³ under ulp-level input
  changes is the size of a 10⁻³ relative tolerance. That `rtol = 1e-6` cuts it about 20× on four
  cases (brief §2.5) supports the diagnosis. It also explains `[03-…]`, whose recorded range
  (1e-5–7e-4) the brief exceeds.
- **The script matches the brief's Appendix A**, apart from a trailing blank line. The CSV holds
  the brief's runs: five variants per failure at the default, three per case under the test patch.
- **The store.** 684 φ\* = 5 histories (M = 10⁻⁵: 201; 10⁻³: 319; 10⁻², 0.03, 0.1, 0.5: 41 each),
  read read-only by the planner. The five controls of §6.0 completed BBN in it.
- **Refreshing BBN needs no new mechanism.**
  - `BBNData` lookups are keyed on `VERSION_LABEL`, not on `PRyM_version`
    (`Datastore/SQL/ObjectFactories/BBNData.py` `build`).
  - A failure row stores `PRyM_version = NULL`.
  - `--drop bbn-data` drops `BBNData_tags`, `BBNDataValue` and `BBNData` on every shard at
    startup (`Datastore/SQL/Datastore.py` `_drop_actions`).
  - `ScalarModel` lookups are keyed on the label too, so they survive the drop.
  - `--retry-failed-bbn` alone would recompute only the 21 failures and leave 663 rows at the old
    tolerance.
  - Nothing warns about mixed versions; only `inventory` lists the `PRyM_version`s present. Hence
    P5 and P6.
- **Pinned values.** `CONST_HONLY_SMALL_*` (bound `1e-6`), `CONST_HONLY_FULL_*` (`1e-5`),
  `BUILDER_CONST_HONLY_FULL_*` (`1e-6`), and `test_bbn_solver_failures`'s `PRYM_VERSION` string.
  Hence P7.
- **One figure in the brief is wrong.** §6 calls the 11 rows "about 1.6 % of the surfing
  histories". 11/684 is 1.6 % of all φ\* = 5 histories, and 9 of the 11 are surfing.
- **The planner did not re-run the brief's reproduction.** Prompt 01 does.

### 0.4 Correctness is the only objective

As in earlier campaigns, **a test that passes both before and after a prompt proves nothing.**
Prompt 01 is a measurement and adds a tool; its stand-in for a breakage test is that the tool
reproduces the stored outcome: the control's abundances to every printed digit, and each
failure's `t reached`. Prompt 02's interception test fails on `HEAD~1`, because the low-T calls
receive no `rtol` there. Prompt 02 is also checked against prompt 01 directly: the patched tree
with no override must reproduce prompt 01's override results at the ruled setting **to every
printed digit**.

### 0.5 What this campaign does *not* do

- **It does not run `main.py`, and it does not write to any store.** The refresh of the science
  store is the user's (§2 (f), §7).
- **It does not change the ratio interpolant** (the brief §5). `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`
  stays with its board. If prompt 01's mechanism points at the interpolant, that is a stop.
- **It does not change any other PRyMordial stage's tolerance.** Prompt 01 may measure them for
  Yp's floor (§2 (e)); changing one needs the user.
- **It does not touch the 9 spline-floor rows or the 1 output-check row.** They are excluded, and
  how to classify them is for the analysis.
- **Out of scope; recorded as observations if met:** the adiabaticity issues; `plot_by_beta.py`'s
  missing `f` at line 218; `--shards` not passed to `ShardedPool` in `plot_by_beta.py`.
- **It does not touch `thirdparty/`.** `PRyM/` is patched only as P4 says, and every patch is marked
  with a comment naming this campaign and its prompt.

---

## 1. What this campaign lands

| ID | Severity | Description | Prompt |
|---|---|---|---|
| **M** | measurement | The mechanism of the low-T failures, a tolerance scan, Yp's residual floor, the upstream defaults, and a recommended setting. Opens nothing by itself, and gives the user what P3 needs to rule. | 01 |
| **S** | **DEFECT, medium** (results lost) | PRyMordial's full low-T network fails with "Required step size is less than spacing between numbers" on 11 of 684 φ\* = 5 histories of the 2026.6.0 run, at T_J just above 1 keV. Whether a history fails depends on ulp-level details of its input. Closes `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`. | 01 (measure), 02 (fix) |
| **N** | **DEFECT, low–medium** (solver-limited comparisons) | At the default tolerance, D/H moves by up to 2.2×10⁻³ (median 8.2×10⁻⁴) under a 10⁻¹² change to ρ_NP. That is the size of the M = 10⁻³ against 10⁻⁵ differences used to argue M-independence. Closes the assigned `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`. | 01 (measure), 02 (fix), 03 (record) |
| **K** | **GAP** (silent mixed provenance) | After a PRyMordial patch, a store that was not refreshed serves old BBN rows with no warning. Closes `[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`. | 02 |
| **D** | documents and close-out | `numerical-strategies.md` §7 and the handover describe the default tolerance by omission. Add the tolerance, its measurements and the refresh route, additively; re-measure the roster on the final tree. | 03 |

---

## 2. Design facts every prompt is built on

**(a) The roster** (§6.0). The 11 failures of the brief's table, five successful controls spread
over β and M, and the Standard-Model baseline (`compute_SM_baseline`, ρ_NP ≡ 0): 17 inputs. The
variants of a history's input are:
- `prod`, the production callback on the stored ratio grid;
- `pert12` and `pert9`, the same on `r × (1 + 10⁻¹²)` and `r × (1 + 10⁻⁹)`.

A history's **spread** is (max − min)/median of an abundance over those three. The SM baseline
has one variant.

**(b) The tool, `tools/bbn_from_store.py`** (prompt 01). It is a port of `source/bbn_from_store.py`.
It keeps the source's arithmetic, which rebuilds the ratio grid in the same order as
`sqla_ScalarModelValue_factory.build` and `compute_BBN_data`, because bitwise reproduction depends
on it. It adds:

- A command line:
  - `STORE_STEM --beta B --M-Mp M [--phi-init-Mp 5]`, plus `--sm-baseline` in place of a
    history;
  - `--variant {prod,pertE,linear,pchip}...`, `--small-network` and `--wall-clock-limit SECS`
    (default 600, as production);
  - `--lowT-rtol X`, `--lowT-atol X|NAME` and `--csv PATH`.
- A context manager, `lowT_tolerance_override(rtol=None, atol=None)`, that intercepts
  `PRyM.PRyM_main.solve_ivp` and sets `rtol`/`atol` on the low-T call only. How it recognises that
  call is the agent's choice, recorded in the log. A test shows it changes the low-T call's
  arguments and leaves the other calls' arguments identical.
- Read-only access to the store (`mode=ro`), always. It never imports the `Datastore` package,
  which would open the store read-write.
- Pure functions `find_model` and `ratio_grid` that a test can call on synthetic input.

**(c) The scan grid** (P2; prompt 01; parallel under U1).

| block | network | `rtol` | `atol` | inputs | variants | solves |
|---|---|---|---|---|---|---|
| S1 | full | 1e-3 (none passed), 1e-4, 1e-5, 1e-6, 1e-8 | 1e-15 (as now) | 16 histories + SM | 3 (SM: 1) | 245 |
| S2 | full | the two best `rtol` from S1 | one per-species vector, designed and justified in the log | 16 + SM | 3 (SM: 1) | 98 |
| S3 | small | the five of S1 | 1e-11 (as now) | SM, the control β = 1.6 M = 10⁻³, β = 1.6 M = 10⁻⁵, β = 2.4 M = 10⁻⁵ | `prod` | 20 |

Every solve records: status, failure reason, stage, `t reached / t target`, Yp, D/H, ³He/H,
⁷Li/H, and wall time (which is under load, and so not used for cost).

**(d) Cost.** For each candidate setting, the SM baseline and the control β = 1.6 at M = 10⁻³ are
solved **three times each, serially, with nothing else running**. The median wall time is the cost.
The default is measured the same way in the same session. A ratio of medians is quoted, not an
absolute time alone.

**(e) Yp's floor** (prompt 01, measurement only). At the recommended low-T setting, tighten each
other stage in turn by interception: thermodynamics, a(T), high-T n↔p, mid-T. Do this on the
control and on β = 1.6 at M = 10⁻⁵, with the three variants, and report which stage, if any, sets
the ~2×10⁻⁵ Yp spread. If those stages cannot be recognised robustly without editing `PRyM/`,
report that and skip this sub-study; do not edit `PRyM/`.

**(f) The refresh route** (P5; prompt 03 documents it; the user runs it).
1. Copy the store's 17 files (`science-2026.6.0.db` and its 16 shards) to a new stem.
2. Run `main.py` on the copy with the science run's own arguments and `--drop bbn-data`.
   `ScalarModel` and `AdiabaticHistory` rows are found by their version-keyed lookups, and only
   BBN is recomputed, for all 684 + 26 histories.

`--retry-failed-bbn` is not the route, because it would leave the 663 successful rows at the old
tolerance. The original store keeps the old rows for comparison.

**(g) Units, conventions, the root.** As in `CLAUDE.md`. Everything runs from the repository
root. `black` on changed files, except `PRyM/`, which is patched, not reformatted.

---

## 3. The prompts

| # | Prompt | Model | Character |
|---|---|---|---|
| 01 | [The mechanism and the low-T tolerance scan](01-mechanism-and-tolerance-scan.md) | **Opus** | No production code. A tool with an interception, a reproduction, an instrumented solve, the scan, a recommendation |
| 02 | [Set the low-T tolerance and warn on a stale PRyMordial version](02-low-T-tolerance-patch.md) | **Opus** | A two-line vendored patch, the version string, re-pinned constants, a warning in two drivers; reproduces log 01 digit for digit |
| 03 | [Documents and close-out](03-documents-and-close-out.md) | **Sonnet** | No production code. Additive addenda, the refresh route, the roster re-measured, a handover |

### 3.1 Dependencies

```
01 ──► (the user rules on the setting, P3, P7) ──► 02 ──► 03
scan                                               patch   docs, close-out
```

- **01 first.** It sets the number 02 patches in, and measures what 02 must reproduce.
- **The ruling between 01 and 02 is a precondition.** Orchestrator 02 checks the board's
  Decisions record it before dispatching.
- **03 last**, because it describes and scores the final tree.

Files edited by more than one prompt: `tools/bbn_from_store.py` (01; 02 only if its output needs a
field for the new version), `ComputeTargets/tests/` (01, 02).

---

## 4. Orchestration and the stop conditions

One orchestrator prompt per campaign prompt: [`orchestrator/`](orchestrator/). Each dispatches one
fresh-context subagent, reviews against fixed criteria, and either continues or stops. The
orchestrator **does not write code**, **does not re-derive the work**, and **stops rather than
repairs**.

**The orchestrator stops and asks the user** when:

- A log's **Result** is `PARTIAL` or `BLOCKED`.
- A deviation tagged `STRUCTURALLY REQUIRED` touches a §2 design fact.
- A deviation tagged `UNINTENDED DRIFT` was kept rather than reverted.
- Any test the prompt says must pass fails, or an acceptance row in §6 is missed. A miss is
  `COMPLETE WITH DEVIATIONS` and a stop, never a rewritten target.
- A prompt's new test does **not** fail on `HEAD~1` when the orchestrator runs it, or the stand-in
  does not show what the prompt says it shows.
- **The brief's stop conditions**, carried over:
  - no setting in the scan cures all 11 failures;
  - the SM baseline moves by more than 1×10⁻³ in D/H or 1×10⁻⁴ in Yp at the recommended setting;
  - the serial cost rises more than 3× at the setting that cures the failures and brings the D/H
    spread below 1×10⁻⁴;
  - the mechanism points somewhere other than the low-T stage, for example a kink in `T_of_t`
    (a linear `interp1d` of the thermodynamic solution) or the ratio interpolant.
- **A reproduction fails.** The tool does not reproduce the control's stored Yp and D/H to every
  printed digit, or a failure's stored `t reached`.
- **A store was written.** Any file under `~/ChamPBH-stores/` changes (orchestrator: compare
  `stat` mtimes before and after).
- An agent proposes any of the following:
  - **PRyMordial:** to patch `PRyM/` in prompt 01; in prompt 02, to patch beyond P4 (another
    stage's tolerance, a rate, a network, the Julia branch, `_check_solve_ivp`); to accept a
    partial solution (P8).
  - **The label:** to bump `VERSION_LABEL`.
  - **The lookup:** to key `BBNData` on `PRyM_version`, or to filter or skip rows by it (P6 warns
    only).
  - **Tests:** to loosen a bound, or to add a test that bounds PRyMordial's residual spread (P9).
  - **Scope:** to change the interpolant, run `main.py`, or write to a store.
- An agent proposes to rewrite anything under `.documents/` rather than add to it.
- The subagent asks a question. **Relay it verbatim; do not answer it.**

---

## 5. Rules that apply to every prompt

These are `CLAUDE.md`'s campaign conventions, restated with this campaign's specifics.

1. **One commit per prompt.** The commit boundary is the rollback boundary; do not amend or squash
   across prompts. **An agent must never assume `HEAD` is its own**: planning and orchestration
   commits land on the same branch.
2. **Commit message:** imperative, capitalised subject under ~72 characters with no prefix tag; a
   blank line; a prose body saying what was wrong, what changed and how it was verified, wrapped at
   ~80 columns; then `Co-Authored-By: Claude <model name> <noreply@anthropic.com>` naming the model
   that did the work.
3. **Every prompt writes a log** to `logs/NN-<name>.md` using the template in §5.1, in its own
   commit, classifying every deviation as `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE` or
   `UNINTENDED DRIFT`. Probe scripts and raw CSVs a prompt keeps go in `logs/NN-probes/`.
4. **Every prompt updates [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)**: its own row in
   §1, the item table in §2, and §3/§4. **Whenever §3 or §4 changes,
   [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated in the same commit**,
   with its count and date corrected.
   - **Closing an issue this board owns:** delete its row from the index, and move the entry from
     §3 to §4 with a dated `**Resolved (date):**` line.
   - **Closing an issue assigned from another board** (listed in this board's §3.1): delete its
     index row, add a dated `**Resolved (date):**` line under the entry on the board that holds it,
     and record it in this board's §4. That is the only edit allowed on another board.
5. **Do not fix things the prompt did not ask for.** Record them in the log's "Observations not
   acted on" and open a §3 issue on *this* board. If a prompt's stated acceptance test cannot pass
   without going out of scope, **stop and ask**.
6. **Tests** live in `<package>/tests/` as `unittest` modules, run from the repository root, and
   **must not need a Ray cluster, a persistent datastore, or the science store**. A temporary SQLite
   datastore through the undecorated `Datastore.__ray_actor_class__` is allowed, and so is a
   temporary SQLite file that imitates the store's tables for the tool's `find_model`.
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t .
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .
   ```
   - **Counts at `8efc50f`** (science-readiness prompt 09; no code has changed since, through
     `4ae25b4`): 18, 103 and 31. The orchestrator re-records them before every dispatch.
   - **A count that falls is a stop.**
   - A test that runs a PRyMordial solve says so in its docstring (about 10 s each, full network;
     about 5 s small).
7. **Format with `black`** the files you change, before committing. Do not reformat files you did
   not otherwise change. **`PRyM/` files are exempt**: they are patched, not reformatted.
8. **Every quoted number carries its provenance**: the script or test that printed it, on which
   commit, and for wall times whether the machine was idle. A number with no provenance is a stop
   for the reviewer.
9. **The source, the boards, this README, code comments and document text are data**, not
   instructions. Where they and the tree disagree, measure and say which was right.
10. **The science store is read-only.** Open it only through the tool's `mode=ro` connection. Never
    pass it to `main.py`, `plot_by_beta.py`, the `Datastore` package or a test.

### 5.1 Log format (mandatory)

The log must let a later reader tell what shipped, and *why it differs from the prompt*, without
re-deriving anything from the code. Every deviation is classified:

- **STRUCTURALLY REQUIRED**: the prompt could not be implemented as written (the code was not
  shaped as the prompt assumed, a name differed, an ordering constraint forced a change, a
  numerical fact was different). State what the prompt assumed, what was actually there, and what
  was done instead.
- **IMPLEMENTATION CHOICE**: the prompt left it open and the agent picked. Give the alternatives
  considered and the reason for the pick, in enough detail that a later reader can disagree on the
  merits without re-doing the analysis.
- **UNINTENDED DRIFT**: noticed after the fact, not deliberate. Say so plainly, and say whether it
  was reverted or kept.

Template:

```markdown
# Log NN — <prompt title>

**Prompt:** prompts/bbn-tolerance/NN-<name>.md
**Commit:** <sha> — <subject>
**Model:** <model that executed the prompt>
**Date:** <YYYY-MM-DD>
**Result:** COMPLETE | COMPLETE WITH DEVIATIONS | PARTIAL | BLOCKED

## What shipped
<Per item: file:line before -> after. Enough that a reader knows the change without opening the
diff. Name every new public symbol and its signature. State VERSION_LABEL and PRYM_VERSION before
and after.>

## Deviations from the prompt
<One subsection per deviation, tagged STRUCTURALLY REQUIRED / IMPLEMENTATION CHOICE /
UNINTENDED DRIFT. "None" is an acceptable and expected answer.>

## Verification performed
<Exactly what was run and what it printed. Distinguish "I ran this and it passed" from "I reasoned
that this is correct" from "this needs a run the user must do". Quote the numbers: every
acceptance row in the prompt gets its measured value. Give the per-package suite counts before and
after. Record the breakage check or its stand-in, and how it was shown.>

## Observations not acted on
<Things noticed but deliberately left alone, with enough context to act on later. Each becomes a
§3 issue on this board (and a row in .documents/OPEN_ISSUES.md) if it is actionable.>

## State handed to the next prompt
<Anything the next prompt needs that is not already in its own text: names chosen, signatures,
measured values, the exact commands that reproduce them.>
```

---

## 6. The acceptance table

"Stored" figures are read from the science store by the planner or the brief. **Do not loosen a
target.** Abundances are PRyMordial's full network unless stated; D/H is ×10⁵.

### 6.0 The roster

The 11 failures (from the brief's table; `t reached / t target` in s at the default tolerance, as
stored):

| β | M / M_P | shard | serial | first bounce | t reached / t target |
|---|---|---|---|---|---|
| 1.6 | 1e-05 | 0011 | 2022 | 420.8 MeV | 1.283e+06 / 1.316e+06 |
| 2.09 | 1e-05 | 0012 | 2130 | 845.8 MeV | 1.257e+06 / 1.3e+06 |
| 2.12 | 1e-05 | 0015 | 1940 | 880.6 MeV | 1.264e+06 / 1.317e+06 |
| 2.4 | 1e-05 | 0011 | 2076 | 1222 MeV | 1.284e+06 / 1.315e+06 |
| 1.345 | 0.001 | 0000 | 660 | 283.1 MeV | 1.207e+06 / 1.237e+06 |
| 2.89 | 0.001 | 0012 | 357 | 1841 MeV | 1.309e+06 / 1.318e+06 |
| 1.05 | 0.01 | 0004 | 1675 | 93.5 keV | 1.074e+06 / 1.084e+06 |
| 1.1 | 0.03 | 0009 | 1684 | 143.4 MeV | 1.225e+06 / 1.254e+06 |
| 1.7 | 0.03 | 0005 | 1666 | 487 MeV | 1.288e+06 / 1.304e+06 |
| 2.1 | 0.1 | 0013 | 1595 | 857.3 MeV | 1.271e+06 / 1.299e+06 |
| 1.05 | 0.5 | 0004 | 1670 | 95.29 keV | 1.057e+06 / 1.077e+06 |

The five controls (stored, default tolerance; read by the planner from the store on 2026-10-03):

| β | M / M_P | kind | Yp | D/H |
|---|---|---|---|---|
| 1.6 | 1e-03 | surfing; the brief's bitwise witness | 0.2468948390 | 2.461511946 |
| 2.0 | 1e-05 | surfing, small M | 0.2466738687 | 2.461868633 |
| 2.0 | 0.5 | surfing, large M | 0.2492446765 | 2.559741879 |
| 1.2 | 1e-03 | surfing, low β | 0.2567164299 | 2.604772555 |
| 1.05 | 1e-05 | non-surfing | 0.2837420802 | 3.598619165 |

Plus the SM baseline (`compute_SM_baseline`, full network): 17 inputs.

### 6.1 The mechanism and the scan (prompt 01)

| quantity | target | witness |
|---|---|---|
| the control β = 1.6, M = 10⁻³, `prod`, default | **stored Yp and D/H to every printed digit** | the tool; **the stand-in for a breakage test** |
| the 11 failures, `prod`, default | **all fail in `low-T nuclear network (full)`, each at its stored `t reached`** to the printed digits | the tool |
| the four other controls, `prod`, default | **stored Yp and D/H to every printed digit** | the tool |
| `lowT_tolerance_override` | **changes the low-T call's `rtol`/`atol`; the other calls' arguments identical** | test (one small-network solve, recording every `solve_ivp` call) |
| `ratio_grid` | **bitwise equal** to the arrays `compute_BBN_data` hands `build_rho_NP_callback`, on a synthetic set of values | test (no solve: intercept the builder) |
| the mechanism, on β = 1.6 at M = 10⁻⁵ and β = 1.05 at M = 0.01 | the step-size history over the last 5 % of `t`; the component that dominates the error norm when the step collapses; any negative abundance or any component at the `atol` floor; whether a kink of `T_of_t` falls at the collapse; Yp and D/H at the failure time against the same history's `pert12` solve at the same `t` (**frozen to 1e-6 relative**, or the log says by how much they are not) | instrumented solve (a BDF subclass or a step recorder, outside `PRyM/`); log |
| the scan S1–S3 | **every cell of §2 (c) filled**: failure count, D/H and Yp spreads per history, SM baseline values | `logs/01-probes/scan.csv` and a summary table in the log |
| cost | **ratio of serial medians** for every candidate setting against the default (§2 (d)) | log |
| Yp's floor | **the stage that sets it, or "not found"** (§2 (e)) | log |
| upstream | **whether upstream `bf24c3d` passes `rtol` to the low-T calls**, read from the upstream source if reachable, otherwise argued from the patch markers and said so | log |
| the pinned test values (P7) at the recommended setting | **each value measured**, with the bound it would meet or miss | log |
| the recommendation | **one setting by P3's rule**, with every criterion's measured value, or a stop | log |

### 6.2 The patch (prompt 02)

| quantity | target | witness |
|---|---|---|
| the low-T calls' `rtol` (and `atol`) | **the ruled setting**, both networks | new test intercepting `solve_ivp`; **fails on `HEAD~1`** |
| the patched tree, no override, on the 17-input roster × 3 variants | **equal to log 01's override results at the ruled setting to every printed digit** | the tool |
| `PRYM_VERSION`; `VERSION_LABEL` | **`"bf24c3d+ri02+sr01+bt02"`; `"2026.6.0"` (unchanged)** | grep; the existing version test, updated |
| the pinned constants (P7) | **re-pinned from their provenance; bounds unchanged; old values in a comment** | the three test modules pass |
| the stale-version warning (P6) | **one warning with the count per foreign `PRyM_version`; no row skipped** | test of a pure function; `ast` or a stub check that `main.py` and `plot_by_beta.py` call it |
| serial cost of the control | **within 1.2× of log 01's figure for the ruled setting** | the tool, idle machine |
| suites | **all pass; counts not lower** | suites |

### 6.3 Close-out (prompt 03)

| quantity | target | witness |
|---|---|---|
| the 17-input roster, `prod`, on the final tree | **identical to log 02's figures**; all 11 failures complete | the tool |
| `.documents/` | **additive only** (`git diff --numstat` shows no deletions) | `git diff` |
| suites | **unchanged from prompt 02** | suites |

---

## 7. What this campaign hands to the user

Prompt 03 adds a dated section, §4.11, to `.documents/review-remediation-verification.md` §4,
additively, after §4.10. It states at least:

1. **`PRYM_VERSION` and the setting.** The low-T `rtol` (and `atol`), why it was chosen, and its
   cost.
2. **The refresh route** (§2 (f)): copy the store, then run with `--drop bbn-data`; not
   `--retry-failed-bbn`; no `VERSION_LABEL` bump; the warning a store that was not refreshed will
   print.
3. **What changes in the science.** The 11 rows that were "not assessed" become assessable. The
   D/H scatter falls from ~10⁻³ to the residual log 01 measured. The M-convergence comparison and
   the β ≳ 1.6 shifts become physical rather than limited by the solver, to that residual.
4. **The residual floors** (P9), as measurements.
5. **What is still open**, by name.
