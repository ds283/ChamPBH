# Prompt 08 — close

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-19, on branch `power-law-wall` in its worktree, after 07d landed on its restart and was reviewed
(`b5f863a`, `69d5d45`), at the user's request: "The close-out prompt 08 and its orchestration note now need to be
written." It is written from:
- README §1, §2 (row 08), §4 ("08 → the science run"), §5, §6 whole and §7, and §6.17 (this prompt's proposals,
  P135–P148);
- every log's "State handed to the next prompt", and logs 04 §7, 06 §6–§7, 07 §7 and 07d §7 in particular;
- the board's §1–§5 and its orchestrator reviews;
- `one-loop-drain` prompt 08 and `electroweak-eos` prompt 03, the last two closes, whose form this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the worktree `/Users/ds283/Documents/Code/ChamPBH/.claude/worktrees/power-law-wall`, on branch
`power-law-wall`, from its root, with `PYTHONPATH=.` and `./venv/bin/python` (the symlink). Scratch goes in your
subdirectory of the session scratchpad, never `/tmp`. **The main checkout is not touched**: it is on `main`, which this
branch does not contain, and what this prompt reads of it, it reads by `git show main:<path>` (P136). Its `var/` holds
the stores, which you read only as §2 item 4 says.

**Gate:**
- on branch `power-law-wall`, at or after the commit that adds this prompt; `b5f863a` is an ancestor of `HEAD`;
- `git status --porcelain` shows `?? venv` only;
- `config/version.py`: `VERSION_LABEL = "2026.14.0"`; `config/fingerprint.py`: `FINGERPRINT_FORMAT = 8`;
- `tools/ray_cluster.sh status`: no ChamPBH process, the lock free. You start none;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- in the main checkout's `var/datastores/`: `power-law-wall-07d-r2-{exp,pl,pl-resumed,mixed}`, `power-law-wall-07-base` and
  `power-law-wall-07-exp` each have 16 shards, the primary `.db` and the manifest;
- `.documents/power-law-wall-verification.md` ends with §11 and has no §12.

If any of these fails, stop and ask.

**Closes:** nothing. No issue is assigned to this campaign (board §3.1); check that `.documents/OPEN_ISSUES.md` §1.26
lists exactly the board's four open issues. **Opens:** only what the work finds.

**Recommended model:** **Opus**, as README §2 plans. There is no code. The work is reading thirteen prompts' records and
writing two walls' laws exactly enough to be quoted in a paper, together with a handover from which the user can plan a
science run of both potentials without reading the campaign.

**Read first:**
1. [`README.md`](README.md): §0–§5, then §6 in order. §6.5 (R6–R8), §6.12–§6.16 (k_ex, the law v2, the pulse rate) and
   §6.17 (this prompt's proposals) govern; §0.3's estimates and P4, P11 and P16–P17 are the plan's history where later
   sections amend them.
2. Every log of this campaign (`logs/01`–`07d`), the board, and `.documents/power-law-wall-verification.md` §1–§11.
3. `.documents/numerical-strategies.md` §6.8, §6.11 and §6.12, and the dated notes of `one-loop-drain` in §3 (search
   `one-loop-drain`). `.documents/architecture-summary.md` §7.2's dated notes, §8.4a, §9 and §14. `CLAUDE.md`.
4. `.documents/review-remediation-verification.md` §4.20 and §4.21, the last two handovers.
5. On `main`, by `git show main:<path>`: `science-runs/README.md`, and `science-runs/2026-11-13-science-2026.11.0/`'s
   `README.md`, `PLAN.md`, `run_science_2026.11.0.sh` and the five analysis scripts (`fig6/fig6.py`, `fig6b/fig6b.py`,
   `m-trend/m_trend.py`, `relativistic/relativistic.py`, `changes/changes.py`).
6. `prompts/one-loop-drain/08-close.md`, `prompts/electroweak-eos/03-documents-and-handover.md` and their logs: the form
   of a close.
7. The code, wherever a document states what it does: `CosmologyConcepts/Models/` (all five modules;
   `AbstractModel.py` holds the turning point, the turnaround, the pulse rate's check and `ONE_LOOP_MAX_ITERATIONS`),
   `CosmologyConcepts/drain_model.py`, `ComputeTargets/ScalarHistory.py` (the drain's dispatch, `drain_alpha_effective`,
   the step-start assessment and the key's entry check), `ComputeTargets/quanta_spectrum.py`, `ObjectFactories/InversePowerModel.py`, `config/argument_parser.py`,
   `config/version.py`, `config/fingerprint.py`, `extract_common.py`, `tools/quanta_measures.py`. Quote the code; do not
   paraphrase a log's account of it.

---

## 0. Where this prompt sits

01–07d added the inverse-power-law model V = V₀[1 + (M/φ)ⁿ] beside the exponential and took it through the pipeline. In
order, they:
- built the model and the tool route (01);
- measured the wall (02) and put the law and the spectrum the user ruled on it (03);
- measured a sample of forty (04);
- stored it (05) and carried it through the drivers and exports, every potential under its own root (06);
- rehearsed both potentials under Ray (07);
- re-measured everything k_ex defines (07b, 07b2), put one k_ex, ½E^{1/2}|V‴/V″|, and the exponential's refitted law
  `one-loop-exponential-v2` into production (07c), formed the pulse rate in closed form (07c2), and rehearsed again (07d).

Every gate but Close holds. This prompt makes the repository say what the campaign changed, writes both walls' laws in
the form the paper will quote them, and hands the science run of record to the user (README §7). With it, the user can
plan that run, of both potentials, from one section.

**The potentials are described symmetrically.** No document gives the exponential a layout, a section or a default that
the power wall lacks, or the reverse (R12). Where the two differ, say so side by side.

**`CLAUDE.md` gains no rule.** This prompt updates what `CLAUDE.md` *describes*: counts, times, options and the export
layout. It adds no sentence that tells a reader what they must or must not do. If you think such a sentence is needed,
write it as a description in the architecture summary's §14, and put it in the log's §5 as a proposal for the user.

**Measurements, not bounds.** Every number written is a measurement from a record, cited to its source. No document
says that a value passes or fails a threshold the user has not ruled, or that a change is small or large.

**Records are records.** `science-runs/`, the campaign's README, prompts, orchestrator notes and earlier logs, §1–§11
of the verification document, other campaigns' documents and the earlier sections of
`review-remediation-verification.md` are not edited (`CLAUDE.md` invariant 6). In the four documents this prompt
extends, a correction is a dated note beside the statement it corrects.

---

## 1. What is wanted

### 1.1 C1 — the two walls, for the paper (P137)

A new **§12 of `.documents/power-law-wall-verification.md`**, "The two walls as they stand, for the paper". It is
written for a reader who will quote it: complete in its definitions, exact in its numbers, and with its sources. Both
potentials are treated in one scheme, each item giving the exponential and the power wall side by side. It is not written
into the briefing notes' repository or the paper.

1. **The two models, as coded.** V, the coupling, and the hooks in log space for each (`ExponentialModel`,
   `InversePowerModel`); the floor `_log_V_floor`, `reflects_at_origin`, the seed at any n (P3), and φ ≤ 0 as a failure
   on the power wall (R4). The type ids (`model_ids.py`) and the option values (`model_options.py`).
2. **k_ex, one definition** (R13–R16). k_ex = ½E^{1/2}|V‴/V″|, taken at φ_t for the classical turning point and at φ_ta
   for the one-loop turnaround; the pulse rate |V‴/V″| as each model's `_pulse_rate_hook`, in closed form ((n + 2)/φ on
   the power wall; the exponential's rational form in u = (M/φ)ⁿ); ℓ = ln(k_ex/m_ta), ℓ_t, R and L as the code forms
   them; φ_t the root of V(φ_t) − V_floor = E, reading no V′ (R16). Its check against Erickcek et al. Eqs. (64), (66) and
   (71) (§7.1), and its range: where V″ leaves float range on each wall, and the least margin measured over 07's grid
   (§10.5). The fixed point's cap `ONE_LOOP_MAX_ITERATIONS` and the slowest point measured.
3. **The laws, as coded.**
   - The fixed law: α_eff = α · w_gate. α = 0 is natural mode.
   - The one-loop law: α_eff from A and the model's own fraction f (`_drain_fraction`, called by
     `drain_alpha_effective`), with the cap and the gate as the code applies them.
   - **f on the power wall** (`one-loop-inverse-power-v1`): f_n(ln R), its two branches and the switch, each constant
     with its value; the classical branch R·S_n with S₁ and S₂ (R6); unassessed 0.
   - **f on the exponential** (`one-loop-exponential-v2`): f = 1 − exp(−min(y_rise, y_fall)) for ln R > 0 (U13), y_rise
     per n and y_fall shared, each constant at full precision; where f = 0; the classical branch R·S(L) with S's table,
     its interpolation and continuation; unassessed 0.
   - The regimes (classical, one-loop, gap, unassessed): how a candidate is classified, and what each regime selects
     under each law (under v2 the label selects only whether the fall exists).
   - **The domain.** Both laws at n = 1 and 2 only, refused at entry otherwise (R7, U14); v2 fitted on
     ln(V₀/M⁴) ≤ 10 (U13), with what 07d measured outside it (§11.11); the power wall's law reads no ℓ (R7).
   - The drain models and the key: `alpha_value`'s pair, the two current names, the two retired names and where they
     are refused (`drain_model.py`'s `law_of`).

   Write each formula from the code, with its `file:line` at the dispatch commit, and give each constant its source
   (the verification section that measured it and the file that holds it at full precision).
4. **The spectrum, as coded** (R8–R10, P104). For each wall's spectrum model (`SPECTRUM_MODELS`): the table and its range
   in x = k/k_ex, k² below it, the matched exponential above it, x_p and the tail's share; the associated piece and
   `X_STAR`, flagged as unmeasured on the power wall (U5, R10); `G_Y_MIN` and `OMEGA_U_MAX`; which wall each potential
   and n reads (`spectrum_wall`).
5. **Where each constant was measured, and how well.** One table: constant, value, the verification section, the fit's
   residuals and errors as recorded there (the power wall's f_n, §3.1; S_n, §2.4; v2's joint fit, §8.2–§8.4, with its
   prediction of the held-out points; the tables, §7.7). Restate nothing the cited section does not hold.
6. **What the sample and the rehearsals showed**, cited by section: 04's forty (§4.1–§4.9: failures, thresholds, the
   surfing edges as R11 ruled, R9's share, M_crit); the power wall beside the exponential at the same (M, β) (§4.2); v2
   against v1 on the exponential's one-loop histories (§11.5); the margins (§11.10 and 07d's P119); the costs (§4.7,
   §6.9, §11.10).
7. **What is not measured, and what is not modelled.** x_* and the associated piece on the power wall; the k² ln²k term
   at n = 1 (R8); n ≥ 3 on both walls; v2 outside its fitted domain; the surfing edges at n = 1 (R11); natural mode at
   n = 1 and large M (R5). Then point to `one-loop-drain` verification §8's list of what is not modelled, and say which
   items of it this campaign changed, if any. Name the open issue each item belongs to, where one exists.

Write the formulas in the document's existing notation (plain text and Unicode). Where a log and the code disagree on a
formula or a constant, the code is what the document states; record the disagreement in the log, and if it is more than
wording, stop and ask (§5).

### 1.2 C2 — the reading guides (P138)

- **`numerical-strategies.md`, a dated §6.13, "the power wall and one k_ex as they stand"**, after §6.12 and before
  `## 7.`, in §6.12's form. A reading guide, not a restatement. Say, in order, what a history does under each potential
  and law: the assessment at each candidate (the turning point, the turnaround and k_ex, through the model's hooks); the
  regime and f, by the model's own fraction; the gate and the cap; the drain into the ledger; parking and release; the
  instrumentation; what is stored and what the quanta tools derive afterwards, with which wall. Point each step to
  verification §12 and to the section that measured it. Then name the statements of §6.8, §6.11 and §6.12, and of
  `one-loop-drain`'s dated notes in §3, that this campaign superseded (§6.12 describes `one-loop-measured-v1` as the law;
  the spectrum note gives x_p = 0.62, κ and x_* = 0.0058; k_ex is written with −d ln V/dφ). Check each one; do not rely
  on this list.
- **`architecture-summary.md`**, dated notes that map the campaign onto the code; point to verification §12 and to the
  logs rather than repeat them:
  - **§8.4a, at its end:** `InversePowerModel`; the hooks every model now has, with no generic default
    (`_d3_logV_dphi3`, `_pulse_rate_hook`, `_drain_fraction`, `one_loop_drain_model`, `spectrum_wall`; `_drain_L`
    optional), and which raise at which n; one k_ex in `AbstractModel.classical_turning_point` and
    `one_loop_turnaround`, and `ONE_LOOP_MAX_ITERATIONS`; the S table's move into `ExponentialModel` (P99);
    `model_ids.py` and `model_options.py`.
  - **§7.2, after its last dated note:** the drain's dispatch to the model's own fraction; the step-start assessment
    through the model's hooks; the entry check of the key's drain model (P36).
  - **§9, at its end:** `--models` and the two config files; `--n-values` one list for every potential of a run; the
    one-loop rows per potential; the export root `<output>/<label>/<model>/` and the `model` column; the registered run's
    scope; the store (`ObjectFactories/InversePowerModel.py`, `FINGERPRINT_FORMAT` 8, `VERSION_LABEL` 2026.14.0).
  - **§14, at the end of the file:** a dated note only for a gotcha a log names that §14 lacks. Candidates: every raw
    join of a model table filters `model_type`, since the first rows of the two model tables share serial 1 (P37); a
    one-loop run of both potentials at n = 1, 2 builds the exponential at n = 2 as well; the retired drain-model names
    are refused, not read. Check each against §14 and the dated notes before adding it.
- **Any statement in either document that describes the present, and that this campaign made false,** is corrected by
  a dated note at the statement. Do not edit the statement. Search both for `one-loop-measured-v1`, `DRAIN_MODEL_ONE_LOOP`,
  `drain_fraction`, `SPECTRUM_MODEL`, `U4-U17`, `x_p`, `0.0058`, `k_ex`, `λ_t`, `FINGERPRINT_FORMAT`, `VERSION_LABEL`,
  `ExponentialModel` (where it is said to be the only model), `histories.csv` and the export paths.

### 1.3 C3 — `CLAUDE.md`, by description only (P139)

- The suite counts and the two halves, from your own run (§2 item 1): 26, 809 (226 + 583), 175, 312, 79, unless your run
  says otherwise. `CLAUDE.md` still shows 06's counts (783 = 226 + 557), which 07c and 07c2 changed.
- Replace the parenthesis that dates the counts with one sentence giving the date and the prompt that last changed a
  count (07c2, 2026-11-19), and what 07c and 07c2 added, from their logs. Say in the log that you did this.
- The times of `ComputeTargets/tests`, its halves and `tools/tests`, from your own run, with the loads, and that they were
  taken in a worktree.
- The plot-scripts bullet: each α's and each A's output is under `<label>/<model>/…` (R12), and `--models` is matched as
  the other options are (a potential with no stored row is refused). Check the Redshift bullet's drain-model names and
  the science-runs bullet. Anything else a log names as made false in `CLAUDE.md` and not yet fixed.
- No new rule (§0).

### 1.4 C4 — the handover (P140, P141)

A dated addendum to `.documents/review-remediation-verification.md`, **§4.22**, after §4.21's point 8 and before the
`---` that precedes `## 5. Reproduce`, in §4.20's and §4.21's form. It opens with:
- what the campaign did, in order, each prompt with its commit and its gate (01 `3aae9a5` … 07d `b5f863a`, 08 "this
  commit"), and the stops the user ruled;
- its board, plan, verification and dated notes;
- the labels: `VERSION_LABEL` 2026.14.0 and `FINGERPRINT_FORMAT` 8;
- that the branch is not merged when §4.22 is written.

Then a "This supersedes" list, by statement and not by edit: anything in §4.20 or §4.21 that names
`one-loop-measured-v1`, the spectrum `U4-U17-v1`, the old k_ex, the export root `<output>/<label>/`, or the `2026.11.0`
stores as current. Search; do not guess. Then the points:

1. **What changed for a build.**
   - **The model:** `inverse_power.yaml` beside `exponential.yaml`, `--models` (default `[exponential]`), and
     `--n-values` one list for every potential of a run.
   - **The laws:** the drain models `one-loop-exponential-v2` and `one-loop-inverse-power-v1`; the retired names, and
     where each is refused and with what exit; the one-loop law at n = 1 and 2 only on both walls, refused at parse.
   - **One k_ex:** every stored k_ex, ℓ, peak, log q, birth margin and margin record now means the V‴ form.
   - **The exports:** every potential under `<output>/<label>/<model>/`, `histories.csv` with `model` after `n`; the
     quanta tools take each row's wall from its `model` and `n`; `spectrum_model` names.
   - **The registered run's scope** names the potentials.
   - **The labels, and the older stores.** A run of record starts new stores. State, from §2 item 4, what the tip does
     with a `2026.11.0` store (format 7), with a `2026.12.0` store (format 8), and with a `2026.14.0` store, each with the
     hashes before and after.
   - **What did not move:** every history whose law reads no ℓ (the fixed law, natural mode, and the power wall under
     both laws) keeps its integration and BBN; only the k_ex columns moved (§11.4). The exponential's one-loop histories
     moved (§11.5).
2. **What a science run of record after this campaign repeats** (convention 8; P89, R8). Every row of `science-runs/
   README.md`'s "Standard analyses", as the file on `main` names it, and for which potentials: the exponential's
   Figure 6 and Relativistic quanta move with its law and the spectrum; the power law's are new. Say that the tip cannot
   read the `2026.11.0` stores, so the exponential is rebuilt, not re-exported.
3. **The run script's and the analyses' changes against 2026.11.0's** (P141), read on `main`, block by block:
   - **The builds.** `--config` and `--models`; one store of both potentials or one per potential, with the facts that
     bear on the choice (07's and 07d's mixed store equal to the separate stores, §6.7 and §11.6; `--n-values` shared, so
     both potentials at n = 1, 2 in one run build the exponential at n = 2 too).
   - **The exports and measures.** The root per potential in place of `$OUT/main/QCD_Cosmology`; `quanta_measures` per
     root, at c_a = 1, 0 and 10; `plot_quanta_grid` and `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`, with §2
     item 5's measurement of the per-n roots; `plot_quanta_history`.
   - **The analyses.** For each of the five scripts, what a tip export needs it to change: each hard-codes
     `ONE_LOOP = "one-loop-measured-v1"`, a retired name; which join on keys with no `n` or `model` (`changes.py`'s
     `KEY`, `relativistic.py`'s join); which read a copy of `histories.csv` that a power-law root holds two n in. State
     these from the scripts; 08 edits none of them and writes no script.

   Every command you give must run as written (§2 item 3).
4. **The power law's grid: what is known.** For each block of the 2026.11.0 grid (its `PLAN.md` §1 and run script), what
   04 and 07d measured on the power wall at its M and β, and what was not measured. Among them:
   - the per-history cost by the tool route (§4.7) and under Ray (§6.9, §11.10), beside the exponential's;
   - natural mode at n = 1 failing at G2 at 10⁻³ M_P (R5), and the M of that block that were not sampled;
   - the surfing edges: n = 2 as the exponential's, n = 1 recorded as measured (R11);
   - M_crit on the power wall (§4.5, `thresholds.txt` §5), and R9's share (§4.4);
   - on the exponential, v2's domain: 47 candidates with ln(V₀/M⁴) > 10 at 10⁻³³ M_P (§11.11), and that the grid's
     10⁻³⁶–10⁻⁴² M_P were not measured.

   Then **a projection** of the power law's build time, labelled as one (P144): its basis (the 2026.11.0 run record's
   block walls, "The registered runs"), the per-history ratio power/exponential it uses with its sections, the number of
   n, and its weakest assumption (the M_crit block, which no sample of this campaign reached). Do not run anything to
   refine it.
5. **What to expect while it runs:** the failures (R5's kind, and how `main.py` stores them); the tuning cap and the step
   budget (from the code, with `file:line`; neither is a wall-clock limit); the heartbeat (07d's longest silence) and
   `runregistrykit list --stale-after`; the disk; the workers' working directory (the cluster started from the tree that
   builds); `compare_stores` and a failed history (`[07-failure-reason-carries-the-run-label]`).
6. **The stores and outputs kept** (P124, P134). For each of 07d's four stores, 07's five and the first attempt's two:
   label, format, `store_id`, shards and size; which ones 08 read; that `power-law-wall-07d-pl` has no sidecar, so its
   retirement needs a `store create` first. Then this worktree's ignored `var/power-law-wall/` (each subdirectory and its
   size), and that removing the worktree deletes it. Retiring, moving or deleting any of them is the user's.
7. **What stays open.** The board's four issues, each with its next step, and which bear on the science run. Then, each
   in one line with its log, the observations the logs left unopened that a reader of the run should know. Among them:
   `REMOVED_OPTIONS["--potential-type"]`'s message (log 06 §6); `plot_inputs.exponential_models` with no production
   caller (log 06 §6); the surfing scans' T_max and `n_crossings` on a reflected first bounce (log 04 §6); the Ray session
   directory left under `/tmp/ray` by 06 (log 06 D4).
8. **The merge into `main` is the user's** (P146). State, from commands run at the time of writing: the merge base;
   `git merge-base --is-ancestor main power-law-wall` (not an ancestor: `main` has run-record commits under
   `science-runs/` only); that the merge is therefore not a fast-forward; the result of
   `git merge-tree --write-tree main power-law-wall`; how far local `main` is ahead of `github/main`; and that
   `power-law-wall` has no upstream. Then the dated note the `2026.11.0` run record needs after the merge (its one-loop
   rows are under a retired law, and the tip cannot read its stores), named as a step for after the merge. Pushing is the
   user's.

### 1.5 C5 — the issues (P145)

- **None is closed.** None is assigned.
- **`[06-plot-quanta-grid-refuses-two-n-at-one-cell]`:** add a dated line (the date of the work, prompt 08) with §2 item 5's
  measurement and the handover's point that names it. Its next step is unchanged.
- **The other three:** a dated line only where the handover's point adds a fact; otherwise unchanged.
- **The index.** Update `.documents/OPEN_ISSUES.md` in the same commit: §1.26's paragraph says the campaign is closed,
  the header's clause for this board says the same, and the date. The count stays 36 unless the work opens something. A
  new issue goes on the board's §3 and in the index, per `CLAUDE.md`.

### 1.6 C6 — the board and the index closed (P147)

- **The board:**
  - row 08 and the gate Close in §2, with the gates as they stand;
  - the status line rewritten as a short statement of the closed campaign. Move the present line, unchanged, as the
    first bullet under "History of the status line", headed with today's date, so that nothing is lost;
  - the opening description ("The campaign adds the inverse-power-law model …") followed by a dated note naming what
    07b–07c2 added (one k_ex, the exponential's law v2);
  - the `Code:` line checked against `git diff --stat a7e0978 <dispatch> -- '*.py' '*.yaml'`; files it misses are added,
    and anything it names that the diff does not is reported;
  - **Last updated**, and §5's baseline for 08.
- **`prompts/INDEX.md`:** the campaign's row says **complete**, with each prompt's commit, the dates, the final suites,
  and where the verification and the handover are. Correct the header's date and counts.

---

## 2. Verification

1. **The five suites**, from the worktree's root with `CLAUDE.md`'s commands, run serially: `ComputeTargets/tests` in two
   halves, and `tools/tests` alone. Run the `test_[g-z]` half and `tools/tests` in the background. Record each count,
   each time, and `uptime` at each start. The counts must equal 07d's (26, 809 (226 + 583), 175, 312, 79), with the two
   worktree failures (board §5) and no other. A count that differs, or another failure that recurs when re-run alone, is
   a stop.
2. **Every line addressed to 08** — the close, the documents, `CLAUDE.md`, the handover or the science run — is done, or
   named in the log as left, with why.
   - Look in logs 01–07d, the README, the board (its reviews and "For 08" paragraphs) and the orchestrator notes.
   - Search for "08", "close", "handover", "the science run", "made false", "for the documents".
   - Put the lines in a table in the log.
3. **Every command in C4 runs as written.** One script, `.documents/power-law-wall/close-08/check_help.py`, runs the
   `--help` of every driver and tool §4.22 names (`main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
   `RunDrivers/registered_run.py`, `tools/quanta_measures.py`, `tools/plot_quanta_grid.py`,
   `tools/plot_quanta_history.py`, `tools/bbn_from_store.py`, `tools/history_and_bbn.py`, `tools/surfing_scan.py`,
   `runregistrykit list`, and any other), writes each to `out/`, and checks that every option §4.22 names appears in its
   own command's help, spelled as §4.22 spells it. The list it checks is taken from §4.22's final text.
4. **The inventories**, with no Ray (`main.py --inventory` starts none) (P142):
   - **in place on `power-law-wall-07d-r2-exp`**: `shasum -a 256` of its files before and after, which must be
     identical;
   - **on copies**, in your scratchpad, never the stores: of `power-law-wall-07-base` (format 7, `2026.11.0`, built at the
     base) and of `power-law-wall-07-exp` (format 8, `2026.12.0`). For each, the copy's hashes against the original's
     before running anything, what the tip reports or refuses, its exit, and the copy's hashes after. Either outcome is a
     measurement for C4 point 1. A copy that changed is an issue (a reader writing to a store): open it, and do not fix
     it.

   Keep the commands, outputs and hashes in `close-08/out/`, with the scratchpad's path written `<scratch>`.
5. **The per-n roots for `plot_quanta_grid`** (P143). In your scratchpad, from a copy of 07d's power-law export root
   (`var/power-law-wall/07d-r2/export-pl/QCD_Cosmology/inverse-power/`), make one root per n: each keeps every per-history
   directory of its n and the rows of its n in each root-level CSV file (07's gate R (iv), §6.7, found a one-n export's
   `histories.csv` equal to the other's rows at that n). Run `tools/quanta_measures.py --assoc-scale 1` and then
   `tools/plot_quanta_grid.py` on each, and on the unsplit copy. Record each exit, what each drew or refused, and its
   wall. This is a measurement for the issue and for C4 point 3; it changes no file outside `<scratch>` and
   `close-08/out/` (text only; no figure is committed).
6. **Twenty numbers**: ten of verification §12 and ten of §4.22, chosen across their sections, each checked against its
   cited source and listed in the log with the source line.
7. **The board and the indexes agree:** the count, §1.26's rows, and the campaign's row in `prompts/INDEX.md`.
8. **`git diff --stat`** touches only the files of §4. No Python file changes but the scripts in `close-08/`, which
   `black` formats.

## 3. Acceptance

1. C1–C6 as §1 says, and §2's checks recorded.
2. **One commit**, with the log `logs/08-close.md` in the form of `dk-and-rpk-migration` README §5.1. The log has:
   - the suite counts;
   - every deviation, classified;
   - §2 item 2's table;
   - "Observations not acted on";
   - "State handed on", to the user: the merge, the science run, the stores and the worktree.

   Stage by explicit path, and show `git diff --cached --name-status` before committing. The commit message follows
   `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 4. What this prompt does not do

- **It changes no code and no test.** It builds no store, starts no Ray, and runs no `main.py` build, plot script or
  registered run. The only programs it runs on stores are §2 item 4's inventories; the only tools it runs on exports are
  §2 item 5's, on scratch copies.
- **It edits no record** (§0): not `science-runs/` on either branch, its scripts or plans; not the campaign's README,
  prompts, orchestrator notes or earlier logs; not §1–§11 of the verification document, or any earlier section of
  `review-remediation-verification.md`.
- It writes no run script and no plan for the science run, and nothing in the briefing notes' repository or the paper.
- It retires, moves or rewrites no store, and deletes nothing under `var/`.
- It does not merge, push or remove the worktree.
- **Files it changes:**
  - `.documents/power-law-wall-verification.md` (a new §12, and §13 below);
  - `.documents/numerical-strategies.md`;
  - `.documents/architecture-summary.md`;
  - `.documents/review-remediation-verification.md` (§4.22 only);
  - `CLAUDE.md`;
  - `.documents/power-law-wall/close-08/` (new: §2's scripts and outputs);
  - the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`;
  - the log.

  The verification document also gains a **§13, "Prompt 08 — the close"**: the suites, the inventories' hashes, the
  per-n measurement and §2's checks, item by item.

## 5. Stop conditions: stop and ask

- An item of the Gate fails, or an issue is assigned in the board's §3.1 or in the index's §1.26.
- A count differs, or a failure other than the two worktree failures recurs.
- **A log, the verification or the README disagrees with the code** on a formula, a constant, a column, a name or a
  default, beyond wording. State both, and ask which the documents follow; the code is not changed here.
- A number the documents need was never measured. Say it was not measured; do not measure it. If the handover cannot be
  written without it, ask.
- A statement made false by this campaign, in the documents or `CLAUDE.md`, cannot be corrected without a code change.
- The inventory changes a byte of `power-law-wall-07d-r2-exp`.
- A command §4.22 would give does not run as written.
- C3 cannot be done without adding a rule to `CLAUDE.md`.
