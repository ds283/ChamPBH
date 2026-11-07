# Prompt 08 — close

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-07, on branch `one-loop-drain` in the main checkout, after 07 landed and was reviewed (`867b267`,
`c8b87d6`), at the user's request: "Please write prompt 08". It is written from:
- README §8.4 (row 08), §8.7 (what the campaign hands to the user) and §8.16 (the review before this prompt, P66–P73);
- every log's state handed on, and log 07 §6 in particular;
- the board's §2 and §3;
- `inelastic-reflection` prompt 12 and its log, the last close of this kind, whose form this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Scratch goes in your subdirectory of the session
scratchpad, never `/tmp`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `tools/ray_cluster.sh status`: no ChamPBH process. You start none;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/datastores/one-loop-drain-06-reference.db` and `one-loop-drain-06-base.db` exist, each with its 16 shards and its
  manifest.

If any of these fails, stop and ask.

**Closes:** nothing. No issue is assigned to this campaign (board §3.1); check that `.documents/OPEN_ISSUES.md` §1.23
lists only the three the board holds open. **Opens:** only what the work finds.

**Recommended model:** **Opus**, as README §8.4 plans. There is no code. The work is reading eleven prompts' records
and writing the law and its measures exactly enough to be quoted in a paper, together with a handover from which the user
can plan a rebuild without reading the campaign.

**Read first:**
1. [`README.md`](README.md): §8 whole, in order. §8.10 is the law as ruled, and §8.16 is this prompt's proposals.
   §§0–7 are the plan as first written; read §1.1 and §6 only for what §8 says still stands.
2. Every log of this campaign (`logs/01`–`07`, with `02b`, `02c` and `02d`), the board, and
   `.documents/one-loop-drain-verification.md` §1–§7.
3. `source/crossover/README.md` and `source/drain-probe_2026-11-02.md`: where the law and its constants come from.
4. `.documents/numerical-strategies.md`: §6.1–§6.11, and the dated notes of this campaign (search for
   `one-loop-drain`). `.documents/architecture-summary.md`: §7.2's dated notes of this campaign, the notes of 04, 05
   and 07, and §14. `CLAUDE.md`.
5. `.documents/review-remediation-verification.md` §4.16–§4.19 (the last handovers), and
   `science-runs/2026-11-01-science-2026.8.0/` (its `README.md`, `PLAN.md`, `run_science_2026.8.0.sh`,
   `run_figure_exports.sh` and `fig6b/PLAN.md`).
6. `prompts/inelastic-reflection/12-close.md` and its log: the form of a close.
7. The code, wherever a document states what it does: `ComputeTargets/ScalarHistory.py` (the law's functions and
   constants), `ComputeTargets/quanta_spectrum.py`, `CosmologyConcepts/drain_model.py`, `tools/quanta_measures.py`,
   `tools/quanta_figures.py`. Quote the code; do not paraphrase a log's account of it.

---

## 0. Where this prompt sits

01–07 built the one-loop drain law beside the fixed one. In order, they:
- replaced it by the law measured in `crossover` (02c);
- measured it on thirty histories at U14's eight A (02d);
- stored its instrumentation (03) and carried it through the drivers and exports (04);
- derived the spectrum-dependent measures outside the integrator (05);
- rehearsed both laws under Ray (06);
- wrote the figure tools (07).

Every gate holds. This prompt makes the repository say what the campaign changed, writes the law and its measures in
the form the paper will quote them, and hands the rebuild of record to the user (U2). With it, the user can plan that
rebuild from one section without reading the campaign.

**`CLAUDE.md` gains no rule.** Some of its "rules" were written by agents and never agreed by the user. This prompt
updates what `CLAUDE.md` *describes*: counts, times, options and conventions. It adds no sentence that tells a reader
what they must or must not do. If you think such a sentence is needed, write it as a description in the architecture
summary's §14, and put it in the log's §5 as a proposal for the user.

**Measurements, not bounds.** Every number in the documents is a measurement from a record, with its source. No
document says a value passes or fails a threshold the user has not ruled. A large backreaction ratio or a phase slip
below 1 rad is stated as measured, with what it means for the reading of the measures, and not as a verdict on the law.

---

## 1. What is wanted

### 1.1 C1 — the law and the measures, for the paper (P67)

A new **§8 of `.documents/one-loop-drain-verification.md`**, "The law and the measures, for the paper". It is written
for a reader who will quote it: complete in its definitions, exact in its numbers, and with its sources. It is not
written into the briefing notes' repository or the paper.

1. **The two laws, as coded.**
   - The fixed law: α_eff = α · w_gate.
   - The one-loop law `one-loop-measured-v1`: α_eff = min(α_cap, A · f) · w_gate, with f by regime as README §8.10's
     table gives it, including the user's two rulings during 02c:
     - the gap's f = 0 where 1 + q ln L ≤ 0;
     - L = ln(1 + E/V₀), defined by the model (`_drain_L`, no default) and equal to M/φ_t at n = 1 (U15 (b), "for now").

     Write each formula from the code (`drain_fraction`, `drain_S_of_L`, `drain_alpha_effective` and their constants),
     and give each constant its value and its source: `crossover`'s fits, the Born coefficient of `wall_backreaction.tex`,
     the cap and the plateau as judgement, and S(L)'s table with its interpolation and continuation.
   - The regimes: how a candidate is classified (one-loop, gap, classical, unassessed), from the assessment's ℓ, L and
     ln R.
   - The gate w_gate (U9), and what the fixed law and α = 0 are under the key (`drain_model`).
   - The law's domain:
     - what it was fitted on: the n = 1 wall in stage 2, and the n = 2 check;
     - what is unmeasured: n ≠ 1, 2, and the physics inside the gap, which is a fit, not a solve.
2. **What the law did on the sample.** From verification §2d, which replaced 02's and 02b's measurements, and §5.7:
   - the regime shares;
   - where the cascade reaches the gap and R ≤ 1;
   - parking, the first impact and the cap;
   - ΔN_eff, Y_p and D/H against the old law and the fixed law at laboratory M;
   - the failures and the timeouts.

   Cite each number's section. Restate nothing that the cited section does not hold.
3. **The stored measures (model-free).** Each defined in one or two sentences, with its column or file, its unit, and
   the condition under which it is written (one-loop law only, or every law):
   - the bounce rows;
   - the history's drain record: the regime shares, the parking share, the counts, ℓ_min, the least ln R;
   - the all-generation margins and their failing shares;
   - the sample masses.

   Say how the all-generation margins differ from `inelastic-reflection`'s weighted ones, and why that mattered
   (board §4, `[00-weighted-margins-hide-the-least-relativistic-generations]`).
4. **The derived measures (05).**
   - The spectrum model `U4-U17-v1`: its shape, its constants and their sources, and the associated piece's scale c_a
     (default 1; measured at 0 and 10).
   - n_g, n̄_j, δ_stim, E(N) with the field's and the tracker's mass, the phase slip, k_ex/H and the backreaction ratio:
     each with its formula as `tools/quanta_measures.py` computes it and its assumptions (the spectrum, random phases,
     first order).
   - What was checked against what (verification §5.3–§5.5).
5. **What they say on the sample.** δ_stim, E, the margins, n̄, the phase slip and the backreaction ratio, with their
   ranges and where on the grid the extremes sit (verification §5.7; log 05 §6; log 07 §6), and their dependence on c_a.
6. **Where the reading does not hold, and what is not modelled.**
   - **The random-phase premise.** It fails in the histories with n̄ > 1 (33) and the phase slip below 1 rad (15). Where
     these sit, and that the large overlaps fall at bounces that drain almost nothing.
   - **The backreaction ratio above 1**, by group.
   - **The parking leftover**, which has no spectrum.
   - **The fixed law** has no instrumentation.
   - **Not modelled** (README §8.1, out of scope): stimulated emission, rescattering, backreaction, production at R < 1
     in the 1/φ tail, the gap's physics, and parking at physical M.

   Name the open issue each item belongs to.

Write the formulas in the document's existing notation (plain text and Unicode, as §1–§7). Where a log and the code
disagree on a formula or a constant, the code is what the document states; record the disagreement in the log, and if
it is more than wording, stop and ask (§5).

### 1.2 C2 — the reading guides (P68)

- **`numerical-strategies.md`, a dated §6.12, "the one-loop drain law as it stands"** (after §6.11, in its form). A
  reading guide, not a restatement. Say, in order, what a history under `one-loop-measured-v1` does:
  1. the assessment at each candidate (the wall search with 01's fix for unresolvable roots, and the declined unresolved
     reflection of ruling (b));
  2. the regime and f;
  3. the gate and the cap;
  4. the drain into the ledger;
  5. parking and release;
  6. the instrumentation gathered (the bounce rows, the history's drain record, the all-generation margins);
  7. the sample masses at every law;
  8. what is stored, and what the tool derives afterwards.

  Point each step to the dated note that describes it (01, 02c, 03, 05) and to the verification. Then name the
  statements of §6.1–§6.11 that hold only for the fixed law, or that this campaign superseded. For example, §6.11
  describes α as a fixed fraction, and §6.8's peak estimate is reused under the one-loop law (P18). Check each one; do
  not rely on this list.
- **`architecture-summary.md`, one dated note at the end of §7.2,** after 03's, that maps the campaign onto the code.
  It covers:
  - the modules: `CosmologyConcepts/drain_model.py`, `ComputeTargets/quanta_spectrum.py`, `tools/quanta_measures.py`
    and the figure tools;
  - the public functions and constants of the law;
  - the `"drain"` payload key;
  - the table `ScalarHistoryBounce`, the new history and sample columns, and `alpha_value`'s pair;
  - `FINGERPRINT_FORMAT` 7 and `VERSION_LABEL` 2026.10.0;
  - the three exports' new columns;
  - the drivers' `--A-values`.

  Point to each prompt's note rather than repeat it.
- **`architecture-summary.md` §14:** a dated note only for anything a log names as a gotcha of this campaign that §14
  lacks. Candidates:
  - the drain model is never defaulted in the path and label helpers (note 04 rulings 2, 3);
  - `_drain_L` has no default, by the user's ruling;
  - the workers run the cluster's tree whatever tree the driver runs from (log 06 §6).

  Check each against §14 and the dated notes before adding it.
- **Any statement in either document that describes the present, and that this campaign made false,** is corrected by
  a dated note at the statement. Do not edit the statement. Search both documents for α_eff, `alpha_value`, the table
  list, `FINGERPRINT_FORMAT`, `VERSION_LABEL`, "elastic", the exports' columns and `histories.csv`.

### 1.3 C3 — `CLAUDE.md`, by description only (P69)

- The suite counts and the two halves, from your own run (§2 item 1): 26, 665 (219 + 446), 163, 259, 72, unless your
  run says otherwise. Do not extend the parenthesis that dates the counts with another clause for 08, which adds no
  test. If it is to change, replace it with one sentence giving the date and the campaign that last changed a count.
  If you do this, say so in the log.
- The time of `ComputeTargets/tests` and its halves, and of `tools/tests`, from your own run, with the loads.
- Anything a log or the README names as made false in `CLAUDE.md` and not yet fixed. Check the bullets on Redshift
  (α as a key, the drain model), the plot scripts (`--A-values`), and science runs.
- No new rule (§0). No sentence on the figure tools or the derivation tool beyond what a bullet describing the tools
  already says.

### 1.4 C4 — the handover (P70)

A dated addendum to `.documents/review-remediation-verification.md`, **§4.20**, in §4.18's and §4.19's form. It opens
with:
- what the campaign did, in order, with each prompt's commit;
- its board, plan, verification and dated notes;
- the labels: `VERSION_LABEL` 2026.10.0 and `FINGERPRINT_FORMAT` 7;
- that the branch is not merged when it is written.

Then a "This supersedes" list, by statement and not by edit: §4.19 point 4's "after `one-loop-drain` 08" is met, and
anything of §4.18 or §4.19 that this campaign made false. Then the points:

1. **What changed for a build.**
   - **The drain-model key on `alpha_value`.** A lookup that names no model is `fixed`.
   - **`--A-values`** beside `--alpha-values`. It has no default. The `--alpha-values` rows come first. A run given only
     `--A-values` also builds α = 0.
   - **The one-loop products** go under `<label>/one-loop-measured-v1/A=<A>/`.
   - **The new export columns and `bounces.csv`.**
   - **The labels.** A rebuild starts a new store, at format 7 and 2026.10.0. State what the tip does with a format-6
     store, from §2 item 4's inventory of the copy of `one-loop-drain-06-base`.
   - **The fixed law under the tip** equals the base's in every pre-existing column (gate Bitwise). It writes no bounce
     row, NULL drain columns, and the sample masses.
2. **The run scripts' changes against 2026.8.0's** (`science-runs/2026-11-01-science-2026.8.0/`), block by block:
   - **What `--A-values` adds.** Give U14's values, A ∈ {0.05, 0.1, 0.5, 1, 1.5, 2, 5, 10}, verbatim.
   - **What becomes of the fixed-α blocks.** This is the user's choice. Give the facts: the fixed law is bitwise in its
     old columns, carries no instrumentation, and is what Fig 6a drew for comparison.
   - **The post-build steps**, with their commands:
     - the exports;
     - `tools/quanta_measures.py` at c_a = 1, and at 0 and 10 for the sensitivity;
     - `tools/bbn_from_store.py --drain-law` where it is used;
     - the two figure tools.
   - **The registered run's scope** (P40).

   08 writes no script; a new `science-runs/` folder holds the user's scripts when the user writes them. Every command
   you give must run as written (§2 item 3).
3. **What to expect.**
   - **The cost per history**, by group and A: 02d's tool-route times (verification §2d.10) and 06's times under Ray (log
     06 §7). Include the classical-heavy ladder at 5 × 10⁻²⁰ M_P (1115 s at A = 0.1).
   - **C-3873's kind** (β = 1.11 at 10⁻¹² M_P): about an hour per tuning in the tool route. `main.py` has no wall-clock
     limit per history: what bounds it is `--tuning-max-integrations` (give the default from `config/`) and the step
     budget per integration (give the value). State this from the code.
   - **The failures to expect** at U14's A: R-632's kind at A ≤ 0.1, the 5 × 10⁻²⁰ M_P ladder at A = 0.05. Say how
     `main.py` stores and counts them, and what `--retry-failed-scalar` would do.
   - **A projection** of the 2026.8.0 grid's blocks at U14's eight A: per-history costs measured by group, times the
     block's histories, over the worker count. Label it a projection, state its basis and its weakest assumption, and do
     not run anything to refine it.
   - **The disk.** The raylet's 95% warning against the purgeable space (README §8.15), for a build or rehearsal on this
     machine.
   - **The workers' working directory** (log 06 §6).
   - **The shard warning**, DatastoreKit's (`befa7f3`), cosmetic.
   - **The longest heartbeat silence**, and whether `--stale-after` needs raising.
4. **The figures of record.** Fig 6 at laboratory M first (README §8.7), and `fig6b/PLAN.md`'s prerequisite, which is
   this rebuild. Then the two figure tools: what they draw, their commands and costs (log 07 §6), and what 07's
   demonstration shows. Include that the grid of 02d is a "plus", not a rectangle, so that a rebuild's grid page will
   look different. Add the tools' two weak readings from 07's review (§8.16), and that the tools are for a handful of
   histories, not thousands (log 07 §5 item 7).
5. **The stores kept.** `one-loop-drain-06-base`, `-reference` and `-resumed` (log 06 §7: their labels, formats,
   `store_id`s and sizes), and `inelastic-reflection-11-reference`. Retiring any of them is the user's.
6. **What stays open.** The board's three issues, each with its next step. Name the one that bears on the rebuild,
   `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`. Then the observations the logs left unopened that a
   reader of the rebuild should know, each in a line with its log. Among them: `drained_share_classical` of
   1.0000000000000007 (log 07 §5); the unassessed regime met nowhere in 02d's sample; the ΔN_eff legend's α with no
   point (log 06 §6).
7. **The merge into `main` is the user's.** State the result of `git merge-base --is-ancestor main one-loop-drain`, and
   that `reflection-restart` is contained in this branch (`1561b60`). Pushing is the user's.

### 1.5 C5 — the issues (P71)

- **None is closed.** `[00-later-bounces-act-on-quanta-already-present]` stays open as the user ruled (board §3, narrowed
  2026-11-06). Add a dated line: documented in verification §8, with the point where the random-phase reading fails.
- **Narrow `[00-the-parking-leftover-has-no-spectrum]`** with 02d's measured parking share (its range, its median, and
  where it is largest), from the verification's sections. Its next step is unchanged: out of this campaign's scope.
- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`:** add a dated line naming the handover's point on
  it.
- **The index.** Update `.documents/OPEN_ISSUES.md` in the same commit: §1.23's description says the campaign is closed,
  the header's sentence for this board says the same, and the date. The count stays 29 unless the work opens something.
  A new issue goes on the board's §3 and in the index, per `CLAUDE.md`.

### 1.6 C6 — the board and the index closed (P73)

- **The board:**
  - row 08 and its items, with the gates as they stand;
  - the status line rewritten as a short statement of the closed campaign. Do not extend the present run-on; move it,
    unchanged, below a heading such as "History of the status line", so that nothing is lost;
  - the opening description ("The campaign replaces the fixed-α drain by a one-loop law, α_eff = min(α_cap, A·2.7/ℓ)
    …") corrected to the measured law by a dated note beside it;
  - **Last updated**, and §5's baseline for 08.
- **`prompts/INDEX.md`:** the campaign's row says complete, with each prompt's commit, the dates, the final suites, and
  where the verification and the handover are. Correct the header's date and counts.

---

## 2. Verification

1. **The five suites**, from the repository root with `CLAUDE.md`'s commands, `ComputeTargets/tests` in two halves and
   `tools/tests` alone. Record each count, each time, and `uptime` at each start. The counts must equal 07's (26, 665,
   163, 259, 72). A count that falls is a stop.
2. **Every line addressed to 08**, the close, the documents, `CLAUDE.md` or the handover is done, or named in the log as
   left, with why. Look in the logs 01–07, README §8.11–§8.16, the orchestrator notes and the board (search for "08",
   "close", "handover", "made false", "for the documents"). Put them in a table in the log.
3. **Every command in C4 runs as written:**
   - `--help` for each driver and tool it names: `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
     `RunDrivers/registered_run.py`, `tools/quanta_measures.py`, `tools/plot_quanta_grid.py`,
     `tools/plot_quanta_history.py`, `tools/bbn_from_store.py`, and any other;
   - each option the handover names appears in that `--help`, spelled as the handover spells it. Check this with a
     script, which goes in `.documents/one-loop-drain/close-08/` with its output in `out/`.
4. **The inventory**, with no Ray (`main.py --inventory` starts none):
   - **On `var/datastores/one-loop-drain-06-reference.db`.** Take `shasum -a 256` of its 17 files (and the manifest)
     before and after. They must be identical.
   - **On a copy of `one-loop-drain-06-base`** (its 17 files) in your scratchpad, never the store itself. Take
     `shasum -a 256` before and after, and record what the tip does with a format-6, 2026.9.0 store: what it reports or
     refuses, and whether the copy changed. Either outcome is a measurement for C4 point 1. A copy that changed is also
     an issue (a reader writing to a store); open it, and do not fix it.

   Keep the commands and the hashes in `close-08/out/`.
5. **Ten numbers** of verification §8 and ten of §4.20, chosen across their sections, checked against their cited sources
   and listed in the log with the source line.
6. **The board and the index agree:** the count, §1.23's rows, and the campaign's row in `prompts/INDEX.md`.
7. **`git diff --stat`** touches only the files of §4. No Python file changes but the scripts in `close-08/`, which
   `black` formats.

## 3. Acceptance

1. C1–C6 as §1 says, and §2's checks recorded.
2. **One commit**, with the log `logs/08-close.md` in the form of `dk-and-rpk-migration` README §5.1. The log has:
   - the suite counts;
   - every deviation, classified;
   - §2 item 2's table;
   - "Observations not acted on";
   - "State handed on", to the user: the merge, the rebuild and the stores.

   Stage by explicit path, and show `git diff --cached --name-status` before committing. The commit message follows
   `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 4. What this prompt does not do

- **It changes no code and no test.** It builds no store, starts no Ray, and runs no `main.py` build, plot script,
  derivation tool, figure tool or registered run. The only programs it runs on stores are §2 item 4's inventories.
- **It edits no record.** Not `science-runs/`, its scripts or plans; not the campaign's README, the prompts, the
  orchestrator notes or the earlier logs; not §1–§7 of the verification document, or any earlier section of
  `review-remediation-verification.md`. Per `CLAUDE.md` invariant 6, these are records.
- It writes no run script for the rebuild, and nothing in the briefing notes' repository or the paper.
- It retires, moves or rewrites no store.
- It does not merge or push.
- **Files it changes:**
  - `.documents/one-loop-drain-verification.md` (a new §8, and §9 below);
  - `.documents/numerical-strategies.md`;
  - `.documents/architecture-summary.md`;
  - `.documents/review-remediation-verification.md` (§4.20 only);
  - `CLAUDE.md`;
  - `.documents/one-loop-drain/close-08/` (new: §2's scripts and outputs);
  - the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`;
  - the log.

  The verification document also gains a **§9, "Prompt 08 — the close"**: the suites, the inventories' hashes, and §2's
  checks, item by item.

## 5. Stop conditions: stop and ask

- An item of the Gate fails, or an issue is assigned in §3.1 or in the index's §1.23.
- A count falls, or a suite fails.
- **A log, the verification or the README disagrees with the code** on a formula, a constant, a column or a default,
  beyond wording. State both, and ask which the documents follow; the code is not changed here.
- A number the documents need was never measured. Say it was not measured; do not measure it. If the handover cannot
  be written without it, ask.
- A statement made false by this campaign cannot be corrected without a code change.
- The inventory changes a byte of `one-loop-drain-06-reference`.
- C3 cannot be done without adding a rule to `CLAUDE.md`.
