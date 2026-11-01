# Prompt 12 — close

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-01, after 11 landed (`c797831`) and was reviewed (`d029573`), at the user's word ("Now
please write prompt 12"). It is written from:
- README §2.2's charter for the close (the old 08) and §8.4's row for 12, with §8.6, what the campaign hands to
  the user;
- log 11 §7, "State handed to the next prompt", and the board's §5.13, including its note "The stores,
  2026-11-01, after the review" (`707be48`);
- `jordan-normalization` prompt 10 and its log, the last close, whose form this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: no ChamPBH process. You start none;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/datastores/inelastic-reflection-11-reference.db` and its 16 shards exist.

If any of these fails, stop and ask.

**Closes:** nothing. Every issue assigned to this campaign is closed (board §3.1). Check that
`.documents/OPEN_ISSUES.md` §1.20 lists none. **Opens:** only what the work finds.

**Recommended model:** **Sonnet**, as the plan has it (README §8.4). Documents, a handover and the indexes,
from the logs. No code changes.

**Read first:**

1. [`README.md`](README.md): §1, §2.2 (the old 08's charter), §5, §7, and all of §8 (the re-plan, its rulings
   R6–R31, P18–P47 and §8.6).
2. Every log of this campaign (`logs/01`–`11`, with `07b`), the board, and
   `.documents/inelastic-reflection-verification.md`.
3. `.documents/architecture-summary.md` (§7.2's dated notes of this campaign, §8.4a's, §14),
   `.documents/numerical-strategies.md` (§6.1–§6.9), and `CLAUDE.md`.
4. `.documents/review-remediation-verification.md` §4.16 and §4.17 (the last handovers: the run scripts'
   changes, the cost, the held rebuild), and `science-runs/README.md`.
5. `prompts/jordan-normalization/10-close.md` and its log: the form of a close.

---

## 0. Where this prompt sits

01–11 built dissipation mode, stored it, carried α through every driver and reader, measured it, and
rehearsed a build with two α under real Ray. Every gate holds: DR, Inject, Taper, Wall, Unresolved, Park,
Release, Band, Margin, Store, Driver, Measure and R. Four issues stay open on the board and none is
assigned. This prompt makes the repository say what the campaign changed. With it, the user can plan the
science rebuild from one section of the handover without reading the campaign.

**What the user has decided since 11** (board §5.13, "The stores"): the store
`inelastic-reflection-11-reference` is kept as the reference store at 2026.8.0.
`inelastic-reflection-11-resumed` and `jordan-normalization-09-reference` are retired. The 09 store was
retired without a fingerprint, because this campaign's code cannot read a store from before format 6
(`StoreSchemaMismatch`).

**`CLAUDE.md` gains no rule.** The user has said that some of its "rules" were written by agents and
never agreed. This prompt updates what `CLAUDE.md` *describes*: counts, times, options and conventions.
It adds no sentence that tells a reader what they must or must not do. If you think such a sentence is
needed (log 11 §7 raises one, D3's `sys.modules` patch), it goes into the architecture summary's §14 as
a description, and into the log's §6 as a proposal for the user.

---

## 1. What is wanted

### 1.1 The items

- **C1 — the documents.** Each of 01–10 added dated notes where it changed what a document describes
  (P12). The close adds what those notes cannot: the whole, and 11's measurement.
  - `numerical-strategies.md`: a new dated subsection, §6.10, on 11's measurement. It covers:
    - the drain's independence of M below 10⁻¹² M_P;
    - ΔN_eff against α;
    - natural mode at 10⁻⁷;
    - the parking ε;
    - the BBN spline across injections (P14).

    Each comes with its numbers and points to the verification document's section.
  - `numerical-strategies.md`: a dated §6.11, "dissipation mode as it stands". This is a reading guide
    over §6.1–§6.10. It says, in order, what a history at α > 0 does:
    - the drain;
    - the gate;
    - the unresolved rule;
    - parking;
    - the release;
    - the wall search;
    - the peak and the margins;
    - the stored values.

    It also says which statements in §6.1–§6.9 are superseded and by what. The stopping rule of §6.3 was
    withdrawn on 2026-10-28; check each of the others.
  - `architecture-summary.md`: one dated note at the end of §7.2, after 10's, that maps dissipation mode
    onto the code. Name the modules, the public functions and the payload keys, and point to each
    prompt's note rather than repeat it.
  - `architecture-summary.md` §14: a dated note on two things. First, D3:
    `import ComputeTargets.ScalarHistory as X` binds the class that `ComputeTargets/__init__.py` exports
    under the module's name, so the module is reached through `importlib.import_module` or `sys.modules`.
    Second, `PARKING_EPSILON` is read at call time.
  - Any statement in either document that describes the present and that this campaign made false is
    corrected by a dated note at the statement. Do not edit the statement itself.
- **C2 — `CLAUDE.md`**, descriptions only (§0):
  - **The suite counts and the two halves.** These are 26 / 497 (122 + 375) / 129 / 123 / 68, unless your
    own run in §2 says otherwise. Update the parenthesis that dates the counts.
  - **The time of `ComputeTargets/tests`.** Use your own run's times and load. Log 11 measured 447 s for
    the `test_[g-z]` half at a load of 13, so "about 320 s" for the whole is low.
  - **The plot-scripts bullet.** `--alpha-values` is matched (log 10 §8). A plot asked for an α the store
    lacks exits 2. Per-α output goes under `alpha=<α>/`.
  - **The "Units" or "Redshift" bullet** gains ρ_χ, by description:
    - ρ_χ,E = K e^{−4N} in the Einstein frame, K a piecewise-constant ledger;
    - ΔN_eff is stored at T_CMB and at 1 MeV, with 1 MeV under R22's T_ν = T_γ convention;
    - the per-sample ρ_χ/ρ_R is a payload key and not a column;
    - α is a key of every history, with its own table.
  - **Anything a log names as made false in `CLAUDE.md` and not yet fixed.**
- **C3 — the handover.** A dated addendum to `.documents/review-remediation-verification.md`, §4.18, in
  §4.16's and §4.17's form. Write it so that a reader who knows only §4.16 can plan the rebuild.
  1. **What changed for a build.**
     - α is a key. `--alpha-values` (default `[0.0]`) replaces the default rather than extending it, and
       runs one pipeline per α.
     - α = 0 is bitwise 2026.7.0 where the code did not change it (11's 548 922 values).
     - The format is 6 and the label 2026.8.0. This code cannot read a 2026.7.0 store, so a rebuild starts
       a new store.
     - `inelastic-reflection-11-reference` is the reference store at 2026.8.0.
  2. **The run scripts' changes against §4.16.**
     - The option.
     - The `alpha=<α>/` level on every per-α product, including α = 0's.
     - `dark_radiation.pdf`, `Delta_N_eff.pdf` and `histories.csv`'s new columns.
     - The wrapper's scope.
     - `--alpha` on `tools/history_and_bbn.py` and `tools/bbn_from_store.py`.
     - The tools' `dissipation` line.

     A new folder under `science-runs/` holds the scripts when the user writes them; 12 does not write
     them.
  3. **The two modes and where each holds.**
     - Natural mode (α = 0) at large M, with the P23 warning at M = 10⁻⁷.
     - Dissipation mode down to laboratory M through parking.
     - R17's unofficial large-M dissipation regime, with the rehearsal's β = 0.5, M = 10⁻³, α = 0.1 history
       (ΔN_eff today 8.95; log 11 §6) as its example.
     - The margins: every birth margin fails at M = 10⁻¹², and none fails at 4×10⁻³¹.
  4. **The α grid**, which is the user's choice, with the facts log 11 §7 gives for it:
     - the measured span;
     - ΔN_eff saturating at β ≥ 2;
     - the one-loop α ≈ 0.08 at laboratory M (README §8.0);
     - the ratio of ΔN_eff at 1 MeV to today's.
  5. **The cost.**
     - Per history, in each mode, from log 11 §7.
     - Per build, from the rehearsal.
     - The longest heartbeat silence measured, 153 s (verification §2.5), and whether `--stale-after`
       is needed.
  6. **What stays open.** README §8.6's list, and the four open issues of the board, each with its next
     step. Name the two that bear on a rebuild:
     - the Ray deduplication of the warnings. A build's log does not show which histories warned; the
       stored columns do;
     - the ratio steps in the 10–100 keV window.
  7. **The merge into `main` is the user's** (U1). `main` has not moved since `6f635e6`, so check this
     with `git merge-base`.
- **C4 — the board closed.**
  - Row 12, and every row landed.
  - The gates.
  - §3 and §4 as they stand.
  - The status line rewritten as a short statement of the closed campaign. Do not extend the present
    run-on.
  - The **Verification** line's "(created by 07)" made right.
  - A `### 5.14 After prompt 12 (<date>)` section and a closing paragraph.

  Outside the board:
  - `prompts/INDEX.md`: its row says complete, with each prompt's commit and the date. Correct the
    header's date and counts.
  - `.documents/OPEN_ISSUES.md`: the header's sentence for this board says the campaign is closed, and
    §1.19 and §1.20 say the same. The count changes only if you open or close something.
- **C5 — the final suites**, all five, with their counts, which `CLAUDE.md` then states (C2).

---

## 2. Verification

1. **The five suites**, run from the repository root with `CLAUDE.md`'s commands, `ComputeTargets/tests`
   in two halves. Record each count, each time, and the load (`uptime`) at each start. The counts must
   equal log 11's. A count that falls is a stop.
2. **Every "for the documents", "for `CLAUDE.md`" and "made false" line** in logs 01–11 is either done or
   named in the log as left, with why. List them in a table.
3. **Every command in C3 runs as written:**
   - `--help` for each driver and tool it names;
   - `main.py --inventory` on `var/datastores/inelastic-reflection-11-reference.db` (no Ray), with
     `shasum -a 256` of the store's 17 files before and after. They must be identical.
4. **The board and the index agree:** the count, the rows of §1.19, and the campaign's row in
   `prompts/INDEX.md`.
5. **`black --check`** finds no Python file changed. `git diff --stat` touches only §4's files.

## 3. Acceptance

1. C1–C5 as §1.1 says.
2. **One commit**, with the log `logs/12-close.md` in the form of `dk-and-rpk-migration` README §5.1. The
   log classifies every deviation and has its "Observations not acted on" and "State handed on". The
   merge into `main` is the user's (U1); you do not merge or push.

## 4. Stop conditions: stop and ask

- An issue is still assigned in §1.20.
- A count falls, or a suite fails.
- A statement made false by this campaign cannot be corrected without a code change.
- The inventory changes a byte of the store.
- C2 cannot be done without adding a rule to `CLAUDE.md`.

## 5. What this prompt does not do

- It changes no code. It builds no store, starts no Ray, and runs no production run, grid or plot script.
- It does not edit `science-runs/`'s records or write the rebuild's scripts. It does not edit the
  campaign's README, the earlier logs, or the verification document; per `CLAUDE.md` invariant 6, these
  are records.
- It does not retire, move or rewrite any store.
- **Files it changes:**
  - `.documents/architecture-summary.md`;
  - `.documents/numerical-strategies.md`;
  - `.documents/review-remediation-verification.md` (the addendum only);
  - `CLAUDE.md`;
  - the board;
  - `prompts/INDEX.md`;
  - `.documents/OPEN_ISSUES.md`;
  - the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
