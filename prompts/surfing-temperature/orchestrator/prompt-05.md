# Orchestrator — prompt 05, close

Read [`../README.md`](../README.md) first: §0.4, §2.2 (05's charter), §5, §6.1 (U9) and §7. Then the
board in full, from §5.5 on most closely. The conventions are note 03's
([`prompt-03.md`](prompt-03.md), "Conventions"), restated here where 05 needs them.

**You do not write code.** You may:
- run the suites;
- read the documents 05 changes, and check its statements against the records they cite;
- fix small residue in a follow-up commit of your own (§4);
- after the review, and with the user's go-ahead, merge the branch into `main` (P10).

**The prompt:** [`05-close.md`](../05-close.md)
**Board items:** Z1–Z4 · **Gate:** none of its own · **Closes:** the campaign · **Opens:** nothing
(U9's issue is already open) · **Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 04b has landed (`a65bd74`, reviewed `5603a7b`), and U9 is recorded (`29e34db`). `HEAD` is the
  commit that adds this note, or a later one that touches no code. Since `a8823a9`, only
  `2c98370`, `f166b31`, `1f40b53`, `ee4a2df`, `90d44f8` and `a65bd74` have touched code, and
  `5603a7b` and `29e34db` have touched documents outside `prompts/`.
- One prompt at a time in this checkout. This is the last prompt.

## 0. What the agent needs that the prompt does not say

**This note governs where it differs from the prompt.** §0.1–§0.6 correct or complete it, and §0.7
restates the conventions.

### 0.1 U9: one issue is open, and the handover names it

After 04b's review the orchestrator found that `tools/surfing_scan.py` builds every history's
potential with `LAMBDA_EV` = 1e-3 eV, imported from `tools/history_and_bbn.py:111`. It has no option
to change Λ, and φ_ref does not exist in the code. So the claim in README §0.4 and §7, and in the
verification document §1.7, that "the re-run after `jordan-normalization` is the same commands on
that campaign's tree" holds only if that campaign changes the tool. **The user ruled (U9, README
§6.1) that the handover says so, and that an issue is opened.** The orchestrator opened it in
`29e34db`:
- the board's §3, `[05-surfing-scan-runs-at-a-fixed-lambda]`, with its what, impact and next step;
- `.documents/OPEN_ISSUES.md` §1.13, and the count 26 → 27.

So for 05:
- **the prompt's "Opens: nothing expected" and "Only if the campaign opened an issue" both resolve
  to: one issue, already open.** 05 does not open it again and does not close it. It stays open
  when the campaign closes, on this board, until a later campaign takes it. `jordan-normalization`
  has no board yet, so it is not assigned.
- **Z3** names the issue, and says what the re-run command does today: it runs at Λ = 1 meV and
  φ_ref = 0, whatever the tree.
- **Z4, the index.** `OPEN_ISSUES.md`'s header keeps **27 open** and its date. Its narrative for
  `surfing-temperature`, and §1.13's first line, say the campaign closed on 2026-10-14 with 7 of 7
  prompts landed, and that the one issue stays open on the board. The index row in
  `prompts/INDEX.md` says "1 opened (U9), 1 open; 27 open in the whole index".

### 0.2 Z3: the capture claim is qualified

README §0.4 says the capture will not move with `jordan-normalization`. The fine scan shows that
near β = 3.0 the wall's timing decides *whether* the first episode is captured at all (verification
§1.7 and §1.8; log 04, "Observations not acted on"). The handover says what moves, in three parts:
- **the roots: nothing** (Σ is a function of T_J alone);
- **the capture's dynamics: nothing**, where the capture comes before the wall (the surfing system
  depends only on β/M_P);
- **possibly something:**
  - which β near 3.0 are captured: QCD to 2.998, EW from 3.020, and nothing at 2.999–3.019;
  - the largest temperature, the second dip and delivery, which the wall sets.

Each of these moves only if the scan is re-run at the tuned Λ, which needs the change in §0.1. The
handover cites §1.7 and §1.8 for the measurements, and does not repeat their tables.

### 0.3 Z3: the re-run command must name a new output file

`tools/surfing_scan.py` resumes into an existing CSV. Run with the committed path, it does one of
two things:
- with the same options, it skips every β and adds nothing;
- with another φ* or M, it exits 2, because a row's inputs differ from the options (`_INPUT_MATCH`,
  `tools/surfing_scan.py:155`).

So the handover's command writes a new file, and its name carries the options, as the committed
names do: `surfing_scan_phi<P>_M<M>.csv` and the figure prefix `surfing_temperature_phi<P>_M<M>`.
It gives:
- **the coarse scan**: verification §1.1's S1 command, with φ* and M as placeholders and a new
  output path;
- **its figure**: the plot command after it;
- **the fine scan near β = 3.0**, S5's command, in one line, as optional.

It says:
- that both run from the repository root and start no Ray;
- that the scan writes a sidecar beside the CSV, which the plot reads;
- what each step cost: S1 287 s, S5 155 s, the figures 52 s and 4 s (verification §1.1). Other φ*
  and M will cost differently.

Its options are those of the sidecar `surfing_scan_phi5_M1e-3.csv.meta.json`: `beta_min` 1.1,
`beta_max` 5.0, `beta_step` 0.05, `phi_init_Mp` 5.0, `M` 0.001, `T_stop_GeV` 0.001 (the default).
**Read, not run** (prompt §2 item 2).

### 0.4 Z1: where things go in `architecture-summary.md`

The prompt's line numbers are a line off, and two of its places need a ruling.

- **The tree has no `tools/` entry at all** (`.documents/architecture-summary.md:21-129`). The
  2026-10-02 note below the tree names `tools/history_and_bbn.py` and `tools/bbn_baseline.py`, but
  the tree never did. **Ruled:**
  - add a `tools/` block between `PRyM/` (`:120-121`) and `config/` (`:123`);
  - it lists `surfing_scan.py` and `plot_surfing_temperature.py`, each with a `#` comment in the
    tree's style, and then a `...` line, as `CosmologyModels/GenericEOS/` has (`:104-107`);
  - nothing else is listed.

  The directory holds six tools and `tests/`. Listing the other four would be more than §1.1 asks.
- **The dated notes** are at `:131-142` (`science-readiness`) and `:144-165` (`read-only-plots`),
  and the section ends with `---` at `:167`. The new note goes after the second, before the
  `---`, and is headed `> **Note, 2026-10-14 (`surfing-temperature`, prompts 01 to 04b).**`.
- **§7.2's `first_bounce` appears only inside the dated note at `:713-747`** (the 2026-10-02 note on
  the `ScalarModel` columns), which is kept byte for byte. **Ruled:** the sentence for §7.2 is a
  short dated note of its own, placed after that one and before `**ScalarModelProxy:**` (`:749`),
  one or two sentences long. The reason T_J turns where β π/M_P = −1 is the temperature law
  (`ComputeTargets/ScalarModel.py:441`): d ln T_J/dN = −(1 + (d ln Ω/dφ) π)/(1 + (d g_s/d ln T)/(3 g_s)),
  whose denominator is positive. For the exponential coupling, d ln Ω/dφ = β/M_P.
- Every existing `> **Note, …**` block is unchanged. The check: `git diff` of the file has no `-`
  line inside a block that starts `> **Note`.

The facts for the note, with the code's own names:
- `find_kicks(cosmology, T_lo, T_hi, n_grid)` (`extract_common.py:649`) locates Σ's peaks, and the
  troughs between them, on the spline that `cosmology.w(T)` reads (the integrator's own w). It
  labels them `EW`, `QCD`, `mu-pi`, `ee` (U5).
- `reachability_roots(cosmology, beta, kicks, n_grid)` (`:712`) gives the hot and cold roots of
  Σ_eff = 1/(3β²) per kick, their slopes, and the merged intervals.
- `surfing_turning_points(result, coupling)` (`ComputeTargets/ScalarModel.py:878`, with
  `SurfingCrossing` at `:866`) gives every T_J turning point up to and including the first bounce.
  `compute_scalar_model` does not call it, so nothing is stored.
- `tools/surfing_scan.py` gives one CSV row per β, with a sidecar, and resumes.
  `tools/plot_surfing_temperature.py` draws the two-panel figure from that CSV. Neither starts Ray
  or opens a store.

### 0.5 Z2: only the counts change in `CLAUDE.md`

The four suites' counts are at `CLAUDE.md:78-81`, and the two halves at `:86-87`. They become:
- CosmologyModels **18**;
- ComputeTargets **176**, in halves **61** and **115**;
- ObjectFactories **69**;
- tools **70**.

"(The counts are those of 2026-10-14.)" at `:89` keeps its date, since that is today. No log 01–04b
records a statement in `CLAUDE.md` that the campaign has made false, so nothing else changes. That
includes "about 200 s" at `:83`, whatever the run takes under today's load: record the time in the
log. If the agent finds such a statement itself, that is the prompt's §4 stop.

### 0.6 Z3 and Z4: where, and what the records say

**The handover**, `.documents/review-remediation-verification.md`:
- §4.13 runs from `:1466` to `:1567`, and ends with the paragraph "Added 2026-10-14, by the user's
  ruling: point 4's first behaviour is by design." Then come `---` and `## 5. Reproduce` (`:1569-1571`).
- §4.14 goes after that paragraph and before the `---`, headed
  `### 4.14 Addendum 2026-10-14 — the `surfing-temperature` campaign`.
- It follows §4.13's form:
  - an opening paragraph, "Added by `surfing-temperature` prompt 05, written at `<HEAD>`", saying
    what the campaign did, with its commits;
  - **"This supersedes"**, which may be "nothing" if no earlier point of §4 is made false. Say so
    in one line, after checking §4.10 and §4.13 point 5;
  - numbered points.
- §4.1–§4.13 and §5 are not touched.
- **The merge.** Say that the branch `surfing-temperature` is merged into `main` after this prompt's
  review, by the orchestrator with the user's go-ahead (P10), and is not yet merged when this is
  written.

**The board** (`### 5.6 Close (2026-10-14)`, and the status line):
- The status line: **COMPLETE — 7 of 7 landed**, with the commits of 01, 02, 03, 03b, 04, 04b and
  "this commit"; one issue open (§3, U9); not yet merged (P10).
- Rows 05 and Z1–Z4 ✅. The gates are already ✅.
- §3 is unchanged: the issue stays open.
- The notes table is the orchestrator's, after review. Do not add note 05's row.

**`prompts/INDEX.md`**:
- the `surfing-temperature` row (`:23`) becomes **complete**:
  - the landed commits, each with a few words on what it did;
  - the verification document and §4.14;
  - the final suites 18 / 176 / 69 / 70;
  - "not yet merged into `main` (P10)";
  - U1–U9 ruled;
- the open-issue column, as §0.1 says;
- the header (`:11`) becomes "9 campaigns: 0 planned, 0 live, 9 closed".

The `read-only-plots` row (`:22`) says "not yet merged into `main`", which is stale since
`5740690`. It is not 05's to fix. Record it in "Observations not acted on".

**Commits for the records:**
- plan `8adb806`, prompts `a8823a9`;
- landed: 01 `2c98370`, 02 `f166b31`, 03 `1f40b53`, 03b `ee4a2df`, 04 `90d44f8`, 04b `a65bd74`;
- reviews: `b5d1c94`, `61e0c42`, `9ab3e65`, `3fa4d53`, `25cdd2e`, `5603a7b`;
- U9: `29e34db`.

### 0.7 Conventions

Note 03's hold. In short:
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the suites show.
- **The SHA.** Write "this commit". The orchestrator substitutes the SHA in its follow-up.
- **Suites.** `CLAUDE.md`'s commands, with ComputeTargets in its two halves. Run each in the
  foreground and serially. Send each run's output to a file in the agent's scratch directory, grep
  the verdict from it, and record the load (`uptime`) beside it. Re-run a failure alone before
  calling it real.
- **Scratch.** Use the agent's own subdirectory of the session scratchpad, never `/tmp`.
- **The venv.** Install or uninstall nothing.
- **Stores and Ray.** Open none and start none. Run no history and no PRyMordial, and do not run
  the scan or the plot.
- **`black`** does not apply: 05 changes no Python.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

**The baseline at `HEAD`**, serially, by the orchestrator: given at dispatch.

**What the review exists to establish:**
- **(E1)** every statement 05 adds is true of the tree and of the records it cites;
- **(E2)** nothing outside the prompt's §5 list moves, and no dated block is edited;
- **(E3)** the handover's command would do what it says if it were run;
- **(E4)** the records close the campaign consistently: the board, the index row, the issue index,
  and `CLAUDE.md`'s counts.

## 1. Before you dispatch

1. **The tree.** Check the branch and `HEAD`. `git status --short` shows only the untracked set
   (23 entries: the 17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, the
   two `full_run_2026.6.0*.sh` and `prompts/jordan-normalization/`). Nothing is staged.
2. **The baselines.** Run the four suites at `HEAD`, serially, with the load, and give the agent the
   numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and the board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad. The
  agent does not read `orchestrator/`;
- its scratch directory, `agent05/`.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/05-close.md`, in `dk-and-rpk-migration` README
  §5.1's form.
- **The files it may change are the prompt's §5 list, and nothing else.** `OPEN_ISSUES.md` is on
  it, for §0.1's narrative only.
- **§0 governs** where it differs from the prompt: U9, the capture qualified, the new output path,
  and the placements in `architecture-summary.md`.
- **It runs the four suites as its final check**, and runs no scan, plot, history, Ray or store.
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - the full diff of `architecture-summary.md`, `CLAUDE.md` and §4.14;
  - the suites, with the load;
  - `git diff --stat` of the commit;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — eight checks

1. **Scope (E2).** `git diff <agent>~1 <agent> --stat` lists `.documents/architecture-summary.md`,
   `CLAUDE.md`, `.documents/review-remediation-verification.md`, `prompts/INDEX.md`,
   `.documents/OPEN_ISSUES.md`, the board and the log, and nothing else. `git status` shows §1's
   untracked set and nothing new. No `*.py` is in the diff.
2. **The dated blocks are unchanged (E2).**
   - `architecture-summary.md`: the diff has no `-` line, or the only `-` lines are in the tree,
     where the new block is inserted. Every `> **Note` block is byte-identical.
   - `review-remediation-verification.md`: the diff only adds lines, between §4.13's last paragraph
     and `---`.
   - `CLAUDE.md`: the diff touches only `:78-81` and `:86-87`, and only their numbers.
3. **Z1 by reading (E1).** The tree block, the new note and the §7.2 note are where §0.4 says. Their
   statements match the code:
   - the function names and signatures;
   - "the integrator's own w";
   - "not called by `compute_scalar_model`" (`git grep surfing_turning_points` outside tests and
     tools);
   - "neither starts Ray or opens a store": `grep -n "import ray\|ray\.init\|ShardedPool"` on
     the two tools finds nothing.
4. **Z3 by reading (E1, E3).**
   - The command's options equal the sidecar's, and its output path is new.
   - The plot command reads that path's CSV.
   - The three-part "what moves" of §0.2, with §1.7 and §1.8 cited.
   - U9's issue named, and the fixed Λ stated.
   - "Not stored; a later campaign's `ScalarModel` columns on the science rebuild".
   - The outputs' paths exist.
   - "This supersedes" checked against §4.10 and §4.13.
   - No number in §4.14 differs from the verification document or the board. Check each number
     against its source.
5. **Z4 (E4).**
   - **The board:** COMPLETE, the commits right, Z1–Z4 and row 05 ✅, §3 still open, a `### 5.6`.
   - **The index row:** the commits, the suites and the issue column.
   - **The header:** 0 live, 9 closed.
   - **`OPEN_ISSUES.md`:** 27 open, the date, and the narrative and §1.13 saying closed and still
     open.
6. **Z2 (E4).** `CLAUDE.md`'s counts equal the final suites.
7. **The suites** at the agent's commit, serially: 18, 61 + 115, 69, 70, all OK.
8. **The log.** It is in §5.1's form, every deviation is classified, and it names the stale
   `read-only-plots` row as an observation.

## 4. After the review

- **Record it on the board** as an *Orchestrator review of prompt 05* paragraph in §5.6. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (the board, the log, the index row, §4.14 if it names it);
  - this note in the notes table, "used for 05";
  - the status line, if it needs it.
- Anything the review finds that is not residue is opened on the board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the suites;
  - §4.14's re-run command;
  - the open issue;
  - the stale `read-only-plots` row, which is housekeeping for the orchestrator, in a standalone
    commit, if the user agrees.
- **Then ask for the go-ahead to merge** `surfing-temperature` into `main` (P10). Do not push unless
  the user asks.
