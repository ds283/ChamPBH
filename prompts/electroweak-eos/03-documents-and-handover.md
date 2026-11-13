# Prompt 03 — documents and handover

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-13, on branch `electroweak-eos` in the main checkout. 02 had landed and been reviewed (`be81f8b`,
`4fa0aa7`), and the user asked: "Please write prompt 03 and its orchestration note". It is written from:
- README §1, §4, §6.2 P10–P12, §7 and §10 (U12, P21–P29);
- every log's "State handed to the next prompt", log 02 §5 and §6 in particular;
- the board's §2, §3 and §5;
- `one-loop-drain` prompt 08, whose form this prompt follows.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file. In
particular, the note says how U12 was ruled. Items marked **(U12 a)** below are done only if the user ruled U12 (a).

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `electroweak-eos`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Scratch goes in your subdirectory of the session
scratchpad, never `/tmp`.

**Gate:**
- on branch `electroweak-eos`, at or after the commit that adds this prompt; `be81f8b` is an ancestor of `HEAD`;
- `git status`:
  - **(U12 a)** no tracked change and nothing untracked;
  - **(U12 b)** only the user's files that the orchestrator note lists;
- `tools/ray_cluster.sh status`: no ChamPBH process. You start none;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `tools/build_w_table.py --check` exits 0;
- `var/datastores/electroweak-eos-02-old.db` and `-new.db` exist, each with its 16 shards and its manifest;
- `.documents/electroweak-eos-verification.md` has §1 and §2 and no §3.

If any of these fails, stop and ask.

**Closes:** nothing. No issue is assigned to this campaign (board §3.1). **Opens:** under (U12 a), the one of P26;
otherwise only what the work finds.

**Recommended model:** **Sonnet**, as README §2 plans. There is no code and no measurement but one inventory and the
suites. The work is reading four prompts' records and writing them where the repository's readers look, exactly
enough that the user can plan a rebuild from one section.

**Read first:**
1. [`README.md`](README.md): §0, §1, §4, §5, §6.1 as ruled, §6.2 P10–P12, §7, and §10 whole.
2. Every log of this campaign (`logs/01a`, `01`, `02`), the board, and `.documents/electroweak-eos-verification.md`
   §1–§2.
3. `.documents/review-remediation-verification.md` §4.20, the last handover, and its opening list of what it covers.
4. `.documents/architecture-summary.md` §8.5 and §14 (the note of 2026-11-07 on a model's wall variable and the
   workers' working directory); `.documents/numerical-strategies.md`, searched for the table (`Xav`, `Σ`, `w(T`,
   `1 − 3w`, `dw_dlogT`, "kick", "electroweak", "386.7", "25 TeV").
5. `.documents/surfing-temperature-verification.md` §1.8 and §3; `.documents/peakewsurf-kicking-function/report.tex`,
   its addendum section.
6. The run record `science-runs/2026-11-08-science-2026.10.0/README.md` and `science-runs/README.md`, as they stand at
   the Gate.
7. `prompts/one-loop-drain/08-close.md` and its log: the form of a close.
8. `CLAUDE.md`.

---

## 0. Where this prompt sits

01a made the turning-point classifier ignore pairs the solver does not resolve. 01 installed the new w table as
`2026.11.0`. 02 measured the electroweak-capture threshold and what the table does to 48 of the `2026.10.0` run's
histories. Every gate but Close holds.

This prompt makes the repository say what the campaign changed, and hands the rebuild to the user (README §7).
With it, the user can plan the rebuild from §4.21 without reading the campaign.

**`CLAUDE.md` gains no rule.** This prompt changes only what `CLAUDE.md` *describes*: counts, times and the table's
source. It adds no sentence that tells a reader what they must or must not do. If you think such a sentence is
needed, put it in the log's §5 as a proposal for the user.

**Measurements, not bounds.** Every number written is a measurement from a record, cited to its source. No document
says that a figure "stands" or "falls", or that a change is small or large against a threshold the user has not ruled.

**Records are records.** `science-runs/`, the audit, other campaigns' verification documents and the earlier sections
of `review-remediation-verification.md` gain dated additions only (invariant 6). Nothing above an addition is edited.

---

## 1. What is wanted

### 1.1 D1 — the documents (P21)

- **`architecture-summary.md` §8.5.** Add a dated note after "Currently `QCD_Cosmology` is the main implementation,
  using a lattice QCD equation of state for g*(T)". It says that:
  - w, read through `Xav_EOS_spline` from `CosmologyModels/GenericEOS/Xav_EOS_data.csv`, is a table separate from
    g_ρ and g_s;
  - from `2026.11.0` it follows Laine & Meyer through the electroweak crossover, with Σ = 0 from 386.7 GeV, and keeps
    the old e⁺e⁻ tail below 50.1 keV;
  - it is the output of `tools/build_w_table.py` from committed inputs;
  - g_ρ and g_s are unchanged (U4);
  - the labels are `VERSION_LABEL` 2026.11.0 and `FINGERPRINT_FORMAT` 7, unchanged.

  Cite verification §1 for the numbers. Read the code and the CSV for what you state; do not paraphrase a log.
- **`architecture-summary.md` §14.** Add one dated line to the 2026-11-07 note's bullet on the workers' working
  directory. It says that 02 measured the effect with two different tables, and gives the issue's name and
  verification §2.4.1. Do not repeat the mechanism, which the bullet already states.
- **`numerical-strategies.md`.** Add a dated note at each statement that describes the old table, or a property of Σ
  that the new table changed, as a present fact. Do not edit the statement. Known candidates:
  - "Σ_T's source": "0 beyond the table", where the table now ends at 386.7 GeV with Σ = 0;
  - any value of Σ, of its peak, or of the old table's top row (25.1 TeV);
  - any threshold quoted as 3.020 or 3.0195.

  Search, as "Read first" item 4 lists, and do not rely on this list. A statement inside a dated note of an earlier
  campaign is that campaign's record: leave it, unless the note describes the present.
- **`surfing-temperature-verification.md`.** Add a new dated §4, "The threshold on the 2026.11.0 table (pointer,
  2026-11-13)". It gives F-new's and F-old's edges and windows, and points to `electroweak-eos-verification.md` §2.2.
  §1–§3 are not edited.
- **The audit.** Add one dated paragraph at the end of `report.tex`'s addendum section, before "Reproducing these
  results". It gives:
  - the campaign;
  - `VERSION_LABEL` 2026.11.0;
  - the threshold measured in ChamPBH, beside the notebook's 3.00285 that the audit quotes;
  - the verification document's path.

  Rebuild `report.pdf` with `latexmk -pdf` in a scratch copy of the folder, and copy back only `report.pdf`. If the
  build fails, leave the PDF as it is and say so in the log. That is not a stop.

### 1.2 D2 — the handover (P22)

A dated addendum to `.documents/review-remediation-verification.md`, **§4.21**, in §4.20's form. It opens with:
- what the campaign did, in order, each prompt with its commit and gate: 01a `634f141` (Resolved), 01 `5947b4f`
  (Table), 02 `be81f8b` (Threshold, Effect), and 03 "this commit" (Close);
- its board, plan, verification and dated notes;
- the labels;
- that the branch is not merged when §4.21 is written.

Then a "This supersedes" list, by statement and not by edit. It covers anything in §4.20 or earlier handovers that
named the w table, the 3.020 threshold or the `2026.10.0` histories as current. Search; do not guess.

Then the points:

1. **What changed for a build.**
   - **The table.** Its sources by range, and its build and check.
   - **01a's classifier.** A pair of sign changes the solver does not resolve is dropped. It is counted in an unstored
     field.
   - **`VERSION_LABEL` 2026.11.0.** `FINGERPRINT_FORMAT` stays 7.
   - **What the tip does with a `2026.10.0` store.** Run `main.py --inventory` on a copy of `electroweak-eos-02-old`
     (§2 item 3) and state what it reports, with the hashes before and after.
   - A rebuild starts a new store.
2. **What a rebuild moves**, from verification §2.
   - The threshold and the window, at M = 10⁻³ M_P, on both tables.
   - On the run's dense grid at laboratory M, A = 1, the jump moves from between 3.000 and 3.005 to between 2.980 and
     2.985.
   - The 13 histories whose T_deliver crosses 10 GeV, by β, M and row.
   - The changes by run: largest and median.
   - Each figure of the run (`fig6/`, `m-trend/`, `fig6b/`, `relativistic/`), with what moved in its own units. That
     includes the two edge histories whose parking temperature leaves Figure 6's indicative BBN window.
   - What did not move: the β away from the edge, and the bounce sequences in kind and source.
   - The P8 expectation as 02 found it, and the bath drift on both tables.
3. **The standard analyses a rebuild repeats** (P12). Every row of `science-runs/README.md`'s "Standard analyses",
   named as the file names them. **(U12 b)**: if the file at the Gate has no such section, say that the list is the
   user's uncommitted draft, and name the two analyses from the run's figures instead (Figure 6 and the relativistic
   quanta).
4. **What to expect.**
   - **The cost.** 02's sample times (log 02 §3.3: about 10 min per side of 48 histories at loads of 8–52) beside the
     run's block times (run record, "The registered runs"). Say that the run's long blocks were set by tunings at
     M_crit near β ≈ 1.1–1.2, which 02's sample did not include. Do not project from 02's times.
   - **The disk** (the run record's "B's disk failure").
   - **The workers' working directory**, for any build or comparison run from a worktree (§14, the issue).
5. **The stores kept** (P28). For `electroweak-eos-02-old` and `-new`: label, format, `store_id`, shard count and size.
   Also `var/electroweak-eos/02/` and its size. Retiring any of them is the user's.
6. **What stays open.** The board's issues, each with its next step, and the index's count. Then, each in one line
   with its log, the observations the logs left unopened that a reader of the rebuild should know. Among them:
   - test_injection's item-(11) classes read the campaign's copy of the old table (log 01 §5);
   - `plot_surfing_temperature.py` reads its own tree's table (log 02 §5);
   - the shard-count warning of the plot scripts (log 02 §5);
   - `ray_cluster.sh stop` sending SIGKILL after its grace period (log 02 §5);
   - `release_T_GeV` blank in every sampled history (log 02 §5).
7. **The surfing tests' reference rows** (P27): what each record is and where.
8. **The merge into `main` is the user's.** State the result of `git merge-base --is-ancestor main electroweak-eos`,
   and that it would be a fast-forward. Pushing is the user's.

### 1.3 D3 — the run record (P23) **(U12 a)**

Add a dated note at the end of `science-runs/2026-11-08-science-2026.10.0/README.md` saying:
- its histories were made at `2026.10.0`, with the w table that agrees with a vacuum-mass ideal gas above about
  100 GeV (the audit's addendum §1);
- `electroweak-eos` replaced that table;
- in four or five lines, what 02 measured of the change: the threshold, the 13 histories, and the figures;
- pointers to §4.21 and to `electroweak-eos-verification.md` §2.4.

Nothing above it is edited. `science-runs/README.md` is not changed: the campaign changes no input's layout, and P12's
statement lives in the handover.

### 1.4 D4 — `CLAUDE.md`, by description only (P24) **(U12 a)**

- The suite counts and the two halves, from your own run (§2 item 1): 26, 684 (219 + 465), 163, 292 and 76, unless
  your run says otherwise.
- Replace the long parenthesis that dates the counts with one sentence. It gives the date and the campaign that last
  changed a count (`electroweak-eos` 01, 2026-11-12: `test_build_w_table`, and 01a's `test_resolved_turning_points`).
  Say in the log that you did this.
- The times of `ComputeTargets/tests`, its halves and `tools/tests`, from your own run, with the loads.
- In the bullet "Run everything from the repository root", one clause saying that `Xav_EOS_data.csv` is the output of
  `tools/build_w_table.py`, which `--check` compares.
- Nothing else. No rule (§0).

### 1.5 D5 — the issues (P25, P26)

- **None is closed.**
- **`[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`.** Add a dated correction. Its
  *What* and *Impact* say that `one-loop-drain` 06's probe "did not see" the effect. Correct this: 06 recorded the
  workers' working directory, and `architecture-summary.md` §14's note of 2026-11-07 already stated that the data
  files are the cluster's start directory's. What 02 adds is the measured effect with two different tables. Change
  the index row's hook to match, if it repeats the claim. The issue stays open, with no next step proposed.
- **(U12 a) The user's open point (P26).** Open on this board's §3, and in the index §1.25, the issue that
  `tools/plot_quanta_grid.py`'s β axis is categorical and cannot be read when the dense and sparse grids share it.
  - Its *What* and *Measured* cite `relativistic/PLAN.md` §4.3 and the run record's "Standard analyses".
  - Its *Next step* quotes the user's ruling there (Q4, "retire for figures; keep as a diagnostic") as the plan's table
    gives it.
  - Name it `[03-…]` in the board's style.
- **The index.** In `.documents/OPEN_ISSUES.md`, in the same commit:
  - §1.25's description says the campaign is closed;
  - the header's sentence for this board says the same;
  - the date and the count are updated.

### 1.6 D6 — the board and the index closed (P29)

- **The board:**
  - row 03 and its items;
  - the Close gate;
  - §5's baseline after 03;
  - **Last updated**;
  - the status line rewritten as a short statement of the closed campaign. Move the present line, unchanged, below a
    heading "History of the status line", as `one-loop-drain`'s board did.
- **`prompts/INDEX.md`:**
  - the campaign's row says complete, with each prompt's commit, the dates, the final suites, and where the
    verification and the handover are;
  - the header's date and counts.

---

## 2. Verification

1. **The five suites**, from the repository root, run serially with `CLAUDE.md`'s commands: `ComputeTargets/tests` in
   two halves, and `tools/tests` alone. Run the long ones in the background. Record each count, each time, and
   `uptime` at each start. The counts must equal 02's: 26, 684 (219 + 465), 163, 292 and 76. A count that differs is a
   stop.
2. **Every line addressed to 03** — the close, the documents, `CLAUDE.md`, the run record or the handover — is done,
   or named in the log as left, with why.
   - Look in logs 01a–02, README §7 and §10, and the board.
   - Search for "03", "close", "handover", "for the documents", "`CLAUDE.md`'s counts" and "the user's".
   - Put the lines in a table in the log.
3. **The inventory**, with no Ray (`main.py --inventory` starts none).
   - Work on a copy of `electroweak-eos-02-old`'s 17 files and manifest, in your scratchpad, never on the store.
   - Take `shasum -a 256` of the copy before and after.
   - Record what the tip reports for a `2026.10.0`, format-7 store, and whether the copy changed.
   - A copy that changed is an issue (a reader writing to a store): open it, and do not fix it.

   Keep the commands, outputs and hashes in `.documents/electroweak-eos/close-03/out/`.
4. **Ten numbers** of §4.21 and the run record's note, chosen across their points. Check each against its cited
   source and list it in the log with the source line.
5. **The board and the index agree:** the count, §1.25's rows, and the campaign's row in `prompts/INDEX.md`.
6. **`git diff --stat`** touches only the files of §4. No Python file changes. `tools/build_w_table.py --check` exits 0
   at the end.

## 3. Acceptance

1. D1–D6 as §1 says, and §2's checks recorded.
2. **One commit**, with the log `logs/03-documents-and-handover.md` in the form of `dk-and-rpk-migration` README §5.1.
   The log has:
   - the suite counts;
   - every deviation, classified;
   - §2 item 2's table;
   - "Observations not acted on";
   - "State handed on", to the user: the merge, the rebuild and the stores.

   Stage by explicit path, and show `git diff --cached --name-status` before committing. The commit message follows
   `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the orchestrator substitutes it.

## 4. What this prompt does not do

- **It changes no code and no test**, including the surfing tests' reference rows (P27). It builds no store, starts no
  Ray, and runs no `main.py` build, plot script or registered run. The only program it runs on a store is §2 item 3's
  inventory, on a copy.
- **It edits no record** except by a dated addition:
  - not the campaign's README, prompts, orchestrator notes or earlier logs;
  - not §1–§2 of the verification document;
  - not any earlier section of `review-remediation-verification.md`;
  - not the audit's text above its new paragraph.
- It retires, moves or rewrites no store.
- It does not merge or push.
- **Files it changes:**
  - `.documents/electroweak-eos-verification.md` (a new §3, "Prompt 03 — the close": the suites, the inventory's
    hashes, and §2's checks, item by item);
  - `.documents/architecture-summary.md`;
  - `.documents/numerical-strategies.md`;
  - `.documents/surfing-temperature-verification.md` (a new §4);
  - `.documents/peakewsurf-kicking-function/report.tex` and `report.pdf`;
  - `.documents/review-remediation-verification.md` (§4.21 only);
  - `.documents/electroweak-eos/close-03/` (new: §2's commands and outputs);
  - **(U12 a)** `CLAUDE.md` and `science-runs/2026-11-08-science-2026.10.0/README.md`;
  - the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`;
  - the log.

## 5. Stop conditions: stop and ask

- An item of the Gate fails, or an issue is assigned in §3.1 or in the index's §1.25.
- A count differs, or a suite fails.
- **A log, the verification or the README disagrees with the code or the CSV** on a value, a column or a default,
  beyond wording. State both, and ask which the documents follow; the code is not changed here.
- A number the documents need was never measured. Say it was not measured; do not measure it. If the handover cannot
  be written without it, ask.
- A statement made false by this campaign cannot be corrected without a code change.
- **(U12 b)** Any item would need one of the user's uncommitted files.
- D4 cannot be done without adding a rule to `CLAUDE.md`.
