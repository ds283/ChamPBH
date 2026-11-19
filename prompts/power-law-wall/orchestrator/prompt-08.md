# Orchestrator — prompt 08, the close

**Written** 2026-11-19 with the prompt, at the user's request: "The close-out prompt 08 and its orchestration note now
need to be written." Not dispatched. P135–P148 (README §6.17) stand unless the user rules otherwise; none needs a ruling
first. 08 is dispatched at the user's word.

Read [`../README.md`](../README.md) first: §1, §2 (row 08), §4, §5, §6.12–§6.17 and §7. Then the prompt. This note binds
08 only, and governs where it differs from the prompt. The conventions of earlier notes bind as well: a record states only
what a command or a file shows, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, `--inventory` and the `--help` commands the prompt names, and §2 item 5's tools on scratch copies;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`08-close.md`](../08-close.md) · **Gate:** "Close" · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-19 at 11:23 GMT, before this note was committed:
- **The worktree.** `.claude/worktrees/power-law-wall` on `power-law-wall` at `69d5d45` (review of 07d), on top of
  `b5f863a` (07d). `git status --porcelain` showed `?? venv`, plus the planning files before their commit.
  `git worktree list`: the main checkout (on `main` at `9df9e13`, clean) and this worktree.
- **The labels.** `config/version.py:134` `VERSION_LABEL = "2026.14.0"`; `config/fingerprint.py:100`
  `FINGERPRINT_FORMAT = 8`.
- **Ray and the lock.** `tools/ray_cluster.sh status`: ports 6380, 6381, 8267 and 52367 free;
  `raypoolkit.rehearsal_lock status`: "lock: free".
- **The registry.** 96 runs, all finished; none running, stale or unknown. Thirteen are `power-law-wall-07d-r2-*`.
- **The stores**, in `<main>/var/datastores/`: `power-law-wall-07d-r2-{exp,pl,pl-resumed,mixed}`, `power-law-wall-07-{base,
  exp,pl,pl-resumed,mixed}` and `power-law-wall-07d-exp` each have 16 shards, the primary `.db` and a
  `.manifest.json`; `power-law-wall-07d-pl` has the 16 shards and the primary and no manifest (README §6.16).
- **The index.** `.documents/OPEN_ISSUES.md`: **36 open**, last updated 2026-11-19; §1.26 lists exactly the board's four,
  none assigned (board §3.1). **The prompt's first stop condition does not trigger.**
- **The suites** were not re-run for this note. 07d's review left them at 26, 809 (226 + 583), 175, 312 and 79, with the
  two worktree failures (board §5) and no other; only `69d5d45` (the review, records only) has landed since.
- **The load.** 6.32 / 8.84 / 9.59 at 11:22. 80 GB free on the volume (92% used).

## 0. What the planner found

For this note the planner read: README §1–§7; the board's header, §1, §3–§5 and its "For 08" and "For the user"
paragraphs; logs 04 §7, 06 §6–§7, 07 §7 and 07d §7; `one-loop-drain` prompt 08 and its note, and `electroweak-eos` prompt
03; the section maps of the verification document, `numerical-strategies.md`, `architecture-summary.md` and
`review-remediation-verification.md`; `CLAUDE.md`; and, on `main` by `git show`, `science-runs/README.md` and the 2026.11.0
run record's README, run script and five analysis scripts.

Here is what it found where the prompt is silent, loose or wrong.

- **No dated note of this campaign exists in `architecture-summary.md` or `numerical-strategies.md`.** `grep -c
  power-law-wall` finds none in either. 01–07d added none (their prompts put the documents to 08), so 08 writes the
  campaign's whole account there, not a map of earlier notes.
  - **Placement.** `numerical-strategies.md` (2635 lines): §6.12 (`one-loop-drain` 08) starts at `:1944`, `## 7.` at
    `:2022`. `architecture-summary.md` (2493 lines): §7.2 at `:916`, `### 7.3` at `:1608`; §8.4a at `:1798`, `### 8.5` at
    `:1943`; §9 at `:1987`, `## 10.` at `:2148`; §14 at `:2335` to the end. `review-remediation-verification.md`: §4.21
    starts at `:2686`; its point 8 is followed by `---` and `## 5. Reproduce` at `:2959`. The verification document ends
    with §11.12 at `:3016`.
  - **Statements made false, by count** (the agent's search decides; these are where to start):
    `one-loop-measured-v1` 4 in each document (architecture `:1486`, `:1504`, `:1512`, `:1567`; numerical `:1946`,
    `:1955`, `:1993`, `:2004`); `U4-U17` 2 | 1 (architecture `:1600`, `:2411`; numerical `:421`); `x_p` 12 in
    numerical; `0.0058` at numerical `:431`; `k_ex` 5 | 10; `VERSION_LABEL` 13 | 16 and `FINGERPRINT_FORMAT` 2 | 10, most
    inside dated notes of earlier campaigns, which are records unless they describe the present.
- **Where the code is.** `classical_turning_point` (`AbstractModel.py:807`), `one_loop_turnaround` (`:891`), `_pulse_rate`
  (`:789`) and `ONE_LOOP_MAX_ITERATIONS = 400` (`:127`) are in `AbstractModel`, not `ScalarHistory.py`. The hooks:
  `ExponentialModel.py` `_drain_L` `:238`, `one_loop_drain_model` `:258`, `_drain_fraction` `:266`, `spectrum_wall`
  `:282`, `_d3_logV_dphi3` `:334`, `_pulse_rate_hook` `:352`; `InversePowerModel.py` `one_loop_drain_model` `:170`,
  `spectrum_wall` `:179`, `_drain_fraction` `:190`, `_d3_logV_dphi3` `:271`, `_pulse_rate_hook` `:292`.
  `drain_alpha_effective` is `ScalarHistory.py:1881`. `drain_model.py`: the two current names `:66`–`:67`, the retired
  ones `:77`–`:89`. `quanta_spectrum.py`: `SPECTRUM_MODELS` `:155`, `X_STAR` `:188`. `DEFAULT_TUNING_MAX_INTEGRATIONS =
  30` (`ScalarHistory.py:5498`); the step budget's default 2 000 000 (`:296`).
- **`CLAUDE.md` is behind the code.** It shows 783 (226 + 557) for `ComputeTargets/tests` and dates the counts to 06
  (`:85`, `:94`–`:95`, `:97`–`:102`); 07c added 22 and 07c2 4, both to the `test_[g-z]` half. The plot-scripts bullet
  (`:142`) names `<label>/alpha=<α>/` and `<label>/<drain_model>/A=<A>/`, which R12 moved under `<label>/<model>/`, and
  does not name `--models`. The Redshift bullet (`:139`–`:141`) already names both current drain models (07c, P108).
- **The 2026.11.0 analyses hard-code a retired name.** `fig6.py:52`, `fig6b.py:42` and `relativistic.py:61` set
  `ONE_LOOP = "one-loop-measured-v1"`; `m_trend.py` selects `drain_model != "fixed"`; `changes.py:61`'s `KEY` is
  `["M_Mp", "beta", "drain_model", "alpha"]`, with no `n` or `model`; every script reads a copy of
  `fig6/inputs/histories.csv`. The run script exports to `--output "$OUT/main"` and runs `quanta_measures` and
  `plot_quanta_grid` on `$OUT/main/QCD_Cosmology`, which at the tip is the parent of the potentials' roots. These live on
  `main` only; 08 reads them by `git show` and edits none.
- **The 2026.11.0 build's block walls** (its README, "The registered runs"): A 4 h 00 min, B 4 h 09 min, D 23 min, R-lab
  3 min, R-ladder 14 min, R-natural 1 min, C (M_crit) 3 h 13 min, R-Mcrit 25 min; 12 h 28 min in all. These are the
  projection's basis (P144).
- **The merge facts, at `69d5d45`.** Merge base `a7e0978`. `git merge-base --is-ancestor main power-law-wall` exits 1:
  `main` (`9df9e13`) has 15 commits since the merge base, every one under `science-runs/` (the 2026.11.0 record, its
  `science-runs/README.md` row and its analyses); this branch changes nothing under `science-runs/`. `git merge-tree
  --write-tree main power-law-wall` exits 0 (no conflict). `github/main` is `84a2494`, 16 commits behind local `main`.
  `power-law-wall` has no remote branch. So the merge is a true merge, not a fast-forward; the handover states this from
  commands it runs itself.
- **The worktree's ignored outputs**, under `var/power-law-wall/`: `06` 76 MB, `07` 654 MB, `07d` 788 KB, `07d-r2` 515 MB.
  `git worktree remove` would delete them; the stores are in the main checkout's `var/` and would not be touched.
- **The inventories' stores.** `power-law-wall-07-base` was built at `a7e0978` (format 7, `2026.11.0`) and
  `power-law-wall-07-exp` at `945bf5d` (format 8, `2026.12.0`); each is about 120 MB, so a scratch copy is cheap. They
  stand in for the 2026.11.0 science stores, which are ten times larger and are not copied.
- **The per-n split (P143).** 07d's `export-pl/QCD_Cosmology/inverse-power/` holds `histories.csv`, `quanta_histories.csv`,
  `alpha=0/`, `alpha=0.01/`, `alpha=0.1/` and `one-loop-inverse-power-v1/`; 07d's `quanta-grid-pl/` is empty (the
  refusal). Per-history directories are named `M=…eV_n=<n>`, so a split by n keeps directories by name and CSV rows by
  their `n` column.
- **`prompts/INDEX.md`'s header** reads "**Last updated:** 2026-11-18 · 19 campaigns: 0 planned, 1 live, 18 closed",
  and the table agrees. After the close: 0 live, 19 closed.

**These rulings bind the agent.** Each is recorded in the log as a deviation of the class given, where it reads the prompt
otherwise. `NOTED` ones are expectations, recorded as found.

1. **The worktree, not the main checkout** (`STRUCTURALLY REQUIRED`; prompt "Where it runs"). Every edit is in the
   worktree. `main`'s files are read by `git show main:<path>` and quoted with that path; no file of the main checkout is
   opened for writing and none of its tracked files is read from its working tree.
2. **Additive, in the documents' own form** (`STRUCTURALLY REQUIRED`; `CLAUDE.md` invariant 6).
   - In the verification document, `numerical-strategies.md`, `architecture-summary.md` and
     `review-remediation-verification.md`, **no existing line is deleted or reworded.**
   - A correction is a dated note directly after the paragraph, list or code block it corrects, in the form
     `> **Note, <date> (`power-law-wall`, prompt 08): <topic>.** …`. In `numerical-strategies.md` it may be one line
     pointing to §6.13. It goes only where the text is false of the present and no later dated note already says so. A
     statement inside an earlier campaign's dated note that describes that campaign's state is a record and gets no
     note.
   - **Placement**, by §0: verification §12 and §13 after §11.12; `### 6.13 Added <date> (`power-law-wall` prompt 08):
     the power wall and one k_ex as they stand` after §6.12's last line and before `## 7.`; the architecture notes at the
     ends of §7.2, §8.4a and §9, and any §14 note at the end of the file; `### 4.22 Addendum <date> — the
     `power-law-wall` campaign` after §4.21's point 8 and before the `---`.
3. **The code is what the documents state** (`STRUCTURALLY REQUIRED`; prompt §1.1).
   - Every formula and constant of verification §12 and §6.13 carries its `file:line` at the dispatch commit, read in the
     code. Constants are given at the precision the code holds.
   - The README's superseded readings are history, not disagreements: §0.3's estimates; P4 (`_drain_L` for the power
     wall), P11 and P16–P17 as amended; `one-loop-measured-pl-v1` (renamed by U11); v1's law and plateau (replaced by
     U13). They go in the log's §2 item 2 table as "superseded within the README", and are not a stop.
   - A disagreement the agent finds between a log, the verification or the README and the code, beyond wording, is the
     prompt's stop.
4. **Symmetry** (`STRUCTURALLY REQUIRED`; R12, prompt §0). Verification §12, §6.13 and §4.22 treat the two potentials in
   one scheme: each item gives both, side by side, or says why one has nothing there. No section is the power wall's
   alone with the exponential as a footnote, or the reverse.
5. **The inventories** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 4).
   - In place on `power-law-wall-07d-r2-exp`: its 18 files hashed before and after.
   - Copies of `power-law-wall-07-base` and `power-law-wall-07-exp`, all 18 files each, made with `cp -p` into
     `<scratch>/copy-07-base/` and `<scratch>/copy-07-exp/`; each copy's hashes checked against its original's before
     anything runs, and hashed again after. The originals are hashed once more at the end and must be unchanged.
   - The planner expects the format-7 copy to be refused at the schema (as `one-loop-drain` 08 found for a format-6 store
     and P41 for a store lacking `InversePowerModel`), and does not predict the format-8, `2026.12.0` copy. Whatever each
     does is the measurement. A changed byte of `07d-r2-exp` is the prompt's stop; a changed copy is an issue.
   - **Committed outputs.** In `close-08/out/`, the scratchpad's path is written `<scratch>`, and the head of each such
     file says so. Inventories are committed whole.
6. **The `--help` check** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 3). As the prompt says, with the option list
   taken from §4.22's final text and the check run on it then. `--stale-after` is checked against `runregistrykit list`.
   A command §4.22 gives with values has each option checked by `--help`; none is run against a store or an export.
   `black` formats the script.
7. **The per-n roots** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 5, P143).
   - The copy is `cp -Rp` of 07d's `export-pl/QCD_Cosmology/inverse-power/` into `<scratch>/split/all/`, and the split
     roots `<scratch>/split/n1/` and `<scratch>/split/n2/` are made from it by a script, `close-08/split_by_n.py`
     (committed, `black`-formatted), which keeps directories whose name ends `_n=<n>` and the rows of every root-level CSV
     whose `n` is n, and writes what it kept and dropped to `out/`.
   - The quanta files 07d wrote (`quanta_histories.csv` and each history's) are removed from each split root before
     `quanta_measures` runs, so that each root's quanta files are its own.
   - `tools/quanta_measures.py <root> --assoc-scale 1`, then `tools/plot_quanta_grid.py <root>`, on `all`, `n1` and `n2`:
     each exit, its last lines, its wall, and the figures it wrote, by count. No PDF or PNG is committed.
   - The expected result, from the issue: `all` refused, `n1` and `n2` drawn. A different result is a measurement, put
     on the issue as found; it is not a stop.
8. **The projection** (`IMPLEMENTATION CHOICE`, ruled; prompt §1.4 point 4, P144). Labelled a projection. Its basis is
   §0's block walls, the power/exponential ratio per history from 07d's §11.10 at laboratory M and from 04's §4.7 by the
   tool route where 07d has none, and the number of n. It names the C (M_crit) block as its weakest assumption: no history
   of this campaign was near M_crit. Nothing is run to refine it.
9. **`CLAUDE.md`, descriptions only** (`STRUCTURALLY REQUIRED`; prompt §0, §1.3).
   - The counts become the run's; the parenthesis becomes one sentence (prompt §1.3); the times are the run's, each with
     its one-minute load, and say "in a worktree".
   - The plot-scripts bullet names `<label>/<model>/` and `--models`. Anything else the agent's pass finds is a
     description.
   - **No added text contains an imperative or a modal of obligation.** The log shows
     `git diff -U0 <dispatch> -- CLAUDE.md | grep '^+' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'`
     and accounts for every hit. An unchanged clause carried inside an edited line is listed as such.
10. **The lines addressed to 08** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 2).
    - The agent may read the orchestrator notes 01–07d and the board's review paragraphs for this table, read-only. **It
      does not read this note's §2 and §3.**
    - A statement in code that only a code change would correct (for example `REMOVED_OPTIONS["--potential-type"]`'s
      message, log 06 §6) is not a stop. It is listed as "left: code, outside 08's files", and named in §4.22 point 7. The
      prompt's stop "a statement made false … cannot be corrected without a code change" is about a statement in the
      documents or `CLAUDE.md`.
11. **The issues and the index** (`NOTED`; prompt §1.5).
    - The board's `[06-plot-quanta-grid-refuses-two-n-at-one-cell]` gains a dated line with ruling 7's measurement.
    - `.documents/OPEN_ISSUES.md`: its header gives the date and the count 36, and this board's clause says the campaign
      closed, 13 of 13 prompts landed, with four open; §1.26's paragraph says closed; the `[06-…]` row's hook takes the
      measurement in a few words. Nothing more is copied from the board.
    - Anything opened goes on the board's §3 and in the index in the same commit, and the count moves with it.
12. **The board and `prompts/INDEX.md`** (`NOTED`; prompt §1.6).
    - **The status line.** The present run-on moves unchanged into "History of the status line" as its first entry, dated.
      Its place in the header is taken by a short line: "CLOSED <date> — 13 of 13 prompts landed …; every gate holds; four
      issues open, none assigned; the merge into `main` is the user's".
    - **The opening description** gets a dated note naming one k_ex and the exponential's law v2.
    - **The `Code:` line** is checked against `git diff --stat a7e0978 <dispatch> -- '*.py' '*.yaml'`; files it misses
      (for example `RunDrivers/registered_run.py`, `extract_common.py`, `inverse_power.yaml`) are added.
    - Row 08 ✅ with "this commit"; the gate Close in §2; `### 5.N After prompt 08` in §5's form. The list of orchestrator
      notes is the orchestrator's.
    - **`prompts/INDEX.md`.** The row is **complete**, in the closed rows' form: 13 of 13 landed, each with its commit;
      the dates `2026-11-13 → <date>`; the final suites; verification §12 and §4.22; "§1.26; 4 open; 36 open in the whole
      index". The header reads "**Last updated:** <date> · 19 campaigns: 0 planned, 0 live, 19 closed". No other row is
      edited.
13. **The suites** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 1).
    - Once, before the edits, serially, with `CLAUDE.md`'s commands from the worktree's root; `ComputeTargets` in its two
      halves; `uptime` at each start and end.
    - The `test_[g-z]` half (about 540 s at loads of 3–7) and `tools/tests` (about 315 s) run as background Bash calls,
      and the agent waits for each notification without polling. The others run in the foreground at 600 000 ms.
    - No `.py` file outside `close-08/` changes, so this run is the final one. The log shows `git diff --stat <dispatch>`
      at the end.
    - The two worktree failures (board §5) are expected. Another failure is re-run alone before it is called real; a
      timing test that fails across the machine's sleep or under load is ruled (a) as in 07d (record and move on). A
      count that differs, or a failure that recurs, is a stop.
14. **The log** (`STRUCTURALLY REQUIRED`). `logs/08-close.md`, in `dk-and-rpk-migration` README §5.1's form. It holds:
    - what shipped, item by item;
    - the deviations with their classes, these rulings among them;
    - verification: the suites with times and loads, the inventories with their hashes, the `--help` check, the per-n
      measurement, the modal grep, the twenty numbers with their source lines, and the board–index agreement;
    - §2 item 2's table;
    - "Observations not acted on";
    - "State handed on", to the user: the merge, the science run, the stores and the worktree.

**What moves on purpose:** exactly the prompt's §4 list:
- `.documents/power-law-wall-verification.md` (§12 and §13, added);
- `.documents/numerical-strategies.md`;
- `.documents/architecture-summary.md`;
- `.documents/review-remediation-verification.md` (§4.22 only);
- `CLAUDE.md`;
- `.documents/power-law-wall/close-08/` (new: `check_help.py`, `split_by_n.py`, `out/`);
- the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`;
- `logs/08-close.md`.

**What must not move:**
- every other file of the tree: code, tests, goldens, the config files, and `science-runs/` above all;
- `orchestrator/`, the README, the prompts and the earlier logs;
- verification §1–§11, and every earlier output under `.documents/power-law-wall/`;
- every other campaign's records;
- `var/` in both checkouts, every store above all; the venv;
- the main checkout, in every file.

No Ray is started, no store is built, and no plot script, grid, figure tool or registered run is run.

**The stop conditions.** The prompt's §5, as rulings 3 and 10 read them, and:
- a suite that fails twice, or a count that differs from 07d's;
- `--inventory` changing a byte of `power-law-wall-07d-r2-exp`, or either original copied;
- a command §4.22 would give that does not run as written.

A stop is a reply headed **STOP**, with the work left uncommitted; it goes to the user through the orchestrator, and the
agent resumes by message.

**Conventions.** All of these bind the agent.
- **Order.**
  1. The Gate, re-checked.
  2. The suites.
  3. The reading: the files of the prompt's "Read first", and the table of ruling 10.
  4. Verification §12.
  5. §6.13, the architecture notes, §14 and the dated notes.
  6. `CLAUDE.md`.
  7. The inventories and the per-n measurement, then §4.22, then ruling 6's check.
  8. The issues, the board and the indexes.
  9. Verification §13, then the log.
- **Staging and reporting.**
  - One commit, staged by explicit path only. Show `git diff --cached --name-status` before committing; it must hold only
    the files of "What moves on purpose".
  - Check `git log -1` before committing. `HEAD` must still be this note's commit, or a later one that is not the
    agent's; other sessions may land commits.
  - "this commit" goes where the SHA goes; the orchestrator substitutes it.
  - The message is in `CLAUDE.md`'s form.
- **Scratch** goes in `<scratch>`, the agent's subdirectory of the session scratchpad (`agent-08`). Nothing goes in
  `/tmp` itself, and nothing is written under `var/`.

## 1. Before you dispatch

- Measure the Gate again, as above, and record any difference; a run `running`, a held lock or a store missing is a stop
  before dispatch.
- Re-run nothing else: the suites' baselines are 07d's review's, and the agent runs them.
- If `main` has moved since `9df9e13`, re-read §0's merge facts and pass the new ones to the agent.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

Of `orchestrator/`, it reads notes 01–07d for ruling 10's table only, and not this note.

Tell it plainly:
- **One commit, at the end**, of exactly the files of "What moves on purpose".
- **§0's rulings bind it**, each recorded with its class.
- **No existing line of the four documents is deleted or reworded** (ruling 2), the potentials are described
  symmetrically (ruling 4), and **`CLAUDE.md` gains no rule** (ruling 9).
- **It starts no Ray and builds nothing.** It opens no store but `power-law-wall-07d-r2-exp`, and the scratch copies of
  `07-base` and `07-exp`, through `--inventory`.
- **Stop and ask** on any of the stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites with counts, times and `uptime`;
  - each section and note it added, by file and line, with what it corrects;
  - `CLAUDE.md`'s diff and the modal grep;
  - §4.22's points, one line each, its "supersedes" list, and the projection with its basis;
  - the `--help` check's output, the inventories' results and hashes, and the per-n measurement;
  - the twenty numbers it checked, with their sources;
  - the board's status line, the INDEX row and header, and OPEN_ISSUES's header clause and §1.26;
  - ruling 10's table;
  - every deviation with its class;
  - "Observations not acted on" and "State handed on", verbatim.

## 3. The review — nine checks

1. **Scope.**
   - `git show --stat <agent>` lists only the files of "What moves on purpose". Its parent is this note's commit, or a
     later commit that is not the agent's.
   - `git diff <dispatch> <agent> -- '*.py' ':!.documents/power-law-wall/close-08/' '*.sh' '*.toml' '*.yaml'
     requirements.txt science-runs/ prompts/power-law-wall/README.md prompts/power-law-wall/orchestrator/
     prompts/power-law-wall/0*.md` is empty, and so is the diff of `prompts/power-law-wall/logs/` but for `08-close.md`.
   - `git status --porcelain` shows `?? venv` only; the main checkout's `git status` is unchanged; nothing is pushed; no
     PDF or PNG is committed.
2. **Additive.**
   - `git diff <dispatch> <agent>` on the four documents has no `-` line, bar a moved blank line that is explained.
   - Verification §1–§11 are byte-identical.
   - §4.22, §6.13, the architecture notes and verification §12–§13 sit where ruling 2 places them.
3. **The suites, re-run by me**, serially: 26, 809 (226 + 583), 175, 312 and 79, with the two worktree failures and no
   other.
4. **The code claims.** Twelve `file:line` references from verification §12, §6.13, the architecture notes and §4.22,
   chosen across the models, k_ex, both laws, the spectrum, the store and the drivers, read in the code at `<agent>`. I
   check both laws' constants against `ExponentialModel.py` and `InversePowerModel.py` myself, and v2's against
   `minform.json` (`with_new`, `n12/S3/v-J`).
5. **The numbers.** Fifteen numbers of verification §12 and §4.22 that are not among the agent's twenty, traced to their
   cited sections. Among them: S₁ and S₂; the power wall's largest miss (0.054 | 0.056); v2's A_f and ℓ₀; the 45.0-decade
   margin; 07d's ΔN_eff moves (−0.4% to −6.7%); the 47 candidates and 27.3; the per-history costs; 54 s; the stores'
   sizes; the block walls of the projection.
6. **The commands and the measurements.** I run `close-08/check_help.py` myself, and check five commands of §4.22 against
   their `--help` by hand. I hash `power-law-wall-07d-r2-exp`, `07-base` and `07-exp` myself against `out/`. I re-run
   `split_by_n.py` and `plot_quanta_grid` on one split root in my own scratchpad and compare the exit and figure count.
7. **The grep.** My own grep of `numerical-strategies.md` and `architecture-summary.md` for `one-loop-measured-v1`,
   `DRAIN_MODEL_ONE_LOOP`, `U4-U17`, `0.0058`, `x_p = 0.62`, `−d ln V/dφ`, `FINGERPRINT_FORMAT`, `VERSION_LABEL` and
   `histories.csv`. Every hit that describes the present is true, already noted, or noted by 08.
8. **`CLAUDE.md`.** Its diff changes only descriptions. I run the modal grep myself, and every hit is accounted for.
9. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The board, the index and `prompts/INDEX.md` agree: the count 36, §1.26's four rows, the campaign closed, and the
     INDEX header's counts.
   - Verification §13 holds the suites, the hashes, the per-n measurement and §2's checks.
   - §4.22 point 8's merge facts equal mine, run at review time.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- this note added to the board's list of orchestrator notes, marked "used for 08";
- an *Orchestrator review of prompt 08* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s row, if the SHA appears there.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3 and in
`.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- that the campaign is closed, with what the close wrote and where;
- the handover's points that need the user: the science run of record (both potentials; the exponential rebuilt), the
  2026.11.0 analyses' scripts, the stores to retire, the worktree's outputs, the merge into `main` (not a fast-forward)
  and the push; local `main` is ahead of `github/main`;
- anything opened, and any proposal in the log's §5.
