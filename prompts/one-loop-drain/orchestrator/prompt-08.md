# Orchestrator — prompt 08, the close

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.4 (row 08), §8.7, §8.10 (the law as ruled)
and §8.16 (P66–P73). Then the prompt. This note binds 08 only, and governs where it differs from the prompt (prompt,
"Written"). Note 01's conventions bind as well: a record states only what a command or a file shows, timing is taken
under load, and the suites run serially.

**The prompt:** [`08-close.md`](../08-close.md) · **Gate:** none of its own (it closes the board) · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-07 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work").

**Gate**, checked at `e951b8e` on 2026-11-07 between 20:32 and 20:45 GMT, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `e951b8e`, the commit that adds prompt 08.
  `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main checkout only.
- `tools/ray_cluster.sh status` exits 0: the lock free (`~/.raypoolkit/rehearsal-lock` does not exist), no Ray process of
  the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
- `./venv/bin/python -m runregistrykit list`: 33 runs, all finished; none running, stale or unknown.
- `var/datastores/one-loop-drain-06-reference`, `-base` and `-resumed` each have **18 files**: 16 shards, the primary
  `.db` and the manifest. The prompt's "17 files (and the manifest)" is the same 18.
- `.documents/OPEN_ISSUES.md`: **29 open**, last updated 2026-11-06. §1.23 lists exactly the board's three, and none is
  assigned to this campaign (board §3.1). **The prompt's first stop condition does not trigger.**
- `git merge-base --is-ancestor main one-loop-drain` exits 0. `main` is `0209aac`. `github/main` is `7b35dc8`, **four
  commits behind local `main`** (`626b04f`, `4249829`, `cc7fe75`, `0209aac`, unpushed), and also an ancestor of this
  branch. The local branch `reflection-restart` (`25b1242`) is an ancestor of this branch (merged at `1561b60`).
- The suites were not re-run for this note. 07's review left them at 26, 665 (219 + 446), 163, 259 and 72, all passing,
  and only `e951b8e` (prompt 08, a document) has landed since. The `test_[g-z]` half took 466 s and `tools/tests` 255 s at
  loads of 3–6.
- **The load.** 13.49 / 6.39 / 4.74 at 20:32. The machine has 10 cores and 16 GB of memory, and it is shared. 44 GB free
  by `df` (96% used; the rest is purgeable, README §8.15).
- `PYTHONPATH` is unset in this session's shell.

## 0. What the orchestrator found

For this note the orchestrator read: prompt 08; README §5, §8.10 and §8.16; `inelastic-reflection`'s note 12 and the
form of its close; the board's header, §1, §2 and §3; `.documents/OPEN_ISSUES.md` §1.23 and its header; `prompts/INDEX.md`'s
header and row; the section maps of the verification document, `numerical-strategies.md`, `architecture-summary.md` and
`review-remediation-verification.md` §4.19; log 06 §7 and the layouts of logs 05–07; `ComputeTargets/ScalarHistory.py`'s
law (`:195`–`:291`, `:1818`–`:1891`), `StepControl` (`:350`–`:381`) and `DEFAULT_TUNING_MAX_INTEGRATIONS` (`:5453`);
`config/argument_parser.py`'s `--tuning-max-integrations` and `--retry-failed-scalar`; and `RunDrivers/registered_run.py`'s
account of the heartbeat (`:47`–`:60`, `:136`–`:148`). It ran the two inventories and every `--help` of §2 item 3 (§1).

Here is what it found where the prompt is silent, loose or wrong.

- **The code's law is README §8.10's, term by term.** `drain_fraction` (`ScalarHistory.py:1831`) and the constants at
  `:211`–`:221` give the table's five rows, with 0.95 where ℓ + 0.9691 − 0.41 ln L ≤ 0, f = 0 in the gap at ln R ≤ 1.5601
  and where 1 + 0.841 ln L ≤ 0 (the user's ruling), and R·S(L) in the classical regime. `drain_S_of_L` (`:1818`)
  interpolates in (ln L, ln S), holds S(0.3) below and continues as S(134)(134/L)² above. `drain_alpha_effective`
  (`:1872`) is min(0.99, A f) w_gate, and 0.0 when unassessed. The comment block at `:195`–`:210` gives each constant's
  source. **No stop is expected on the law.** Two statements read otherwise, and neither is a disagreement:
  - README §8.2's 2.7/ℓ, and P26's prescriptions. §8.10 and §8.16 say these are the plan's history.
  - §8.10's "L = M/φ_t … the assessment's". U15 (b), ruled later in the same section, makes L = ln(1 + E/V₀), the model's
    `_drain_L`, equal to M/φ_t at n = 1.
- **This campaign's dated notes are not where §6.12 and the §7.2 map go.**
  - In `numerical-strategies.md` they are in §3: 01 at `:317`, 02c at `:358`, 03 at `:394` and 05 at `:417`. §6 holds
    `inelastic-reflection`'s §6.1–§6.11 only. §6.11 ends at `:1902`, and `## 7.` is `:1903`.
  - In `architecture-summary.md`, 01, 02c and 03 are in §7.2 (`:1430`, `:1459`, `:1485`), and 03's ends at `:1535`, before
    `### 7.3` (`:1537`). 04, 05 and 07 are in §14 (`:2281`, `:2317`, `:2345`). 06 added none.
  - `review-remediation-verification.md` §4.19 ends at `:2419`, before the `---` (`:2421`) and `## 5. Reproduce`
    (`:2423`).
  - The verification document ends with §7.5, at `:2224`.
- **The inventories, run by the orchestrator** (§1).
  - **The reference store**, in place: exit 0 in 26 s, no Ray. It printed "read-only; 16 shards", 75,121 records in 16
    classes, `problems: none`, and label 2026.10.0. `alpha_value` has 6 records (0, 0.01, 0.1, 0.15, A = 0.1 and A = 1
    under `one-loop-measured-v1`), and `BBNData` 26 (β = 0.5's two `failure=True`). The `shasum -a 256` of all 18 files
    was identical before and after.
  - **A copy of the base store** (all 18 files, `cp -p`, hashes equal to the original's) in the orchestrator's
    scratchpad: **exit 1 in 1.7 s.** The tip printed
    `!! --inventory: Cannot read sharded datastore "…": shard #0 "…" differs from the tables this code declares (it lacks
    the table(s) "ScalarHistoryBounce"; its table "alpha_value" lacks the column(s) "drain_model"; its table
    "ScalarHistory" lacks the column(s) … "bounce_rows"; its table "ScalarHistoryValue" lacks the column(s)
    "log_m_eff_Mp", "log_m_tracker_Mp"), so nothing was read`. The 18 hashes of the copy were identical before and after.
    So the tip refuses a format-6, 2026.9.0 store and does not write to it. That is the measurement C4 point 1 needs, and
    it agrees with 03's note ("No migration (P32): a 2026.9.0 store is read with the tree of its own commit").
- **Every `--help` §2 item 3 names exits 0.** These are `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
  `RunDrivers/registered_run.py`, `tools/quanta_measures.py`, `tools/plot_quanta_grid.py`, `tools/plot_quanta_history.py`,
  `tools/bbn_from_store.py`, `tools/history_and_bbn.py` and `runregistrykit list`.
  - `main.py`'s includes `--A-values`, `--alpha-values`, `--tuning-max-integrations`, `--retry-failed-scalar`,
    `--n-values` and `--inventory`.
  - `registered_run.py` takes only `--campaign`, `--prompt`, `--register` and `--purpose`.
  - **`--stale-after` is an option of `runregistrykit list`** (default 900), not of the wrapper.
  - `quanta_measures.py` takes `--assoc-scale`, and `bbn_from_store.py` takes `--drain-law`, `--small-network` and
    `--variant`.
- **The bounds C4 point 3 asks for, from the code.**
  - `--tuning-max-integrations` defaults to `DEFAULT_TUNING_MAX_INTEGRATIONS` = 30 (`ScalarHistory.py:5453`; the
    argument parser takes it as its default). The seed's integration is included.
  - The step budget is `StepControl`'s `step_budget`, default 2 000 000 accepted steps (`:350`–`:381`). Exceeding it is a
    failure of the history. The build path does not override it (`StepControl(atol=atol, rtol=rtol)` at `:5188`).
  - Neither is a wall-clock limit. `main.py --help` also lists `--bbn-wall-clock-limit`, which applies to the BBN stage
    only. The agent says what it bounds, from the code.
- **The heartbeat.** Log 06 §7 measured the longest silence as 160 s of the 900 s window. The wrapper's docstring
  (`registered_run.py:47`–`:60`) says how a beat is taken. A beat comes from a stdout line, and `RayWorkPool` prints a
  progress line only after completed work. So a stage whose remaining items each take longer than 900 s is reported
  stale while it works. C-3873's kind takes about an hour per tuning (02d). The handover states both facts (ruling 5).
- **`CLAUDE.md` already describes most of this campaign.** The Redshift bullet names `alpha_value`'s pair and the drain
  model, and the plot-scripts bullet names `--A-values` and the one-loop paths. The counts are 07's, and 08 adds no test.
  What C3 changes is probably the times (`:86`, `:111`) and anything the agent's own pass finds (ruling 6).
- **`prompts/INDEX.md`'s header is wrong.** It reads "**Last updated:** 2026-11-02 · 17 campaigns: 1 planned, 0 live, 16
  closed". The table has 17 rows: 16 **complete** and this campaign's, **live**.
- **The handover needs the merge facts above:** local `main` is unpushed by four commits, and `github/main` is behind it.

**These rulings bind the agent.** Each is recorded in the log as a deviation of the class given, where it reads the prompt
otherwise. `NOTED` ones are expectations, recorded as found.

1. **Additive, in the documents' own form** (`STRUCTURALLY REQUIRED`; `CLAUDE.md` invariant 6; prompt §4).
   - In the verification document, `numerical-strategies.md`, `architecture-summary.md` and
     `review-remediation-verification.md`, **no existing line is deleted or reworded.**
   - A correction is a dated note directly after the paragraph, list or code block it corrects, in the form
     `> **Note, 2026-11-07 (`one-loop-drain`, prompt 08): <topic>.** …`. In `numerical-strategies.md` it may be one line
     pointing to §6.12. It goes only where the text is false of the present and no later dated note already says so.
   - **Placement**, by §0:
     - verification §8 and §9 after §7.5;
     - `### 6.12 Added 2026-11-07 (`one-loop-drain` prompt 08): the one-loop drain law as it stands` after §6.11's last
       line and before `## 7.`. It points to this campaign's notes in §3 by line;
     - the §7.2 map after 03's note and before `### 7.3`. It points to 01's, 02c's and 03's notes in §7.2 and to 04's,
       05's and 07's in §14, by line;
     - any §14 note at the end of the file;
     - `### 4.20 Addendum 2026-11-07 — the `one-loop-drain` campaign` after §4.19's point 5 and before the `---`.
   - The dated notes and sections of 01–07, and every earlier campaign's, are records and are not edited.
2. **The code is what the documents state** (`STRUCTURALLY REQUIRED`; prompt §1.1).
   - Every formula and constant of verification §8 and §6.12 carries its `file:line` at the dispatch commit, read in the
     code.
   - §0's two README readings (2.7/ℓ; L = M/φ_t) are history, not disagreements. They are listed in the log's §2 item 2
     table as "superseded within the README", and they are not a stop.
   - **A disagreement the agent finds** between a log, the verification or the README and the code, on a formula, a
     constant, a column or a default beyond wording, is the prompt's stop.
3. **The inventories** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 4).
   - The reference store's 18 files are hashed before and after. The copy of the base is all 18 files, made with
     `cp -p` into `<scratch>/basecopy/`. The agent checks the copy's hashes against the originals' before running
     anything, and hashes the copy again after.
   - The expected result is §0's: exit 0 and unchanged for the reference, and exit 1, refused and unchanged for the copy.
     A different result is recorded as measured. A changed byte of the reference is the prompt's stop, and a changed copy
     is an issue (prompt §2 item 4).
   - **Committed outputs.** In `close-08/out/`, the scratchpad's absolute path is written `<scratch>`, and the head of each
     such file says so. The reference store's inventory (about 130 lines) is committed whole.
4. **The `--help` check** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 3).
   - One script, `close-08/check_help.py`, runs every `--help` §4.20 relies on, `runregistrykit list --help` among them.
     It writes each to `out/`, and checks that every option §4.20 names appears in its own command's help, spelled as
     §4.20 spells it.
   - The list of options it checks is taken from §4.20's final text after the text is written, and the check is run on
     it then. `--stale-after` is checked against `runregistrykit list`. `black` formats the script.
   - **Commands with values.** A command §4.20 gives with values (an A list, a `--assoc-scale`) has each option checked
     by `--help`. No such command is run against a store or an export: prompt §4.
5. **What §4.20 point 3 says of the bounds and the heartbeat** (`IMPLEMENTATION CHOICE`, ruled).
   - The tuning cap and the step budget are given from the code (§0), with `file:line`. It says that neither is a
     wall-clock limit.
   - The heartbeat: 160 s measured (log 06 §7). It then gives, from the wrapper's docstring, why a stage whose remaining
     items each take longer than 900 s reads stale while it works, and C-3873's cost.
   - It names `runregistrykit list --stale-after SECONDS` as the reader's option. It does not prescribe a value. If the
     agent thinks one is needed, that goes in the log's §5 as a proposal for the user.
   - **The projection** (prompt §1.4 point 3) is labelled a projection. It gives its basis, the per-history costs it used
     with their sections, the block counts read from `run_science_2026.8.0.sh`, and the worker count it assumes. If the
     2026.8.0 record gives that count, it uses that; otherwise it uses 10 and says so. Its weakest assumption is named.
     Nothing is run to refine it.
6. **`CLAUDE.md`, descriptions only** (`STRUCTURALLY REQUIRED`; prompt §0, §1.3).
   - The counts stay unless the run says otherwise. **The parenthesis that dates them is left as it is**: it stays true,
     and 08 adds no test. If the run's counts differ from 07's, that is a stop.
   - The times (`:86`–`:91` and `:111`) are replaced by this run's, each with the one-minute load at its start.
   - The agent's own pass covers the Redshift, plot-scripts and science-runs bullets. What it changes is a description.
   - **No added text contains an imperative or a modal of obligation.** The log shows
     `git diff -U0 <dispatch> -- CLAUDE.md | grep '^+' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'`
     and accounts for every hit. An unchanged clause carried inside an edited line is listed as such.
7. **The lines addressed to 08** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 2).
   - The agent may read the orchestrator notes 01–07 and the board's review paragraphs for this table, read-only. **It does
     not read this note's §2 and §3.**
   - A statement in code that only a code change would correct is not a stop. It is listed as "left: code, outside 08's
     files; for the orchestrator's housekeeping". An example is `reflection_count`'s docstring "elastic reflections"
     (log 07 §5). The prompt's stop "a statement made false … cannot be corrected without a code change" is read as one
     about a statement in the documents or `CLAUDE.md`.
8. **The issues and the index** (`NOTED`; prompt §1.5).
   - The board's three §3 entries gain dated lines (2026-11-07, prompt 08) as §1.5 says.
   - `.documents/OPEN_ISSUES.md`:
     - its header gives the date 2026-11-07, the count 29, and this board's clause with "prompt 08 opened and closed none,
       and the campaign closed 2026-11-07, 11 of 11 prompts landed, with these three open";
     - §1.23's paragraph says the campaign is closed;
     - the parking row's hook takes the narrowing in a few words. Nothing more is copied from the board.
   - Anything opened goes on the board's §3 and in the index in the same commit, and the count moves with it.
9. **The board and `prompts/INDEX.md`** (`NOTED`; prompt §1.6).
   - **The status line.** The present run-on moves unchanged under `### History of the status line`, placed after §5.
     Its place in the header is taken by a short line: "CLOSED 2026-11-07 — 11 of 11 prompts landed …; every gate holds;
     three issues open, none assigned; the merge into `main` is the user's".
   - **The opening description** gets a dated note beside it naming the measured law (README §8.10).
   - **The `Code:` line** is checked against `git diff --stat 3b8a553 <dispatch> -- '*.py'` (the branch point). Files it
     misses (for example `CosmologyConcepts/drain_model.py`) are added. Anything it lists that the diff does not is
     reported.
   - Row 08 ✅ with "this commit". `### 5.N After prompt 08` in §5's form. The list of orchestrator notes is the
     orchestrator's.
   - **`prompts/INDEX.md`.** The row is **complete**, in the closed rows' form: 11 of 11 landed, each with its commit; the
     dates `2026-11-02 → 2026-11-07`; the final suites; verification §8 and §4.20; "§1.23; 3 open; 29 open in the whole
     index". The header reads "**Last updated:** 2026-11-07 · 17 campaigns: 0 planned, 0 live, 17 closed". The old
     header's wrong counts are recorded in the log. No other campaign's row is edited.
10. **The suites** (`IMPLEMENTATION CHOICE`, ruled; prompt §2 item 1).
    - Once, before the edits, serially, with `CLAUDE.md`'s commands. `ComputeTargets` runs in its two halves, and
      `uptime` is taken at each start and end.
    - The `test_[g-z]` half (466 s at a load of 3–6) and `tools/tests` (255 s) run as background Bash calls. The agent
      waits for each notification without polling. The others run in the foreground at 600 000 ms.
    - No `.py` file outside `close-08/` changes, so this run is the final one. The log shows
      `git diff --stat <dispatch>` at the end.
    - A failure is re-run alone before it is called real. `test_prym_passenger.test_a_oscillating_case_completes` is a
      60 s wall-clock test that fails under load. A count that falls, or a test that fails again, is a stop.
11. **The log** (`STRUCTURALLY REQUIRED`). `logs/08-close.md`, in `dk-and-rpk-migration` README §5.1's form. It holds:
    - what shipped, item by item;
    - the deviations with their classes, these rulings among them;
    - verification: the suites with times and loads, the inventories with their hashes, the `--help` check, the modal grep,
      the twenty numbers with their source lines (prompt §2 item 5), and the board–index agreement;
    - §2 item 2's table;
    - "Observations not acted on";
    - "State handed on", to the user: the merge, the rebuild and the stores.

**What moves on purpose:** exactly the prompt's §4 list:
- `.documents/one-loop-drain-verification.md` (§8 and §9, added);
- `.documents/numerical-strategies.md`;
- `.documents/architecture-summary.md`;
- `.documents/review-remediation-verification.md` (§4.20 only);
- `CLAUDE.md`;
- `.documents/one-loop-drain/close-08/` (new);
- the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`;
- `logs/08-close.md`.

**What must not move:**
- every other file of the tree: code, tests, goldens, `exponential.yaml`, and `science-runs/` above all;
- `orchestrator/`, the README, the prompts and the earlier logs;
- verification §1–§7, and every earlier figure or output under `.documents/one-loop-drain/`;
- every other campaign's records;
- `var/`, and the three 06 stores and `inelastic-reflection-11-reference` above all;
- the venv.

No Ray is started, no store is built, and no tool, plot script, grid or registered run is run.

**The stop conditions.** The prompt's §5, as §0 and rulings 2 and 7 read them, and:
- a suite that fails twice, or a count that differs from 07's;
- `--inventory` changing a byte of the reference store;
- a command §4.20 would give that does not run as written.

A stop goes to the user through the orchestrator, and the agent resumes by message.

**Conventions.** All of these bind the agent.
- **Order.**
  1. The Gate, re-checked.
  2. The suites.
  3. The reading: the files of the prompt's "Read first", and the table of ruling 7.
  4. Verification §8.
  5. §6.12, the §7.2 map, §14 and the dated notes.
  6. `CLAUDE.md`.
  7. §4.20, then the inventories and ruling 4's check.
  8. The issues, the board and the indexes.
  9. Verification §9, then the log.
- **Staging and reporting.**
  - One commit, staged by explicit path only. Show `git diff --cached --name-status` before committing; it must hold only
    the files of "What moves on purpose".
  - Check `git log -1` before committing. `HEAD` must still be this note's commit, or a later one that is not the
    agent's; other sessions may land commits.
  - "this commit" goes where the SHA goes; the orchestrator substitutes it.
  - The message is in `CLAUDE.md`'s form.
- **Scratch** goes in `<scratch>` =
  `/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-v7u1Ol/a9865546-946b-4a9a-b78d-1e1189c004dc/scratchpad/agent-08`.
  Nothing goes in `/tmp` itself, and nothing is written under `var/`.

## 1. Before you dispatch

Done at `e951b8e`, before this note was committed:
- the Gate above;
- the two inventories with their hashes. They are in `orch08/` in the session scratchpad: `inventory-ref.txt`,
  `ref-before.txt`, `ref-after.txt`, `inventory-basecopy.txt`, `basecopy-before.txt` and `basecopy-after.txt`;
- the `--help` of each command (`orch08/help_*.txt`);
- the reading of §0.

The suites' baselines are 07's review's.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and this note's §0 (up to "## 1. Before you dispatch"), which it reads in this file;
- `HEAD`, and the Gate as measured.

It reads the logs, the documents and the code itself. Of `orchestrator/`, it reads this note's §0 and, for ruling 7's
table only, notes 01–07.

Tell it plainly:
- **One commit, at the end**, of exactly the files of "What moves on purpose".
- **§0's rulings bind it**, each recorded with its class.
- **No existing line of the four documents is deleted or reworded** (ruling 1), and **`CLAUDE.md` gains no rule**
  (ruling 6).
- **It starts no Ray and builds nothing.** It opens no store but the reference, and the scratch copy of the base, through
  `--inventory`.
- **Stop and ask** on any of the stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites with counts, times and `uptime`;
  - each section and note it added, by file and line, with what it corrects;
  - `CLAUDE.md`'s diff and the modal grep;
  - §4.20's points, one line each, and its "supersedes" list;
  - the `--help` check's output, and the inventories' results and hashes;
  - the twenty numbers it checked, with their sources;
  - the board's status line, the INDEX row and header, and OPEN_ISSUES's header clause and §1.23;
  - ruling 7's table;
  - every deviation with its class;
  - "Observations not acted on" and "State handed on", verbatim.

## 3. The review — nine checks

1. **Scope.**
   - `git show --stat <agent>` lists only the files of "What moves on purpose". Its parent is this note's commit, or a
     later commit that is not the agent's.
   - `git diff <dispatch> <agent> -- '*.py' ':!.documents/one-loop-drain/close-08/' '*.sh' '*.toml' '*.yaml' requirements.txt
     science-runs/ prompts/one-loop-drain/README.md prompts/one-loop-drain/orchestrator/ prompts/one-loop-drain/0*.md` is
     empty, and so is the diff of `prompts/one-loop-drain/logs/` but for `08-close.md`.
   - `git status --porcelain --untracked-files=all` is empty, `var/` is in no commit, and nothing is pushed.
2. **Additive.**
   - `git diff <dispatch> <agent>` on the four documents has no `-` line, bar a moved blank line that is explained.
   - Verification §1–§7 are byte-identical.
   - §4.20, §6.12, the §7.2 map and verification §8–§9 sit where ruling 1 places them.
3. **The suites, re-run by me**, serially: the counts 26, 665 (219 + 446), 163, 259 and 72, all passing.
4. **The code claims.** Twelve `file:line` references from verification §8, §6.12, the §7.2 map and §4.20, chosen across
   the law, the measures, the store and the drivers, read in the code at `<agent>`. Each says what the document says.
   I check the law's constants and the five rows of f against `ScalarHistory.py` myself.
5. **The numbers.** Fifteen numbers of verification §8 and §4.20 that are not among the agent's twenty, traced to their
   cited sections. Among them: the regime shares, the 230/240 and the failures, δ_stim's range, the n̄ > 1 count (33), the
   phase-slip count (15), the backreaction ratio's maximum, the costs (1115 s; the hour of C-3873), 160 s, and the stores'
   sizes.
6. **The commands.** I run `close-08/check_help.py` myself. Then I take five commands from §4.20 and check each option
   against its `--help` by hand. I check the inventories' hashes in `out/` against my own of §1.
7. **The grep.** My own grep of `numerical-strategies.md` and `architecture-summary.md` for `α_eff`, `alpha_value`,
   `FINGERPRINT_FORMAT`, `VERSION_LABEL`, `elastic`, `histories.csv`, `2.7/ℓ` and `gap prescription`. Every hit that
   describes the present is true, already noted, or noted by 08.
8. **`CLAUDE.md`.** Its diff changes only descriptions. I run the modal grep myself, and every hit is accounted for.
9. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The board, the index and `prompts/INDEX.md` agree: the count 29, §1.23's three rows, the campaign closed, and the
     INDEX header's counts.
   - Verification §9 holds the suites, the hashes and §2's checks.

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
- the handover's points that need the user: the rebuild of record, the fixed-α blocks, the stores, the merge into `main`
  (a fast-forward) and the push. Local `main` is four commits ahead of `github/main`;
- anything opened, and any proposal in the log's §5.
