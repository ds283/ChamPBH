# Orchestrator — prompt 12, close

Read [`../README.md`](../README.md) first: §1, §2.2 (the old 08's charter, which 12 carries), §5, §7, and all
of §8. Then the prompt, the board, every log (`logs/01`–`11`, with `07b`),
`.documents/inelastic-reflection-verification.md`, and `.documents/review-remediation-verification.md` §4.16
and §4.17, whose form §4.18 follows. The conventions of notes 02–11 bind here as well: a record states only
what a command or a file shows, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, `main.py --inventory` on 11's reference store, and the `--help` of every command §4.18
  names;
- run the registry's read-only commands (`list`, `store show`, `store fingerprint` without `--write`);
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`12-close.md`](../12-close.md) · **Board items:** C1–C5 · **Gate:** none of its own (it
closes the board) · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Sonnet, as the
prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `67034fe` on 2026-11-01 between 08:58 and 09:05 GMT, before this note was committed:
- 11 landed (`c797831`) and was reviewed (`d029573`); the user's retirement of two stores is recorded
  (`707be48`); 12 was written (`67034fe`). Every gate of the board holds (§2). No code changed since
  `8c12afa` (10).
- `inelastic-reflection` is checked out in the main checkout; `git status --porcelain
  --untracked-files=all` is empty and `git worktree list` shows the main checkout alone. `HEAD` is the
  commit that adds this note, which touches only `orchestrator/`: it is the prompt's **dispatch commit**.
- `ray_cluster.sh status`, exit 0: the lock free (`~/.raypoolkit/rehearsal-lock` does not exist), no Ray
  process of the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
- `./venv/bin/python -m runregistrykit list`: 14 runs, all finished (11's `reference-a` and `resumed-a2`
  `done`, `resumed-a` `killed`; JN 09's seven; `ray-sqla-align` 02's three; `rrk-adoption-03-unit-…`
  `lost`). None `running`, stale or unknown.
- `var/datastores/inelastic-reflection-11-reference.db`, its 16 shards and its manifest exist (18 files).
  `inelastic-reflection-11-resumed` has its manifest alone (a tombstone, retired by the user).
- `.documents/OPEN_ISSUES.md` §1.20 reads "None is assigned now": **the prompt's first stop condition does
  not trigger.** 27 open in the index, four of them on this board (§1.19).
- `git merge-base main inelastic-reflection` is `6f635e6`, which is `main` itself; `main` is level with
  `github/main`. `inelastic-reflection` has no upstream and is not pushed.
- The load was 4.73 / 6.62 / 7.98 at 08:58, and 41.61 / 20.12 / 13.08 at 09:01. The spike is the machine's
  own daemons (`snowagent`, `duetexpertd`, `mediaanalysisd`), as log 11 §6 saw; no ChamPBH process ran.

## 0. What makes this prompt unusual

It changes no code and runs nothing long. Its claims are of three kinds:
- what the code does now, each with a `file:line` at the dispatch commit, read in the code and not copied
  from a log;
- what the campaign measured, each with the log or the verification section that holds it;
- estimates, each said to be an estimate, with how it was made.

**A sentence that is none of the three is not written.**

The orchestrator read the files the prompt names at `67034fe` and ran the probes of §1. Here is what it found
where the prompt is silent, loose or wrong for this tree.

- **The documents are layered records.** `architecture-summary.md` adds dated notes
  (`> **Note, 2026-10-31 (`inelastic-reflection`, prompt 10): …**`); this campaign's are at the end of §7.2
  (01, 02, 03, 05, 06, 07, 09, 10; `:1056`–`:1318`) and in §8.4a (04, 07b, 08; `:1569`, `:1599`, `:1622`).
  `numerical-strategies.md` adds dated subsections: this campaign's are §6.1–§6.9 (`:1119`–`:1552`), each
  headed `### 6.N Added <date> (`inelastic-reflection` prompt NN): <title>`. 04 and 10 added none there.
  `CLAUDE.md` invariant 6 makes everything under `.documents/` additive.
- **§6.10 and §6.11 go after §6.9's last line (`:1552`) and before `## 7.` (`:1553`).** The §7.2 map goes
  after 10's note, whose last line is `:1318`, before `### 7.3` (`:1321`). §14 ends the file (`:2021`–`:2039`).
  §4.18 goes after §4.17's last line (`review-remediation-verification.md:2161`) and before its `---`
  (`:2162`).
- **The shard key is still β.** `config/sharding.py` keeps `ShardKeyType` as `beta_value` and adds
  `alpha_value` to `replicated_tables` (`:24`). The α = 0 and α = 0.1 histories of one model sit on one
  shard. (Note 11 §0 said the shard key now includes α. That was wrong, and nothing in 11's work depended
  on it.) `architecture-summary.md` §5.2's listing (`:725`–`:735`) and its prose (`:747`–`:749`) do not
  name `alpha_value`, and 07's dated note at `:756` does not either. Whether 09's note in §7.2 (`:1224`)
  already says so the agent checks; if not, §5.2 gets a dated note.
- **Statements in code that this campaign made stale.** The logs name these:
  - `ComputeTargets/ScalarHistory.py:75`, `REFLECTIONS_KEY`'s comment, "elastic reflections (the
    floor-triggered reflection model …)" (log 05 §7). The count now includes unresolved reflections, and at
    α > 0 they drain.
  - `ComputeTargets/ScalarHistory.py:4024`, `integrate_history`'s verbose line, "elastic reflection(s)"
    (log 05 §7).
  - `ComputeTargets/tests/test_injection.py:396`–`:402`, test_4's docstring, "every ratio on this history
    measured 0.0" (log 03 §7). It says "Under prompt 02's drains", so it may read as a dated record; the
    agent decides.

  Each needs a code change, which 12 may not make. Ruling 4 says how they are handled.
- **The logs' lines for 12.** No log uses "made false". The lines the prompt means are in the logs'
  "Observations not acted on" and "State handed to the next prompt". This is the orchestrator's starting
  list; the agent's own pass decides:
  - 01–10: `CLAUDE.md`'s suite counts are stale (each log; C2);
  - 03, 05: the code statements above;
  - 07 §7: the M = 10⁻³, β = 1.2 release given as 0.27 eV and 0.25 eV in `architecture-summary.md`'s 06
    note (`:1182`) and `numerical-strategies.md` §6.5 (`:1341`). 07's dated notes (`:1215`, `:1397`) already
    correct it, so nothing is added;
  - 07b §7: `assess_infall`'s docstring is right for its own expression (true, nothing to do);
  - 10 §7: `registered_run.py --help` does not list `--alpha-values`. That is how the wrapper works, so it
    is true and not false;
  - 11 §7: "For `CLAUDE.md`", "For the architecture summary", "For the handover §4.18" (C2, C1, C3).
- **Already superseded inside §6.1–§6.9.** This is a starting list for §6.11:
  - §6.3's stopping rule. §6.3 itself says it was withdrawn (`:1216`–`:1219`), so it is listed and nothing
    is added;
  - §6.3's "every drain point and every floor reflection is a candidate, drained at α_eff = α · w_gate".
    Since 05 (R10, P20), w_gate = 1 at an unresolved reflection and at the floor rule (§6.4);
  - §6.5's failures of the M = 10⁻³ and M = 0.5 release cases are superseded by §6.6;
  - §6.4 and 05's issue on the wall search below the rounding of ln V are superseded by §6.7 (07b).

  The agent checks every other statement of §6.1–§6.9.
- **`CLAUDE.md`.** These lines change:
  - the five counts (`:80`–`:84`) and the two halves (`:89`–`:90`). They are 269 / 77 + 192 / 103 / 115 / 64
    there and 26 / 497 (122 + 375) / 129 / 123 / 68 now (log 11 §4.2). The campaign added 228 tests to
    `ComputeTargets/tests`, 26 to `ObjectFactories/tests`, 8 to `tools/tests` and 4 to `RunDrivers/tests`;
  - "about 320 s" (`:86`);
  - the parenthesis that dates the counts (`:92`–`:95`), with its `tools/tests` sentence checked against
    the suite as it now is;
  - the plot-scripts bullet (`:128`–`:131`);
  - the "Units" (`:115`) or "Redshift" (`:121`) bullet.

  The existing imperatives in those bullets ("Do not treat any two of these as interchangeable") stay as
  they are. 12 adds no new one (prompt §0).
- **`prompts/INDEX.md`'s header is wrong, and its row is stale.** The header reads "**Last updated:**
  2026-10-27 · 15 campaigns: 1 planned, 1 live, 13 closed". The table has 15 rows: 14 **complete** and this
  campaign's, **live**. The row stops at "3 of 8 prompts written (03, ready to dispatch …)", with "1
  opened by the planner, 1 open; 25 open in the whole index".
- **The board's header.** "**Verification:** … (created by 07)" is wrong: `git log --diff-filter=A` gives
  `c797831`, 11. The **Code:** line lists ten paths. 01's `ComputeTargets/dark_radiation.py`, 04's
  `CosmologyConcepts/Models/AbstractModel.py` and 09's `CosmologyConcepts/alpha_value.py` are not among
  them; the agent checks the line against `git diff --stat 6f635e6 <dispatch> -- '*.py'`.
- **The inventory on the reference store**, run by the orchestrator:
  - `PYTHONPATH=. ./venv/bin/python main.py --config exponential.yaml --database
    var/datastores/inelastic-reflection-11-reference.db --shards 16 --inventory`;
  - exit 0 in 32 s; no Ray;
  - "read-only; 16 shards", 75,050 records in 16 classes, `problems: none`, label 2026.8.0;
  - `alpha_value` 2 records; 8 `ScalarHistory`, 8 `AdiabaticHistory`, 8 `BBNData` (β = 0.5's two `failure=True`,
    as log 11 §6 says);
  - `shasum -a 256` of all **18** files (16 shards, the primary and the manifest) identical before and
    after.
  
  The prompt's "17 files" leaves out the manifest. Ruling 6.
- **Every command §4.18 will name has a working `--help`** (exit 0): `main.py`, `plot_by_beta.py`,
  `plot_ScalarHistory.py`, `RunDrivers/registered_run.py`, `tools/history_and_bbn.py`, `tools/bbn_from_store.py`,
  and `runregistrykit list`. `--alpha-values` (`main.py --help` :58), `--alpha` on both tools.
- **The cost and the heartbeat**, from log 11 §7 and verification §2.5:
  - per history: dissipation at M ≤ 10⁻¹² 11–31 s with BBN; natural mode at 10⁻⁷ 54–191 s, at 0.5 19–118 s;
  - per build: the two-α build of four β at M = 10⁻³ in 4 min 46 s on 10 CPUs;
  - the longest silence: 153 s, against §4.17 point 4's read bound of about 430 s and the 900 s window. So no
    `--stale-after` is needed.

  Anything scaled from these to a rebuild is an estimate and is said to be one.
- **`main` has not moved.** The merge of `inelastic-reflection` into `main` would be a fast-forward. It is
  the user's (U1).

**Baselines**, from log 11 §4.2 (load up to 33.9): CosmologyModels **26**, ComputeTargets **122 + 375**,
ObjectFactories **129**, tools **123**, RunDrivers **68**, all passing. The `test_[g-z]` half took 447 s at a
load of 13.

**These rulings bind the agent.** Each is recorded in the log as a deviation of the class given, where it
reads the prompt otherwise. `NOTED` ones are expectations, recorded as found.

1. **Additive, in the documents' own form** (`STRUCTURALLY REQUIRED`; `CLAUDE.md` invariant 6).
   - In `architecture-summary.md`, `numerical-strategies.md` and `review-remediation-verification.md`, **no
     existing line is deleted or reworded.**
   - A correction is a dated note at the statement. It says what is true now and points to where the
     campaign's account is. In `architecture-summary.md` its form is
     `> **Note, 2026-11-01 (`inelastic-reflection`, prompt 12): <topic>.** …`, directly after the paragraph,
     list or code block it corrects. In `numerical-strategies.md` it is a one-line dated note pointing to
     §6.11.
   - The new subsections are `### 6.10 Added 2026-11-01 (`inelastic-reflection` prompt 12): the measurement
     of prompt 11` and `### 6.11 Added 2026-11-01 (`inelastic-reflection` prompt 12): dissipation mode as it
     stands`, with numbered sub-subsections where they help (`#### 6.11.1` …).
   - A note goes only where the text is false of the present and no later dated note already says so. The
     dated notes and sections of 01–11 are records and are not edited.
2. **What C1 writes** (`IMPLEMENTATION CHOICE`, ruled).
   - **§6.10.** The prompt's five items, each with its numbers and the verification section that holds them:
     the drain across M (§1.4), ΔN_eff against α (§1.5), natural mode at 10⁻⁷ (§1.6), the parking ε (§1.7),
     and the BBN spline across injections (§3, the open issue). Add the margins at M = 10⁻¹² and 4×10⁻³¹,
     which §4.18 point 3 needs.
   - **§6.11.** The prompt's order. Each step names its `file:line` at the dispatch commit, its section of
     §6.1–§6.10 and its ruling (R6–R31, P18–P47). It ends with a table: the statement of §6.1–§6.9, the
     section and line, what superseded it, and where. §0's list starts it.
   - **The §7.2 map.** One dated note after 10's. Per piece, it gives the module, the public functions and
     classes, and the payload keys and columns, read at the dispatch commit. It points to each prompt's note
     by line and does not repeat it.
   - **§14.** One dated note, as the prompt words it, a description. Show D3 from the code
     (`ComputeTargets/__init__.py` exports the class `ScalarHistory`) and the test modules that already take
     the module by `importlib.import_module` (the review of 11 counted fourteen). Give `PARKING_EPSILON`'s
     read in `_parking_cut` by `file:line`.
   - **§5.2.** A dated note on `alpha_value` in `replicated_tables` and the shard key still β (§0), unless
     09's note already says it in words a reader of §5.2 would find. In that case §5.2's note points to it.
3. **The stale-statement grep** (`IMPLEMENTATION CHOICE`, ruled).
   - The agent runs its own greps over the two documents and `CLAUDE.md`. At least these terms:
     `elastic`, `stopping rule`, `w_rate`, `taper`, `0.25 eV`, `0.27 eV`, `FINGERPRINT_FORMAT`, `format 5`,
     `2026.7.0`, `replicated_tables`, `shard key`, `jordan-normalization-09-reference`, `golden`, `alpha`,
     `α`, `ρ_χ`, `dark radiation`, `ΔN_eff`, `parking`, `--alpha`.
   - It records each command, and for each hit one of: "noted at §x (`:line`)", "already noted at `:line`",
     "a dated record, left", or "true".
   - The two paper notes and the `peakewsurf-*` reports are outside the file list and are not read for
     this.
4. **The "made false" lines** (`IMPLEMENTATION CHOICE`, ruled).
   - The agent's own pass over logs 01–11 goes in its log as a table: the log, the line, and either who
     fixed it (commit) or that it was left, and why. §0's list is the expected start.
   - **A statement in code that only a code change would correct is not a stop.** That covers the three of
     §0 and any the pass adds. Each is listed as "left: code, outside 12's files; for the orchestrator's
     housekeeping". The prompt's stop is read as one about a statement in the documents or `CLAUDE.md`
     (C1, C2) that no note or description can make true without a code change.
5. **`CLAUDE.md`, descriptions only** (`STRUCTURALLY REQUIRED`; prompt §0).
   - What is changed is §0's list. The counts and times are this prompt's own run (ruling 9). The
     parenthesis names `inelastic-reflection` 12, the date, and the tests the campaign added per package.
   - The time is stated as measured: each half's seconds with its load. It is not a rule about how to run
     them.
   - ρ_χ goes into the "Units" or the "Redshift" bullet, the agent's choice, recorded.
   - **No added text contains an imperative or a modal of obligation.** The log shows
     `git diff -U0 <dispatch> -- CLAUDE.md | grep '^+' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'`
     and accounts for every hit. A hit that is an unchanged clause carried inside an edited line is listed
     as such.
   - D3's sentence goes to §14 as a description and to the log's §6 as a proposal for the user.
6. **§4.18** (`IMPLEMENTATION CHOICE`, ruled).
   - **Its heading and opening.** `### 4.18 Addendum 2026-11-01 — the `inelastic-reflection` campaign`, at
     §0's place. The first paragraph is in §4.16's form:
     - what the campaign did, prompt by prompt with each commit (01–11 with 07b, then 12 "this commit");
     - the board, the plan (README §8), the verification document, and §6.10–§6.11 and the notes;
     - that it bumped `VERSION_LABEL` to `"2026.8.0"` and `FINGERPRINT_FORMAT` to 6
       (`config/version.py`, `config/fingerprint.py`; `file:line`);
     - that the branch, cut from `main` at `6f635e6`, **is not merged into `main` when this is written.**
   - **"This supersedes, by statement and not by edit."** At least:
     - §4.16's reference store `jordan-normalization-09-reference`, retired on 2026-11-01 and unreadable by
       this code (`StoreSchemaMismatch`);
     - §4.16's format 5 and 2026.7.0;
     - §4.17 point 4's heartbeat bound, which was read; 11 measured 153 s.
   - **The prompt's seven points, written for a reader of §4.16 as §4.17 amends it.** Where §4.17 changed
     §4.16 (the quiet lines, `--retry-failed-scalar`), §4.18 says so in one line.
     - **Point 2** is a table in §4.16 point 2's form, by change: the option and its default, the
       `alpha=<α>/` level, the new figures and columns, the wrapper's scope, the tools' `--alpha` and their
       `dissipation` line. Each row says what a script that passes nothing new gets: α = 0 only, under
       `alpha=0/`.
     - **Point 5** separates measured from estimated.
     - **Point 6** gives each open item's next step from the board, and names the two that bear on a
       rebuild.
     - **Point 7** gives the `git merge-base` result and that it is a fast-forward.
   - **Commands.** Every command it gives is one the agent ran with `--help` (exit 0). `main.py --inventory`
     runs on the reference store, with `shasum -a 256` of its **18** files before and after, unchanged.
7. **The board closed** (`NOTED`).
   - Row 12 ✅ with "this commit"; the gates as they stand (no new gate).
   - §3 as it stands, its first paragraph saying the campaign closed with four open and none assigned; §3.1
     and §4 unchanged.
   - `### 5.14 After prompt 12 (2026-11-01)` in §5.13's form, and a closing paragraph.
   - The status line is rewritten short: "CLOSED 2026-11-01 — 13 of 13 prompts landed …; every gate holds;
     four issues open, none assigned; the merge into `main` is the user's (U1)". The present run-on goes.
     The status line is the board's own state, not a record; the dispatch record keeps the history.
   - "(created by 07)" → "(created by 11)"; the **Code:** line checked (§0).
   - The list of orchestrator notes is the orchestrator's (§4).
8. **The indexes** (`NOTED`).
   - **`prompts/INDEX.md`.** The row is **complete**, in the closed rows' form: 13 of 13 landed, each with
     its commit and a few words; the verification document and §4.18; the final suites; the branch from
     `6f635e6`, not merged; the rulings. The dates read `2026-10-27 → 2026-11-01`, and the issues column
     reads "§1.19; 4 open; 27 open in the whole index". The header reads "**Last updated:** 2026-11-01 · 15
     campaigns: 0 planned, 0 live, 15 closed". The old header's wrong counts are recorded in the log. No
     other campaign's row is edited.
   - **`.documents/OPEN_ISSUES.md`.** The header's date, and this board's clause, which gains "prompt 12
     opened and closed none, and the campaign closed 2026-11-01, 13 of 13 prompts landed, with these four
     open". §1.19's and §1.20's paragraphs say the campaign is closed. The count stays 27 unless the work
     opens or closes one.
9. **The suites** (`IMPLEMENTATION CHOICE`, ruled).
   - Once, before the edits, serially, with `CLAUDE.md`'s commands. `ComputeTargets` runs in its two halves,
     and `uptime` is taken at each start and end.
   - The `test_[g-z]` half runs as a background Bash call (447 s at a load of 13 against the 600 s cap); wait
     for its notification without polling. The others run in the foreground at 600 000 ms.
   - No `.py` file changes, so this run is C5's final run. The log shows `git diff --stat <dispatch>` at the
     end touching only the eight files.
   - A failure is re-run alone before it is called real (`test_prym_passenger.test_a_oscillating_case_completes`
     is a 60 s wall-clock test that fails under load). A count that falls, or a test that fails again, is a
     stop.
   - The machine's daemons can drive the load past 30 (§0). A slow run is recorded with its load, not called
     a defect.
10. **The log** (`STRUCTURALLY REQUIRED`). `logs/12-close.md`, in `dk-and-rpk-migration` README §5.1's form.
    It holds:
    - what shipped, item by item;
    - the deviations with their classes, these rulings among them;
    - verification: the suites with times and loads, the greps with commands and dispositions (ruling 3),
      the `--help`s, the inventory with its hashes, the `CLAUDE.md` modal grep (ruling 5);
    - the "made false" table (ruling 4);
    - "Observations not acted on";
    - "State handed to the next prompt": the campaign's end state, for the user.

**What moves on purpose:** exactly the prompt's §5 list:
- `.documents/architecture-summary.md`;
- `.documents/numerical-strategies.md`;
- `.documents/review-remediation-verification.md` (§4.18 only);
- `CLAUDE.md`;
- the board;
- `prompts/INDEX.md`;
- `.documents/OPEN_ISSUES.md`;
- `logs/12-close.md`.

**What must not move:**
- every other file of the tree: code, tests, goldens, `exponential.yaml`, and `science-runs/` above all;
- `orchestrator/`, the README and the earlier logs;
- `.documents/inelastic-reflection-verification.md` and `.documents/inelastic-reflection/`;
- every other campaign's records;
- `var/`, the reference store above all (`--inventory` reads it read-only, and nothing else opens it);
- the venv.

No Ray is started, no store is built, and no tool or grid is run.

**The stop conditions.** The prompt's §4, as §0 and ruling 4 read them, and:
- a suite that fails twice, or a count that falls;
- `--inventory` changing a byte of any of the 18 files;
- a command §4.18 would name that does not run as written.

A stop goes to the user through the orchestrator; the agent resumes by message.

**Conventions.** All of these bind the agent.
- **Order.**
  1. The Gate, re-checked.
  2. The suites.
  3. The greps and the reading.
  4. The documents.
  5. `CLAUDE.md`.
  6. §4.18, with its `--help`s and the inventory.
  7. The board and the indexes.
  8. The log.
- **Staging and reporting.**
  - One commit, staged by explicit path only, after showing `git diff --cached --name-only`, which must be
    the eight files exactly.
  - Check `git log -1` before committing. `HEAD` must still be this note's commit, or a later one that is
    not the agent's. Other sessions may land commits.
  - "this commit" goes where the SHA goes; the orchestrator substitutes it.
  - The message is in `CLAUDE.md`'s form.
- **Scratch.** Anything saved while working goes under `var/inelastic-reflection-12/`. Never `/tmp`, and
  never the tree outside `var/`.
- **Not touched:** `orchestrator/`, the README, the earlier logs, any other campaign's records.

## 1. Before you dispatch

Done at `67034fe`, before this note was committed:
- the Gate above;
- `main.py --inventory` on the reference store, with its hashes (`orch12/inventory.txt`, `ref-before.txt` and
  `ref-after.txt` in the session scratchpad);
- the `--help` of each command (`orch12/help_*.txt`);
- the reading and the greps of §0.

The suites' baselines are log 11's.

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout. Give it:
- the prompt, the README, the board and log 11;
- `HEAD`, and the Gate as measured;
- §0 of this note up to "## 1. Before you dispatch", as a copy in the session scratchpad.

It reads the other logs, the documents and the code itself. It does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, of exactly the eight files.
- **§0's rulings bind it**, each recorded with its class.
- **No existing line of the three documents is deleted or reworded** (ruling 1), and **`CLAUDE.md` gains
  no rule** (ruling 5).
- **It starts no Ray, builds nothing, and opens no store but the reference one, through `--inventory`.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites with counts, times and `uptime`;
  - each note and section it added, by file and line, with what it corrects;
  - the greps (ruling 3) with their counts and dispositions, and the "made false" table (ruling 4);
  - `CLAUDE.md`'s diff and the modal grep;
  - §4.18's points, one line each, and its "supersedes" list;
  - the `--help` exits, and the inventory's result and hashes;
  - the board's status line and §5.14, the INDEX row and header, and OPEN_ISSUES's header clause;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.**
   - `git show --stat <agent>` lists exactly the eight files. Its parent is this note's commit, or a later
     commit that is not the agent's.
   - `git diff <dispatch> <agent> -- '*.py' '*.sh' '*.toml' '*.yaml' '*.json' requirements.txt science-runs/
     prompts/inelastic-reflection/orchestrator/ prompts/inelastic-reflection/README.md` is empty.
   - `git status --porcelain --untracked-files=all` is empty, `var/` is in no commit, and nothing is pushed.
2. **Additive.**
   - `git diff <dispatch> <agent>` on the three documents has no `-` line, bar a moved blank line that is
     explained.
   - `review-remediation-verification.md`'s hunk starts after §4.17's last line.
   - §6.10 and §6.11 sit between §6.9 and `## 7.`.
3. **The code claims.**
   - Twelve `file:line` references from §6.11, the §7.2 map, §14 and §5.2, chosen across the steps, read in
     the code at `<agent>`. Each says what the note says.
   - D3 reproduced in a scratch probe: `import ComputeTargets.ScalarHistory as X; X is sys.modules[...]`
     gives `False`, and `X` is the class.
4. **The numbers.** At least fifteen numbers of §6.10, §6.11 and §4.18 traced to the verification document,
   log 11 or the grid's tables. Among them: the drain's 1.47×10⁻⁵, ΔN_eff 0.0027–0.565, the 1 MeV ratio
   0.85–0.97, the birth margins −0.70 to −0.74, ε's 5.416 e-folds and ≤ 2.5×10⁻³, the costs, 153 s, 4 min 46 s,
   548 922 and 8.95.
5. **The grep.** My own grep of the two documents for ruling 3's terms. Every hit is noted, already noted, a
   dated record, or true, as the log says. None is false and unnoted.
6. **The handover's commands.**
   - Every command in §4.18 run here with `--help` (exit 0).
   - `main.py --inventory` on the reference store, its 18 files hashed before and after, unchanged.
   - The `--alpha-values` default and replace-not-extend behaviour, checked in a scratch probe on
     `create_argument_parser`, as §4.18 states it.
7. **The "made false" table** against §0's list and the logs.
8. **`CLAUDE.md`, the board and the indexes,** as rulings 5, 7 and 8 say.
   - The modal grep run myself on `CLAUDE.md`'s diff.
   - INDEX's header count, by counting the table's rows and statuses.
   - OPEN_ISSUES's count, by counting its rows.
9. **The log.** In §5.1's form, every deviation classed, §0's rulings among them. The message in `CLAUDE.md`'s
   form.
10. **The suites.** If check 1 finds no code changed, they are not re-run: the eight files cannot change a test.
    The agent's counts are compared with log 11's.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on the board, in the log, in §4.18 and in `prompts/INDEX.md`;
- the board's list of orchestrator notes, with this one marked "used for 12";
- `### 5.14`'s *Orchestrator review of prompt 12* paragraph, holding what each check found;
- the status line: closed, 13 of 13 landed and reviewed.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not residue is
opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Offer the user, as housekeeping** (a standalone commit, at the user's word): the code statements of ruling 4,
`REFLECTIONS_KEY`'s comment and `integrate_history`'s verbose line above all.

**Report to the user:**
- the commit and the review commit;
- that the campaign is closed;
- §4.18's headline for the rebuild: α a key, format 6, a new store, the reference store, the cost, and the α
  grid as theirs;
- the rulings that read the prompt otherwise than it was written: code statements left for housekeeping, 18
  files and not 17, and the shard key still β;
- that the merge into `main`, a fast-forward, and the push are the user's (U1);
- the four open issues, none assigned.
