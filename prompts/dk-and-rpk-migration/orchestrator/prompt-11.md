# Orchestrator — prompt 11, close the migration

Read [`../README.md`](../README.md) first: §2.2 (11's charter, as amended 2026-10-13), §5, §6.2
(P14, P16, P17), §6.4 and §7. Then this board's §2, §3, §5.15 (10's review) and §5.16 (11
written). The conventions are those of [`prompt-01.md`](prompt-01.md) §0 and are not repeated here
except where 11 changes them. The model for a close is SGK's note for its prompt 09
(`SecondaryGWKit/prompts/dk-and-rpk-migration/orchestrator/prompt-09.md`), whose review of a prose
sweep this one follows.

**You do not write code.** You may:
- run both packages' measuring scripts from their roots, with their own venvs, at any ChamPBH commit;
- run the suites, the sweep and the `ast` check of §0 from the session scratchpad;
- read both packages' extraction boards and checklists;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`11-close-the-migration.md`](../11-close-the-migration.md)
**Board items:** Z1–Z7 · **Closes:** the campaign; no §3 issue · **Opens:** nothing expected ·
**Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 10 landed (`b0d258a`) and was reviewed (`3974f70`, with the log's restore recorded at `1954adf`).
  R2, RayPoolKit G3 and DatastoreKit G3 hold on this board's §2.
- `HEAD` is the commit that adds this note, or a later one that touches no code. Since `1954adf`
  the only code change is `2623905` (two unused imports out of `test_datastore_config.py`,
  housekeeping); `7d3cb18` wrote the prompt.
- `ray_cluster.sh status`: lock free, no ChamPBH process, four ports free. 11 starts no Ray, but its
  suites must not share the machine with a rehearsal.
- One prompt at a time in this checkout; nothing installs into the venv while 11 runs.

## 0. What makes this prompt unusual

**It is records and prose.** No line of code changes but comments, docstrings and two
documentation strings, and no test changes in what it runs. So the review has three halves:
- **the sweep is exact**: 11 lines of prose rewritten in code and documents, the 8 that stay left,
  every touched Python module `ast`-equal with docstrings removed (but for `compare_stores.py`'s
  two reason strings), and the sweep's after-state classified line by line;
- **the documents are true**: `architecture-summary.md` and `CLAUDE.md` describe the packages and
  ChamPBH's part, every citation existing at the tag or in the tree, nothing copied from the
  contract that should be pointed at, no dated note edited;
- **the close is true**: the measurements at the agent's commit; the two G3 drafts each in its
  board's form and mapping to its own checklist; the handover's line-by-line reading of the run
  scripts right against the scripts; the index and the records agreeing by counting.

**One addition of the orchestrator's, overrulable.** The charter did not list `CLAUDE.md`'s Ray
rule (README §2.2, amended 2026-10-13). It was added at 11's writing because U5 holds for any run
on this machine (README §7), and `CLAUDE.md` is where a later session looks. Report it to the user
as the orchestrator's addition.

**The facts, checked by the orchestrator on 2026-10-13 at `7d3cb18`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `7d3cb18` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`.
  Nothing staged. None is the agent's. `git diff --stat 1954adf HEAD -- . ':!prompts'` is
  `Datastore/tests/test_datastore_config.py`, 2 deletions (`2623905`).
- **Ray, the lock, the ports**, at 08:42: `ray_cluster.sh status` exits 0, lock free, no process,
  6380 / 6381 / 8267 / 52367 free; `pgrep -lf 'gcs_server|raylet'` finds nothing.
- **The machine.** Load **18 / 51 / 52 at 08:42**, falling (VS Code helpers, `assistant_service`,
  `contactsd`, Fantastical; no suite, no Ray). Timing is no acceptance row in 11; a suite that
  fails under load is re-run alone before it is called real.
- **The venv.** `pip freeze` 101 lines (copy in the orchestrator's scratchpad); `pip check` clean;
  `utilities.format_package_provenance(installed_package_provenance())` is exactly log 07's:
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **The suites** at `2623905`: Datastore **97** OK in 12 s (the orchestrator's run). CosmologyModels
  18 and ComputeTargets 114 (61 + 53) are 10's, and no Python they import has changed since.
- **Both packages' scripts at `1954adf`**, saved in the orchestrator's scratchpad, are the prompt's
  §1.1 figures. At the agent's commit they should equal them but for the commit line, the file
  counts (`.py` files read; tracked files) and RayPoolKit's count of non-`.py` lines naming
  `RayWorkPool` (it grows with the new verification §4, the handover and the log, and shrinks where
  §12 is rewritten). Z3 adds and removes no import, so the import, literal and call counts do not
  move.
- **The sweep**, the orchestrator's (`stale_paths.py`, beside this brief in the scratchpad), run as
  `./venv/bin/python -I <scratch>/stale_paths.py .` from the root. Its pattern:
  ```python
  mods = ["object", "SQL/ShardedPool", "SQL/Datastore", "SQL/ClientPool", "SQL/SerialPoolBroker",
          "SQL/ProfileAgent", "SQL/ObjectFactories/base"]
  alts = []
  for m in mods:
      alts.append(re.escape("Datastore/" + m) + r"(?:\.py)?\b")
      alts.append(re.escape("Datastore." + m.replace("/", ".")) + r"\b")
  alts += [r"ObjectFactories/base\b", r"ObjectFactories\.base\b", r"RayTools", r"inventory_config\b",
           r"_drop_actions\b", r"_drop_order\b", r"_default_serial_batch_size\b",
           r"pool\.inventory\("]
  pat = re.compile(r"(?<![\w./])(?:" + "|".join(alts) + ")")
  skip = re.compile(r"^\s*(?:from\s+\S+\s+import\b|import\s+\S)|import_module\(")
  ```
  over `git ls-files` outside `prompts/`. At `7d3cb18` it prints **the prompt's 19 lines**, at the
  prompt's line numbers. `read_table_config` and a bare `drop_actions` are deliberately not in it:
  the first is a current package argument (`ShardedPool(read_table_config=…)`), the second a local
  name in `main.py:116`. `architecture-summary.md`'s constructor table row for `drop_actions`
  (`:318`) is §4's to rewrite whether or not the sweep sees it.
- **The `ast` check**, the orchestrator's (`ast_prose_only.py`, beside the sweep):
  `./venv/bin/python -I <scratch>/ast_prose_only.py OLD NEW` compares the two files with module,
  class and function docstrings removed, and prints the differing string constants if any. Tested:
  equal on `tools/compare_stores.py` against itself; a change to `GLOBAL_BOOKKEEPING["timestamp"]`
  is printed as one differing constant at line 110 (the `serial` reason is two adjacent literals,
  one constant). The agent takes OLD from `git show HEAD:<path>` into its scratch directory.
- **What the reason strings must still say.** In the installed package (equal to the tag):
  `datastorekit/SQL/ShardedPool.py` creates the primary's tables in `_write_shard_data` (`:989`),
  with `replication_in_flight`; `datastorekit/SQL/Datastore.py` leases serials in `_insert`
  (`:760`) and stamps a row with `insert_timestamp if insert_timestamp is not None else
  datetime.now()` (`:813`), the pool passing one timestamp per replicated write (`:514`, `:666`).
  So for a sharded row the reason stays "datetime.now() at insert", and for a replicated row it is
  the pool's one timestamp, the same on every shard. The agent re-reads these at its `HEAD`.
- **`architecture-summary.md`'s 20 `RayWorkPool` lines** (of 25 in `.documents/`): `:35`, `:152`,
  `:154`, `:244`, `:355`, `:585`, `:648`, `:829`, `:855`, `:856`, `:862`, `:868`, `:920`, `:969`,
  `:999`, `:1056`, `:1086`, `:1108`, `:1110`, `:1116`. All true of `raypoolkit` `v0.2.0` but
  `:999` ("`RayWorkPool` (entire file)"), which §12's rewrite makes "the `raypoolkit` package", and
  `:868`, inside §9's diagram, which says `payload={"small_network": True}` and is corrected by
  §9's dated notes; it is not 11's (not the database layer, and a dated note covers it). Line
  numbers move once §4 is rewritten; the log gives each line's text.
- **The two package boards** (read at their checkouts, `DatastoreKit` `04a2ed9`, `RayPoolKit`
  `1b84df0`, both clean):
  - DatastoreKit `prompts/extraction/IMPLEMENTATION_STATE.md:1538`, **G3**: the gate's name column
    "CPBH adopted `v0.2.3` or later (…)", and a cell beginning `⬜` followed by dated italic notes.
    Its G2 cell (`:1537`) is the form: `✅ 2026-10-12: holds. <sentence>. Against the checklist,
    [link] §9: (1) … ; (2) … ; (3) … . <record pointer>.` then the existing notes verbatim.
  - RayPoolKit `prompts/extraction/IMPLEMENTATION_STATE.md:665`, **G3**: "not yet (CPBH adopts
    `v0.2.0`). Checklist: [`docs/adoption/champbh.md`](…), measured at CPBH `52142d7`; its item 8
    is the acceptance". Its G1 (`:663`) is `**holds** <date>, …`; its G2 (`:664`) is the long form
    to follow: the five parts of item 8, each with its commit, then a re-check line, then the
    checklist pointer.
  - SGK's G2 records were also each followed, on those boards, by a short paragraph under §2
    ("**G2 recorded (2026-10-12).** …"). The drafts may include one for G3, marked optional.
- **The run scripts** (untracked, the user's; read only). `full_run_2026.6.0.sh` (57 lines) and
  `full_run_2026.6.0-refresh.sh` (59 lines) differ in `STORE_DIR` (`:10`: `~/ChamPBH-stores`
  against `~/ChamPBH-stores-bt02`) and in the refresh's `DROP="--drop bbn-data"` (`:20`, `:25`,
  `:26`). In both: `:3` "with a Ray cluster already up"; `:7` "add --prune-unvalidated" (the
  default is already `True`, `config/argument_parser.py:257-261`); the `main.py` call in `run()`
  (`:23-24` / `:24-25`), the `--inventory` call (`:48-49` / `:50-51`) and the `plot_by_beta.py`
  loop (`:55-56` / `:57-58`) pass no `--ray-address`; `--bbn-wall-clock-limit 600` is the default
  and harmless. `--drop bbn-data` alone is accepted by U4 (nothing names `BBNData`'s rows), so the
  refresh script would still run; it is moot because a fresh build computes BBN on today's network,
  and its store is refused anyway.
- **`.documents/OPEN_ISSUES.md`**: **27** rows (`grep -c '^- \`\['`), 4 in §1.11; header dated
  2026-10-13. After 11: **27**, unchanged; the header's prose for this board says it closed.
- **`prompts/INDEX.md`**: header `:11`, "**Last updated:** 2026-10-11 · 7 campaigns: 0 planned, 1
  live, 6 closed"; this campaign's row is the last of the table and stale (7 landed). After 11:
  "0 planned, 0 live, 7 closed", dated 2026-10-13, the row **complete** in `bbn-tolerance`'s form.
- **The siblings.** DatastoreKit `04a2ed9`, RayPoolKit `1b84df0`, SecondaryGWKit `a5a6f6d`,
  StochasticInstantons `5fb4a1d`, each clean. Read only.

**What moves on purpose:**
- **Modified:** `.documents/architecture-summary.md`; `CLAUDE.md` ("Repository mechanics" only);
  `tools/compare_stores.py` (`:89-92`, `:107-111`); `Datastore/tests/test_bbn_failure_lookup.py`,
  `ComputeTargets/tests/test_foreign_bbn_provenance.py`,
  `Datastore/tests/test_scalarmodel_failure_reason.py`, `Datastore/tests/test_compare_stores.py`,
  `Datastore/tests/test_datastore_config.py` (prose only); `.documents/dk-and-rpk-migration-verification.md`
  (§4 appended); `.documents/review-remediation-verification.md` (§4.12 appended);
  `.documents/OPEN_ISSUES.md` (header); `prompts/INDEX.md`; this board.
- **Added:** `prompts/dk-and-rpk-migration/measure/out/11-*`; `logs/11-close-the-migration.md`.
- **Nothing else.** No tool, no rehearsal script, no `requirements.txt`, no driver; nothing in a
  sibling; nothing under the rehearsal root or `~/ChamPBH-stores*`; the run scripts untouched.

**What the review exists to establish.**
- **(V1) The sweep is prose only.** 19 lines before; after, the 8 that stay and the agent's new
  lines naming deleted paths *as deleted*, each classified. Every touched module `ast`-equal with
  docstrings removed, `compare_stores.py` but for its two strings, which say what the package does.
- **(V2) The documents are true.** `architecture-summary.md`'s rewritten sections against the
  contract at the tag and the tree; pointers, not copies; the dated notes intact; `CLAUDE.md`'s diff
  only "Repository mechanics", with the counts right.
- **(V3) The close is true.** The measurements at the agent's commit; the two G3 drafts in their
  boards' forms, every part mapped to a commit or rehearsal that holds it, nothing claimed that the
  records do not show; R0–R2's table against verification §1–§3.
- **(V4) The handover is right.** §4.12 against the run scripts, line by line; against README §7;
  nothing run, nothing copied.
- **(V5) The records close the campaign.** The board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`
  agreeing by counting; §5.17 naming every issue left open and every decision left to the user.
- **(V6) The three roots pass** at 18 / 114 / 97, before and after.

**Conventions.** prompt-01's, carried. In addition:
- **Stage by explicit path only**, and show `git diff --cached --name-only` before committing. Count,
  then paste. A log states only what the record shows.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
  The measurements are taken at the agent's `HEAD` before it commits (the note's commit, whose code
  differs from the agent's commit only by Z3's prose); the log says so.
- **Suites** in the foreground, serially, output to a file in the agent's scratch directory, the
  verdict grepped from it; ComputeTargets in its two halves. **Before any edit, and after.**
- **Scratch** in the agent's own subdirectory of the session scratchpad. No scratch file in the
  repository.
- **No store is opened**, not even read-only; no build; no Ray; nothing installed.
- **Never touch a process you did not start.**
- **Shell traps.** zsh: a bare word starting `=` is command expansion (quote it); an unquoted
  variable holding several words is not split (use `${=T}` or an array). BSD `sed -i ''`.
  `grep` is BSD's; use `git grep` or Python for a regex the log relies on.
- **Stop conditions are real.** A stop leaves the tree as it is, goes to the user through the
  orchestrator, and the agent resumes with `SendMessage`.

## 1. Before you dispatch

Done on 2026-10-13 at `7d3cb18`, before this note was committed. Repeat 1–3 at the dispatch `HEAD`
if anything lands in between.

1. **The tree.** Branch and `HEAD` recorded; `git status --short` shows only §0's untracked set;
   `git diff --cached` empty; `git log --oneline 1954adf..HEAD` is `2623905` and `7d3cb18`.
2. **Ray and the machine.** `ray_cluster.sh status` clean; no Ray process; the load recorded (§0).
3. **The venv.** `pip freeze` saved in the scratchpad (101 lines); `pip check` clean; the
   provenance line as §0.
4. **Baselines.** Datastore 97 at `2623905`; both packages' scripts at `1954adf`; the sweep at
   `7d3cb18` (19 lines); the `ast` check tested; the siblings' `HEAD`s.

## 2. Dispatch

One fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `CLAUDE.md`;
- logs 04, 08, 09 and 10 (their "Observations not acted on" and "State handed to the next prompt");
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad,
  with `stale_paths.py` and `ast_prose_only.py` beside it. The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the very end**, after every check and the suites. Its log is
  `logs/11-close-the-migration.md`, in README §5.1's form.
- **The order.** The suites and the sweep first, before any edit, to record their "before"; copies
  of the six Python files' `HEAD` versions into its scratch for the `ast` check. Then Z3 and the
  `ast` check; then Z1, Z2; then the sweep again; then Z4 at `HEAD`; then Z5, Z6, Z7; then the
  suites again.
- **The files it may change** are §0's "What moves on purpose", and nothing else.
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:** the log's path and the commit; `git diff --cached --name-only` before it
  and `git show --stat HEAD` after; the sweep's before and after, classified; the `ast` results;
  Z4's figures against §0's; the two G3 drafts' location; §4.12's line-by-line table of the
  scripts; the index counts; the suites before and after with the load; every deviation with its
  tag; and the "State handed to the next prompt" section verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff --name-status HEAD~1 HEAD`: `A` for the log and `measure/out/11-*`; `M` for
   exactly §0's list. In both verification documents the diff only adds lines after the last
   section (`git diff --numstat`: 0 deletions). The untracked set is §0's; no other commit of the
   agent's; no branch left.
2. **(V1) The sweep.** Run `stale_paths.py` at `HEAD~1` (19 lines) and at `HEAD`; every line at
   `HEAD` is one of the 8 that stay or a new line the log classifies as naming a deleted path *as
   deleted*. None names one as current.
3. **(V1) Prose only.** For each of the six Python files, `ast_prose_only.py` on `HEAD~1` against
   `HEAD`: five equal; `compare_stores.py` differing only in `GLOBAL_BOOKKEEPING`'s two values, each
   naming the package's file and a line that is what §0 says (re-read in the installed package).
   `git diff -U0` on each: only comment, docstring or those string lines. `black --check` on all six.
4. **(V2) `architecture-summary.md`.** Read the diff whole.
   - The tree shows the files that exist (`git ls-files Datastore config` agrees).
   - §4 says what ChamPBH supplies and what the package does, with pointers to the contract at
     `v0.3.0` (sections by number); spot-check five claims against the contract and the installed
     package, and every ChamPBH path, name and line it cites against the tree.
   - §5.2 matches `config/sharding.py`; §9's example passes the mapping form.
   - §§12–14 name no deleted file; §13 registers in `config/datastore.py`.
   - Every `> **Note, …**` block above `HEAD~1`'s §4 and §§9–14 is present and byte-identical
     (extract them at both commits and `diff`).
   - The 20 `RayWorkPool` lines: the log reads each.
5. **(V2) `CLAUDE.md`.** The diff is inside "Repository mechanics": the packages bullet (pins, the
   upstream-defect rule with U3's clause), the commands with 18 / 114 / 97 and ComputeTargets' halves
   with the ten-minute sentence, the Ray paragraph (script, lock, `--ray-address 127.0.0.1:6380`,
   never `ray stop`). The bullet's sentences after the block unchanged.
6. **(V3) The measurements.** Run both packages' scripts at the agent's commit yourself; each
   equals its saved `measure/out/11-*` and §0's figures but for the lines §0 names. `pip freeze`
   equals §1's copy; `pip check` clean; the provenance line.
7. **(V3) Verification §4.** The R0–R2 table against verification §1–§3 (trees `a72ca5e`, `5bb2be6`,
   `60e000c`, whose code is `d7791c5`, `a4894ea`, `47cfcfb`; exits 0 / 134 / 0, 0 / 134 / 0,
   0 / 15 / 0; every comparison `RESULT: equal`). Each G3 draft in its board's form (§0), every
   checklist part mapped to a commit or record that holds it, the DatastoreKit cell's existing notes verbatim (`diff` against the board's cell), and nothing
   claimed that logs 04 and 10, verification §2–§3 or Z4 do not show. The "what goes stale" list
   covers DatastoreKit's §10 (5 bullets) and RayPoolKit's §9 (3 bullets).
8. **(V4) §4.12.** Every line the handover cites in the two scripts, against the scripts; each
   statement of what a line would become, true at `HEAD` (`--inventory` before Ray; `--drop` and
   U4; the `--prune-unvalidated` default; `--ray-address`'s default `auto`). The check of the rebuild
   against README §7. No script edited, copied or run (`ls -l` and `shasum` of both scripts against
   §1's).
9. **(V5) The records.** The board: status **COMPLETE** with 18 / 114 / 97; row 11 ✅; Z1–Z7 ✅;
   §2 with the drafts' line; §3's four unchanged; `### 5.17 After prompt 11 (2026-10-13)` naming the
   four issues and every decision of README §6.4, the G3 commits and the merge. `prompts/INDEX.md` as
   §0. `.documents/OPEN_ISSUES.md`: 27 by counting, header and date right, no row moved.
10. **(V6) The suites.** Re-run all three roots at the agent's commit: 18, 114 (61 + 53), 97, OK.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 11* paragraph in §5.17. Fix small
  residue in a follow-up commit of its own, by pathspec:
  - "this commit" → the SHA (board, log, verification §4 and the two drafts);
  - this note added to the board's list of notes, marked "used for 11";
  - the status line: 12 of 12 landed; campaign closed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in the
  same commit.
- **Report to the user:**
  - V1–V6, each in a line;
  - the final measurements and counts;
  - where the two G3 drafts are, and that committing them in DatastoreKit and RayPoolKit is the
    user's (P16);
  - the handover's headline: the science store is refused, the next run is a fresh rebuild (after
    `jordan-normalization`, if that is next), and what changes in the run scripts;
  - the orchestrator's addition to `CLAUDE.md` (the Ray rule), as overrulable;
  - what is left to the user (README §6.4): **the merge of `dk-and-rpk-migration` into `main`
    (P17) — ask before doing it**; the two G3 commits; the rebuild; the rehearsal stores; read-only
    plot pools; the `Datastore` rename; a required `--ray-address`.
