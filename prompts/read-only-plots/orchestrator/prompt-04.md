# Orchestrator — prompt 04, close

Read [`../README.md`](../README.md) first: §1 (the two recorded behaviours under "Out of scope"),
§2.2 (04's charter), §5, §6 and §7. Then the board's §5.2–§5.4, with the three reviews. The
conventions are those of [`prompt-03.md`](prompt-03.md) §0 ("Conventions"), less everything about
Ray, which 04 does not start. The model for a close is `dk-and-rpk-migration`'s note for its prompt
11 ([`../../dk-and-rpk-migration/orchestrator/prompt-11.md`](../../dk-and-rpk-migration/orchestrator/prompt-11.md)),
whose review of the documents and the handover this one follows, minus its sweep and measurements.

**You do not write code.** You may:
- run the suites and the checks of §3 from the session scratchpad;
- read the run scripts (never edit, copy or run them);
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-close.md`](../04-close.md)
**Board items:** Z1–Z4 · **Closes:** the campaign; no §3 issue · **Opens:** nothing expected ·
**Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 01, 02 and 03 have landed and been reviewed (`04486f4`, review `000be0c`; `9dc23db`, review
  `4ec9879`; `e5ef6cb`, review `7658738`). The gates "Ray address", "Read-only plots" and "R" hold
  on the board's §2.
- `HEAD` is the commit that adds this note, or a later one that touches no code. Since `e5ef6cb`
  nothing outside `prompts/` has changed but one line of `.documents/read-only-plots-verification.md`
  (`7658738`, "this commit" → the SHA). `985819c`, the orchestrator's housekeeping, corrected two
  stale rows of `prompts/INDEX.md` (below).
- `ray_cluster.sh status`: lock free, no ChamPBH process, four ports free. 04 starts no Ray, but
  its suites must not share the machine with a rehearsal.
- One prompt at a time in this checkout.

## 0. What makes this prompt unusual

**It is records and prose.** No code and no test changes. So the review has three parts:
- **the documents are true**: `architecture-summary.md` and `CLAUDE.md` describe the code at `HEAD`;
  every path, function, option and line they cite exists; no dated note is edited;
- **the handover is right**: §4.13 against the two run scripts, line by line, and against what the
  code now does to each line;
- **the close is true**: the board, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md` agree by
  counting, and §5.5 names everything left to the user.

**Two amendments of the orchestrator's to the prompt. This note governs where they differ.**
1. **`CLAUDE.md`'s test counts are stale, and Z2 corrects them.** The prompt says "Nothing else
   changes" in `CLAUDE.md`, but its "Repository mechanics" still gives ComputeTargets as
   **114** tests (`CLAUDE.md:79`) and its second half (`test_[g-z]*.py`) as **53** (`:87`), with
   "(The counts are those of 2026-10-13.)" (`:89`). Since 01 and 02 they are **152** and **91**. A later session
   reads them as the baseline, and "a count that falls is a stop", so a stale count is a false stop
   waiting to happen. Z2 therefore also changes those three figures and the date in that
   parenthesis to the date of the agent's own run, after measuring them; nothing else in the block.
   Report this as the orchestrator's addition, overrulable.
2. **§5.1 of `architecture-summary.md` may gain two parentheses.** Its `--prune-unvalidated` and
   `--drop` lines (`:431`, `:432`) say nothing of the plot scripts. If the agent
   judges §5.1 incomplete without it, it may add to each, in the form of the `--ray-address` line
   (`:451`), that it is `main.py`'s: the plot scripts refuse `--drop` (exit 2, before Ray) and ignore
   `--prune-unvalidated`. Nothing else in §5.1 changes. In particular `--shards N (default 20)` is
   right (`DEFAULT_SHARDS = 20`, `config/argument_parser.py:11`) and stays.

**The facts, checked by the orchestrator on 2026-10-14 at `985819c`.** Pass them on.
- **The tree.** Branch `read-only-plots`. Untracked, all the user's: the 17 `pilot-2026.6.0*.db`
  files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`,
  `full_run_2026.6.0-refresh.sh`, `prompts/jordan-normalization/`. Nothing staged.
- **Ray and the machine**, at 01:21 BST: `ray_cluster.sh status` exit 0, the lock free, no process,
  6380 / 6381 / 8267 / 52367 free; `pgrep -lf 'gcs_server|raylet|ray::'` empty; `/tmp/ray/session_*`
  160. Load 4.3 / 7.6 / 8.2. `test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall
  bound that fails at load ≳ 70 (board §5.2, check 10): if it fails, re-run it alone and record
  the load before calling it anything.
- **The suites.** No code has changed since `9dc23db`, whose review ran CosmologyModels **18**,
  ComputeTargets **61 + 91 = 152**, ObjectFactories **63**, tools **34**; 03 re-ran them before its
  cluster with the same counts.
- **`architecture-summary.md`'s lines** (the prompt's line numbers are approximate; these are the
  file's at `985819c`):
  - the tree: `plot_by_beta.py` `:29`, `plot_ScalarModel.py` `:30`; `extract_common.py` `:28`, which
    is where a sibling line for `plot_inputs.py` sits naturally;
  - the dated note the new one copies in form: `:130-142`, `> **Note, 2026-10-02
    (`science-readiness`, prompts 01, 04 and 07).**`; §3 begins at `:145`, so the new note goes
    after `:142`, before the `---`;
  - §4.2's paragraph on the drivers' pools: `:270-274` ("The three drivers that open a pool …
    `tools/inventory_report.py`."); §4 begins with a dated note at `:214`;
  - §5.1: `:419-451`, the `--ray-address` line, as 01 left it, at `:451`;
  - §9 step 1 (`:868`, `ray.init(address=args.ray_address)`) is still true (log 01, Observation 1)
    and is not 04's.

  The `> **Note, …**` blocks of the file at `985819c` are **11** (`grep -c '^> \*\*Note,'`: `:130`,
  `:214`, `:453`, `:541`, `:584`, `:645`, `:679`, `:681`, `:729`, `:817`, `:998`). After 04: **12**,
  the 11 byte-identical.
- **What the code does, for Z1's note** (read it; do not take this as the text):
  - `plot_inputs.py` (top level, 02, `9dc23db`): `PlotOptionNotStored` (`:64`), `parameters`
    (`:106`), `stored_grid` (`:165`), `potentials` (`:183`), `couplings` (`:229`), `models`
    (`:268`), `T_stop` (`:283`). Its docstring (`:16` ff.) says what it is for.
  - Both scripts: `ShardedPool(…, read_only=True, …)` (`plot_by_beta.py:1046`,
    `plot_ScalarModel.py:1617`); `except plot_inputs.PlotOptionNotStored` and exit 2
    (`plot_by_beta.py:1078`, `plot_ScalarModel.py:1649`); the `--drop` refusal before 01's check
    (`plot_by_beta.py:110-117`, `plot_ScalarModel.py:91-98`), then `ray_address_problem` and
    `ray.init` (`plot_by_beta.py:121-125`, `plot_ScalarModel.py:102-106`).
  - Under real Ray: verification §1.6 (`.documents/read-only-plots-verification.md:223`).
  - A read-only pool starts no `SerialPoolBroker` (log 03, Observation 1). The note may say it; it
    need not.
- **`CLAUDE.md`.** "Ray on this machine" (`:118-125`) as 01 left it: required `--ray-address`,
  `auto` refused, exit 2 before `ray.init`, `--inventory` exempt. It is right. Z2's new sentence
  goes in "Repository mechanics"; where is the agent's choice (the packages bullet or the
  long-running-jobs bullet are the natural homes), as one sentence.
- **`review-remediation-verification.md`**: 1472 lines; §4.12 at `:1359-1466`, then `---` (`:1466`)
  and `## 5. Reproduce` (`:1468`). §4.13 goes between §4.12's last line and that `---`, so the
  diff adds lines and deletes none.
- **What §4.13 supersedes, by statement** (the prompt names the first two; the orchestrator found
  the others; all are in §4.12 and none is edited):
  - point 3's row `:20` / `:21`, `:23-24` / `:24-25` (the `main.py` call in `run()`): "the driver
    takes the default `auto`". There is no default now; without the switch the driver exits 2.
  - point 3's row `:54-57` / `:56-59` (the plot loop): "It opens a read-write pool on the finished
    store (`[08-plot-scripts-open-read-write-pools]`, open)". It opens read-only; the issue is
    closed.
  - point 4's last sentence: "ChamPBH's drivers still default to `auto`; making `--ray-address`
    required, as SI's are, is the user's (README §6.4)."
  - point 7's two items: "whether the plot scripts should open read-only" and "whether
    `--ray-address` should be required". Both done. (Point 7's merge and `Datastore` rename were
    done outside this campaign; §4.13 may say so in a clause, citing `78769a0` and `de57ec0`, or
    leave them.)
- **The run scripts** (untracked, the user's; read only). `full_run_2026.6.0.sh` (57 lines, SHA-256
  `0b27c02e…ac0d`, mtime 2026-10-03 00:15) and `full_run_2026.6.0-refresh.sh` (59 lines,
  `2c70e4e3…90ed`, 2026-10-03 22:55). They differ in `STORE_DIR` (`:10`) and the refresh's
  `DROP="--drop bbn-data"` (`:20`, `:25`, `:26`). Both have `set -euo pipefail` (`:8`).
  - **Would now refuse to start:**
    - the `main.py` call in `run()`, `:23-24` / `:24-26`: no `--ray-address`, so exit 2, before
      `ray.init`. Under `set -euo pipefail`, with `| tee`, the pipeline's status is 2 and the script
      ends at its first `run` (C1, `:28` / `:30`), before anything is computed;
    - the plot loop, `:54-57` / `:56-59`: no `--ray-address`, exit 2, and the script ends at φ* = 5.
  - **Would not:** `main.py --inventory`, `:48-49` / `:50-51`: it returns before the check and
    needs no address (U1).
  - **Needs:** `--ray-address 127.0.0.1:6380` on the `main.py` line in `run()` and on the plot
    line, with the cluster started by `ray_cluster.sh` (§4.12 point 4). Nothing for `--inventory`.
  - **The refresh's `--drop`** goes only to `main.py`, which still accepts it; the plot loop does
    not pass it, so 02's refusal does not touch the scripts.
- **The plot loop's options against the build's.** Read `main.py`, `exponential.yaml` and the
  scripts; this is what the orchestrator found:
  - Both the build (`run()`) and the plot loop pass `--config exponential.yaml`, which gives
    `potential-type Exponential`, `abs-tol 1e-8`, `rel-tol 1e-8` and `Lambda-values-eV 1E-3`.
    Neither passes `--T-init-GeV` or `--T-stop-GeV`, so both take the defaults (20000 GeV; the
    cosmology's `T_CMB`). So rows 1–3, 8 and 9 match as written.
  - `--phi-init-Mp`: the plot loop draws φ* = 5, 2, 1 (`:54` / `:56`). The build stores φ* = 5 in
    C1, C4, L, C3 and C2 (the default) and φ* = 1, 2 and 5 in the C5 loop (`:38-40` / `:40-42`).
    Every φ* the loop draws is stored, **if C5 ran**; a plot run whose φ* no block used stops (U4)
    and, under `pipefail`, ends the script.
  - M and Λ come from the store (`read_table`), not from the options. With the one Λ of the YAML,
    every stored M × Λ was stored by some block, so no potential is skipped (U3); the agent
    confirms against `main.py`'s potential and coupling loops before saying so.
  - `--shards "$SHARDS"` on the plot line is ignored by a read-only open (P4), as on
    `--inventory`; harmless.
  - So **the scripts' options already match**; what §4.13 hands on is the rule (give the plot the
    build's options, or it stops), the list of options it applies to, and the φ*-and-C5 dependence.
- **`.documents/OPEN_ISSUES.md`**: **26** rows (`grep -c '^- \`\['`), header `Last updated:
  2026-10-13`, §1.12 empty and saying so (`:212-218`). The header's sentence on this campaign is
  inside the `dk-and-rpk-migration` clause (`:15-18`: "one assigned on 2026-10-13 to
  `read-only-plots`, planned the same day, §1.12, and resolved by its prompt 02 the same day"). Z4
  adds a clause for this campaign: closed 2026-10-14 (or the agent's date), 4 of 4 landed, one
  assigned and closed by 02, none opened, none open. After 04: **26**.
- **`prompts/INDEX.md`**: header "**Last updated:** 2026-10-13 · 8 campaigns: 0 planned, 1 live,
  7 closed." After 04: "0 planned, 0 live, 8 closed", the date the agent's. This campaign's row is
  the last; it becomes **complete** in `dk-and-rpk-migration`'s form (commits per prompt, final
  suites 18 / 152 / 63 / 34, verification and handover pointers, branch, "not yet merged (P5)",
  rulings, and the open-issue column from counting). `985819c` corrected the `bbn-tolerance` and
  `dk-and-rpk-migration` rows, which still said "not yet merged"; both are in `main`
  (`git merge-base --is-ancestor`). 04 touches no other row.
- **What §5.5 lists as left to the user**, beyond the prompt's list:
  - the merge of `read-only-plots` into `main` (P5): the prompt says 04 does not merge; the
    orchestrator asks the user after the review;
  - `$R`, 03's store, `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` (it exists);
  - the run scripts' lines (Z3);
  - the two behaviours of README §1, recorded and not opened: with `tags=[]`, two validated rows for
    one model that differ only by tag raise `MultipleResultsFound`; `AdiabaticHistory.build` and
    `BBNData.build` do not filter on `validated` (`dk-and-rpk-migration` log 09, Observation 3);
  - log 02's Observation 1, offered to the user at 02's review as housekeeping and not done:
    `plot_by_beta.py:134`'s `ProfileAgent` label still reads `plot_ScalarModel`;
  - the science rebuild, held until `jordan-normalization` lands.

**What moves on purpose** (the prompt's §5):
- **Modified:** `.documents/architecture-summary.md`; `CLAUDE.md` ("Repository mechanics" only:
  one sentence, and amendment 1's three counts and date);
  `.documents/review-remediation-verification.md` (§4.13 appended); `.documents/OPEN_ISSUES.md`
  (header); `prompts/INDEX.md` (header and this campaign's row); this board.
- **Added:** `logs/04-close.md`.
- **Nothing else.** No code, test or script; nothing under `$R` or `~/ChamPBH-stores*`; the run
  scripts untouched; `.documents/read-only-plots-verification.md` unchanged (03's handover: "04 adds
  what it needs to the documents; it changes neither").

**What the review exists to establish.**
- **(V1) The documents are true.** Z1 and Z2 against the code at `HEAD`; every citation exists;
  the 11 dated notes byte-identical; `CLAUDE.md`'s diff inside "Repository mechanics" and as §0 says.
- **(V2) The handover is right.** §4.13 against the two scripts, line by line; each statement of
  what a line would do, true at `HEAD`; the supersessions by statement, with §4.12 unedited; nothing
  run or copied.
- **(V3) The close is true.** The board, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md` agree by
  counting; §5.5 names everything §0 lists.
- **(V4) The four roots pass** at 18 / 152 (61 + 91) / 63 / 34, before and after.

**Conventions.** prompt-03's, less Ray. In addition:
- **Stage by explicit path only**, and show `git diff --cached --name-only` before committing. A log
  states only what the record shows.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its follow-up.
- **Suites** in the foreground, serially, output to a file in the agent's scratch directory, the
  verdict grepped from it, the load beside each; ComputeTargets in its two halves. **Before any edit,
  and after.** Never two suites at once.
- **Scratch** in the agent's own subdirectory of the session scratchpad. No scratch file in the
  repository.
- **No store is opened**, not even read-only; no driver run; no Ray; nothing installed.
- **Never touch a process you did not start.**
- **Shell traps.** The Bash tool's shell is zsh: a bare word starting `=` is command expansion
  (quote it, e.g. `echo '====='`); `${PIPESTATUS[0]}` is empty (capture a status without a pipe);
  an unquoted variable holding several words is not split. BSD `sed -i ''`. Use `git grep` or
  Python for a regex the log relies on.
- **Stop conditions are real** (the prompt's §4). A stop leaves the tree as it is and goes to the
  user through the orchestrator; the agent resumes with `SendMessage`.

## 1. Before you dispatch

Done on 2026-10-14 at `985819c`, before this note was committed. Repeat 1–2 at the dispatch `HEAD`
if anything lands in between.

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git diff --stat e5ef6cb HEAD -- . ':!prompts'` is the one line of the verification
   document.
2. **Ray and the machine.** `ray_cluster.sh status` clean; no Ray process; the load (§0).
3. **The run scripts' SHA-256 and mtimes** recorded (§0), for check 7.
4. **Baselines.** The suites are 02's review's and 03's (§0); the agent runs them as its step 1 and
   the orchestrator does not duplicate them.

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the background. Give it:
- the prompt;
- the campaign README and the board;
- `CLAUDE.md`;
- logs 01, 02 and 03 ("What shipped", "Observations not acted on", "State handed to the next
  prompt");
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the very end**, after every check and the suites. Its log is `logs/04-close.md`,
  in `dk-and-rpk-migration` README §5.1's form.
- **The order.** The suites first, before any edit, and the `> **Note, …**` blocks extracted at
  `HEAD` into its scratch. Then Z1, Z2, Z3; then Z4; then the prompt's §2 checks; then the suites
  again.
- **The files it may change** are §0's "What moves on purpose", and nothing else.
- **The two amendments** of §0, as the orchestrator's.
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:** the log's path and the commit; `git diff --cached --name-only` before it and
  `git show --stat HEAD` after; `git diff --numstat HEAD~1 HEAD -- .documents` with what each
  deletion is; the notes' `diff` (11 identical, one added); the citation list; §4.13's line-by-line
  table of the scripts; the index counts; the suites before and after with the load; every
  deviation with its tag; and the "State handed to the next prompt" section verbatim.

## 3. The review — nine checks

1. **Scope.** `git diff --name-status HEAD~1 HEAD`: `A` for the log; `M` for exactly §0's list. The
   untracked set is §0's; no other commit of the agent's; no branch left.
2. **Additive.** `git diff --numstat HEAD~1 HEAD -- .documents`: `review-remediation-verification.md`
   0 deletions, its added lines all between §4.12's last line and `---`; `OPEN_ISSUES.md`'s
   deletions in the header only; `architecture-summary.md`'s in descriptive lines only (§4.2's
   paragraph, the tree, §5.1 if amendment 2 is taken).
3. **(V1) The dated notes.** Extract every `> **Note, …**` block of `architecture-summary.md` at
   `HEAD~1` and `HEAD` (Python, block = the `> ` lines from a `> **Note,` line to the first line not
   starting `>`); `diff`: 11 identical, one added, dated, naming the campaign and prompts.
4. **(V1) `architecture-summary.md`, by reading.** The tree line exists and is commented. The new
   note says what `plot_inputs.py` holds and why (drivers that parse `sys.argv` at import cannot be
   tested; P1), that both scripts open `read_only=True`, what each kind of miss does (U3, U4, with
   02's exceptions named right: `ReadOnlyMiss` for a potential, `ReadOnlyWrite` "an insert" for a
   coupling), and that `--drop` is refused. Every name and line it cites, against `HEAD`. §4.2's
   paragraph says the plot scripts' pools are read-only. §5.1 as 01 left it, or with amendment 2's
   parentheses only.
5. **(V1) `CLAUDE.md`.** `git diff -U0 HEAD~1 HEAD -- CLAUDE.md`: one added sentence in "Repository
   mechanics" that is true of the code; 114 → 152, 53 → 91, the date; nothing else. "Ray on this
   machine" unchanged. Re-count the two halves (`-p 'test_[a-f]*.py'` 61, `'test_[g-z]*.py'` 91)
   from the suites' output.
6. **(V2) §4.13, against the scripts.** Every line it cites, read in both scripts; each "would
   refuse" true at `HEAD` (01's check precedes `ray.init` in all three drivers; `--inventory`
   returns before it; `set -euo pipefail` with `tee`); the options it says the plot loop must match
   are rows 1–5, 8, 9's options and are the ones the scripts pass or default; the φ*/C5 dependence
   stated; the supersessions of §0 all present, by statement, with `git diff` showing §4.12
   unedited; the rebuild held until `jordan-normalization` (the user's ruling of 2026-10-13).
7. **(V2) Nothing run or copied.** `shasum -a 256` and `stat -f '%m %z'` of both scripts equal §1's;
   no copy of either anywhere in the repository (`git ls-files`, `git status`).
8. **(V3) The records.** The board: status **COMPLETE** with 18 / 152 / 63 / 34; row 04 ✅; Z1–Z4 ✅;
   §2 as it stands; §3 and §4 unchanged; `### 5.5 After prompt 04 (<date>)` naming everything of
   §0's list. `prompts/INDEX.md`: header 0 / 0 / 8, the row **complete**, no other row changed.
   `.documents/OPEN_ISSUES.md`: 26 by `grep -c`, the header's count, date and clause right, no row
   moved, §1.12 still saying none open.
9. **(V4) The suites.** Re-run all four roots at the agent's commit, serially: 18, 61 + 91, 63, 34,
   OK. `git status` clean afterwards but for the untracked set.

## 4. After the review

- **Record it on the board** as an *Orchestrator review of prompt 04* paragraph in §5.5. Fix small
  residue in a follow-up commit of its own, by pathspec:
  - "this commit" → the SHA (the board, the log, and wherever §4.13 or `prompts/INDEX.md` uses it);
  - this note added to the board's list of notes, marked "used for 04";
  - the status line, if the agent left anything to fill.
- Anything the review finds that is not residue is opened on the board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - V1–V4, each in a line;
  - the handover's headline: what in the run scripts now refuses to start and what it needs; that
    the plot loop already matches the build's options, with the φ*/C5 dependence;
  - the orchestrator's two amendments (the `CLAUDE.md` counts; §5.1's parentheses, if taken), as
    overrulable, and the housekeeping commit `985819c`;
  - what is left to the user (§5.5): **the merge of `read-only-plots` into `main` (P5) — ask before
    doing it**; `$R`; the run scripts; the two recorded behaviours; the `ProfileAgent` label; the
    rebuild after `jordan-normalization`.
