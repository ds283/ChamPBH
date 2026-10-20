# Log 05 — close

**Subject:** Close the rrk-adoption campaign with a handover and two gate drafts
**Commit:** this commit, on branch `rrk-adoption`, on top of `e0a52da`
**Date:** 2026-10-20, by Claude Sonnet 5.5
**Result:** **COMPLETE.** Z1–Z5 shipped; the board is closed (**COMPLETE**, 5 of 5). No code, no test,
no script, no store and no run: seven documents changed, and this log is the eighth file. The suites
18, 61 + 115, 78, 84 and 61, OK before and after. No §4 stop condition was met. No issue opened or
closed; `.documents/OPEN_ISSUES.md` stays at 28 open.

The orchestrator's note for 05 (§0, fourteen rulings) governed where it differs from the prompt; each
ruling is classified below as it says. One of the note's sentences could not be followed to the
letter (ruling 10, deviation 10) and is reported there. The record is the documents named below.

## What shipped

- **Z1 — `.documents/architecture-summary.md`.**
  - The tree gains `runregistrykit.toml` (among the root files), `RunDrivers/` (with
    `registered_run.py` and `tests/`, beside `tools/`), `fingerprint.py` in `config/`, and `var/`
    (ignored; `runs/`, `datastores/`) last. `config/` is now `├── config/`, so its heading and five
    children change their prefix (deviation 9).
  - A dated note, `> **Note, 2026-10-20 (`rrk-adoption`, prompts 01 to 04).**`, at the end of §2 after
    the 2026-10-14 standalone note and before the `---`. It has six bullets: what the registry
    records and where; how a run is registered (the wrapper, `main.py` launchable without it); what the
    heartbeat means; the fingerprint and its guard; that the sidecar is made by a person; and a pointer to
    the verification document and to §4.15. It states no measurement of 04's.
  - The last sentence of the §4.1 paragraph (`:295-299` at `HEAD`) is replaced; the paragraph's first
    two sentences are byte for byte. §9 and §10 are read and not edited.
- **Z2 — `CLAUDE.md`.** Three edits and no others (`git diff CLAUDE.md`, 18 changed lines): the wrapper
  sentence loses "which `rrk-adoption` prompt 03 adds; until it lands, no run is registered" and gains
  the usage line as a fenced `bash` block; `ObjectFactories/tests` `# 69` becomes `# 78` and
  `tools/tests` `# 79` becomes `# 84`; the parenthesis gives one date, "(The counts are those of
  2026-10-20.)".
- **Z3 — `.documents/review-remediation-verification.md` §4.15**, after §4.14's last paragraph and
  before the `---` above "## 5. Reproduce": 223 lines, a pure insertion. It opens with a paragraph on the
  campaign and four statements of supersession, then eleven points: the scripts as they
  are today (their SHA-256s, `ls -lT` times and the `diff` of the two); what changes in each, by line,
  with the wrapped command written out once; the launch of the whole script detached; `store create`,
  with two routes; `STORE_DIR` dragging `LOG` and the plot output with it; the route after a `SIGTERM` and
  after a `SIGKILL`, under `set -euo pipefail`; what stays unregistered, and the heartbeat's limit;
  the root's 22 untracked entries, and the scripts' place; 04's stores and runs; the production
  machine; and what is left to the user and what comes next.
- **Z4 — `.documents/rrk-adoption-verification.md` §2**, appended after §1 (81 lines, a pure
  addition): the introduction; the checks made for the drafts; the `extraction` board's `G3` row
  (§2.1) and the `interrupted-runs` board's `G2` row (§2.2), each one line in a `text` fence; and
  what the drafts do not claim (§2.3). The two drafts are quoted in the report to the orchestrator.
- **Z5 — the records.**
  - **This board** (`IMPLEMENTATION_STATE.md`): status line **COMPLETE**, with the final suites; row
    05 ✅; Z1–Z5 ✅; the gate "RunRegistryKit G3 / G2" "drafted 2026-10-20 … recording is the user's";
    §3 and §4 as they stood; a new `### 5.6 After prompt 05 (2026-10-20)` with what is left to the
    user (the merge, the two gate records, the run scripts, the root's files, the old stores and 04's
    stores and runs, and the two stale docstrings, each with the line and the sentence that would
    replace it).
  - **`prompts/INDEX.md`**: the row **complete**, in `read-only-plots`' form; the header "Last updated
    2026-10-20 · 10 campaigns: 0 planned, 0 live, 10 closed".
  - **`.documents/OPEN_ISSUES.md`**: the header's description of this campaign, and §1.14's intro
    (deviation 13). The count stays 28 (counting below).

## Deviations from the prompt

Each of the note's fourteen rulings is classified as the note says. Where I added a fact the ruling
did not ask for, it is said.

1. **`STRUCTURALLY REQUIRED`: the scripts are cited as read today** (ruling 1). §4.15 cites
   `full_run_2026.6.0.sh` and the refresh script by their current lines, gives the two SHA-256s and
   the `ls -lT` times (both `Oct 14 09:59:54 2026`), and says by statement that §4.12 point 3's line
   numbers and §4.13 point 1's table are of the earlier text, and that its `--ray-address` rows are
   done in the scripts as they stand. `diff` of the two scripts, run here:
   ```text
   13c13
   < STORE_DIR="${STORE_DIR:-$HOME/ChamPBH-stores}"      # outside the repository
   ---
   > STORE_DIR="${STORE_DIR:-$HOME/ChamPBH-stores-bt02}"      # outside the repository
   23a24
   > DROP="--drop bbn-data"
   28c29,30
   <         --ray-address "$RAY_ADDR" --job-name "$job" --bbn-wall-clock-limit 600 "$@" 2>&1 | tee "$LOG/$job.log"
   >         --ray-address "$RAY_ADDR" --job-name "$job" --bbn-wall-clock-limit 600 "$@" $DROP 2>&1 | tee "$LOG/$job.log"
   >     DROP=""
   ```
   (`:13`, and the refresh script's `:24`, `:29`, `:30`, which shift its later lines by two.) The SHA-256s
   are `9c25562f…60e4` and `3e07d2e1…7761`; the commits are `1905065` (§4.13, 2026-10-14 01:36:40
   +0100) and `960d098` (§4.14, 21:56:15). I added to the ruling the fact that §4.12 point 3's
   first row (the cluster comment) is also done in the scripts.
2. **`STRUCTURALLY REQUIRED`: "each block runs detached" is the whole script, detached** (ruling 2).
   §4.15 point 3 gives `nohup bash <script> > <log> 2>&1 &` from the repository root, and says that a
   block run by hand is launched as 04 launched one. It says that the script's `bash` reaping the
   driver is read and not rehearsed in that shape (04's launches were `nohup sh -c '…' &`).
3. **`IMPLEMENTATION CHOICE`: the wrapped command, the slug and the flags** (ruling 3). Written out
   once in §4.15 point 2, with the refresh script's `$DROP` after `"$@"`. The slug is `--job-name`:
   run here, `runregistrykit.slugify` returns each of the eight job names of the scripts unchanged
   (`C1-main-M1e-3`, `C4-threshold-M1e-3`, `L-large-beta-M1e-3`, `C5-nonsurfing-phi{1,2,5}-M1e-3`,
   `C3-approach`, `C2-confirm-M1e-5`: `True` for each). `--campaign handover --prompt <n>` is
   SecondaryGWKit's form for the user's own launches. I added a paragraph on `-u`: 04 used it, the
   command does not, and by the code the heartbeat and the run directory's copy do not depend on it
   (`registered_run.py:230`, `:237`); read, not measured without `-u`.
4. **`STRUCTURALLY REQUIRED`: `store create` in a script that is re-run** (ruling 4). §4.15 point 4
   gives the guarded line and the by-hand route, and **prefers the guarded line** (the ruling did not
   say which; my reason, given in the text: a step by hand is the one that gets forgotten, and a block
   begun before the sidecar exists records no `results_store_id`). The package's check, as the ruling
   asked: `create_sidecar` (`stores.py:846-875` at `v0.2.0`) refuses a tombstone, a missing primary,
   an existing sidecar or its `.tmp`, a move that did not finish and an empty purpose; **it does not
   refuse a store that an unfinished run names** — it has no call of `_running_runs_naming`, which
   `fingerprint` (`:1402`), `copy`/`move` (`:1093`), `retire` (`:1733`) and `amend` (`:2090`) do. The
   sidecar's name was checked with the package's own `sidecar_path`: for `var/datastores/science-2026.6.0.db`
   it returns `var/datastores/science-2026.6.0.manifest.json`, which is `${DB%.db}.manifest.json`.
5. **`IMPLEMENTATION CHOICE`: `--prune-unvalidated` is the default** (ruling 5). §4.15's supersession
   list and point 6 say so (`config/argument_parser.py:256-261`) and do not tell the user to edit
   `:10`.
6. **`IMPLEMENTATION CHOICE`: the route after a kill, under `set -euo pipefail`** (ruling 6). §4.15
   point 6: a `SIGTERM` to the pid in `status.json` (`killed`/143, no `end`); a `SIGKILL`, the OOM
   killer or a power loss (`stale`, the refused `begin` naming `end`, a person's check, `end`, re-launch);
   re-launching re-registers the finished blocks, which I say is by the scripts' header (`:8-9`) and
   **not rehearsed for a complete block**, since 04's resumes recomputed missing work. Each claim cites
   verification §1.6–§1.8 or `CLAUDE.md` rule 5. I added the pointer to the open board issue on the
   pool's wait, because it is why an interruption can cost completed work.
7. **`IMPLEMENTATION CHOICE`: `STORE_DIR` drags `LOG` and the plot output** (ruling 7). §4.15 point 5,
   with both facts (`stores.py:1550`, the walk; neither directory holds a sidecar) and the `:13` comment
   corrected in the table. It does not choose for the user.
8. **`IMPLEMENTATION CHOICE`: the scripts count for `git_dirty`** (ruling 8). §4.15 point 8: keep them
   outside the checkout and run them from the root, or commit them. The 22 entries by counting
   (`git status --porcelain | wc -l` at the start, before any edit): 22; `pilot-2026.6.0.db`, its 16
   shards, `pilot-out/`, `pilot-main.log`, `pilot-plot.log` and the two scripts = 1 + 16 + 1 + 2 + 2.
   `begin`'s `git_dirty` is `bool(git status --porcelain)` (`runregistrykit/__init__.py:163`).
9. **`IMPLEMENTATION CHOICE`: the note's place, and the tree** (ruling 9). As ruled. **The descriptive
   deletions the prompt's §2 item 2 allows**, listed by `git diff -U0` (8 lines, all in
   `architecture-summary.md`):
   ```text
   -└── config/
   -    ├── argument_parser.py           # configargparse CLI + YAML
   -    ├── datastore.py                 # The registry of factories, drop groups, batch sizes
   -    ├── defaults.py                  # Tolerance/precision constants
   -    ├── model_list.py                # Cosmological model registry
   -    └── sharding.py                  # Shard-key type and table lists
   -reads changed, so ChamPBH's stores and code are unaffected. ChamPBH does not call the module yet:
   -it is there for the RunRegistry component, which needs this release.
   ```
   The first six are `config/` and its five children, rewritten with the new prefixes. The last two
   are the §4.1 paragraph's lines of ruling 10.
10. **`STRUCTURALLY REQUIRED` (the ruling was `IMPLEMENTATION CHOICE`): the §4.1 sentence says one
    thing more than the ruling.** Ruling 10 has the replacement say that "ChamPBH's own code still
    imports the module nowhere (`git grep`; say the result)". The result:
    ```text
    $ git grep -n -E "datastorekit.store_fingerprint" -- ':!prompts' ':!.documents'
    ObjectFactories/tests/test_store_fingerprint_golden.py:21:is DatastoreKit's (`datastorekit.store_fingerprint`), and when and where it is taken is
    ObjectFactories/tests/test_store_fingerprint_golden.py:64:from datastorekit.store_fingerprint import ENCODING_FORMAT
    config/fingerprint.py:22:(``datastorekit.store_fingerprint``), versioned there by its own ``ENCODING_FORMAT`` and recorded
    ```
    So the sentence "imports the module nowhere" would be false: one test, 02's golden test, imports
    `ENCODING_FORMAT` (used at `:277`, against the golden file's `encoding_format`). The sentence I
    wrote says that ChamPBH's own code imports the module nowhere, and that this one test imports
    `ENCODING_FORMAT`. The first two sentences of the paragraph are byte for byte; the methods and
    lines (`fingerprint_of`, `compare_fingerprints`, `listing_lines` at `runregistrykit/fingerprint.py:125`,
    `:132`, `:162`) are as ruled.
11. **`STRUCTURALLY REQUIRED`: `CLAUDE.md`** (ruling 11): the three edits, and `git diff CLAUDE.md`
    shows nothing else (diff in the report). The usage line is that of log 03's "State handed on".
12. **`STRUCTURALLY REQUIRED`: the Z4 drafts against what the records hold** (ruling 12). Read at
    RunRegistryKit `HEAD` `33a2cd2` through `git show` only: the `extraction` row `G3` and the
    `interrupted-runs` row `G2` (both "not yet (CPBH adopts)"), the two SecondaryGWKit rows for the
    form, `docs/adoption/champbh.md` and `docs/adoption/v0.2.0/README.md` §3 at the tag. As ruled:
    the form is the recorded rows'; no "Read here" clause; the date is the campaign's close; "its pin
    moved in a commit of its own" is not claimed (`b0f3488` is the one commit of the campaign to touch
    `requirements.txt`, adding `:85`; `git log --format=%h 3cd2be4..HEAD -- requirements.txt` gives
    only `b0f3488`); the checklist's item 9 taken point by point (4 hold, point 4 does not apply, point
    6 is the record); the six checks of the `v0.2.0` note taken in turn, with `git grep`; the hard-kill
    rehearsal with its run ids; both name "this commit" and the branch `rrk-adoption` as not merged.
    I quoted the greps the note asked for, and added `foreign` (which occurs only in unrelated
    BBN-provenance code), `git grep pid_alive` and the tree-wide pattern, which finds one comment,
    `StoreFixtures/standin_pool.py:317` ("replicated write is running"), unrelated. The one test that
    matches a refusal and the one that begins a second run are both `TestBeginsRefusal`'s
    `test_refused_then_accepted_after_end` (`RunDrivers/tests/test_registered_run_outcomes.py:227`).
13. **`STRUCTURALLY REQUIRED`: `OPEN_ISSUES.md` §1.14's intro** (ruling 13), a deletion outside the
    header that the prompt's §2 item 2 must allow: "Live. One issue, opened by …" becomes "The campaign
    closed on 2026-10-20 with 5 of 5 prompts landed. One issue was opened, … it stays open on the
    board, unassigned, for the user to rule." The issue's row is unchanged. `git diff --numstat`:
    `4 2 .documents/OPEN_ISSUES.md`, the two deleted lines being the header's sentence on this campaign
    and this intro.
14. **`IMPLEMENTATION CHOICE`: two stale docstrings, for the user** (ruling 14). Board §5.6 lists
    `ObjectFactories/tests/test_inventory_specs.py:27-29` (in the bullet at `:25-29`) and
    `utilities.py:190-192`, each with the sentence that would replace it. Neither is edited, and
    neither is opened as an issue.
15. **`IMPLEMENTATION CHOICE`: a subsection the prompt did not ask for.** Verification §2.3 says what
    the drafts do not claim: no production run, the `sh` against `bash` parent (read, not rehearsed),
    and log 01's remark that `ClientRoot.runs_root` and `stores_root` are `str` (not a defect).
16. **`UNINTENDED DRIFT`, with no effect: an unquoted heredoc.** One Bash call that was to edit the
    §4.15 draft with `python3 - <<EOF` (unquoted) let the shell try to expand the backticks in the
    Python text. It ran fragments as commands: `command not found` for each (`RAY_ADDR`, `:3`, …),
    and `write`, which printed its usage line and did nothing. The Python never ran; the draft was
    unchanged (`grep -c "registered_run.py:229"` found its one occurrence); I redid the edit from a
    file with a quoted heredoc. Nothing else was run by that call, and no file changed.
17. **`IMPLEMENTATION CHOICE`: documents edited while the "before" suites ran.** The "before" suites
    were started at 00:43 and finished at 00:48; I edited `CLAUDE.md` and `architecture-summary.md`
    while they ran. No test reads either: `git grep -ln "CLAUDE.md\|architecture-summary\|OPEN_ISSUES\|IMPLEMENTATION_STATE"
    -- '*.py'` finds a docstring in `CosmologyModels/tests/test_kicking_function.py:23` and a docstring
    in `RunDrivers/registered_run.py:75`, and no code that opens one.
18. **Left to the orchestrator, not done:** the board's list of orchestrator notes (it ends at
    `prompt-04.md`), since I was told not to read that directory, and the substitution of 05's SHA for
    "this commit" in the board, this log, the verification document's drafts and `INDEX.md`.

## Verification performed

**The gate** (00:43): branch `rrk-adoption`, `HEAD` `e0a52da`, no tracked change, 22 untracked entries;
`ray_cluster.sh status` exit 0 (lock free, "ChamPBH cluster (/tmp/ray-cpbh): no processes", ports 6380,
6381, 8267, 52367 free); `./venv/bin/python -m runregistrykit list`: 4 runs, "4 finished; 0 with no host
recorded", none `running` or stale. I started nothing.

**The suites**, serially (one script, one suite after another, output to a file with `uptime` and `date`
before and after), in `CLAUDE.md`'s commands, ComputeTargets in two halves:

| Suite | Before (00:43–00:49) | After (00:54–00:59) |
|---|---|---|
| CosmologyModels | `Ran 18 tests` OK, 67.7 s, load 4.03 | `Ran 18 tests` OK, 68.3 s, load 2.73 |
| ComputeTargets `test_[a-f]*` | `Ran 61 tests` OK, 98.4 s, load 4.96 | `Ran 61 tests` OK, 95.8 s, load 2.88 |
| ComputeTargets `test_[g-z]*` | `Ran 115 tests` OK, 105.5 s, load 5.16 | `Ran 115 tests` OK, 101.0 s, load 3.93 |
| ObjectFactories | `Ran 78 tests` OK, 7.1 s, load 5.34 | `Ran 78 tests` OK, 7.1 s, load 4.47 |
| tools | `Ran 84 tests` OK, 13.4 s, load 4.83 | `Ran 84 tests` OK, 14.1 s, load 5.39 |
| RunDrivers | `Ran 61 tests` OK, 17.9 s, load 4.82 | `Ran 61 tests` OK, 18.3 s, load 6.55 |

(The load is the 1-minute average that `uptime` printed at each suite's start. The verdicts were
grepped from the files: `^Ran `, `^OK`, `FAILED`; no file has `FAILED`.)

The counts equal the orchestrator's at `6229b63` and 04's, and none fell. Files: `before/` and
`after/` in the session scratchpad (`agent05/`).

**The additive checks** (prompt §2 items 2 and 4; note §0 "The additive check"):

1. `git diff --numstat -- .documents/`:
   ```text
   4	2	.documents/OPEN_ISSUES.md
   75	8	.documents/architecture-summary.md
   223	0	.documents/review-remediation-verification.md
   81	0	.documents/rrk-adoption-verification.md
   ```
   The deletions are `OPEN_ISSUES.md`'s header sentence and §1.14's intro (deviation 13) and
   `architecture-summary.md`'s eight descriptive lines (deviation 9). The other two files have none.
2. **The `> **Note, …**` blocks of `architecture-summary.md`**, extracted at `HEAD` and after (a scratch
   script, the lines of each run of `>`-lines that starts `> **Note,`) and `diff`ed: 15 blocks at
   `HEAD`, 16 after; `diff notes_head notes_after` shows one hunk, `68a69,120` — 52 added lines, the new
   note, and no deleted line (`grep -c '^<'` gives 0).
3. **`review-remediation-verification.md`**: the lines before `### 4.15` after the edit (1,659 lines)
   equal the lines of `HEAD` before `## 5. Reproduce` less its closing `---` and blank line (1,659
   lines), `cmp` exit 0; and a `difflib` opcode comparison of the two whole files gives one opcode,
   `('insert', 1659, 1659, 1659, 1882)`. So §4.1–§4.14, §5 and everything else are unchanged.
4. **`rrk-adoption-verification.md`**: `head -c 41520` of the new file (41,520 bytes is `HEAD`'s size)
   `cmp`s equal to `HEAD`'s, exit 0; the new file is 53,400 bytes. §1 is byte for byte.
5. **The count.** `grep -c '^- \`\['` `.documents/OPEN_ISSUES.md` gives **28**; the header says **28 open**.
   The rows by section: §1.1 4, §1.3 2, §1.5 4, §1.6 9, §1.8 2, §1.10 2, §1.11 3, §1.13 1, §1.14 1 = 28.
   `prompts/INDEX.md`: header "10 campaigns: 0 planned, 0 live, 10 closed" (ten rows, all **complete**);
   the `rrk-adoption` row says "§1.14; 1 opened …, 1 open; 28 open in the whole index", and the board's
   §3 holds the one issue (`[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`).
6. `./venv/bin/python -m runregistrykit list` after: the same four runs as at the start
   (`rrk-adoption-04-resumed-c-20261019T235647` `done`, `…-resumed-b-20261019T235447` `lost`,
   `…-resumed-a-20261019T234840` `killed`, `…-reference-20261019T234113` `done`; "4 run(s) … 4 finished;
   0 with no host recorded"), and `ls var/runs` the same four directories (compared with `diff` against
   the listing taken before the suites).

**Every citation exists** (prompt §2 item 3). The text added by Z1–Z4 (the four documents' added lines
from `git diff -U0`) holds **332** distinct backticked tokens. A scratch script resolved the paths, the
commits and the `path:line` citations: **58** found. The other 274 are options, identifiers, commands,
digests, bare line numbers of the run scripts and prose in code font; I checked them by hand in the
groups below.

- **The 58 the script resolved.** Paths in this tree: `CLAUDE.md`, `main.py`, `plot_by_beta.py`,
  `plot_ScalarModel.py`, `utilities.py`, `requirements.txt`, `runregistrykit.toml`, `config/fingerprint.py`,
  `RunDrivers/`, `RunDrivers/tests`, `RunDrivers/registered_run.py`, `RunDrivers/tests/test_registered_run_outcomes.py`,
  `ObjectFactories/tests/test_fingerprint_format_guard.py`, `ObjectFactories/tests/test_store_fingerprint_golden.py`,
  `ComputeTargets/tests/test_foreign_bbn_provenance.py`, `tools/compare_stores.py`,
  `tools/tests/test_registry_config.py`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `pilot-2026.6.0.db`, `pilot-main.log`, `pilot-out/`, `pilot-plot.log`, `var/`, `var/runs`, `var/runs/`,
  `var/datastores`, `var/datastores/`, `var/datastores/rrk-adoption-04-reference.manifest.json`,
  `var/rrk-adoption-04/`, `.documents/review-remediation-verification.md`,
  `.documents/rrk-adoption-verification.md`, `.documents/rrk-adoption/out/04-reference.manifest.json`,
  `.documents/rrk-adoption/out/04-store.txt`, `prompts/rrk-adoption/IMPLEMENTATION_STATE.md`. Paths in
  RunRegistryKit at `v0.2.0`: `README.md`, `docs/adoption/champbh.md`, `docs/adoption/v0.2.0/README.md`,
  `prompts/extraction/IMPLEMENTATION_STATE.md`, `prompts/interrupted-runs/IMPLEMENTATION_STATE.md`.
  Commits: `3cd2be4`, `b0f3488`, `c9eea37`, `d5dd930`, `d5d8c74`, `e0a52da`, `1905065`, `960d098` (this
  repository); `0bae3a3` and `0bae3a3c501a8264ee4be5bc24f06422af38720f` (`v0.2.0`), `33a2cd2`, `aaab408`
  (RunRegistryKit). Lines: `requirements.txt:85` (103 lines), `config/argument_parser.py:256-261` (340),
  `runregistrykit/__init__.py:129-132` and `:163` (1,426 at the tag), `runregistrykit/stores.py:328-337`
  (2,146 at the tag).
- **The run scripts' lines** (the ~60 bare `:NN` tokens of §4.15 and the `:NN-MM` ranges). Checked
  against `cat -n` of each script and this `grep -n` of both, which shows every line the text names:
  ```text
  == full_run_2026.6.0.sh
  11:set -euo pipefail          13:STORE_DIR=…            14:DB=…            15:LOG=…            17:RAY_ADDR=…
  19:export PYTHONPATH=.        24:run() {                27-28: the main.py call            29:}
  32:run C1…   35:run C4…   39:run L…   42:for PHI in 1 2 5   43: the C5 run   47:run C3…   50:run C2…
  52-53: main.py … --inventory  58:for PHI in 5 2 1   59-60: the plot_by_beta.py call
  == full_run_2026.6.0-refresh.sh
  24:DROP="--drop bbn-data"   25:run() {   28-29: the call (with $DROP)   30:DROP=""   31:}
  34:run C1…   37:run C4…   41:run L…   45: the C5 run   49:run C3…   52:run C2…
  54-55: --inventory   60:for PHI in 5 2 1   61-62: the plot call
  ```
  (a condensation of the `grep -n` output run here; the lines `:3-6`, `:7`, `:8-9`, `:10`, `:31`, `:34`,
  `:37-38`, `:41`, `:46`, `:49` are the header and the block comments, read in the `cat -n`).
- **Lines in RunRegistryKit at `v0.2.0`.** `git show v0.2.0:runregistrykit/stores.py` and
  `…/__init__.py`, read with `sed -n`: `slugify` at `__init__.py:129-132`; `git_dirty` at `:163`;
  `sidecar_path` at `stores.py:252-257`; `_primary_problems` `:328-337`; `create_sidecar` `:846`, its
  refusals to `:875`; `_running_runs_naming` called at `:1093`, `:1402`, `:1733`, `:2090` and not by
  `create_sidecar`; `os.walk(stores_root)` at `:1550`; `fingerprint.py`'s three imports at `:125`,
  `:132`, `:162`. `cmp` of `v0.2.0`'s `stores.py` with the venv's: identical.
- **Lines in this tree.** `registered_run.py:230` (`open(copy_to, "a", buffering=1)`), `:237`
  (`def write`), `:292` (`_observe`); `config/fingerprint.py:57` (`FINGERPRINT_FORMAT = 1`), `:72`
  (`SPEC_DIGESTS`), `:152` (`fingerprinter`); `ObjectFactories/tests/test_store_fingerprint_golden.py:64`
  (the `ENCODING_FORMAT` import), `:304` (`TestConfigFingerprint`); `RunDrivers/tests/test_registered_run_outcomes.py:174`
  (`TestBeginsRefusal`), `:227` (the test); `requirements.txt:17`, `:80`, `:85`; `.gitignore:7` (`var/`);
  `ObjectFactories/tests/test_inventory_specs.py:25-29` and `utilities.py:190-192` (printed with `sed`;
  the sentences are quoted in board §5.6).
- **Run ids, pids and digests.** `ls var/runs`: the four directories; `rrk-adoption-04-reference-20261019T234113`,
  `…-resumed-a-20261019T234840`, `…-resumed-b-20261019T235447`, `…-resumed-c-20261019T235647`. Their
  states from `list`. `46306` (`resumed-b`'s pid) and `47288` (the refused launch's) from verification
  §1.3 and §1.7. `2dd4b39b…932f` on the `digest:` lines of `04-store.txt` (`:27`, `:381`, `:428`), and "matches the
  fingerprint taken …" at `:423` (taken by a person) and `:470` (taken by `resumed-c`). `c8570e84…` and `eaef0ce6…` and `0bae3a3c…` from the
  reference manifest's `packages`. The scripts' SHA-256s from `shasum -a 256`.
- **Options and commands**, from `--help` and from runs here: the wrapper's `--campaign`, `--prompt`,
  `--register`, `--purpose` and the bare `--` (`registered_run.py --help`); `main.py`'s
  `--prune-unvalidated` (a `BooleanOptionalAction`, `default=True`), `--ray-address`, `--job-name`,
  `--inventory`, `--drop`; `list --stale-after` (usage line printed); `store create --purpose TEXT PRIMARY`
  (usage line printed: the order of the option and the positional is free); `end RUN_ID --reason TEXT
  [--unverified]` (usage line printed); `store fingerprint --write` and `store show` (04's record);
  `python -m runregistrykit list` (run here); `DEFAULT_STALE_AFTER` is `900.0` (imported and printed);
  `main.py`'s `MIN_NOTIFY_INTERVAL = 5 * 60` (`:84`); `BEAT_MIN_INTERVAL = 30.0` (`registered_run.py:127`).
- **Identifiers** (grep): `runpy.run_path` (`registered_run.py:830`), `runregistrykit.begin` (`:740`),
  `install_killed_handler` (`:445`), `finish_after_exec` (`:459`), `DatastoreKitFingerprinter`
  (`config/fingerprint.py:43`), `SPEC_DIGESTS`, `FINGERPRINT_FORMAT`, `create_sidecar`, `sidecar_path`,
  `slugify`, `_running_runs_naming` (the package); the manifest's keys (`argv`, `campaign`, `git_dirty`,
  `git_head`, `heartbeat_means`, `host`, `packages`, `purpose`, `results_store_id`, `scope`, `script`,
  `script_sha256`) and the status's (`exit_code`, `fingerprint`, `fingerprint_sidecar`, `heartbeat`,
  `pid`, `stage`, `state`), from `python3 -c 'sorted(json.load(open(…)))'` on the reference run's two
  files; the run directory's `manifest.json`, `status.json`, `stdout.log`, `stderr.log` from `ls -la`.
- **The two RunRegistryKit issue names**, `[03-a-run-whose-manifest-cannot-be-read-names-no-store]` and
  `[04-a-killed-process-not-yet-reaped-answers-kill-0]`, are rows 45 and 49 of `docs/OPEN_ISSUES.md` at
  its `HEAD`; the board issue `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]` is
  in the board's §3.
- **Section citations** (documents of this repository): verification §1.3, §1.6, §1.7 (steps 5 and 6),
  §1.8, §1.12; board §3, §5.1 (third bullet of "Three choices"); campaign README §0.2 (item 5), §0.3,
  P2, P4, P10, R2, R4–R7, U1, U2, U4; `CLAUDE.md` rules 1–5 and the section on `git clean`; log 01
  (`./venv/bin/pip install -r requirements.txt`), log 03 ("State handed", the usage line), log 04. All
  read here.

**Nothing ran** beyond `git`, `grep`/`sed`/`diff`/`cmp`, `shasum`, `ls`, `list`, the `--help` of the
wrapper and of `list`, `store create` and `end`, three pure reads of the package (`slugify`,
`sidecar_path`, `DEFAULT_STALE_AFTER`), and the suites: no Ray, no run begun, no store opened (no
`store show` or `store fingerprint`, with or without `--write`), nothing written under `var/` (the
`ls var/runs` and the `list` match). The run scripts were read only. In RunRegistryKit nothing was run
but `git show`, `git log` and `git cat-file`.

**Acceptance (prompt §3):** 1. Z1–Z5 as §1.1 says, §2 above. 2. No code and no test added or changed. 3. One commit; `git diff --cached --name-only` shown before it. Its files are the eight listed at the end.

## The deliberate-breakage record

None. This prompt changes no code, so there is nothing to break.

## Observations not acted on

- **The scripts' header counts "seven main.py invocations"** (`:7`); there are six call sites and
  eight invocations (the C5 loop runs three). §4.15 point 1 says so. Not edited: the scripts are the
  user's.
- **The refresh script's store.** It defaults to `~/ChamPBH-stores-bt02`, an old-format store the
  tree refuses (§4.12 point 1), and its `--drop bbn-data` on a fresh store drops nothing (§4.12 point 3).
  §4.15 says what changes in it by line, and does not repeat the rest.
- **`git grep -E` on macOS does not read `\s`.** My first search for imports of `runregistrykit`
  missed the indented `import runregistrykit` at `RunDrivers/registered_run.py:732` for that reason; the
  second search, `-e 'import runregistrykit|from runregistrykit'`, finds all ten. The greps the drafts
  quote use no `\s`.
- **The golden test imports `datastorekit.store_fingerprint.ENCODING_FORMAT`** (deviation 10), the one
  place ChamPBH's tree imports that module.
- **The wrapper's tests assert the refusal's text** (`python -m runregistrykit end`, `No run directory
  was created`): they are tied to `v0.2.0`'s wording, as the `v0.2.0` note's §3 warns of any such test.
  The draft says so.
- **The user's scripts launch through `bash` and a pipeline** (`| tee`); 04 launched through `sh -c '… &
  wait'`. The reaping is the same mechanism, read and not rehearsed in the scripts' shape; §4.15 point
  3 and verification §2.3 say so. A first real run through the script is its rehearsal.
- **`CLAUDE.md`'s rule 4 ("do not babysit")** applies to the whole-script launch of §4.15 point 3: launch,
  one `list`, end the turn.

## State handed to the next prompt

There is none in this campaign. For the orchestrator's follow-up and the user:

- Branch `rrk-adoption`; this commit is on top of `e0a52da`. No tracked change after it; the 22
  untracked entries are the user's. `var/` is as 04 left it (95 MB).
- **The SHA** replaces "this commit" in the board (row 05, the status line's neighbour and §5.6), this
  log, `prompts/INDEX.md`, and the verification document's §2.1 and §2.2 (each "CPBH, this commit"
  and "(this commit)"), and §2's text "the drafts … this commit" in its introduction.
- **The board's list of orchestrator notes** ends at `prompt-04.md`; the note for 05 is the
  orchestrator's to add.
- **The merge** of `rrk-adoption` into `main` (P10) is not done.
- **For the user**, in the order the board's §5.6 gives: the merge; the two gate records, from
  verification §2.1 and §2.2 (bring the "not merged" clause up to date, add a "Read here" clause); the
  run scripts, from §4.15; the 22 untracked entries, the old stores, and `var/`'s 04 stores and runs; the
  two stale docstrings. **`jordan-normalization` comes next**, and its rebuild is the first
  production run through the wrapper.
- Suites: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78, tools 84, RunDrivers
  61, all OK before and after.

## The files this commit changes

`.documents/architecture-summary.md`, `.documents/OPEN_ISSUES.md`,
`.documents/review-remediation-verification.md`, `.documents/rrk-adoption-verification.md`,
`CLAUDE.md`, `prompts/INDEX.md`, `prompts/rrk-adoption/IMPLEMENTATION_STATE.md`,
`prompts/rrk-adoption/logs/05-close.md`.
