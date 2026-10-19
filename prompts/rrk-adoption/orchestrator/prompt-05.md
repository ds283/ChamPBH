# Orchestrator — prompt 05, close

Read [`../README.md`](../README.md) first: §1, §2.2 (05's charter), §5, §6.0, §6.2 (P12) and §7.
The conventions are note 01's ([`prompt-01.md`](prompt-01.md) §0, "Conventions"), restated here
where 05 needs them.

**You do not write code.** You may:
- run the suites and the registry's read-only commands (`list`, `store show`, `store fingerprint`
  without `--write`) on 04's stores and runs;
- read RunRegistryKit, SecondaryGWKit and the user's run scripts, never editing them;
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`05-close.md`](../05-close.md)
**Board items:** Z1–Z5 · **Gate:** none of its own; RunRegistryKit's G3 / G2 are drafted, the
user's · **Closes:** the campaign · **Opens:** nothing unless the work finds it ·
**Model:** Sonnet, as the prompt recommends · **Network:** none.

**Gate** (checked by the orchestrator on 2026-10-20 at `6229b63`):
- 01 (`b0f3488`, reviewed `caeadc0`), 02 (`c9eea37`, reviewed `87caa99`), 03 (`d5dd930`, reviewed
  `a61f684`) and 04 (`d5d8c74`, reviewed `6229b63`) landed; the gates "Pin", "Fingerprint",
  "Wrapper" and "R" hold (board §2). `7331391`, `256cf38` and `5d02258` are housekeeping. `HEAD` is
  the commit that adds this note, on top of `6229b63`.
- The prompt's own gate: branch `rrk-adoption`; no tracked change, the 22 untracked entries at the
  root only; `ray_cluster.sh status` exits 0 (the lock free, no ChamPBH process, the four ports
  free); `./venv/bin/python -m runregistrykit list` shows 04's four runs, **4 finished**, nothing
  `running`, stale or unknown.
- One prompt at a time in this checkout.

## 0. What makes this prompt unusual

**It closes the campaign, and most of what it writes is read by someone who was not here**: the
user editing the run scripts, the planner of `jordan-normalization`, and a session in
RunRegistryKit recording two gates. Every sentence must be true of the tree, of `var/runs/`, of the
run scripts as they are on disk today, or of RunRegistryKit at `v0.2.0`. Nothing is written from
memory of what a prompt meant to do: from what its log, its record and the tree show.

**Fourteen statements in the prompt are wrong, loose or silent. Ruled here:**

1. **The run scripts are not the text §4.12 and §4.13 describe.** Both were edited by the user on
   2026-10-14 at 09:59 (`ls -l`), after §4.13 was committed (`1905065`, 01:36 that day) and before
   §4.14. As read on 2026-10-20 they already carry what §4.13 asked for:
   - the cluster comment at `:3-6`, naming `ray_cluster.sh start` and `stop`;
   - `RAY_ADDR="${RAY_ADDR:-127.0.0.1:6380}"` at `:17`;
   - `--ray-address "$RAY_ADDR"` on the `main.py` call and on the plot loop.

   So every line number in §4.12's and §4.13's tables is of an older text. Their SHA-256s are
   `9c25562f…60e4` (`full_run_2026.6.0.sh`) and `3e07d2e1…7761` (the refresh script).
   **Ruled:**
   - §4.15 cites the scripts **as read today**, by their current lines, and gives the two SHA-256s
     and the `ls -l` times, so that a reader can tell whether the text has moved again;
   - it says, by statement, that §4.13 point 1's table and §4.12 point 3's line numbers are of the
     earlier text, and that its `--ray-address` rows are done in the scripts as they stand.

   The two scripts differ only at `:13` (`STORE_DIR`) and in the refresh script's `DROP` lines,
   `:24`, `:29` and `:30`, which shift its later lines by two. Check that with `diff` and say it.
   Classify as `STRUCTURALLY REQUIRED`.

2. **"Each block runs detached" cannot mean each block is put in the background.** The blocks share
   one store and run in order. `begin` refuses a store that an unfinished run names, so a second
   block detached while the first still runs is refused at registration (04's refused `resumed-c`).
   What 04's launch through `sh` gave, and what rule 3 asks for, is two properties:
   - nothing a conversation or a terminal owns;
   - a parent shell that reaps the driver, so that `end` is not refused after a `SIGKILL`
     (board §5.1, third bullet).

   **Ruled:** §4.15 gives the launch of the **whole script**, detached, from the repository root:
   `nohup bash <script> > <log> 2>&1 &`, with the log path chosen by the user. Each block's driver
   is then a foreground child of the script's `bash`, which reaps it. Say also that a block run by
   hand is launched as 04 launched one (`nohup sh -c '…' &`). Classify as `STRUCTURALLY REQUIRED`.

3. **What each `run` call becomes**, as one wrapped command, written out once in §4.15:

   ```
   ./venv/bin/python RunDrivers/registered_run.py --campaign <C> --prompt <N> --register "$job" \
       --purpose "<one line>" -- --config exponential.yaml --database "$DB" --shards "$SHARDS" \
       --ray-address "$RAY_ADDR" --job-name "$job" --bbn-wall-clock-limit 600 "$@" 2>&1 | tee "$LOG/$job.log"
   ```

   The refresh script keeps `$DROP` after `"$@"`, inside `main.py`'s arguments.
   - **The slug is `--job-name`.** `runregistrykit.slugify` keeps `[A-Za-z0-9._]` and turns any
     other run into `-` (`runregistrykit/__init__.py:129-132`), so every job name in the scripts is
     its own slug, `C5-nonsurfing-phi${PHI}-M1e-3` included. Say so.
   - **`--campaign` and `--prompt`** are those of the campaign that orders the run: for the
     rebuild, `jordan-normalization` and its prompt (P2). For a launch of the user's own, give
     SecondaryGWKit's form, `--campaign handover --prompt <n>` (README §0.2 item 5).
   - **`--purpose`** is one line a stranger can read. The block's comment line will do.
   - `export PYTHONPATH=.` at `:19` already serves the wrapper, which also puts the root on
     `sys.path` itself (log 03, W1).

   Classify as `IMPLEMENTATION CHOICE`.

4. **`store create` in a script that is re-run.** `create_sidecar` refuses a store that already has
   a sidecar, and nothing is ever overwritten (`runregistrykit/stores.py:846-873`). A
   `store create` line after C1, run a second time by a restarted script under `set -e`, ends the
   script.
   - It also refuses a store an unfinished run names, if the package does so (check, and say
     which).
   - **Ruled:** §4.15 gives two routes and says which the user should prefer:
     - the line is guarded, `[ -e "${DB%.db}.manifest.json" ] || ./venv/bin/python -m
       runregistrykit store create --purpose "…" "$DB"` (check the sidecar's name against
       `sidecar_path` and 04's `var/datastores/rrk-adoption-04-reference.manifest.json`);
     - or a person runs it once by hand after C1 closes, as 04 did.
   - Until it exists, each block's status says `fingerprint_sidecar` "not written … (there is none)",
     and from then on `written`. C1's own fingerprint is in its `status.json` either way.

   Classify as `STRUCTURALLY REQUIRED`.

5. **`--prune-unvalidated` is the default.** `config/argument_parser.py:256-261` has it as a
   `BooleanOptionalAction` with `default=True`; §4.12 point 3 said so. The scripts' `:10` comment
   ("add --prune-unvalidated to the next one") is therefore redundant, not wrong. 04 passed it
   explicitly. **Ruled:** the hard-kill step says "then the block again; the prune at open is the
   default, and `--prune-unvalidated` states it", and does not tell the user to edit `:10`.
   Classify as `IMPLEMENTATION CHOICE`.

6. **The route after a kill, under the script.** `set -euo pipefail` (`:11`) means a block that
   ends non-zero ends the script.
   - **A `SIGTERM`.** Sent to the pid in the run's `status.json`, never by a pattern: the block is
     recorded `killed`/143 and the script stops. Re-launch the script.
   - **A `SIGKILL`, the OOM killer or a power loss.** `list` shows the run `stale`. The re-launched
     script's next `begin` on the store is refused, and the refusal names `end`. A person checks,
     on the host the run recorded, that it is over. Then
     `./venv/bin/python -m runregistrykit end RUN_ID --reason "…"`, and the script is re-launched.
   - **Re-launching the whole script re-registers the blocks that finished.** Each looks up its
     work, finds it stored, and records a short `done` run that computed nothing. That is true and
     harmless; say so, and that the user may instead start from the block that was killed.

   Every claim cites 04's record (verification §1.6–§1.8) or `CLAUDE.md` rule 5. Classify as
   `IMPLEMENTATION CHOICE`.

7. **`STORE_DIR` under `var/datastores/` drags `LOG` and the plot output with it.** `LOG` is
   `$STORE_DIR/logs` (`:15`), and the plot loop writes `$STORE_DIR/science-out-phi$PHI` (`:60` /
   `:62`). With `STORE_DIR=var/datastores` both land inside the stores root. `store retire` walks
   the stores root for sidecars (`runregistrykit/stores.py:1550`), and neither directory holds one,
   so nothing breaks. But the stores root then holds more than stores, and the wrapper already
   copies each block's stdout and stderr into its run directory.
   **Ruled:** §4.15 says so in one point, with both facts. The user may move `LOG` and the outputs
   elsewhere under `var/` (for example `var/logs/`, `var/out/`, ignored with the rest). It also
   corrects the `:13` comment "outside the repository", now false. It does not choose for the
   user. Classify as `IMPLEMENTATION CHOICE`.

8. **Where the scripts live decides `git_dirty`.** `begin` records `git_dirty` from `git status
   --porcelain`, untracked files included (README §0.3). Two scripts left untracked at the root
   keep every run dirty after the stores and logs are gone (R6). **Ruled:** §4.15's point on the
   root's files says that the scripts themselves count. The user either:
   - keeps them outside the checkout and runs them from the root (`bash ~/…/full_run.sh`: every path
     in them is relative to the working directory);
   - or commits them, which is the user's to do (U4).

   The rest of the untracked set is the pilot store and its 16 shards, `pilot-out/` and the two
   logs. List the 22 entries by counting. Classify as `IMPLEMENTATION CHOICE`.

9. **The `architecture-summary.md` note's place, and the tree's last entry.** The prompt does not say
   where the note goes. **Ruled:**
   - it goes at the end of §2, after the 2026-10-14 standalone note and before the `---`, headed
     `> **Note, <date> (`rrk-adoption`, prompts 01 to 04).**`, in the form of the four before it;
   - in the tree:
     - `runregistrykit.toml` sits among the root files;
     - `RunDrivers/` (with `registered_run.py` and `tests/`) sits beside `tools/`;
     - `fingerprint.py` goes in `config/`;
     - `var/` (ignored: `runs/`, `datastores/`) goes last.
   - Moving `└── config/` to `├── config/` rewrites `config/`'s five lines' prefixes. Those are the
     descriptive deletions the prompt's §2 item 2 allows, and the log lists them.
   - The note says what the prompt's Z1 lists and nothing of 04's measurements beyond one pointer
     to `.documents/rrk-adoption-verification.md`.
   - §9 and §10 are read and **not** edited: `main.py` and `utilities.py` are unchanged (U1), and
     §10 does not list the provenance helper.

   Classify as `IMPLEMENTATION CHOICE`.

10. **The §4.1 paragraph (`:295-299`).** Its last sentence: "ChamPBH does not call the module yet: it
    is there for the RunRegistry component, which needs this release." The true replacement:
    ChamPBH uses `datastorekit.store_fingerprint` through RunRegistryKit's store layer:
    - `config/fingerprint.py`'s `fingerprinter` is a `runregistrykit.fingerprint.DatastoreKitFingerprinter`;
    - its methods import `fingerprint_of`, `compare_fingerprints` and `listing_lines` from
      `datastorekit.store_fingerprint` (`runregistrykit/fingerprint.py:125`, `:132`, `:162`);
    - ChamPBH's own code still imports the module nowhere (`git grep`; say the result).

    Replace that sentence only, and keep the paragraph's first two sentences byte for byte.
    Classify as `IMPLEMENTATION CHOICE`.

11. **`CLAUDE.md`: what "nothing else changes" allows.** The prompt names two edits. The counts'
    date sentence, "(The counts are those of 2026-10-14, and RunDrivers/tests' of 2026-10-19.)", is
    part of the counts. **Ruled:** three edits, and only these:
    - the wrapper sentence at `:139-140` loses "which `rrk-adoption` prompt 03 adds; until it lands,
      no run is registered", and gains the usage line of log 03's "State handed on", as a fenced
      `bash` block;
    - `ObjectFactories/tests` `# 69 tests` → `# 78 tests`, and `tools/tests` `# 79 tests` → `# 84
      tests`;
    - the parenthesis gives one date, that of the final suites.

    `git diff CLAUDE.md` shows these and nothing else. Classify as `STRUCTURALLY REQUIRED`.

12. **The two Z4 drafts, against what the records hold.** Read the rows `G1`/`G2` of RunRegistryKit's
    `extraction` and `interrupted-runs` boards at its `HEAD` (`33a2cd2`, where `G2` of
    `interrupted-runs` and `G1` of `extraction` are SGK's recorded rows). The prompt says "at
    `v0.2.0`", but the SGK rows it asks you to copy the form of were recorded after the tag.
    Read the checklist (`docs/adoption/champbh.md`) and the `v0.2.0` note
    (`docs/adoption/v0.2.0/champbh.md`, `docs/adoption/v0.2.0/README.md` §3) at the tag.
    **Ruled:**
    - **The form** is the recorded rows':
      `| **G3** | **holds** <date>: CPBH adopted … |`, one line each, ready to paste. The date is the
      campaign's close, as the `interrupted-runs` G1 row explains for SGK's;
    - **no "Read here" clause.** In both SGK rows that clause is the reading made in
      RunRegistryKit by the session that recorded the gate. The verification document's §2 says
      that the recording session adds its own;
    - **"its pin moved in a commit of its own (01's)" is not true as written.** `b0f3488` also adds
      the config, the ignore rule, `CLAUDE.md`'s section, a test and the records. What is true is
      that `b0f3488` is the one commit of the campaign to touch `requirements.txt`: one added line,
      `:85`, `runregistrykit[stores] @ …@v0.2.0`. `datastorekit` (`:17`) and `raypoolkit` (`:80`)
      did not move. Write that (`git log --format=%h -- requirements.txt`, `git show b0f3488 --
      requirements.txt`), and do not claim a commit of its own;
    - **the checklist's item 9 has six points.** Point 4 (the helper) does not apply, by U1, and point
      6 is the record itself. Each of points 1, 2, 3 and 5 names its ChamPBH commit and its
      evidence:
      - the pin (`b0f3488`; `pip show`'s `direct_url.json` at `0bae3a3c…`, not editable);
      - the config and `var/` ignored (`b0f3488`; `.gitignore:7`);
      - one `begin` … `finish` run listed with its `packages` (`d5d8c74`: `reference`, its committed
        manifest naming `runregistrykit`, `datastorekit` and `raypoolkit`, each with its commit);
      - the fingerprinter that loads (`c9eea37`, `TestConfigFingerprint`) and `store fingerprint`
        reading a closed store (`d5d8c74`, `out/04-store.txt`).

      Add, as SGK's rows do, what went beyond the checklist: the wrapper (`d5dd930`), and the
      hard-kill route rehearsed (`d5d8c74`);
    - **the `interrupted-runs` G2 draft** takes each of the six checks of
      `docs/adoption/v0.2.0/README.md` §3 in turn:
      - a comparison with `liveness`'s values;
      - `TERMINAL_STATES`;
      - a store refusal's text;
      - a test that begins two unfinished runs on one store;
      - a caller of `pid_alive`;
      - a test that pins a record's key set.

      For each it says what `git grep` finds in ChamPBH's tree. The orchestrator's grep for
      `liveness|TERMINAL_STATES|pid_alive|names this store|is running|Run\.finish` in `RunDrivers`,
      `config`, `tools` and `ObjectFactories` found nothing. Run it yourself and quote it. Where
      03's tests begin a second run on a store, they are written for `v0.2.0` and assert the
      refusal: say so, naming the test. Then the hard-kill rehearsal (`d5d8c74`, H3) with its run
      ids, and the open issue's absence: neither `[04-a-killed-process-not-yet-reaped-answers-kill-0]`
      nor `[03-a-run-whose-manifest-cannot-be-read-names-no-store]` was met;
    - **every draft names "this commit" for 05**, which the orchestrator replaces with the SHA in its
      follow-up. Both say the branch `rrk-adoption`, and that it is not merged into `main` when
      drafted. §2 tells the user to bring that clause up to date when recording.

    Classify as `STRUCTURALLY REQUIRED`.

13. **`OPEN_ISSUES.md` §1.14 is also stale.** Its intro says "Live." The prompt names only the
    header. **Ruled:** §1.14's intro paragraph becomes the form of §1.13's ("The campaign closed on
    <date> with 5 of 5 prompts landed. One issue was opened, …; it stays open on the board,
    unassigned."). The issue's row is unchanged. The header's sentence on this campaign says it
    closed, with 5 of 5 landed. The count stays **28**, by counting the `- \`[` rows (28 today).
    This is a deletion outside the header that the prompt's §2 item 2 must allow; the log names it.
    Classify as `STRUCTURALLY REQUIRED`.

14. **Two stale docstrings, recorded for the user, not edited.** The prompt names
    `ObjectFactories/tests/test_inventory_specs.py`. Its sentence is at `:27-29`, in the bullet at
    `:25-29` ("The fingerprint of SecondaryGWKit's RunRegistryKit, which ChamPBH is expected to
    adopt, …"); cite the lines as they are. RunRegistryKit's checklist §10 names a second,
    `utilities.py:190-192`, if the helper stays, which it does (U1). There the helper's fields are those of
    "SecondaryGWKit's `RunRegistry.package_provenance()`, which ChamPBH may one day import in place
    of this helper (README P5)". SecondaryGWKit no longer has `RunRegistry/`, and the function is
    `runregistrykit.package_provenance`. `utilities.py` is byte-identical through the campaign
    (README §5). **Ruled:** §5.6 lists both, each with its line and the sentence that would replace
    it, for the user. Neither is edited, and neither is opened as an issue (housekeeping, the user's
    call). Classify as `IMPLEMENTATION CHOICE`.

**Paths and expectations, checked by the orchestrator on 2026-10-20 at `6229b63`.** Pass them on.
- **The suites at `6229b63`**, serially (the orchestrator's run, the load beside each): see §1 step 3.
  The expected counts are 04's: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories
  78, tools 84, RunDrivers 61. `var/` now exists, and the tests do not depend on its absence (02's
  `TestCommandLine` checks that `var/` is unchanged).
- **The registry.** `list`: 4 runs, 4 finished — `rrk-adoption-04-reference-20261019T234113`
  `done`, `…-resumed-a-20261019T234840` `killed`, `…-resumed-b-20261019T235447` `lost`,
  `…-resumed-c-20261019T235647` `done`. The sidecars `var/datastores/rrk-adoption-04-reference.manifest.json`
  (`store_id` `8b8f0e38…b307`) and `…-resumed.manifest.json` (`1e9b249c…aaa6`); fingerprint
  `2dd4b39b…932f` on both.
- **`requirements.txt`**: `:17` `datastorekit` `v0.4.0`, `:80` `raypoolkit` `v0.2.0`, `:85`
  `runregistrykit[stores]` `v0.2.0`. `.gitignore:7` is `var/`.
- **The reference manifest** (`out/04-reference.manifest.json`): `packages` holds `datastorekit`
  0.4.0 (`c8570e84…`), `raypoolkit` 0.2.0 (`eaef0ce6…`) and `runregistrykit` 0.2.0
  (`0bae3a3c…`), each `editable: false`; `git_head` `07aa626…`, `git_dirty` `true`, `script`
  `main.py`, `results` `var/datastores/rrk-adoption-04-reference.db`.
- **RunRegistryKit** is at `/Users/ds283/Documents/Code/RunRegistryKit`, `HEAD` `33a2cd2`, `v0.2.0`
  = `0bae3a3`. Read it with `git show <rev>:<path>` only. Its rows `extraction` G3 and
  `interrupted-runs` G2 both read "not yet (CPBH adopts)".
- **`.documents/dk-and-rpk-migration-verification.md` §4.3–§4.4** is the form of the drafts'
  section: a paragraph saying which cell, in which file, at which commit, then the text in a
  `text` fence. Its §4.3 also says that neither cell is committed in its repository, and that the
  user makes those commits.
- **The handover's form** is §4.12–§4.14 of `review-remediation-verification.md`. The opening says
  "Added by … prompt …, measured at …. Nothing above this heading has been changed; where it is
  superseded, this section says so by statement." Then "**This supersedes**" and "**The points.**".
  It goes after §4.14's last paragraph and before the `---` above "## 5. Reproduce".

**What moves on purpose:** the prompt's §5 list, and nothing else:
- `.documents/architecture-summary.md`;
- `CLAUDE.md`;
- `.documents/review-remediation-verification.md` (§4.15 appended);
- `.documents/rrk-adoption-verification.md` (§2 added; the header's "05 adds §2" may stay);
- `.documents/OPEN_ISSUES.md`;
- `prompts/INDEX.md`;
- the board and `logs/05-close.md`.

No file under `out/` is added: 05 measures only the suites and `list`, which go in the log.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. Every figure is quoted from a file or a command's output.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up, in the board, the log, the verification document's drafts and `INDEX.md`.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two halves,
  output to a file in the agent's scratch directory, the verdict grepped from it, `uptime` beside
  each. A failure is re-run alone before it is called real. A count that moves is a stop.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`.
- **Nothing runs.** No Ray, no store opened but by `store show` / `store fingerprint` without
  `--write`, no run begun. `var/` is not committed and nothing in it is written or deleted. The run
  scripts and RunRegistryKit are read, never written. `ls var/runs` and the `list` output are the
  same before and after.
- **The additive check** (prompt §2 item 2): extract every `> **Note, …**` block of
  `architecture-summary.md` at `HEAD` and after, and `diff` them. Extract §4.1–§4.14 of
  `review-remediation-verification.md` and §1 of `rrk-adoption-verification.md` at `HEAD` and after,
  and `cmp` them. Put the commands and results in the log.
- **Stop conditions are real** (the prompt's §4, and here: a ruling that cannot be followed true).
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Check `git log` before committing, and never
  assume `HEAD` is your own.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only the 22 untracked entries;
   nothing staged; `git log --oneline 6229b63..HEAD` is this note's commit only.
2. **Ray and the registry.** `ray_cluster.sh status` exits 0; `list` as §0 says.
3. **Baselines.** The five roots at `6229b63`, serially, with the load. Give the agent the numbers.
4. **`pip freeze`**, kept in the orchestrator's scratch directory for the review.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/05-close.md`, in `dk-and-rpk-migration` README
  §5.1's form.
- **The files it may change are §0's list, and nothing else.**
- **§0's fourteen rulings** bind it, each classified as §0 says.
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git diff --stat` of its commit;
  - the additive checks' results;
  - the citation list (prompt §2 item 3), with how many entries it has and how each was checked;
  - the two drafts, verbatim;
  - §4.15's supersession statements, verbatim;
  - the counts: `OPEN_ISSUES.md`'s, and the `INDEX.md` header's;
  - the suites before and after;
  - every deviation with its tag.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists §0's files only. `git status` shows the
   22 untracked entries and nothing new. `git diff 3cd2be4 <agent> -- main.py plot_by_beta.py
   plot_ScalarModel.py utilities.py` is empty. `var/runs/`' listing and `list` are unchanged. `pip
   freeze` equals §1 step 4's. The run scripts' SHA-256s are §0's.
2. **Additive.** Re-run the agent's extraction of the `> **Note, …**` blocks, and the `cmp` of
   §4.1–§4.14 and of the verification document's §1. `git diff --numstat -- .documents/` shows
   deletions only where rulings 9, 10 and 13 allow them, and in `OPEN_ISSUES.md`'s header.
3. **Z1, by reading.** The tree's five additions and their comments. The note's five items, each
   true of the tree; nothing about 04's measurements beyond the pointer. The §4.1 sentence, and
   the `git grep` it rests on, re-run.
4. **Z2.** `git diff <agent>~1 <agent> -- CLAUDE.md` is ruling 11's three edits. The usage line
   runs: `--help` exits 0.
5. **Z3, line by line against the scripts.** Every cited line of both scripts is checked against
   `cat -n`, and the wrapped command against `registered_run.py --help` and `main.py`'s parser.
   The slug rule is checked against `slugify`, and the guard against `sidecar_path`. The
   supersessions are checked against §4.12's and §4.13's text. Nothing claims the scripts were
   edited, copied or run.
6. **Z4, against RunRegistryKit.** Every commit, path, run id, digest and line that the two drafts
   name exists (`git cat-file -e`, `git show`, `var/runs/`, the venv's `direct_url.json`). Each of
   item 9's six points and each of the six §3 checks is answered once. Neither draft claims "a
   commit of its own". Re-run the §3 `git grep`. Nothing is written in RunRegistryKit: its `git
   status` and `HEAD` are as before.
7. **Z5, by counting.** The board: status **COMPLETE**, every §1 row ✅, Z1–Z5 ✅, the gate row for
   RunRegistryKit left as the user's, §3 unchanged, §5.6 with ruling 14's two docstrings.
   `OPEN_ISSUES.md`: 28 rows, the header's count 28, §1.14 per ruling 13. `INDEX.md`: the row
   complete, in `read-only-plots`' form, its counts agreeing with the board; the header: 10
   campaigns, 0 planned, 0 live, 10 closed.
8. **The citation list.** Sample ten of the log's entries at random and check each.
9. **The records.** The log is in §5.1's form, with every deviation classified and the commands
   verbatim.
10. **The suites** at the agent's commit, serially: 18, 61 + 115, 78, 84, 61. Nothing for `black`:
    no Python file is changed.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 05* paragraph at the end of
  §5.6. Fix small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board, log, the verification document's drafts, `INDEX.md`);
  - the notes list, with this one marked "used for 05".
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - ruling 1 (the scripts moved since §4.13);
  - ruling 2 (the whole script detached, not each block);
  - ruling 4 (`store create` refuses a second time);
  - the two drafts, for the user to record in RunRegistryKit;
  - what §5.6 leaves to the user: the merge into `main` (P10), the drafts, the scripts, the
    root's files and the old stores, 04's stores and runs, and the two docstrings;
  - and the open issue `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`,
    still the user's to rule.
- **Do not merge.** The merge into `main` (P10) is the user's go-ahead, in the user's own words.
