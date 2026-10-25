# Orchestrator — prompt 01, switch to the `raypoolkit` rehearsal lock

Read [`../README.md`](../README.md) first: §0 (what changes), §1 (scope), §4.1 (rollback), §5 (the
rules) and §6.2 (P3–P7). Then the prompt, and the contract at RayPoolKit's `v0.3.0`
(`docs/rehearsal-lock.md`, §6 "What it asks of a caller" above all).

**You do not write code.** You may:
- run the suites, the new test module and the script's `status`;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-switch-to-the-raypoolkit-rehearsal-lock.md`](../01-switch-to-the-raypoolkit-rehearsal-lock.md)
**Board items:** P1, S1, T1, C1, R1, B1 · **Gate:** "R" · **Closes:** nothing · **Opens:** only what
the work finds · **Model:** Opus, as the prompt recommends · **Network:** GitHub, for the `pip`
install of the tag (2.2); nothing else.

**Gate**, checked at `51c47b7` on 2026-10-25, before this note was committed:
- `rpk-v030-adoption` checked out in the main checkout at the planning commit `51c47b7`, `git status`
  clean. `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
  `jordan-normalization` is idle at `807f175` and stays idle until 01 is reviewed.
- `./venv/bin/python -m runregistrykit list`: no runs.
- The venv: `raypoolkit` 0.2.0 at `eaef0ce6…`, `datastorekit` 0.4.0 at `c8570e84…`, `runregistrykit`
  0.2.0 at `0bae3a3c…` (`pip freeze`), none editable; `ray==2.53.0`. `installed_package_provenance()`
  reports `datastorekit` and `raypoolkit` only (ruling 6).
- Neither `~/ScienceData/.rehearsal-lock` nor `~/.raypoolkit/rehearsal-lock` exists; `~/.raypoolkit`
  exists, empty. The script's `status`: lock free, no processes, the four ports free.
  `pgrep -lf 'gcs_server|raylet'` finds nothing.
- `.documents/rpk-v030-adoption/` does not exist. `./venv/bin/black` is present.
- The load average was 18–48 while this note was written; other workloads share the machine.

## 0. What makes this prompt unusual

**It is the first time ChamPBH's lock is somebody else's code, and its one real-Ray check holds the
real shared lock.** The script's lock half is about 70 lines; what can go wrong is not in them but
in the processes around them: what the module's process rule lists, and what a test may leave
running. Every ruling below is about one of those.

**Six statements in the prompt are wrong, loose or silent for this machine. Ruled here.**

1. **The prompt's fake `gcs_server` is never found by the script** (§2.4, the stub's `ok`). On this
   machine the venv's Python is MacPorts' framework build, which re-executes itself: `ps` shows a
   `nohup ./venv/bin/python -c 'import time; time.sleep(120)' gcs_server <D>/session` as
   ```text
   82559 82557 /opt/local/Library/Frameworks/Python.framework/Versions/3.13/Resources/Python.app/Contents/MacOS/Python -c import time; time.sleep(120) gcs_server /var/folders/…/session
   ```
   so `gcs_server` starts at column 145. `cmd_start` finds the `gcs_server` with
   `describe_pid "$p" | grep -q 'gcs_server'`, and `describe_pid` cuts at 150 columns (cluster half,
   unchanged): `grep -c gcs_server` on the cut line was **0**. Test 9 would exit 1 at "no
   gcs_server". **Ruled:** the fake, in the stub and wherever a test needs a process of the cluster,
   is
   ```bash
   /usr/bin/perl -e 'sleep 120' gcs_server <cluster temp dir>/session
   ```
   measured here: `ps` shows `/usr/bin/perl -e sleep 120 gcs_server /var/folders/…/session`, the cut
   line contains `gcs_server`, the process names the temp dir, and `SIGTERM` ends it at once. The
   test's own cluster process (tests 5 and 6) may use the same command. Classify as `STRUCTURALLY
   REQUIRED`, citing this note.

   The real `gcs_server`'s path is shorter (under `venv/lib/python3.13/site-packages/ray/core/src/
   ray/gcs/`), so the real cluster is found. Step 2.8.3 records the column at which `gcs_server`
   starts in the real `gcs_server`'s line. The cut itself is in the cluster half and out of scope:
   it goes under "Observations not acted on", with the measured column, and **no issue**; the
   orchestrator decides at the review.

2. **The fake must not hold the test's pipes** (§2.4). `nohup` redirects only a terminal. A fake
   started as `nohup … &` inherits the stub's stdout, which is the pipe `subprocess.run` is
   reading, and `communicate()` then waits for EOF until the fake exits, 120 s later. **Ruled:** the
   stub starts it as `nohup … </dev/null >/dev/null 2>&1 &`, and the test's own `Popen` passes
   `stdin`, `stdout` and `stderr` as `subprocess.DEVNULL`. Every `subprocess.run` in the test has a
   `timeout` (60 s is plenty), so a hang fails a test instead of blocking the suite. Classify as
   `STRUCTURALLY REQUIRED`.

3. **"Still alive" for the test's own child is `Popen.poll() is None`**, never `kill -0` (tests 5
   and 6). A child the test has not reaped answers `kill -0` (`CLAUDE.md`, rule 2). The fake started
   by the stub is re-parented to `launchd`, which reaps it, so for it `os.kill(pid, 0)` is right
   (test 9, "the fake gone"); allow it up to the grace period to go. `tearDown` kills and reaps the
   test's own children, and kills the stub's fake by its recorded pid if it is alive. Classify as
   `IMPLEMENTATION CHOICE`.

4. **The stub's mode when none is set**, and the stop's location lookup, are silent.
   - **The stub with no mode** records its arguments and exits 1, starting nothing. A test that
     says "the stub never called" reads the calls file as absent or empty. Classify as
     `IMPLEMENTATION CHOICE`.
   - **`cmd_stop`'s `default_lock_dir()` call failing** (the interpreter missing, the import
     failing): `err` a line saying so and return 1 **before `stop_cluster`**, the same refusal as
     another project's lock. A stop that cannot tell whose lock it is touches nothing. Classify as
     `IMPLEMENTATION CHOICE`.
   - **The tests compute the two locations** with `unittest.mock.patch.dict(os.environ, {"HOME":
     <the test's HOME>})` around `default_lock_dir()` and `old_lock_dir()` in the test process; the
     module is imported normally (it imports Ray's package, and starts nothing). Classify as
     `IMPLEMENTATION CHOICE`.

5. **The `tools/tests` count after.** One test method per item of §2.4: nine. So `tools` goes from
   **84 to 93**, and every other root is unchanged. A different number of methods is allowed only
   if the log says why, and then §2.7's "higher by exactly" is that number. The baselines on `main`
   are `CLAUDE.md`'s: 18 / 61 + 115 / 78 / 84 / 61 (README §0.3). `jordan-normalization`'s counts
   (79 ObjectFactories, 118 in the second half) are not this branch's.

6. **"The others as in 2.1"** (§2.2): `installed_package_provenance()` reports `datastorekit` and
   `raypoolkit` only. `runregistrykit`'s unchanged pin is shown by `pip freeze`, whose diff must be
   the one `raypoolkit` line. The log says which listing shows which package. No ruling needed
   beyond reading it so.

**The expected breakage record** (§3.1), for comparison, not a requirement on the tests' design:
M1 fails tests 3, 4 and 6 (the stub is called); M2 fails test 5; M3 fails test 9 (the owner file's
`pid:`); M4 fails test 7; M5 and M6 fail test 1 only, since the harness overrides the temp dir. A
mutation that fails no test is a stop. A mutation that fails a test other than these is recorded as
it fell, and explained.

**Conventions.** All of these bind the agent.
- **Order.** The gate checks; the baselines of §2.1 (before the install); the dry run, then the
  install; the script, the test and `CLAUDE.md`; the test run and the breakages; §2.5's grep; the
  roots after; §2.8 last, once everything else passes. Then the records.
- **The real cluster (§2.8).** From `start` (step 2) to the end of `stop` (step 6), no shell command
  names `/tmp/ray-cpbh` except steps 4 and 5's two lock commands, each alone in its Bash call. In
  particular no `pgrep -f`, `ps … | grep` or `lsof` naming the temp dir: such a command is listed by
  the module while it runs, and is a process `stop` would signal. The script's `status` is the only
  way to look. Nothing else runs on the cluster, and no driver runs. Steps 1–7 take a few minutes,
  in the foreground.
- **The lock.** If anything leaves the lock held unexpectedly, stop and leave it held, with its owner
  file printed. **Never remove a lock, either location, by hand.** No `ray stop`, ever.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, `ComputeTargets` in its two
  halves; output to a file in the agent's scratch directory, the verdict grepped from it, `uptime`
  beside each. A failure is re-run alone before it is called real. An existing test that fails is a
  stop, never a reason to edit it.
- **Breakage diffs** are recorded exactly as applied, checked with `git apply --check` and
  `-R --check`, replayed in `bash`, and reverted; `git status` shows only the work afterwards.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp` and never
  the repository. The tests use `tempfile`, as the prompt says.
- **Staging and reporting.** Stage by explicit path only. Show `git diff --cached --name-only`
  before committing; it is §3 item 7's list exactly. Count, then paste. A log states only what the
  diff, the tests and the transcripts show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent is
  resumed with `SendMessage`. If it stops after the install, it says that the venv holds `v0.3.0`.
- **Not touched:** `orchestrator/`, `prompts/INDEX.md`, the board's header beyond the prompt's §6,
  any other campaign's records, `jordan-normalization`.

## 1. Before you dispatch

Done at `51c47b7`, before this note was committed: the gate above, and the two probes of rulings 1
and 2 in the session scratchpad (a `mktemp -d` directory, removed; each fake killed and gone).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt, the campaign README and the board;
- `HEAD` and the gate as measured;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly §3 item 7's files. Its log is
  `logs/01-switch-to-the-raypoolkit-rehearsal-lock.md`, in README §5.1's form.
- **§0's six rulings bind it**, each classified as §0 says.
- **It starts Ray only in §2.8**, through the script, and opens no store.
- **Stop and ask** on any of the prompt's §4 conditions, or on an existing test failing.
- **It replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - `pip`'s dry-run line, the `pip freeze` diff and the provenance after;
  - the script's `git diff --stat` and a summary of what changed by function;
  - the nine tests, each with what it asserts, and the module's time;
  - each breakage, with the tests it failed;
  - the five roots, before and after, with `uptime`;
  - §2.5's two numbers;
  - the cluster transcript summarised: each step's exit code, the module's and the script's
    listings compared pid by pid, and the real `gcs_server`'s column (ruling 1);
  - every deviation with its tag; "Observations not acted on" and "State handed on", verbatim.

## 3. The review — eight checks

1. **Scope.** `git show --stat <agent>` lists exactly §3 item 7's files. `git status` is clean.
2. **The pin and the venv.** `requirements.txt`'s diff is the one line. `pip freeze` and the
   provenance now: `raypoolkit` 0.3.0 at `67a75b97…`, `requested_revision` `v0.3.0`, not editable;
   the other two as in the gate. `pip check` clean. `python -m raypoolkit.rehearsal_lock --help`'s
   first line.
3. **The script.** `git diff <agent>~1 <agent> -- <the script>` read whole. The cluster half is byte
   for byte: extract `temp_dir_regex` through `stop_cluster`, the cluster's values, `say`/`err` and
   the sourcing guard from both trees and `diff` them (empty). Every call passes the values from the
   variables; `cmd_start`'s six paths have today's exit codes; `cmd_stop` refuses before
   `stop_cluster`; no command line of the script names the temp dir. `bash -n`. `--help` names the
   new location and neither the old one nor the brief's section 2.
4. **The test.** Read it against §2.4 and rulings 1–4: no value on a command line, `HOME` and the
   cluster values through the environment, perl fakes with stdio to `/dev/null`, timeouts on every
   `subprocess.run`, `poll()` for its own children, nothing found by name. Run it alone, with its
   time, and check afterwards that no process names a test temp dir and that the real
   `~/.raypoolkit/rehearsal-lock` does not exist.
5. **The breakages.** In `bash`, replay each of M1–M6, run the module, see it fail as the log says,
   revert; compare with §0's expected record. `git status` clean afterwards.
6. **The grep.** §2.5's pipeline at `<agent>`: 0, and at least 1 call of the module in the script.
7. **The real cluster.** Read `01-cluster.txt` against §2.8 and the stop conditions: the
   `gcs_server`, the `raylet` and the client server in the module's listing; `release` 1 and the
   second `acquire` 3 while it ran; the second `start` 3; `stop` 0, then released; status as at the
   start. Now: the script's `status` shows the lock free, no processes, the ports free; neither
   location exists. The orchestrator does not start the cluster again.
8. **The records and the roots.** `CLAUDE.md`'s new bullet against §2.6 (after "Ray on this
   machine", which is unchanged; no old location), the `tools` count 93 and the date sentence; the
   board's header, §1 row, the six items and gate R; the log in §5.1's form with every deviation
   classified. Then the five roots at `<agent>`, serially, at 18 / 61 + 115 / 78 / 93 / 61, and
   `black --check` on the new test.

## 4. After the review

The review closes the campaign (README §2). **Record it on the board** as an *Orchestrator review
of prompt 01* paragraph in §5.1, and in the same commit:
- replace "this commit" with the SHA (the board, the log);
- add this note to the board's notes, marked "used for 01";
- the board's header and §1: 01 ✅, the campaign closed, gate R as found;
- the campaign's line in `prompts/INDEX.md`: closed.

Residue the review finds is fixed in a follow-up commit before the close. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the message as committed, and the closing commit;
- the rulings that read the prompt otherwise than it was written: the perl fake (ruling 1) and its
  stdio (ruling 2);
- gate R: the listings, the refusals, and that nothing was left;
- the five roots' counts;
- that P1–P8 stood as proposed, the user having ruled none otherwise;
- **the hand-on, which is the user's:** merge `rpk-v030-adoption` into `main` and push, then `main`
  into `jordan-normalization` before its 09 (`prompts/INDEX.md` will conflict); the checkout is left
  on `rpk-v030-adoption`, and the venv holds `v0.3.0` for both branches.
