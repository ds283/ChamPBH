# Orchestrator — prompt 03, pin `raypoolkit` `v0.2.0`

Read [`../README.md`](../README.md) first: §0.2 ("The venv"), §2.2 (03's charter), §4.4, §5
(rules 11–13). The conventions are those of [`prompt-01.md`](prompt-01.md) §0 ("Conventions"),
which are not repeated here except where 03 changes them.

**You do not write code.** You may:
- run the suites, `pip freeze`, `pip check`, `pip show` and read-only Python checks of the installed
  package, from the session scratchpad;
- fix small residue in a follow-up commit of your own (§4).

You do not install, uninstall or upgrade anything in the venv. If the agent leaves it wrong, that is
a finding for the user, not something the review repairs.

**The prompt:** [`03-pin-raypoolkit-v0.2.0.md`](../03-pin-raypoolkit-v0.2.0.md)
**Board items:** K1–K3 · **Closes:** nothing · **Narrows:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **Needs the network** (GitHub, and PyPI for
the isolated build's `setuptools`).

**Gate:**
- 02 landed (`6cbf953`) and was reviewed (`70184f1`); gate R0 holds (board §2).
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it, which includes `requirements.txt`).
  `git diff --stat d7791c5 HEAD -- . ':!prompts' ':!.documents'` is empty at `70184f1`.
- One prompt at a time in this checkout, and **no other session installs into this venv** while 03
  runs: the venv is shared (README §4.4).

## 0. What makes this prompt unusual

**It is the first prompt that changes the venv, and the venv is outside git.** A revert of 03's
commit does not undo the install (README §4.4). So the review's evidence is the venv itself, read
after the commit, not the log's account of it.

**Its acceptance is "one distribution added, and it is the tag".** Each half can pass vacuously:
- *one added* — if the "before" freeze was taken after the install, or compared with the wrong
  file, the diff proves nothing. The review compares the venv's freeze now with the committed
  `measure/out/52142d7-pip-freeze.txt`, not with the agent's scratch copy;
- *the tag* — an editable install, or one from the local checkout, would carry the right version
  string and import cleanly. Only `direct_url.json` (no `dir_info`, `vcs_info.commit_id`
  `eaef0ce6…`) and the byte comparison with `git archive v0.2.0` tell them apart.

**Nothing imports the package yet, so the suites cannot fail because of it.** They are the
check that nothing *else* moved. "No deliberate breakage" is the prompt's own ruling (§3.4).

**The facts, checked by the orchestrator on 2026-10-12 at `70184f1`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `70184f1` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's. Since `d7791c5` only `prompts/` and `.documents/` (02's records) have changed.
- **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
- **The venv.**
  - pip `26.2.1`, Python 3.13.16.
  - `pip show datastorekit raypoolkit` finds neither, and `importlib.util.find_spec("raypoolkit")`
    is `None`.
  - **`pip freeze` equals `measure/out/52142d7-pip-freeze.txt` byte for byte** (99 lines).
  - `pip check`: "No broken requirements found."
  - **`requirements.txt` equals that freeze, line for line and in order** (99 lines), so
    `pip install -r` has nothing to do but the new line.
- **`requirements.txt:78` is `ray==2.53.0` and `:79` is `referencing==0.37.0`**, as the prompt
  says. The file was last changed at `1803108` (2026-02-18).
- **The tag.** `git ls-remote --tags https://github.com/ds283/RayPoolKit v0.2.0 'v0.2.0^{}'` gives
  `4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef refs/tags/v0.2.0` and
  `eaef0ce60e2fc354bb32f0ad576010e6a83c1976 refs/tags/v0.2.0^{}`. The local checkout at
  `/Users/ds283/Documents/Code/RayPoolKit` agrees (its `HEAD` is `1b84df0`, a later commit, and
  its working tree is clean; neither matters, since `git archive v0.2.0` reads the tag).
- **The tag's package.** `git archive v0.2.0 raypoolkit` holds 4 `.py` files outside `tests/`
  (`__init__.py`, `RayWorkPool.py`, `_timing.py`, `contract.py`) and 9 under `raypoolkit/tests/`.
  `pyproject.toml` at the tag: `setuptools>=64` build backend, `dependencies = ["ray>=2.43"]`,
  `include-package-data = false`, `packages.find` excluding `raypoolkit.tests*`.
- **The dry run, done by the orchestrator** with a scratch copy of `requirements.txt` holding the
  pin at line 79: exit 0, 4.5 s; 99 "Requirement already satisfied" lines and
  **`Would install raypoolkit-0.2.0`**, nothing else. Its `--report` gives `vcs_info`
  `{"vcs": "git", "commit_id": "eaef0ce60e2fc354bb32f0ad576010e6a83c1976", "requested_revision":
  "v0.2.0"}`.
- **What pip prints that is not a failure:**
  - `Resolved https://github.com/ds283/RayPoolKit to commit 4e7eb153ca98…`. That is the **tag
    object**, not the commit: pip reports the unpeeled ref here. The commit recorded in
    `direct_url.json`, and so in `pip freeze` and the provenance helper, is `eaef0ce6…`. The log
    quotes the line and says this. **It is not the prompt's "installed commit is not `eaef0ce6`"
    stop**; that stop is judged on `pip freeze` and `direct_url.json`.
  - `Installing build dependencies … done`. Pip fetches `setuptools>=64` into a temporary,
    isolated build environment, not into the venv. The freeze after shows it did not move.
  - `Cloning … to revision v0.2.0` and `git checkout -q 4e7eb153…` into a temporary directory.
- **The provenance helper.** `utilities.installed_package_provenance(names=RECORDED_DISTRIBUTIONS)`
  (`utilities.py:177`), with `RECORDED_DISTRIBUTIONS = ("datastorekit", "raypoolkit")` (`:174`), so
  it is called with no argument. Today it prints
  `>> packages: datastorekit not installed; raypoolkit not installed`. After a VCS install,
  01's review saw the format
  `<name> 0.2.0 (git+<url>@<commit>, requested <rev>)`, so the expected line is
  `>> packages: datastorekit not installed; raypoolkit 0.2.0 (git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **No test depends on the venv's real state.** `git grep -i raypoolkit` outside `prompts/` and
  `.documents/` finds only comments in the three drivers, `RECORDED_DISTRIBUTIONS`,
  `test_package_provenance.test_default_names` (which checks the keys, not the values), and a
  fixture string in `test_compare_stores.py:1167`. So the install moves no test.
- **The suites at `d7791c5`**, unchanged since (02's review, 2026-10-12): CosmologyModels 18,
  ComputeTargets 114 (61 + 53 in two pattern groups, `test_[a-f]*.py` and `test_[g-z]*.py`),
  Datastore 66. The agent runs them after the install, in the foreground and serially. The load
  average was 5.3 / 7.6 / 34 at 01:55, falling.
- **The rehearsal stores** under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` are 02's,
  and 03 opens none of them, nor any other store.

**What the review exists to establish.**
- **(E1) K1.** The diff of `requirements.txt` is one added line, exactly the pin, between `ray` and
  `referencing`.
- **(E2) K2, the venv moved by one distribution.** The freeze now differs from the committed
  `52142d7-pip-freeze.txt` by exactly the one line
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`;
  `pip check` is clean.
- **(E3) K3, it is the tag.** `direct_url.json` is a VCS record with that commit and
  `requested_revision` `v0.2.0`, and no `dir_info`. The installed files are the tag's four, byte for
  byte, and nothing else. The import starts no Ray.
- **(E4) The helper sees it.** The provenance line is the one above.
- **(E5) Nothing else moved.** No file but `requirements.txt`, the log and the board (and the index,
  if an issue opened); RayPoolKit's checkout untouched; no store opened; the suites at 18 / 114 / 66.
- **(E6) The records.** The board's §1 row, K1–K3, `### 5.4 After prompt 03` with the rollback.

**Conventions particular to 03.** prompt-01.md's conventions hold, except that **this prompt
installs**, exactly once, as its §1 K2 says. In addition:
- **pip.** Only the three commands the prompt names change or probe the venv: the dry run, then the
  install, both `-r requirements.txt` from the repository root. No `-e`, no local path, no
  `--upgrade`, `--force-reinstall`, `--no-deps`, `--no-build-isolation`, and no `pip uninstall`
  except to restore after a stop (prompt §4). `pip freeze`, `pip check` and `pip show` are free.
- **The archive** is extracted into a new directory in the agent's scratch, never inside either
  repository.
- **RayPoolKit** is read through `git -C … show`, `archive`, `rev-parse` and its files, never
  written, and no command runs from its root.
- **Ray.** None is started. The import check is the prompt's one-liner.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only the untracked
   entries of §0, and `git diff --cached` is empty. `git log --oneline 70184f1..HEAD` touches no
   code.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The venv.** `./venv/bin/pip show datastorekit raypoolkit` finds neither, and `pip freeze`
   equals `measure/out/52142d7-pip-freeze.txt`. Save the freeze in the scratchpad.
4. **The load.** `uptime`, recorded and passed on.
5. **The rehearsal stores' mtimes**, for check 8: `/usr/bin/stat -f '%m %z %N'` over the files of
   `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` and its `logs/`, saved in the
   scratchpad (42 entries on 2026-10-12). RayPoolKit's `git status --porcelain` is empty.

The suites are not re-run by the orchestrator before dispatch: no code has changed since `d7791c5`,
and the agent runs them (§0).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- prompt 01's log;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompt 04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/03-pin-raypoolkit-v0.2.0.md`, in README §5.1's form,
  with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list (`requirements.txt`, the log, the board) and
  `.documents/OPEN_ISSUES.md` if an issue opens. **No code, no test, nothing in RayPoolKit.**
- **It installs once,** after a dry run that reports only `raypoolkit`, and from the tag on GitHub.
- **Stop and ask** on any of the prompt's §4 conditions. If pip has moved anything but `raypoolkit`,
  it records exactly what, restores it from the saved freeze, and stops without committing.
- **It replies with:**
  - the log's path and the commit;
  - the `requirements.txt` diff;
  - the dry run's and the install's lines other than "Requirement already satisfied";
  - the freeze diff, `pip check`, and the provenance line;
  - `direct_url.json` verbatim, and the file comparison's counts and result;
  - the import check's output;
  - the three suites, with the load;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — ten checks

1. **Scope.** At the agent's commit, `git diff HEAD~1 HEAD --stat` touches only `requirements.txt`,
   `logs/03-…` and the board (and the index, if an issue opened). `git status` shows the §1
   untracked set and nothing new. `git log` since the note shows no other commit of the agent's.
2. **E1, K1.** `git diff HEAD~1 HEAD -- requirements.txt` is the one line, at line 79, between
   `ray==2.53.0` and `referencing==0.37.0`; `wc -l` gives 100.
3. **E2, the freeze.** `./venv/bin/pip freeze`, diffed against the **committed**
   `measure/out/52142d7-pip-freeze.txt`: exactly one added line, the `eaef0ce6…` one, and nothing
   removed or changed. Also diff against the §1 scratch copy: the same. `pip check` clean.
4. **E3, the origin.** Read `raypoolkit-0.2.0.dist-info/direct_url.json` in site-packages directly:
   `url` `https://github.com/ds283/RayPoolKit`, `vcs_info.vcs` `git`, `commit_id` `eaef0ce6…`,
   `requested_revision` `v0.2.0`, and no `dir_info`. `INSTALLER` is `pip`. No
   `__editable__*`, `*.pth` or `.egg-link` naming RayPoolKit in site-packages.
5. **E3, the files.** From the scratchpad: extract `git -C <RayPoolKit> archive v0.2.0 raypoolkit`
   into a new directory, and compare with `find_spec("raypoolkit").submodule_search_locations[0]`,
   ignoring `__pycache__`: the same 4 `.py` files, byte for byte (`cmp`), no `tests/`, nothing else.
   Then check the dist-info's `RECORD`: every `raypoolkit/` entry is one of the four (or a
   `__pycache__` file), and each listed hash matches the installed file.
6. **E3–E4, the import and the helper.** The prompt's import one-liner prints `False`. In a fresh
   process, `utilities.installed_package_provenance()` gives exactly `version` `0.2.0`, the URL,
   commit and revision above, `editable: False`, and `datastorekit` `None`; `format_package_provenance`
   prints §0's expected line. `pip show raypoolkit` gives `Requires: ray`.
7. **The log's pip account.** The dry run says `Would install raypoolkit-0.2.0` and nothing else;
   the install says `Successfully installed raypoolkit-0.2.0` and nothing else. If the log quotes
   the "Resolved … to commit 4e7eb153…" line, it says it is the tag object.
8. **E5, hygiene.** RayPoolKit's `git status --porcelain` is empty and its `HEAD` is still
   `1b84df0`. The §1 mtimes of the rehearsal stores are unchanged. No Ray process. No scratch file
   in either repository.
9. **E5, the suites.** The agent's: 18, 114, 66, with the load. The orchestrator re-runs
   `Datastore/tests` (about 5 s) at the agent's commit, since it holds the provenance tests and is
   the root nearest the change: 66 OK. ComputeTargets and CosmologyModels are not re-run unless
   the agent's figures are missing or odd: nothing they import changed.
10. **E6, the records.** The board's §1 row (✅, "this commit", the log), K1–K3, and
    `### 5.4 After prompt 03 (<date>)` naming the rollback (`git revert` plus
    `./venv/bin/pip uninstall -y raypoolkit`). Any §3 entry has the index's row, count and date in
    the same commit. The log follows README §5.1, with every §2 check's command and output.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03* paragraph in §5.4: what each
  check found, and anything handed on to 04. Fix small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 03".
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the pin, and the freeze diff in one line;
  - the origin and the file comparison;
  - the provenance line every driver now prints;
  - the suites;
  - anything opened.
- **Then 04** can be dispatched; its note is written after this review. 05–06 are still to be
  written (README §2.1), with bitwise equality as R1's criterion.
