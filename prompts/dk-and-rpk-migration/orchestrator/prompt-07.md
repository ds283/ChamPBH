# Orchestrator — prompt 07, pin `datastorekit` `v0.3.0`

Read [`../README.md`](../README.md) first: §0.3, §0.4 (the release, U6), §2.2 (07's charter), §4.4,
§5 (rules 11–13) and §6.7. The conventions are those of [`prompt-01.md`](prompt-01.md) §0
("Conventions"), as [`prompt-03.md`](prompt-03.md) adapted them for a pin, and are not repeated
here except where 07 changes them. 07 is 03 for the other package; 03's review (board §5.4) is the
model for this one.

**You do not write code.** You may:
- run the suites, `pip freeze`, `pip check`, `pip show` and read-only Python checks of the installed
  package, from the session scratchpad, K7's replay among them (§3 check 8);
- fix small residue in a follow-up commit of your own (§4).

You do not install, uninstall or upgrade anything in the venv. If the agent leaves it wrong, that is
a finding for the user, not something the review repairs.

**The prompt:** [`07-pin-datastorekit-v0.3.0.md`](../07-pin-datastorekit-v0.3.0.md)
**Board items:** K4–K7 · **Closes:** nothing · **Narrows:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **Needs the network** (GitHub for the tag and
both VCS requirements, and PyPI for the isolated build's `setuptools`).

**Gate:**
- 06 landed (`c9e6bc2`) and was reviewed (`fa874a5`); gate R1 holds (board §2). 07–10 were written
  at `2ea647f`.
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it, which includes `requirements.txt`).
  `git diff --stat c9e6bc2 HEAD -- . ':!prompts' ':!.documents'` is empty at `2ea647f`.
- `ray_cluster.sh status` shows no ChamPBH process; 07 starts none and takes no lock.
- One prompt at a time in this checkout, and **no other session installs into this venv** while 07
  runs: the venv is shared (README §4.4).

## 0. What makes this prompt unusual

**It is 03 again, with three differences.**
- **`requirements.txt` already holds a VCS requirement** (`raypoolkit`, `:79`). Pip cannot see that
  a tag-pinned VCS requirement is satisfied without resolving it, so the dry run and the install
  each **clone RayPoolKit too**, resolve it, find it installed at the same commit, and leave it. It
  is not one of the "Requirement already satisfied" lines, and it is not installed. That is not a
  stop; a `Successfully installed` or `Would install` line naming `raypoolkit` would be.
- **K7 is new**: three checks on the installed package's behaviour, made from a throwaway script.
  They are the evidence for U3 and U6, which until now have been read at the tag but never run.
- **Nothing in ChamPBH imports the package until 08**, as at 03. The suites are the check that
  nothing *else* moved.

**Where the acceptance can pass vacuously.** As at 03, the freeze diff (taken against the wrong
"before") and the provenance (an editable or local install carries the right version). In addition,
K7's checks pass on the wrong package if the script imports `datastorekit` from anywhere but the
installed site-packages: from DatastoreKit's checkout, whose working tree is ahead of the tag, or
from an extracted archive left on `sys.path`. So the script prints `datastorekit.__file__`, and the
review replays K7 itself against the installed package (§3 check 8).

**The facts, checked by the orchestrator on 2026-10-12 at `2ea647f`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `2ea647f` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's. Since `c9e6bc2` only `prompts/` has changed.
- **Ray.** `ray_cluster.sh status`: lock free, no ChamPBH process, ports 6380/6381/8267/52367 free.
  `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
- **The venv.**
  - pip `26.2.1`, Python 3.13.16.
  - **`pip freeze` (100 lines) equals `measure/out/52142d7-pip-freeze.txt` plus exactly
    `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`**
    at line 79. `pip check`: "No broken requirements found." `pip show datastorekit`: "Package(s)
    not found". site-packages holds no `datastorekit*` entry, and its only `.pth` is setuptools'
    `distutils-precedence.pth`.
  - `requirements.txt` (100 lines) equals the freeze but for line 79, which names `@v0.2.0` where the
    freeze names the commit. It was last changed at `2f61943` (03).
- **`requirements.txt:16` is `cycler==0.12.1` and `:17` is `diffrax==0.7.0`**, so the pin goes in at
  line 17 and the file becomes 101 lines. `ray==2.53.0` is `:78`, `raypoolkit` `:79`,
  `SQLAlchemy==2.0.46` `:90`; after the pin each moves down by one.
- **The tag.** `git ls-remote --tags https://github.com/ds283/DatastoreKit v0.3.0 'v0.3.0^{}'` gives
  `bf0a3b25afa5b4379f9cdc01bca93805ba2a695b refs/tags/v0.3.0` and
  `2f5417ea76cdcd0d15d64d80d9085429bfacad21 refs/tags/v0.3.0^{}`. The local checkout agrees. Its
  `HEAD` is `04a2ed9` (another campaign's planning, ahead of the tag) and its working tree is clean;
  neither matters, since `git show` and `git archive` read the tag.
- **The tag's package.** `pyproject.toml`: version `0.3.0`, `requires-python >=3.12`,
  `dependencies = ["ray>=2.43", "sqlalchemy>=2.0.39,<2.1"]`, `setuptools>=64`, `packages.find`
  including `datastorekit*` and excluding `datastorekit.tests` and `datastorekit.tests.*`.
  `git archive v0.3.0 datastorekit` holds **20 `.py` files outside `tests/`** — the root's 9
  (`__init__`, `_timing`, `contract`, `defaults`, `object`, `replication`, `shard_paths`,
  `store_inventory`, `store_reader`), `SQL/`'s 8 (`__init__`, `ClientPool`, `Datastore`,
  `factory_base`, `ProfileAgent`, `schema`, `SerialPoolBroker`, `ShardedPool`) and `tools/`'s 3
  (`__init__`, `shard_key_audit`, `sharded_store`) — **no other file outside `tests/`**, and **54
  files** (of all kinds) under `datastorekit/tests/`.
- **The dry run, done by the orchestrator** with a scratch copy of `requirements.txt` holding the
  pin at line 17: exit 0, 7.6 s. 99 "Requirement already satisfied" lines (among them `ray==2.53.0`
  and `SQLAlchemy==2.0.46`) and **`Would install datastorekit-0.3.0`**, nothing else. Its `--report`
  has one `install` entry, `datastorekit` `0.3.0`, `vcs_info`
  `{"vcs": "git", "commit_id": "2f5417ea76cdcd0d15d64d80d9085429bfacad21", "requested_revision":
  "v0.3.0"}`.
- **What pip prints that is not a failure:**
  - `Resolved https://github.com/ds283/DatastoreKit to commit bf0a3b25afa5…`: the **tag object**, as
    at 03. The commit in `direct_url.json`, `pip freeze` and the helper is `2f5417ea…`. **It is not
    the prompt's "installed commit is not `2f5417ea…`" stop**, which is judged on the freeze and
    `direct_url.json`.
  - `Collecting raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0`, its clone, and
    `Resolved https://github.com/ds283/RayPoolKit to commit 4e7eb153…` (RayPoolKit's tag object):
    the re-resolution described above. RayPoolKit is not reinstalled. Its dist-info and its four
    modules have mtime `1791766684` (03's install), and the review checks they still do.
  - `Installing build dependencies … done`: `setuptools>=64` into a temporary isolated build
    environment, not the venv.
- **The provenance helper** (`utilities.py:177`, `RECORDED_DISTRIBUTIONS` `:174`) gives today
  `{'datastorekit': None, 'raypoolkit': {'version': '0.2.0', 'url': 'https://github.com/ds283/RayPoolKit',
  'commit': 'eaef0ce6…', 'requested_revision': 'v0.2.0', 'editable': False}}`. `format_package_provenance`
  takes that dict as its one argument (`:232`). The expected line after the install, by 03's
  observed format (a PEP 610 VCS `url` carries no `git+`):
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **No test depends on the venv's real state.** `git grep -i datastorekit` outside `prompts/` and
  `.documents/` finds the drivers' comments, `RECORDED_DISTRIBUTIONS`, comments in
  `tools/compare_stores.py`, `test_package_provenance` (fixture URLs, and the keys, not the values,
  in `test_default_names`), and a fixture string in `test_compare_stores.py:1167`. Nothing imports
  the package, so the install moves no test.
- **K7, probed by the orchestrator against the extracted tag archive** (on `sys.path` ahead of
  everything, with ChamPBH's tree; not the installed package, which does not yet exist). The
  agent's figures should agree:
  - **ChamPBH's script must run with `PYTHONPATH=.`** from the repository root (CLAUDE.md's form).
    A script in the scratch directory has its own directory as `sys.path[0]`, not the working
    directory, and without it `import Datastore` fails.
  - **All 27 entries of `config.datastore.factories` are instances**, of **23 classes**. The "two
    quantity factories" are two *classes* covering **six entries**: `beta_value` on
    `sqla_dimensionless_quantity_factory`, and `M_value`, `Lambda_value`, `temperature`,
    `phi_value`, `pi_value` on `sqla_dimensionful_quantity_factory`; each entry's constructor
    argument is its `ObjectType`. The other 21 entries take no argument.
  - Rebuilt as the prompt says, **all 27 instantiate**. By class: `sqla_AdiabaticHistoryFactory`,
    `sqla_BBNDataFactory` and `sqla_ScalarModelFactory` define all three of `store`, `validate`,
    `validate_on_startup`; the other 20 define none. No class defines some but not all.
  - A subclass defining `register` alone raises `TypeError: Can't instantiate abstract class A
    without an implementation for abstract method 'build'`.
  - A `DatastoreObject` subclass constructed with the store id alone has `timestamp` `None`;
    assigning it raises `AttributeError: property 'timestamp' of 'D' object has no setter`.
  - The prompt's import one-liner prints `False`, and `python -m datastorekit.tools.sharded_store
    --help` exits 0 (usage `{copy,move} src dst`).
- **The suites at `c9e6bc2`**, unchanged since (06's review): CosmologyModels 18, ComputeTargets
  114 (61 + 53 in two pattern groups, `test_[a-f]*.py` and `test_[g-z]*.py`), Datastore 73. The agent
  runs them after the install, in the foreground and serially.
- **The load** at dispatch: 7.2 / 21.8 / 23.9 at 21:21, falling.
- **The rehearsal stores** under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` are 02's
  and 06's, and 07 opens none of them, nor any other store.

**What the review exists to establish.**
- **(E1) K4.** The diff of `requirements.txt` is one added line, exactly the pin, at line 17 between
  `cycler==0.12.1` and `diffrax==0.7.0`.
- **(E2) K5, the venv moved by one distribution.** The freeze now differs from the committed
  `52142d7-pip-freeze.txt` by exactly the `raypoolkit` line (03's) and the `datastorekit` line
  `datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21`;
  `pip check` is clean; RayPoolKit's files are untouched.
- **(E3) K6, it is the tag.** `direct_url.json` is a VCS record with that commit and
  `requested_revision` `v0.3.0`, and no `dir_info`. The installed files are the tag's 20, byte for
  byte, and nothing else; no `tests/`. The import starts no Ray.
- **(E4) The helper sees it.** The provenance line is the one above.
- **(E5) K7 holds on the installed package**, by the agent's script and by the orchestrator's own.
- **(E6) Nothing else moved.** No file but `requirements.txt`, the log and the board (and the
  index, if an issue opened); DatastoreKit's and RayPoolKit's checkouts untouched; no store opened;
  the suites at 18 / 114 / 73.
- **(E7) The records.** The board's §1 row, K4–K7, `### 5.12 After prompt 07` with the rollback.

**Conventions particular to 07.** 03's hold: **this prompt installs**, exactly once, after a dry
run. Only those two commands, both `-r requirements.txt` from the repository root, change or probe
the venv. No `-e`, local path, `--upgrade`, `--force-reinstall`, `--no-deps` or
`--no-build-isolation`, and no `pip uninstall` except to restore after a stop. `pip freeze`,
`pip check` and `pip show` are free. In addition:
- **The archive** is extracted into a new directory in the agent's scratch, never inside either
  repository, and is never on `sys.path` while K7's script runs.
- **K7's script** lives in the agent's scratch, runs as `PYTHONPATH=. ./venv/bin/python <scratch>/…`
  from the repository root, prints `datastorekit.__file__` first, and is quoted whole in the log.
  It is never committed.
- **DatastoreKit** is read through `git -C … show`, `archive`, `rev-parse` and `ls-remote`, never
  written, and no command runs from its root.
- **Ray.** None is started. The import check is the prompt's one-liner.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only §0's untracked
   entries, and `git diff --cached` is empty. `git diff --stat c9e6bc2 HEAD -- . ':!prompts'
   ':!.documents'` is empty.
2. **Ray.** `ray_cluster.sh status` as §0; `pgrep` finds nothing.
3. **The venv.** `pip freeze` saved in the scratchpad and diffed against the committed file as §0;
   `pip check` clean; `pip show datastorekit` finds nothing; site-packages listed (207 entries).
4. **The load.** `uptime`, recorded and passed on.
5. **Hygiene baselines**, for the review: `/usr/bin/stat -f '%m %z %N'` over the rehearsal root to
   depth 2 (104 entries); RayPoolKit's dist-info and modules' mtimes; the four siblings' `HEAD`s
   (SecondaryGWKit `a5a6f6d`, StochasticInstantons `5fb4a1d`, RayPoolKit `1b84df0`, DatastoreKit
   `04a2ed9`), each with a clean working tree.

The suites are not re-run by the orchestrator at dispatch: no Python has changed since `c9e6bc2`,
where they were 18 / 114 / 73, and the agent runs them.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- prompt 03's log;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompts 08 onward.

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/07-pin-datastorekit-v0.3.0.md`, in
  README §5.1's form, with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list (`requirements.txt`, the log, the board) and
  `.documents/OPEN_ISSUES.md` if an issue opens. **No code, no test, nothing in DatastoreKit or
  RayPoolKit.**
- **It installs once,** after a dry run that reports only `datastorekit`, and from the tag on
  GitHub.
- **Stop and ask** on any of the prompt's §4 conditions. If pip has moved anything but
  `datastorekit`, it records exactly what, restores it from the saved freeze, and stops without
  committing. A K7 failure is a finding about the release, recorded and stopped on, not worked
  around.
- **It replies with:**
  - the log's path and the commit; `git diff --cached --name-only` before it and
    `git show --stat HEAD` after it;
  - the `requirements.txt` diff;
  - the dry run's and the install's lines other than "Requirement already satisfied";
  - the freeze diff, `pip check`, and the provenance line;
  - `direct_url.json` verbatim, and the file comparison's counts and result;
  - the import check's and `sharded_store --help`'s output;
  - K7's script and its output, whole;
  - the three suites, with the load;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — eleven checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only `requirements.txt`,
   `logs/07-…` and the board (and the index, if an issue opened). `git status` shows §0's untracked
   set and nothing new. `git log` since the note shows no other commit of the agent's.
2. **E1, K4.** `git diff HEAD~1 HEAD -- requirements.txt` is the one line, at line 17, between
   `cycler==0.12.1` and `diffrax==0.7.0`; `wc -l` gives 101. Nothing else in the file moved.
3. **E2, the freeze.** `./venv/bin/pip freeze`, diffed against the **committed**
   `measure/out/52142d7-pip-freeze.txt`: exactly two added lines, the `datastorekit` one at line 17
   and the `raypoolkit` one, and nothing removed or changed. Against the §1 scratch copy: the one
   `datastorekit` line. `pip check` clean. RayPoolKit's dist-info and modules keep mtime
   `1791766684`.
4. **E3, the origin.** Read `datastorekit-0.3.0.dist-info/direct_url.json` in site-packages
   directly: `url` `https://github.com/ds283/DatastoreKit`, `vcs_info.vcs` `git`, `commit_id`
   `2f5417ea…`, `requested_revision` `v0.3.0`, and no `dir_info`. `INSTALLER` is `pip`. No
   `__editable__*`, `.egg-link` or new `.pth` in site-packages; the listing differs from §1's by the
   package and its dist-info only.
5. **E3, the files.** From the scratchpad: extract `git -C <DatastoreKit> archive v0.3.0
   datastorekit` afresh and compare with `find_spec("datastorekit").submodule_search_locations[0]`,
   ignoring `__pycache__`: the same 20 `.py` files, byte for byte (`cmp`), no `tests/`, nothing
   else. Then the dist-info's `RECORD`: every `datastorekit/` entry is one of the 20 (or a
   `__pycache__` file), and each listed hash matches the installed file.
6. **E3–E4, the import and the helper.** The prompt's import one-liner prints `False`;
   `python -m datastorekit.tools.sharded_store --help` exits 0. In a fresh process,
   `installed_package_provenance()["datastorekit"]` gives exactly `version` `0.3.0`, the URL, commit
   and revision above and `editable: False`, and `["raypoolkit"]` is unchanged;
   `format_package_provenance` prints §0's expected line. `pip show datastorekit` gives
   `Requires: ray, sqlalchemy`.
7. **The log's pip account.** The dry run says `Would install datastorekit-0.3.0` and nothing else;
   the install says `Successfully installed datastorekit-0.3.0` and nothing else. If the log quotes
   either `Resolved … to commit` line, it says each names a tag object; the RayPoolKit re-clone is
   explained, not taken for a reinstall.
8. **E5, K7.** The agent's script, read in the log: it imports the installed package (its
   `datastorekit.__file__` is under `venv/lib/python3.13/site-packages/`), imports the layer before
   `config.datastore`, builds each class from its own `vars()` less `__dict__` and `__weakref__` on
   the installed `SQLAFactoryBase`, and passes `ObjectType` to the six quantity entries. Its output
   agrees with §0's probe: 27 instantiate; 3 classes define all three hooks, 20 none; the
   `register`-only subclass's `TypeError` names `build`; `timestamp` is `None` and unassignable.
   **Then the orchestrator replays it** with its own script (§0's probe, `sys.path` left alone so
   the installed package is the one imported) and gets the same.
9. **E6, hygiene.** DatastoreKit's and RayPoolKit's `git status --porcelain` empty and `HEAD`s as
   §1 (if DatastoreKit's moved, `git log` shows another session's commits, nothing the agent's). The
   §1 rehearsal mtimes unchanged. No Ray process. No scratch file in either repository.
10. **E6, the suites.** The agent's: 18, 114, 73, with the load. The orchestrator re-runs
    `Datastore/tests` (about 5 s) at the agent's commit: 73 OK. ComputeTargets and CosmologyModels
    are not re-run unless the agent's figures are missing or odd: nothing they import changed.
11. **E7, the records.** The board's §1 row (✅, "this commit", the log), K4–K7, and
    `### 5.12 After prompt 07 (<date>)` naming the rollback (`git revert` plus
    `./venv/bin/pip uninstall -y datastorekit`). Any §3 entry has the index's row, count and date
    in the same commit. The log follows README §5.1, with every §2 check's command and output, and
    K7's script whole.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 07* paragraph in §5.12: what
  each check found, and anything handed on to 08. Fix small residue in a follow-up commit of its
  own:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 07".
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the pin, and the freeze diff in one line;
  - the origin and the file comparison;
  - the provenance line every driver now prints;
  - K7: U3 and U6 hold on the installed package;
  - the suites;
  - anything opened.
- **Then 08** can be dispatched; its note is written after this review.
