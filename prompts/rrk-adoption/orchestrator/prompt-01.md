# Orchestrator — prompt 01, the pin, the config and the rules

Read [`../README.md`](../README.md) first: §0.1, §0.3, §2.2 (01's charter), §5 and §6 (R1–R7, P9,
P11). This is the campaign's first note. Its conventions are those of `dk-and-rpk-migration`'s notes
([`../../dk-and-rpk-migration/orchestrator/prompt-01.md`](../../dk-and-rpk-migration/orchestrator/prompt-01.md)
§0, "Conventions"), restated here where 01 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-pin-config-and-rules.md`](../01-pin-config-and-rules.md)
**Board items:** C1–C4 · **Gate:** "Pin" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **Network:** the install's fetch of the tag
from GitHub, and nothing else.

**Gate:**
- The prompts are written (`db5bfcc`). `HEAD` is the commit that adds this note, or a later one that
  touches no code. ("Code" is anything outside `prompts/`. Since `3cd2be4` nothing outside
  `prompts/` has changed.)
- The prompt's own gate (its header): no tracked change, no `var/`, no ChamPBH Ray process.
- One prompt at a time in this checkout. 02 and 03 are not dispatched until 01 is reviewed.

## 0. What makes this prompt unusual

**It changes the venv.** This is the first prompt of the campaign that does, and the venv is shared
by every later prompt and by the user. The install must add exactly one distribution, at the tag's
commit, not editable. The prompt's §4 makes anything else a stop. Record `pip freeze` before and
after into the agent's scratch directory, and put the `diff` of the two in the log.

**Three statements in the prompt are wrong or conflict with another prompt. Ruled here:**

1. **`git check-ignore -q` with two paths is fatal.** C4 item 3 gives `git check-ignore -q
   var/runs/x var/datastores/x.db`. With git 2.56.0 this prints `fatal: --quiet is only valid with a
   single pathname` and exits **128**, whatever `.gitignore` says (the orchestrator's probe in a
   scratch repository). Without `-q`, the exit is 0 if *any* path is ignored, so it cannot check
   both either. **Ruled:** one `git check-ignore -q <path>` per path, run in the repository root
   by `subprocess`, each asserted to exit **exactly 0** (not "truthy", not "non-zero"). With `var/`
   removed from `.gitignore` each exits 1. Classify as `STRUCTURALLY REQUIRED`, citing this note.
2. **"Exactly the keys above" (C4 item 2) would break when 02 lands.** 02 adds a `[stores]` table
   to `runregistrykit.toml` (its F2) and does not touch `tools/tests/test_registry_config.py`. The
   campaign's rule is that existing tests are not modified (README §5). **Ruled:** the test reads
   the file with `tomllib` and asserts that its top-level keys
   - **include** `runs_root`, `stores_root` and `distributions`; and
   - **are a subset** of the four keys the package knows: `runs_root`, `stores_root`,
     `distributions`, `stores` (RunRegistryKit's `README.md`, "The client's root";
     `runregistrykit/client.py`'s `_read_config`).

   Write the four as a literal in the test, with a comment naming the README section. Do not import
   the package's private `_DEFAULTS`. A misspelt key (`run_root`) then fails this test, and fails
   item 1 too, as the package's `ConfigError`. Classify as `STRUCTURALLY REQUIRED`, citing this note.
3. **No existing test pins what the venv holds.** C4 item 4 says "in the form of
   `ObjectFactories/tests/test_package_provenance.py`'s for the other two packages". That module
   checks the provenance helper on stand-in distributions, and its docstring says "No test asserts
   what the venv itself holds". So item 4 is **the first test in this repository that asserts the
   venv's contents**. Its "form" is the five-field dict, compared whole with `assertEqual` against
   a literal:
   ```python
   {"version": "0.2.0", "url": "https://github.com/ds283/RunRegistryKit",
    "commit": "0bae3a3c501a8264ee4be5bc24f06422af38720f", "requested_revision": "v0.2.0",
    "editable": False}
   ```
   The commit is the full SHA of the tag's commit, since the prompt's §4 makes any other commit a
   stop. The `url` form is what pip writes for this repository's other two VCS pins
   (`datastorekit`'s `direct_url.json` has `"url": "https://github.com/ds283/DatastoreKit"`). The
   module's docstring says this is the pin's check: it fails on a venv not reinstalled from
   `requirements.txt` (the production machine, README §7), and a later pin moves the literal with
   the line. Classify the reading as an `IMPLEMENTATION CHOICE`, citing this note.

**The rules in `CLAUDE.md` (C3 item 3) are seven, not six.** RunRegistryKit's README has six rules:
discovery, liveness, launch, do not babysit, durability, provenance. SecondaryGWKit's section at
`031bcb4` numbers **seven**: it inserts "The route after a hard kill" as rule 5, so that
durability is 6 and provenance 7. The prompt asks for "SecondaryGWKit's numbering" and lists the
route as a rule of its own. **Ruled:** seven numbered rules in SecondaryGWKit's order, each one or two
sentences. Classify as `STRUCTURALLY REQUIRED`, citing this note.

**What of SecondaryGWKit's section does not carry over** (C3 says adapt, never copy its paths or
history):
- its paragraph on `RunDrivers/` and the old `docs/` paths, and its citation of
  `prompts/run-registry/README.md` §0. Cite RunRegistryKit's `README.md` ("The rules" and "Ending
  a run whose end was never recorded") instead;
- in rule 5, the check with `tools.rehearsal_lock ray-processes` and the left-behind GCS server
  (`[03-a-sigkilled-drivers-gcs-server-deadlocks-in-shutdown]`). That belongs to a driver that owns
  its cluster, and ChamPBH's owns none (README §0.2), which 04 measures. Leave it out; 05 may add
  what 04 finds;
- the `foreign`/earlier-boot detail of rule 2 may be cut to "judged on the run's own host".
  The one sentence on a killed process its parent has not reaped
  (`[04-a-killed-process-not-yet-reaped-answers-kill-0]`, and that a launch through `sh` is reaped
  by it) may stay, as one sentence. It is true of ChamPBH's launches.

**The wrapper's sentence (C3 item 2) must be true at this commit.** `RunDrivers/` does not exist.
Write it as "launched through `RunDrivers/registered_run.py`, which `rrk-adoption` prompt 03 adds",
and say that until then no run is registered. Do not create `RunDrivers/`.

**Where the section goes.** "Repository mechanics" (`CLAUDE.md:56`) is the last section of the file
(127 lines). So the new section `## Long-running jobs — the run registry` is appended at the end,
and the bullet at `:115-117` is deleted from "Repository mechanics". The old bullet's "made on
another machine" stays true (README §7, "The production machine"); keep it in item 5. The tools
count in the test commands (`# 79 tests`) is **not** changed here; 05 updates the counts (README
§2.2).

**What moves on purpose:**
- `requirements.txt`: one line, at `:85` (after `rsa==4.9.1`, before `scipy==1.17.0`). 101 lines
  become 102.
- `runregistrykit.toml` (new), with its two-line comment and the three keys; no `[stores]`.
- `.gitignore`: `var/` and its one-line comment (5 lines become 7).
- `CLAUDE.md`: the bullet out, the section in. Nothing else.
- New: `tools/tests/test_registry_config.py`.
- The log (`logs/` exists and is empty) and the board.
- No issue is expected. The index (`.documents/OPEN_ISSUES.md`, 27 open) is untouched unless one
  opens.

**The facts, checked by the orchestrator on 2026-10-19 at `db5bfcc`.** Pass them on.
- **The tree.** Branch `rrk-adoption`, `HEAD` `db5bfcc` before this note. Nothing staged. The
  untracked set is the prompt's gate list: 22 entries (17 `pilot-2026.6.0*.db`, `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`). No
  `var/`. This is the main checkout (`git rev-parse --git-common-dir` is `.git`), so the record
  root and the found root are both the repository root.
- **Ray.** `ray_cluster.sh status` exits 0: lock free, no ChamPBH process, ports 6380 / 6381 /
  8267 / 52367 free. `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. `RAY_ADDRESS` is unset.
- **RunRegistryKit.** `v0.2.0^{}` is `0bae3a3c501a8264ee4be5bc24f06422af38720f`. Its checkout's
  `HEAD` is `33a2cd2`, and `runregistrykit/` and `pyproject.toml` are unchanged between the two.
  Read it with `git -C … show v0.2.0:<path>`; never edit it.
- **The venv.** pip 26.2.1, Python 3.13.16, black 26.1.0. `pip freeze` has 101 lines; `datastorekit`
  is `@ …DatastoreKit@c8570e8…`, `raypoolkit` `@ …RayPoolKit@eaef0ce…`. No `runregistrykit`.
  `pip install --dry-run "runregistrykit[stores] @ git+https://github.com/ds283/RunRegistryKit@v0.2.0"
  -r requirements.txt` ends `Would install runregistrykit-0.2.0`, and nothing else. After the
  install, expect one new freeze line,
  `runregistrykit @ git+https://github.com/ds283/RunRegistryKit@0bae3a3c501a8264ee4be5bc24f06422af38720f`.
- **The package's API at `v0.2.0`** (`runregistrykit/client.py`, `__init__.py`):
  - `find_client_root(start=None, client_root=None) -> ClientRoot`. The fields are `root`,
    `found`, `config_file`, `runs_root` and `stores_root` (absolute), `distributions` (a
    **tuple**) and `stores` (a dict).
  - An unknown top-level key is a `ConfigError` naming the key and listing the four known ones.
  - `package_provenance(names)` maps each name to `None` or to a dict of exactly `version`, `url`,
    `commit`, `requested_revision`, `editable`.
  - `python -m runregistrykit list` on a runs root that does not exist prints `no runs under
    <runs root>` (lower case, `__main__.py:631`) and exits 0. The prompt's capitalised "No runs
    under …" is a paraphrase. Paste the real line.
- **The baselines** at `db5bfcc`, run by the orchestrator: see §1. The agent runs its own before
  any edit (the prompt's §2.1), and its own governs.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two
  halves, output to a file in the agent's scratch directory, the verdict grepped from it, `uptime`
  beside each. A failure is re-run alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under `tools/`, `config/` or `.documents/`.
- **The venv.** The agent runs exactly one install, `./venv/bin/pip install -r requirements.txt`,
  after the line is added. No `-e`, no `--upgrade`, no other `pip install` or `uninstall`. A failed
  or partial install is a stop.
- **Stores.** None is opened or named, the pilot store included. No `runregistrykit store`
  command is run.
- **Ray.** The agent starts no Ray and runs no driver.
- **`var/`.** It must not exist at the commit, or after the `list` check. Check with `ls -d var`
  after each step that could create it. Nothing creates it on purpose.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline db5bfcc..HEAD` touches no code.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`.
3. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.
4. **`pip freeze`** into the orchestrator's scratchpad, for check 3.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 02–05, except that it may read
  02's F2 to see the `[stores]` table that §0's ruling 2 allows for.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/01-pin-config-and-rules.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **§0's three rulings and the seven rules** bind it, each classified as §0 says.
- **One install, from the file, not editable.** The freeze diff is the one line or it stops.
- **It starts no Ray, opens no store and creates no `var/`.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is a stop,
  never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the freeze diff, `pip show runregistrykit` and the `direct_url.json`;
  - the `list` line and its exit;
  - the new tests by name, with counts and wall time;
  - each breakage with the tests it failed and how;
  - the four suites;
  - the new `CLAUDE.md` section, verbatim;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists the prompt's §5 files only. `git status`
   shows §0's untracked set and nothing new. No `var/`.
2. **C1, the line.** `git diff -U0 <agent>~1 <agent> -- requirements.txt` is the one added line,
   at `:85`, spelt as the prompt gives it.
3. **C1, the venv.** `pip freeze` now against the orchestrator's from §1: one added line,
   `runregistrykit @ …@0bae3a3c501a8264ee4be5bc24f06422af38720f`. `pip show runregistrykit`
   (0.2.0) and its `direct_url.json` (the commit, `requested_revision` `v0.2.0`, no `dir_info`).
   `./venv/bin/python -c 'import runregistrykit; print(runregistrykit.__file__)'` resolves under
   `venv/lib/…/site-packages`, not under the RunRegistryKit checkout.
4. **C2.** `runregistrykit.toml` read whole: the comment, the three keys, no `[stores]`.
   `.gitignore`: `var/` and its comment, nothing else. `git check-ignore -v` on `var/runs/x` and
   on `var/datastores/x.db` names the new rule.
5. **The package, by probe** from the scratchpad: `find_client_root(client_root=<root>)` as the
   test asserts; `python -m runregistrykit list` from the root prints the `no runs under` line and
   exits 0; `ls -d var` fails afterwards.
6. **C3, the prose.** `git diff <agent>~1 <agent> -- CLAUDE.md`: the bullet at `:115-117` gone, the
   section appended, nothing else moved. Read the section against RunRegistryKit's README and
   SecondaryGWKit's at `031bcb4`:
   - items 1–5 of C3 all present;
   - seven rules in SecondaryGWKit's order;
   - the `list` and `end` commands as `./venv/bin/python -m runregistrykit …`;
   - `git clean -x`/`-X` forbidden;
   - the wrapper sentence true at this commit;
   - no SecondaryGWKit path or campaign history, and no GCS-server paragraph;
   - no longer than SecondaryGWKit's section.
7. **C4, by reading.** Four tests as C4 and §0's rulings say: per-path `check-ignore` asserting
   exit 0 exactly; the subset-and-include key check; the provenance dict compared whole. No Ray, no
   store, nothing written outside `tempfile`. The docstring says what it pins and why, including
   that it asserts the venv.
8. **The breakages.** In `bash`, replay each diff with `git apply`, run the new module, see it fail
   as the log says, revert with `git apply -R`, `git status` clean. Also, as the orchestrator's own
   probe: a `[stores]` table with `fingerprinter = "config.fingerprint:fingerprinter"` added to
   the config passes check 2 of the module (02 will add exactly that). Revert.
9. **The records.** The board: row 01 ✅ with "this commit" and the log; C1–C4 ✅; the gate "Pin"
   ✅; `### 5.2 After prompt 01 (<date>)`. §3 and the index untouched, or, if one opened, the index
   row, count and date in the same commit. The log in §5.1's form, every deviation classified, the
   freeze diff, the breakage diffs.
10. **The suites** at the agent's commit, serially: 18, 61 + 115, 69, 79 + the new tests.
    `black --check` on the new module.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 01* paragraph in §5.2. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - a list of the orchestrator notes on the board, with this one marked "used for 01";
  - the status line: 1 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the freeze diff;
  - the new `CLAUDE.md` section, and §0's four rulings (the per-path `check-ignore`, the key check
    that survives 02, the venv check, seven rules), each of which reads the prompt otherwise than
    it was written;
  - the four roots' counts;
  - that the production machine's venv must be reinstalled from `requirements.txt` before its
    tools suite passes (README §7).
- **Then 02 or 03.** Each needs only 01 (README §2.1). Their notes are written one at a time, after
  this review.
