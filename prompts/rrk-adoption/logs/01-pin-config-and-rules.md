# Log 01 — the pin, the config and the rules

**Subject:** Pin RunRegistryKit v0.2.0, configure it, ignore var/, and write the registry's rules
**Commit:** this commit, on branch `rrk-adoption`, on top of `6dd54a0`
**Date:** 2026-10-19, by Claude Sonnet 5.5
**Result:** **COMPLETE.** C1–C4 shipped. tools 79 → 84 (+5); CosmologyModels 18, ComputeTargets 176
(61 + 115) and ObjectFactories 69 unchanged. All three deliberate breakages caught. No §4 stop
condition was met. No issue opened.

## What shipped

- **C1.** `requirements.txt`: `runregistrykit[stores] @ git+https://github.com/ds283/RunRegistryKit@v0.2.0`
  after `rsa==4.9.1`. The venv was installed once, with `./venv/bin/pip install -r requirements.txt`
  (not editable; "Successfully installed runregistrykit-0.2.0", and nothing else).
- **C2.** `runregistrykit.toml` (three keys, two comment lines, no `[stores]`); `.gitignore` gains
  `var/` and a one-line comment.
- **C3.** `CLAUDE.md`: the bullet "Long-running jobs" removed from "Repository mechanics"; the
  section `## Long-running jobs — the run registry` appended at the end of the file, with seven
  rules, the `git clean -x`/`-X` prohibition, and "What stays".
- **C4.** `tools/tests/test_registry_config.py`, 5 tests, no Ray, no store, nothing written:
  `TestRegistryConfig.test_roots_and_distributions`, `TestRegistryConfig.test_config_keys_are_known`,
  `TestVarIsIgnored.test_runs_ignored`, `TestVarIsIgnored.test_datastores_ignored`,
  `TestPin.test_runregistrykit_is_the_tag`. 5 tests, 0.06 s.

## Deviations from the prompt

1. **`STRUCTURALLY REQUIRED`: one `git check-ignore -q` per path.** C4 item 3 gives one call with two
   paths; git 2.56.0 refuses `-q` with more than one path (exit 128). The orchestrator's note
   (§0, ruling 1) rules one call per path, each asserted to exit exactly 0.
2. **`STRUCTURALLY REQUIRED`: the config's keys are checked as "required ⊆ keys ⊆ known", not
   "exactly".** Prompt 02 adds a `[stores]` table and must not modify this test (README §5). The
   known four are a literal in the test, citing the README's "The client's root". Orchestrator's
   note, ruling 2.
3. **`IMPLEMENTATION CHOICE`: the pin test compares the whole provenance dict with a literal,
   including the full SHA `0bae3a3c…`.** No existing test asserts what the venv holds; the
   prompt's "form of `test_package_provenance.py`" is read this way. Orchestrator's note, ruling 3.
4. **`STRUCTURALLY REQUIRED`: seven rules, not six.** SecondaryGWKit's numbering inserts "the route
   after a hard kill" as rule 5, so durability is 6 and provenance 7. Orchestrator's note.
5. **`IMPLEMENTATION CHOICE`: the roots are compared as `Path(found.runs_root)`.** At `v0.2.0`
   `ClientRoot.runs_root` and `stores_root` are `str`, not `Path`; the values are the absolute paths
   the prompt names.
6. **`IMPLEMENTATION CHOICE`: SecondaryGWKit's rule 5 omits its `rehearsal_lock ray-processes` check,
   and rule 2 its `foreign`/earlier-boot detail** (the note's "does not carry over" list). The
   one sentence on an unreaped killed process stays, in rule 2.
7. **`IMPLEMENTATION CHOICE`: the `CLAUDE.md` test-command counts (`# 79 tests`) are not changed.**
   Prompt 05 updates them (README §2.2).

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the end of each run. Baselines are mine, before any edit,
at `6dd54a0`.

| Suite | Before | After |
|---|---|---|
| CosmologyModels | 18 OK, 77 s, load 10.4 | 18 OK, 67 s, load 10.5 |
| ComputeTargets `test_[a-f]*` | 61 OK, 116 s, load 5.3 | 61 OK, 96 s, load 5.3 |
| ComputeTargets `test_[g-z]*` | 115 OK, 128 s, load 13.1 | 115 OK, 101 s, load 2.4 |
| ObjectFactories | 69 OK, 6 s, load 11.5 | 69 OK, 5 s, load 10.0 |
| tools | 79 OK, 16 s, load 13.4 | **84 OK**, 13 s, load 9.4 |

(Load was 28 at the very start; the figures above are the readings after each run.)

**Gate** (before any edit): branch `rrk-adoption`; no tracked change; the 22 untracked entries the
user's; no `var/`; `ray_cluster.sh status` exit 0, lock free, no ChamPBH process, ports free.

**The install (C1).** `pip freeze` before (101 lines) and after (102), in the session scratchpad;
`diff before after`:

```
84a85
> runregistrykit @ git+https://github.com/ds283/RunRegistryKit@0bae3a3c501a8264ee4be5bc24f06422af38720f
```

`pip show runregistrykit`: `Name: runregistrykit`, `Version: 0.2.0`, `Requires:` and `Required-by:`
empty, location `…/ChamPBH/venv/lib/python3.13/site-packages`. `direct_url.json`:

```
{"url": "https://github.com/ds283/RunRegistryKit", "vcs_info": {"commit_id": "0bae3a3c501a8264ee4be5bc24f06422af38720f", "requested_revision": "v0.2.0", "vcs": "git"}}
```

The commit is the tag's; there is no `dir_info`, so the install is not editable. `datastorekit` and
`raypoolkit` lines are unchanged.

**`list`** (from the repository root):

```
$ ./venv/bin/python -m runregistrykit list
no runs under /Users/ds283/Documents/Code/ChamPBH/var/runs
$ echo $?
0
$ ls -d var
ls: cannot access 'var': No such file or directory
```

The package prints lower case; the prompt's "No runs under" is a paraphrase. `var/` did not appear,
and did not at any later step. No store was opened or named; no `runregistrykit store` command
was run; no Ray was started.

**Ignore rule.** `git check-ignore -q var/runs/x` and `git check-ignore -q var/datastores/x.db`
each exit 0.

**`black --check tools/tests/test_registry_config.py`:** unchanged (the file was formatted by
`black` once).

## The deliberate-breakage record

Each diff is replayable from a clean tree at this commit with `git apply` (checked with
`git apply --check` and `git apply -R --check`; run in `bash`). None is committed.

### B1 — `var/` removed from `.gitignore`

Fails `TestVarIsIgnored.test_runs_ignored` and `TestVarIsIgnored.test_datastores_ignored` (each
`git check-ignore -q` exits 1, not 0). 5 run, failures=2.

```diff
--- a/.gitignore
+++ b/.gitignore
@@ -4,4 +4,3 @@
 *.sqlite
 /CodeComparisonTools/XavEOS_data_out.csv
 # The registry's run directories and this repository's datastores (rrk-adoption README 6.0, R4)
-var/
```

### B2 — a misspelt key, `run_root`

Fails `TestRegistryConfig.test_roots_and_distributions` (ERROR: the package's `ConfigError` on the
unknown key) and `TestRegistryConfig.test_config_keys_are_known` (FAIL: `run_root` unknown, and
`runs_root` missing). 5 run, failures=1, errors=1.

```diff
--- a/runregistrykit.toml
+++ b/runregistrykit.toml
@@ -1,5 +1,5 @@
 # Configuration of RunRegistryKit (README, "The client's root") for the `rrk-adoption` campaign:
 # where runs and datastores are recorded, and which distributions every manifest records.
-runs_root = "var/runs"
+run_root = "var/runs"
 stores_root = "var/datastores"
 distributions = ["datastorekit", "raypoolkit"]
```

### B3 — `distributions` reduced to `["datastorekit"]`

Fails `TestRegistryConfig.test_roots_and_distributions` (the tuple is `("datastorekit",)`). 5 run,
failures=1.

```diff
--- a/runregistrykit.toml
+++ b/runregistrykit.toml
@@ -2,4 +2,4 @@
 # where runs and datastores are recorded, and which distributions every manifest records.
 runs_root = "var/runs"
 stores_root = "var/datastores"
-distributions = ["datastorekit", "raypoolkit"]
+distributions = ["datastorekit"]
```

The pin test (`TestPin`) was not exercised by a breakage: it would need a different install, which
the prompt does not ask for.

## Observations not acted on

- The pin test asserts the venv's contents, so it fails on a checkout whose venv was not
  reinstalled from `requirements.txt` (the production machine, README §7). That is its purpose.
- `CLAUDE.md`'s test-command comment `# 79 tests` for `tools/tests` is now 84; prompt 05 updates it.
- `ClientRoot.runs_root` and `stores_root` are `str` where RunRegistryKit's README says "paths".
  Not a defect: nothing here depends on it.

## State handed to the next prompt

- Branch `rrk-adoption`; the commit of this log is on top of `6dd54a0`. Tree clean of tracked
  changes; the 22 untracked entries are the user's; no `var/`.
- The venv holds `runregistrykit` 0.2.0 at `0bae3a3`, not editable. `runregistrykit.toml` has the
  three keys and **no `[stores]`**: until 02 adds `[stores] fingerprinter`, every store operation
  refuses for want of a fingerprinter.
- `var/` is ignored; `CLAUDE.md` carries the registry section, whose wrapper sentence is
  forward-looking (`RunDrivers/registered_run.py` does not exist until 03).
- `tools/tests/test_registry_config.py` accepts a `[stores]` table and must not be modified by 02.
- Suites: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69, tools 84.
