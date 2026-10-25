# Prompt 01 — move `ray` and `SQLAlchemy`, with token mode named

**Campaign:** [`README.md`](README.md) · **Board items:** **B1**, **P1**, **V0**, **D1**, **T1**,
**T2**, **T3**, **V1**, **C1** · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-25, with the plan, at `a144c83`. Its orchestrator note, written just before
dispatch, governs where it differs from this file.
**Where it runs:** on the laptop, in the main checkout, on branch `ray-sqla-align`. The checkout and
its venv are shared with `jordan-normalization`, which must be idle while 01 runs.
**Gate:**
- the planning commit has landed; `git status` is clean, with no untracked file; no commit after it
  touches anything outside `prompts/ray-sqla-align/`;
- `./venv/bin/python -m runregistrykit list` shows nothing `running` or stale;
- the venv holds `ray` 2.53.0 and `SQLAlchemy` 2.0.46, and `datastorekit` 0.4.0 at
  `c8570e849334d09085bbd84876095fc9e22bd005`, `raypoolkit` 0.3.0 at
  `67a75b97a656accb98195a4bda93f64e6a5f3fb2` and `runregistrykit` 0.2.0 at
  `0bae3a3c501a8264ee4be5bc24f06422af38720f`, none editable;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: the lock free, no ChamPBH
  process, the four ports free;
- `ls -l ~/.ray/auth_token`: it exists, 64 bytes, mode `-rw-------`;
- `RAY_AUTH_MODE` is not set in the shell's environment (`env | grep -c RAY_AUTH_MODE` is 0).

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. A dependency move whose failure is an authentication refusal at
the first `ray.init`, measured twice on the real cluster.

**Read first:**

1. `CLAUDE.md`: "The packages", "Tests", "Format with `black`", "Ray on this machine", "The
   rehearsal lock", and "Long-running jobs" rule 4.
2. [`README.md`](README.md) whole: §0.2 and §0.3 are why this prompt exists; §5 is the rules.
3. **StochasticInstantons** (`/Users/ds283/Documents/Code/StochasticInstantons`, read only):
   `config/ray_auth.py`; `tests/test_ray_auth.py`; `git show 3a24ea5 --
   .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`; `git show c5af555` (the equality
   test); and the results note
   `.documents/ray-sqla-align/01-move-ray-and-sqlalchemy-with-token-mode.md`, its "V0", "Deviations"
   (corrections 3, 4, 11, 12) and "The deliberate-breakage record". This prompt follows them; where
   ChamPBH's tree differs, the tree governs.
4. **SecondaryGWKit** (read only): `prompts/py313-venv/IMPLEMENTATION_STATE.md` §4, the two token
   issues.
5. `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` ("the script") and its `--help`;
   `tools/tests/test_registry_config.py`; `ObjectFactories/tests/test_package_provenance.py`
   `TestInstalledPackageProvenance.test_default_names` (`:270-277`); `utilities.py:171-177`.

---

## 1. What is wanted

**Move the two pins; show that ChamPBH's cluster then refuses its drivers; name token mode so that
it does not; record the four distributions; and show on the real cluster that the fix works.** In
one commit: the pins, the module, its three importers, the script, the tests, `CLAUDE.md`, the
outputs, the log and the board.

The venv is shared, so the install takes effect at once. Take the baselines (§2.1) **before** it.

Everything saved while working goes under `var/ray-sqla-align-01/` (ignored by git; README P10),
each block headed by its command. Small text outputs are copied to
`.documents/ray-sqla-align/out/01-*.txt` at §2.11.

**Each lock or script command is a shell command of its own**, its output redirected to a file
(`> F 2>&1`), never piped, with `echo $?` after it and the file read in a later call. **No shell
command that runs the script or a lock command names `/tmp/ray-cpbh`**, and no `PURPOSE` does.
**No `ray stop`.** **The token is never printed**: `ls -l` only, and the final `grep -F -l -f`.

## 2. What to do

**2.1 The baselines, before.** Into `var/ray-sqla-align-01/`:
- `01-pip.txt`: `./venv/bin/pip freeze`; then `utilities.installed_package_provenance()` for the
  seven names `datastorekit raypoolkit runregistrykit ray SQLAlchemy numpy scipy`, printed as JSON
  (`PYTHONPATH=.`);
- `01-roots.txt`: the five roots by `CLAUDE.md`'s commands (`ComputeTargets` in its two halves),
  serially, in the foreground, never with a cluster up. README §0.5 has the expected counts;
- `01-token.txt`: `ls -l ~/.ray/auth_token` and `stat -f '%m %Sm' ~/.ray/auth_token` (its mtime);
- `uptime`, before the roots and before each `start`.

**2.2 The pins.** `requirements.txt:79` `ray==2.59.0` and `:92` `SQLAlchemy==2.0.55`; no other line.
Then:
- `./venv/bin/pip install --dry-run -r requirements.txt`: it must say `Would install
  SQLAlchemy-2.0.55 ray-2.59.0` and nothing else;
- `./venv/bin/pip install -r requirements.txt` (pip not upgraded);
- `pip freeze` again, and its `diff` against 2.1's: exactly the `ray` and `SQLAlchemy` lines;
  `./venv/bin/pip check` clean; the three git packages at the Gate's commits, not editable.

All appended to `01-pip.txt`.

**2.3 The roots on the pins**, before any code change, into `01-roots.txt`: every count as at 2.1.
Run each root once more with `-W default` and record any warning that 2.1's runs did not print
(README §5.1, "What is new"). A failure under 2.59 that 2.1 did not have is a stop; a known
wall-clock test is re-run alone before it is called real (`test_prym_passenger`'s is flaky under
load).

**2.4 V0, the refusal, on the pins alone.** Into `01-v0.txt`, each step its own shell command:
1. `uptime`; `env | grep -c RAY_AUTH_MODE` (0).
2. `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "ray-sqla-align 01 (V0)" >
   var/ray-sqla-align-01/v0-start.txt 2>&1`; `echo $?`. **Exit 3: stop and ask**; never wait,
   retry or remove the lock. Expected: exit 0, and Ray's `Token authentication is enabled for this
   Ray cluster` warning in the output.
3. `timeout 300 ./venv/bin/python -c 'import ray; ray.init(address="127.0.0.1:6380");
   print(ray.cluster_resources()); ray.shutdown()' > var/ray-sqla-align-01/v0-driver.txt 2>&1`;
   `echo $?`. Expected: refused, `InvalidAuthToken` lines, then `AuthenticationError … RAY_AUTH_MODE
   is currently 'disabled'`, exit 1, nothing printed by `cluster_resources()`. Record the exit, the
   time it took and `grep -c InvalidAuthToken`.
4. `ray_cluster.sh stop > var/ray-sqla-align-01/v0-stop.txt 2>&1`; `echo $?`. Then `status` the same
   way: the lock free, no process, the ports free.

**If the driver connects**, §0.2 does not hold for ChamPBH: stop the cluster, then stop and ask. Do
not go on to the fix.

**2.5 The distributions (README §0.3, U3).** In this order:
1. `tools/tests/test_registry_config.py`: `test_roots_and_distributions` asserts
   `found.distributions == utilities.RECORDED_DISTRIBUTIONS` (both as tuples), in place of the
   literal at `:64`; its docstring says the two lists are held equal so that a manifest and a
   driver's `>> packages:` line name the same distributions. Run the module: it passes.
2. **B5**: add `"ray"` to `runregistrykit.toml`'s `distributions` alone; run the module; the test
   must fail. Record the diff and the failure; revert.
3. Add `"ray", "SQLAlchemy", "numpy", "scipy"`, in that order, after `"datastorekit",
   "raypoolkit"`, to **both** `runregistrykit.toml`'s `distributions` and
   `utilities.RECORDED_DISTRIBUTIONS`. The toml's comment says the last four are there so that a
   manifest says which venv wrote a store; `utilities.py`'s comment says the same of the driver's
   line, naming this campaign and U3.
4. `ObjectFactories/tests/test_package_provenance.py`, `test_default_names`: the six names, in that
   order, and its docstring says why.
5. Run `PYTHONPATH=. ./venv/bin/python -c 'import utilities;
   print(utilities.format_package_provenance(utilities.installed_package_provenance()))'`, into
   `01-distributions.txt`: six entries, `ray 2.59.0` and `SQLAlchemy 2.0.55` among them, the four
   PyPI ones `(no recorded origin)`.

**2.6 Token mode named.**
- **T1 — `config/ray_auth.py`** (P3), with the licence header `config/`'s modules carry (as
  `config/version.py`'s). SI's module: `RAY_AUTH_MODE = "token"`; if `"RAY_AUTH_MODE"` is not in `os.environ`,
  set it, and if `"ray._raylet"` is in `sys.modules`, call
  `sys.modules["ray._raylet"].Config.initialize("")`. It imports nothing from Ray. Its comment, in
  ChamPBH's words: the three routes of README §0.2; that ChamPBH's cluster comes up in token mode
  while a driver attaching by address does not, measured by V0; that the three drivers import it
  and `ray_cluster.sh` names the mode for the cluster, guarded by `tools/tests/test_ray_auth.py`;
  why the refresh is needed (the wrapper and the drivers import `ray` first; SGK's
  `[02-the-token-mode-setting-comes-after-import-ray-so-every-ray-start-fails]`); that a value
  already in the environment is left alone; and that the token is `~/.ray/auth_token`, shared by
  every project of this user, kept at mode 600, never printed or copied.
- **T1 — the importers** (P4). `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` each gain,
  straight after their top-level `import ray` and before the first import of the repository's own
  modules:

  ```python
  # names Ray's token mode before ray.init (config/ray_auth.py)
  import config.ray_auth  # noqa: F401
  ```

  Nothing else in them changes. Check that the tests which read these files with `ast`
  (`test_ray_address`, `test_package_provenance`, `test_datastore_config`,
  `test_version_keyed_lookups`, `test_plot_inputs`, `test_network_flag`) still pass.
- **T2 — the script** (P5). In `cmd_start`, before `local t0=$SECONDS` and the `"$RAY_BIN" start
  --head` line:

  ```bash
      # Ray's authentication mode, named (ray-sqla-align 01). Since Ray 2.59, `ray start` infers
      # token mode from the token file (~/.ray/auth_token) unless told; ChamPBH names it, as
      # config/ray_auth.py does for every driver, so that the cluster and its drivers agree.
      export RAY_AUTH_MODE="${RAY_AUTH_MODE:-token}"
  ```

  The `actors:` line prints `RAY_AUTH_MODE=token $RAY_BIN list actors --address $CPBH_ADDRESS`, and
  the usage text gains: "The cluster runs in Ray's token mode (RAY_AUTH_MODE=token, set by start
  unless the environment holds a value), so a Ray CLI command against it needs RAY_AUTH_MODE=token
  in its environment." Nothing of the lock half, the stop, the ports or `ray start`'s arguments
  changes. `bash -n` passes. `tools/tests/test_ray_cluster_lock.py` passes unchanged.
- **T3 — `tools/tests/test_ray_auth.py`** (P6), `unittest`, a module docstring saying what it pins
  and why, after SI's `tests/test_ray_auth.py`. No Ray started, nothing written, the real `HOME`
  untouched. Each probe is a fresh interpreter (`sys.executable`, `cwd` the repository root,
  `PYTHONPATH` it), its environment `os.environ` less `RAY_AUTH_MODE`, plus what the test sets:
  - `test_an_unset_mode_becomes_token`; `test_a_mode_already_set_is_left_as_it_is` (`disabled`
    preset);
  - `test_ray_imported_first` (the wrapper's and the drivers' order), `test_the_module_imported_first`,
    `test_a_mode_already_set_is_what_ray_sees`: `ray._raylet.get_authentication_mode() ==
    AuthenticationMode.TOKEN` after the two imports; true, true, false;
  - one test per driver, as subtests or three methods, naming the file: by `ast`, a module-level
    `import config.ray_auth` before the top-level statement holding the driver's `ray.init`;
  - `test_ray_cluster_sh_exports_the_mode_before_ray_start`: by text, the `export` line before the
    one `"$RAY_BIN" start` line.

  Nine checks. Run it standalone and record its time.

**2.7 The deliberate breakages.** Apply each to the fix tree, run `tools/tests/test_ray_auth.py`
(B5: `test_registry_config.py`, already done at 2.5), record which tests failed and the diff exactly
as applied, and restore the file (`cmp` against a saved copy):
- **B1** — the refresh deleted from `config/ray_auth.py`. Expected: `test_ray_imported_first`.
- **B2** — `if "RAY_AUTH_MODE" not in os.environ:` → `if True:`. Expected: the two `disabled`
  tests.
- **B3** — `plot_by_beta.py`'s comment and import deleted. Expected: its driver test, naming the
  file.
- **B4** — the script's `export` moved after `"$RAY_BIN" start` (after `local rc=$?`; `bash -n`
  still 0). Expected: the script test.

**2.8 The roots after**, serially, no cluster up, into `01-roots.txt`: CosmologyModels, ComputeTargets
and ObjectFactories at 2.1's counts; RunDrivers at 2.1's; tools at 2.1's plus T3's. `black` on every
`.py` file changed (no other file).

**2.9 V1, the fix on the real cluster.** On the fix tree, before the commit. Into `01-v1.txt`, each
step its own shell command:
1. `uptime`; `env | grep -c RAY_AUTH_MODE` (0, so the script's default is what names the mode).
2. `ray_cluster.sh start "ray-sqla-align 01 (V1)"` to `var/ray-sqla-align-01/v1-start.txt`; exit 0;
   **no** token warning in it (`grep -c 'Token authentication'` 0). Its `actors:` line carries the
   variable.
3. The environment of the `gcs_server` (the pid `start` prints) and of the `raylet` (from the
   script's `status` listing): `ps -E -ww -o command= -p <pid> | grep -o 'RAY_AUTH_MODE=[a-z]*'`
   gives `RAY_AUTH_MODE=token` for each; `ps -E -ww -o command= -p <pid> | grep -c 'PATH='` gives 1,
   as the control that `-E` shows an environment at all. Never print the environment whole.
4. The driver, in the wrapper's import order: `timeout 300 env PYTHONPATH=. ./venv/bin/python -c
   'import ray; import config.ray_auth; ray.init(address="127.0.0.1:6380");
   print(ray.get(ray.remote(lambda: 41 + 1).remote())); ray.shutdown()' >
   var/ray-sqla-align-01/v1-driver.txt 2>&1`: exit 0, `42` printed; `grep -c` 0 for
   `InvalidAuthToken`, `AuthenticationError` and `Token authentication`.
5. `RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380 >
   var/ray-sqla-align-01/v1-status.txt 2>&1`: exit 0, with the cluster's resources. Record what
   it says of CPUs and GPUs.
6. `stop`, then `status`: exit 0; the lock free, no process, the ports free.
7. `ls -l` and `stat` of the token file: 64 bytes, mode 600, the mtime of 2.1.

**2.10 `CLAUDE.md`** (C1). After the bullet "The rehearsal lock", a new bullet, "Ray's token
mode": every Ray start runs in Ray's token mode, named by `config/ray_auth.py` (imported by the three
drivers) and by `ray_cluster.sh` for the cluster, and guarded by `tools/tests/test_ray_auth.py`; a
Ray CLI command against the cluster needs `RAY_AUTH_MODE=token`; the token is `~/.ray/auth_token`,
shared with SecondaryGWKit and StochasticInstantons, kept at mode 600 and never printed or copied;
and why (Ray 2.59 starts `ray start --head` in token mode when that file exists, and never a
driver attaching by address). A new bullet, not an edit, so that the merge into
`jordan-normalization` stays simple (README §4.2). The `tools/tests` count line moves to §2.8's
count; the counts' date note says so.

**2.11 The records.** Copy the small text outputs of `var/ray-sqla-align-01/` to
`.documents/ray-sqla-align/out/01-*.txt`, with the breakage diffs as `01-B<n>.diff`. Then
`grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/ prompts/ray-sqla-align/` must print
nothing (exit 1). Write the log and the board (§6).

## 3. Acceptance

1. **The pins.** The dry run installs `SQLAlchemy-2.0.55` and `ray-2.59.0` alone; the freeze moves
   by those two lines alone; `pip check` clean; the git packages unmoved, none editable.
2. **V0** as §2.4 expects: the warning at `start`, the driver refused, the cluster stopped and the
   lock free after.
3. **The distributions.** The two lists are equal and name the six; the equality test catches B5;
   the driver's line names `ray` 2.59.0 and `SQLAlchemy` 2.0.55.
4. **The fix.** The module, the three importers and the script as §2.6 says; `bash -n` 0;
   `tools/tests/test_ray_auth.py` passes, needs no Ray, and catches B1–B4.
5. **The roots** at §2.8's counts, all OK; `black` clean on the changed `.py` files.
6. **V1** as §2.9 expects: no warning at `start`; `RAY_AUTH_MODE=token` in the `gcs_server`'s and
   the `raylet`'s environments; the driver attached and ran its task; `ray status` answered; the
   token file unchanged; the cluster stopped and the lock free.
7. `git diff --cached --name-only` is: `requirements.txt`, `runregistrykit.toml`, `utilities.py`,
   `config/ray_auth.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, the script,
   `tools/tests/test_ray_auth.py`, `tools/tests/test_registry_config.py`,
   `ObjectFactories/tests/test_package_provenance.py`, `CLAUDE.md`,
   `.documents/ray-sqla-align/out/01-*`, the log and the board. Nothing else.

## 4. Stop conditions — stop and ask

- A Gate item is not as stated.
- The dry run would install, upgrade or remove anything but `ray` and `SQLAlchemy`; or the install
  moves anything else.
- A root fails on the pins or after the fix, beyond a known wall-clock test that passes alone; or a
  count differs from §2.8's expectation.
- `start` exits 3 (the lock held) or 4 (a stale cluster), at V0 or V1.
- At V0, the driver connects. At V1, `start` prints the warning; a process of the cluster lacks
  `RAY_AUTH_MODE=token`; the driver is refused or fails; `ray status` is refused.
- `stop` fails, leaves the lock held, or leaves a process of `/tmp/ray-cpbh`.
- The token file is missing, changes size, mode or mtime, or appears in any record.
- A breakage is not caught by the test the prompt names.
- Anything suggests a defect in `ray`, `datastorekit`, `raypoolkit` or `runregistrykit`: record it
  on the board and in `.documents/OPEN_ISSUES.md`, and stop. It is not worked around here.

**If you stop with the cluster up or the lock held**, say so and leave them, with `status`'s output:
the orchestrator decides. If you stop after the install, say that the venv holds the new pins, and
give README §4.1's line to go back.

## 5. What this prompt does not do

- It touches no other repository, and no file but §3.7's list.
- It changes nothing a build computes or stores, builds no store and runs no driver of
  `main.py`'s or the plot scripts'.
- It changes no record: nothing under `prompts/` but this campaign's, `science-runs/`, or
  `.documents/` but the script and `.documents/ray-sqla-align/out/`.
- It does not rehearse (02), merge, or push.

## 6. The log and the board

**The log**, at `logs/01-move-ray-and-sqlalchemy-with-token-mode.md`, in README §5.1's form, with
every count against 2.1's, V0's and V1's transcripts summarised (the lines that matter quoted), the
breakage record (B1–B5), and "What is new in the logs" from 2.3's `-W default` runs and V0/V1.

**The board:** the header ("01 landed, awaiting review"); the §1 row; items B1, P1, V0, D1, T1–T3,
V1 and C1; gate V, and gate Token's V1 part; `### 5.1 After prompt 01 (<date>)`.
`.documents/OPEN_ISSUES.md` only if §3 gains an entry.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`. Nothing pushed.
