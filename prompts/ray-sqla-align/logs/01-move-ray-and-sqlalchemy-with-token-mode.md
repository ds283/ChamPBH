# Log 01 — move `ray` and `SQLAlchemy`, with token mode named

**Subject:** Move ray to 2.59.0 and SQLAlchemy to 2.0.55, naming Ray's token mode
**Commit:** `668d30f`, on branch `ray-sqla-align`, on top of the dispatch commit `551b54c`
**Date:** 2026-10-25, by Claude Opus 5.5
**Result:** **COMPLETE.** B1, P1, V0, D1, T1, T2, T3, V1 and C1 shipped; gate **V** holds, and
gate **Token**'s V1 part holds. The venv holds `ray` 2.59.0 and `SQLAlchemy` 2.0.55, and nothing
else in it moved. On the pins alone ChamPBH's cluster came up with Ray's token warning and refused
a driver attaching by address (exit 1, 21 `InvalidAuthToken` lines, `AuthenticationError`). With
the fix, `start` printed no warning, the `gcs_server` and the `raylet` ran with
`RAY_AUTH_MODE=token`, and a driver importing `ray` first attached and printed `42`. The roots were
18, 61 + 115, 78, 93 and 61 before, and are 18, 61 + 115, 78, **102** and 61 after, all OK. T3
catches B1–B4, and the equality test catches B5. No §4 stop condition was met. No issue opened or
closed, so `.documents/OPEN_ISSUES.md` is unchanged.

The orchestrator's note for 01 (its §0, thirteen corrections and rulings) governed where it
differs from the prompt. Each is classified below as the note says. Outputs are in
[`.documents/ray-sqla-align/out/`](../../../.documents/ray-sqla-align/out/), written while working
to `var/ray-sqla-align-01/` (ignored) and copied at §2.11.

## What shipped

- **P1 — the pins.** `requirements.txt:79` `ray==2.59.0` and `:92` `SQLAlchemy==2.0.55`; no other
  line. The dry run ended `Would install SQLAlchemy-2.0.55 ray-2.59.0`. The install uninstalled
  `SQLAlchemy` 2.0.46 and `ray` 2.53.0 and ended `Successfully installed SQLAlchemy-2.0.55
  ray-2.59.0`. pip was not upgraded. [`out/01-pip.txt`](../../../.documents/ray-sqla-align/out/01-pip.txt).
- **D1 — the distributions.**
  - `tools/tests/test_registry_config.py`: `test_roots_and_distributions` asserts
    `tuple(found.distributions) == tuple(utilities.RECORDED_DISTRIBUTIONS)` in place of the literal.
    Its docstring, and item 1 of the module docstring, say the two lists are held equal so that a
    manifest and a driver's `>> packages:` line name the same distributions. The module imports
    `utilities`.
  - `runregistrykit.toml` `distributions` and `utilities.RECORDED_DISTRIBUTIONS` are both
    `datastorekit, raypoolkit, ray, SQLAlchemy, numpy, scipy`. The toml's comment says the last
    four are there so that a manifest says which venv wrote a store. `utilities.py`'s comment says
    the same of the driver's line, naming this campaign and U3. Both comments name the test that
    holds the lists equal.
  - `ObjectFactories/tests/test_package_provenance.py` `test_default_names`: the six names in that
    order, for both `RECORDED_DISTRIBUTIONS` and `installed_package_provenance()`'s keys. Its
    docstring says which campaign added which names, and why.
- **T1 — `config/ray_auth.py`** (new), with `config/`'s licence header. `RAY_AUTH_MODE = "token"`;
  if `"RAY_AUTH_MODE"` is not in `os.environ` it is set, and if `"ray._raylet"` is in `sys.modules`,
  `sys.modules["ray._raylet"].Config.initialize("")` is called. It imports only `os` and `sys`.
  Its comment covers what the prompt lists: the three routes of README §0.2; that ChamPBH's cluster
  comes up in token mode and a driver attaching by address does not (V0); the three importers, the
  script and the guarding test; why the refresh is needed (the wrapper and the drivers import `ray`
  first; SGK's `[02-the-token-mode-setting-comes-after-import-ray-so-every-ray-start-fails]`); that
  a value already set is left alone; and the token file, shared, mode 600, never printed or copied.
- **T1 — the importers.** `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` each gain the two
  lines
  ```python
  # names Ray's token mode before ray.init (config/ray_auth.py)
  import config.ray_auth  # noqa: F401
  ```
  as the first lines of the block importing the repository's own modules (`main.py:25-26`,
  `plot_by_beta.py:40-41`, `plot_ScalarModel.py:41-42`). Nothing else in them changed (deviation 15
  on the placement).
- **T2 — the script.** In `cmd_start`, before `local t0=$SECONDS` and the `"$RAY_BIN" start --head`
  line, the prompt's comment and `export RAY_AUTH_MODE="${RAY_AUTH_MODE:-token}"`. The `actors:`
  line prints `RAY_AUTH_MODE=token $RAY_BIN list actors --address $CPBH_ADDRESS`. The usage text
  gains the prompt's sentence as a paragraph of its own after the `status` entry and before "Never
  runs `ray stop`." (ruling 8). Nothing of the lock half, the stop, the ports or `ray start`'s
  arguments changed. `bash -n` 0; the file keeps mode `-rwxr-xr-x`.
  `tools/tests/test_ray_cluster_lock.py` is unchanged and passes, within the `tools` root.
- **T3 — `tools/tests/test_ray_auth.py`** (new), `unittest`, nine test methods, no Ray started,
  nothing written, the real `HOME` untouched. Each probe is a fresh interpreter (`sys.executable`,
  `cwd` the repository root, `PYTHONPATH` the root, the environment `os.environ` less
  `RAY_AUTH_MODE`, plus what the test sets):

  | Test | Asserts |
  |---|---|
  | `test_an_unset_mode_becomes_token` | after `import config.ray_auth`, `os.environ["RAY_AUTH_MODE"]` is `token` |
  | `test_a_mode_already_set_is_left_as_it_is` | with `disabled` preset, it stays `disabled` |
  | `test_ray_imported_first` | `import ray`, then `import config.ray_auth`: `ray._raylet.get_authentication_mode() == AuthenticationMode.TOKEN` is true |
  | `test_the_module_imported_first` | `import config.ray_auth`, then `import ray`: true |
  | `test_a_mode_already_set_is_what_ray_sees` | `ray` first, `disabled` preset: false |
  | `test_main_py_imports_it_before_ray_init` | by `ast`: `main.py` has a module-level `import config.ray_auth`, and its line is before the first top-level statement holding `ray.init`; each failure message names the file |
  | `test_plot_by_beta_py_imports_it_before_ray_init` | the same, for `plot_by_beta.py` |
  | `test_plot_ScalarModel_py_imports_it_before_ray_init` | the same, for `plot_ScalarModel.py` |
  | `test_ray_cluster_sh_exports_the_mode_before_ray_start` | by text: the line `export RAY_AUTH_MODE="${RAY_AUTH_MODE:-token}"` exists, there is exactly one `"$RAY_BIN" start` line, and the export comes before it |

  Standalone: `Ran 9 tests in 2.183s`, `OK`, 2 s wall
  ([`out/01-t3.txt`](../../../.documents/ray-sqla-align/out/01-t3.txt)).
- **C1 — `CLAUDE.md`.** A new bullet "Ray's token mode" after "The rehearsal lock", as §2.10 asks,
  with an example of a CLI command carrying the variable. The `tools/tests` count line reads 102,
  and the counts' date note says `tools/tests`'s is of 2026-10-25, after `ray-sqla-align` 01.
- **The records**: `.documents/ray-sqla-align/out/01-*` (deviation 16), this log, and the board.

## Deviations from the prompt

**The orchestrator's corrections and rulings** (its note, §0):

1. **`STRUCTURALLY REQUIRED`** (correction 1). The shell's `stat` is GNU's, so every `stat -f '%m
   %Sm'` was `/usr/bin/stat -f '%m %Sm' ~/.ray/auth_token`. It gave `1792932677 Oct 25 12:51:17
   2026` at 2.1 and at V1 step 7. `timeout` is GNU coreutils', as the prompt means.
2. **`IMPLEMENTATION CHOICE`, ruled** (ruling 2). T3's driver checks are three methods, one per
   driver, so the module has nine test methods and `tools/tests` goes from 93 to 102.
3. **`NOTED`** (correction 3). Ray's token warning is a `logging` call, so the `-W default` runs of
   §2.3 could not show it, and no root starts Ray. It was measured where the prompt measures it:
   present in `start`'s output at V0 (`grep -c 'Token authentication'` 1), absent at V1 (0). No
   local `ray.init(address="local")` probe was run.
4. **`NOTED`** (correction 4). V1's driver log being free of the warning shows nothing by itself.
   V1's evidence is `start`'s output without the warning, the two processes' environments, and the
   driver attaching at all, which V0 showed it cannot on the pins alone. **V1's driver exercised
   the refresh:** it imported `ray` before `config.ray_auth`, as `RunDrivers/registered_run.py`
   imports `ray` before `runpy` runs `main.py`, so the module found `ray._raylet` loaded and called
   `Config.initialize("")`. Without that call the driver keeps `disabled` (B1 shows it in a fresh
   interpreter), as at V0.
5. **`STRUCTURALLY REQUIRED`** (correction 5). Every `ray_cluster.sh start`, `stop` and `status`
   ran alone in its Bash call, `> var/ray-sqla-align-01/<name>.txt 2>&1`, the exit by `echo $?` in
   the same call, the file read in a later call. No `PURPOSE` and no command line of mine named the
   cluster's temp dir; pids were read from `start`'s saved output (the `gcs_server`) and the saved
   `status` listing (the `raylet`). The drivers, the `ps -E` checks and `ray status` ran in Bash
   calls of their own, and `stop` ran with no driver alive. The lock module's `status` was not run
   separately: the script's `status` reports the lock. `start` exited 0 both times. No `ray stop`.
6. **`NOTED`** (ruling 6). `ps -E -ww -o command= -p <pid> | grep -o 'RAY_AUTH_MODE=[a-z]*'` gave
   `RAY_AUTH_MODE=token` for the `gcs_server` (99355) and the `raylet` (99403); the control `grep
   -c 'PATH='` gave 1 for each. No environment was printed whole.
7. **`NOTED`** (correction 7). The final `grep -r -F -l -f ~/.ray/auth_token
   .documents/ray-sqla-align/ prompts/ray-sqla-align/` printed nothing, exit 1, run on the tree
   `668d30f` records (with this log and the board written).
8. **`IMPLEMENTATION CHOICE`, ruled** (ruling 8). The usage sentence is a paragraph of its own after
   the `status` entry and before "Never runs `ray stop`.", wrapped as its neighbours are.
9. **`NOTED`** (correction 9). pip printed `Resolved … to commit 8e7c1d77…` (DatastoreKit),
   `caf00765…` (RayPoolKit) and `895e3c70…` (RunRegistryKit), the annotated tag objects, in the dry
   run and the install. The freeze's three git lines did not move (`c8570e84…`, `67a75b97…`,
   `0bae3a3c…`), and pip reinstalled none of them.
10. **`IMPLEMENTATION CHOICE`** (ruling 10). The roots ran by `CLAUDE.md`'s commands, serially, in
    the foreground, never with the cluster up, `ComputeTargets` in two halves, each Bash call at a
    600 000 ms timeout, each root's output to `var/ray-sqla-align-01/roots/<point>-<root>[-W].txt`
    and a summary (the command, `uptime` before, `Ran`, the verdict, the exit) appended to
    `01-roots.txt` by `<scratch>/root.sh`
    ([`out/01-root.sh.txt`](../../../.documents/ray-sqla-align/out/01-root.sh.txt)). The first four
    2.1 summaries took the last line of output rather than the verdict line, so their verdicts were
    appended afterwards by `grep` (`verdict 2.1 CM: OK`, …), and the helper was corrected. No
    failure occurred, so nothing was re-run. The ast- and text-reading tests §2.6 names
    (`test_ray_address`, `test_plot_inputs`, `test_network_flag` in `ComputeTargets`;
    `test_package_provenance`, `test_datastore_config`, `test_version_keyed_lookups` in
    `ObjectFactories`) ran inside their roots and passed; none needed thought.
11. **`IMPLEMENTATION CHOICE`** (ruling 11). Each breakage was applied by `<scratch>/breakage.sh`
    ([`out/01-breakage.sh.txt`](../../../.documents/ray-sqla-align/out/01-breakage.sh.txt)): the file
    saved, the diff written by `diff -u` with `a/`/`b/` labels as `01-B<n>.diff`, then `git apply
    --check` (exit 0) and `git apply -R --check` (exit 1, "patch does not apply") before `git apply`,
    the test module run, `git apply -R`, and `cmp` against the saved copy (exit 0). B5's diff is
    against the tree of §2.5 step 1 (the equality test in place, the four names not yet added), and
    so does not apply to the committed tree. None is committed; `git status` after B4 showed only the
    work. [`out/01-breakage.txt`](../../../.documents/ray-sqla-align/out/01-breakage.txt).
12. **`NOTED`** (ruling 12). The board: header, §1 row (✅, "this commit"), items B1–C1 ✅, gate V
    ✅, gate Token "V1 part ✅; 02's part ⬜", and `### 5.1 After prompt 01 (2026-10-25)` after
    §5.0. `.documents/OPEN_ISSUES.md` unchanged: §3 gained no entry.
13. **`NOTED`** (ruling 13). The one-minute load before each root is in the table below; before
    V0's `start` it was 20.49 (20:08:06), before V1's 14.48 (20:18:25).

**My own:**

14. **`IMPLEMENTATION CHOICE`.** §2.3 asks for each root once more with `-W default` and the
    warnings 2.1's runs did not print. 2.1's runs had no `-W default`, so I ran all five roots with
    `-W default` at 2.1 as well, before the install (19:45–19:51), so that a warning is "new" against
    the same flags. In the event no root printed any Python warning at any point, with or without
    `-W default` (`grep -c -i warn` 0 in all 30 outputs), so either comparison gives the same answer.
15. **`IMPLEMENTATION CHOICE`.** The import's placement. The prompt says "straight after their
    top-level `import ray` and before the first import of the repository's own modules". In
    `main.py` these coincide. In both plot scripts `import ray` is followed by `import seaborn as
    sns`, `from matplotlib import pyplot as plt` and `from numpy import nan`; I put the two lines at
    the head of the repository-module block, after `from numpy import nan`, as StochasticInstantons'
    plot scripts do, rather than splitting the third-party block. Either satisfies T3, which checks
    the import comes before the statement holding `ray.init`, and the module only needs to be
    imported before `ray.init`.
16. **`IMPLEMENTATION CHOICE`.** Outputs beyond those the prompt names: `01-gate.txt` (the gate
    re-checked at 19:36), `01-warnings.txt` (the warning comparison) with its helper
    `01-warnings.py.txt`, `01-t3.txt` (T3 standalone), `01-breakage.txt` with
    `01-breakage.sh.txt`, and `01-root.sh.txt`. The per-root outputs stay in
    `var/ray-sqla-align-01/roots/`.
17. **`IMPLEMENTATION CHOICE`.** `utilities.RECORDED_DISTRIBUTIONS` is written one name per line,
    which is `black`'s form (the one-line form is 92 columns). `test_default_names` holds the six
    names in one list, `names`, compared with `RECORDED_DISTRIBUTIONS` as a tuple and with the
    provenance's keys as a list.
18. **`IMPLEMENTATION CHOICE`.** In `test_registry_config.py` the module docstring's item 1 was
    amended as well as the test's own docstring, since item 1 is where the module says what it
    checks of the distributions.

## Verification performed

### B1 — the baselines, at `551b54c`

- **The Gate**, at 19:36 ([`out/01-gate.txt`](../../../.documents/ray-sqla-align/out/01-gate.txt)):
  `git status --porcelain --untracked-files=all` empty; the only commit after `67e0346` is `551b54c`,
  touching only `prompts/ray-sqla-align/orchestrator/prompt-01.md`; the main checkout alone;
  `runregistrykit list`: `no runs under /Users/ds283/Documents/Code/ChamPBH/var/runs`; Python
  3.13.16, `ray==2.53.0`, `SQLAlchemy==2.0.46`, `datastorekit` at `c8570e84…`, `raypoolkit` at
  `67a75b97…`, `runregistrykit` at `0bae3a3c…`, none editable; `ray_cluster.sh status` exit 0:
  `lock: free (/Users/ds283/.raypoolkit/rehearsal-lock does not exist)`, no Ray process, nothing
  `stop` would signal, ports 6380, 6381, 8267, 52367 free; `ls -l ~/.ray/auth_token`: `-rw------- 1
  ds283 staff 64 Oct 25 12:51`; `env | grep -c RAY_AUTH_MODE` 0.
- `pip freeze`: 102 lines. `installed_package_provenance()` of the seven: `datastorekit` 0.4.0
  (`c8570e84…`, `v0.4.0`, not editable), `raypoolkit` 0.3.0 (`67a75b97…`, `v0.3.0`), `runregistrykit`
  0.2.0 (`0bae3a3c…`, `v0.2.0`), `ray` 2.53.0, `SQLAlchemy` 2.0.46, numpy 2.3.5, scipy 1.17.0, the
  four with no recorded origin.
- The token file: `-rw------- 1 ds283 staff 64 Oct 25 12:51`, `1792932677 Oct 25 12:51:17 2026`
  ([`out/01-token.txt`](../../../.documents/ray-sqla-align/out/01-token.txt)).

### P1 — the pins

- The dry run: `Would install SQLAlchemy-2.0.55 ray-2.59.0`, exit 0.
- The install: `Successfully installed SQLAlchemy-2.0.55 ray-2.59.0`, exit 0.
- `diff` of the freezes (102 lines each):
  ```text
  79c79
  < ray==2.53.0
  ---
  > ray==2.59.0
  92c92
  < SQLAlchemy==2.0.46
  ---
  > SQLAlchemy==2.0.55
  ```
- `pip check`: `No broken requirements found.`, exit 0. The three git packages at the Gate's
  commits, none editable (the freeze, and `installed_package_provenance()` after the install).

### The roots

[`out/01-roots.txt`](../../../.documents/ray-sqla-align/out/01-roots.txt). Each cell: tests, verdict,
the time `unittest` reports; the one-minute load before it.

| Root | 2.1, `ray` 2.53.0 | 2.1, `-W default` | 2.3, the pins | 2.3, `-W default` | 2.8, the fix |
|---|---|---|---|---|---|
| CosmologyModels | 18 OK, 119.3 s; 8.53 | 18 OK, 81.9 s; 3.44 | 18 OK, 74.7 s; 6.78 | 18 OK, 68.9 s; 3.57 | 18 OK, 65.3 s; 12.72 |
| ComputeTargets `[a-f]` | 61 OK, 131.1 s; 8.00 | 61 OK, 95.5 s; 6.00 | 61 OK, 120.5 s; 9.91 | 61 OK, 129.7 s; 4.54 | 61 OK, 96.4 s; 5.73 |
| ComputeTargets `[g-z]` | 115 OK, 127.2 s; 4.08 | 115 OK, 104.3 s; 3.23 | 115 OK, 110.4 s; 4.77 | 115 OK, 182.8 s; 5.76 | 115 OK, 114.3 s; 4.58 |
| ObjectFactories | 78 OK, 15.1 s; 6.43 | 78 OK, 8.9 s; 5.98 | 78 OK, 10.9 s; 8.31 | 78 OK, 8.4 s; 3.04 | 78 OK, 7.2 s; 7.31 |
| tools | 93 OK, 31.0 s; 7.81 | 93 OK, 31.1 s; 5.84 | 93 OK, 46.4 s; 7.40 | 93 OK, 37.9 s; 2.95 | **102** OK, 31.4 s; 7.19 |
| RunDrivers | 61 OK, 25.9 s; 7.02 | 61 OK, 24.1 s; 5.79 | 61 OK, 40.9 s; 6.69 | 61 OK, 29.1 s; 3.36 | 61 OK, 22.4 s; 5.80 |

Every count is 2.1's but `tools` at 2.8, which is 93 + 9. No failure at any point; nothing re-run.
`black --check` on the eight `.py` files changed: `8 files would be left unchanged`, exit 0.

### V0 — the refusal, on the pins alone ([`out/01-v0.txt`](../../../.documents/ray-sqla-align/out/01-v0.txt))

1. 20:08:06: load 20.49; `env | grep -c RAY_AUTH_MODE` 0.
2. `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "ray-sqla-align 01 (V0)"`:
   **exit 0**, `started in 13s; gcs_server pid 95793`. The warning is the output's second line:
   ```text
   2026-10-25 20:08:13,297	WARNING authentication_token_setup.py:85 -- Token authentication is enabled for this Ray cluster. Set RAY_AUTH_MODE=disabled to opt out. For more information, see https://docs.ray.io/en/latest/ray-security/token-auth.html
   ```
3. `timeout 300 ./venv/bin/python -c 'import ray; ray.init(address="127.0.0.1:6380");
   print(ray.cluster_resources()); ray.shutdown()'`, 20:08:30.989 → 20:08:52.074: **refused, exit
   1, 21.08 s.** 20 lines of the form
   ```text
   [2026-10-25 20:08:31,668 W 96041 80683350] gcs_client.cc:210: Failed to get cluster ID from GCS server: Unauthenticated: RPC error: InvalidAuthToken: Authentication token is missing or incorrect
   ```
   from 20:08:31.668 to 20:08:50.943, then the traceback, whose last error lines are
   ```text
   ray.exceptions.AuthenticationError: RPC error: InvalidAuthToken: Authentication token is missing or incorrect. Note: RAY_AUTH_MODE is currently 'disabled' (not 'token'). …
   ConnectionError: Failed to connect to Ray cluster at 127.0.0.1:6380
   ```
   `grep -c InvalidAuthToken` 21 (the 20 lines and the `AuthenticationError`). Nothing printed by
   `cluster_resources()` (no line starting `{`).
4. `ray_cluster.sh stop`: **exit 0**; `SIGTERM to 19 process(es)`, `grace period of 15s over;
   SIGKILL to: 95793`, `no process names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free`,
   `released the rehearsal lock`, `stopped in 19s`. `status`: **exit 0**, `lock: free
   (/Users/ds283/.raypoolkit/rehearsal-lock does not exist)`, `Ray processes of /tmp/ray-cpbh: none`,
   nothing `stop` would signal, the four ports free.

So README §0.2 holds for ChamPBH: on the pins alone the cluster runs in token mode, and a driver
attaching by address is refused.

### D1 — the distributions

- §2.5 step 1, the equality test against the old lists: `Ran 5 tests`, `OK`.
- B5 (below) failed `test_roots_and_distributions`.
- After step 3–4: `test_registry_config` and `test_package_provenance` together `Ran 15 tests`, `OK`.
- §2.5 step 5 ([`out/01-distributions.txt`](../../../.documents/ray-sqla-align/out/01-distributions.txt)):
  ```text
  >> packages: datastorekit 0.4.0 (https://github.com/ds283/DatastoreKit@c8570e849334d09085bbd84876095fc9e22bd005, requested v0.4.0); raypoolkit 0.3.0 (https://github.com/ds283/RayPoolKit@67a75b97a656accb98195a4bda93f64e6a5f3fb2, requested v0.3.0); ray 2.59.0 (no recorded origin); SQLAlchemy 2.0.55 (no recorded origin); numpy 2.3.5 (no recorded origin); scipy 1.17.0 (no recorded origin)
  ```

### V1 — the fix on the real cluster, before the commit ([`out/01-v1.txt`](../../../.documents/ray-sqla-align/out/01-v1.txt))

1. 20:18:25: load 14.48; `env | grep -c RAY_AUTH_MODE` 0, so the script's default named the mode.
2. `ray_cluster.sh start "ray-sqla-align 01 (V1)"`: **exit 0**, `started in 9s; gcs_server pid
   99355`; `grep -c 'Token authentication'` **0**. Its `actors:` line:
   ```text
   >> ray_cluster: actors: RAY_AUTH_MODE=token ./venv/bin/ray list actors --address 127.0.0.1:6380
   ```
3. The `raylet`'s pid, 99403, from the script's `status` listing (exit 0, lock held by ChamPBH,
   19 processes). `ps -E -ww -o command= -p <pid> | grep -o 'RAY_AUTH_MODE=[a-z]*'`:
   `RAY_AUTH_MODE=token` for 99355 (`gcs_server`) and for 99403 (`raylet`); `grep -c 'PATH='`: 1
   for each.
4. `timeout 300 env PYTHONPATH=. ./venv/bin/python -c 'import ray; import config.ray_auth;
   ray.init(address="127.0.0.1:6380"); print(ray.get(ray.remote(lambda: 41 + 1).remote()));
   ray.shutdown()'`, 20:19:02.209 → 20:19:04.735: **exit 0, 2.53 s**. Its output, whole:
   ```text
   2026-10-25 20:19:02,896	INFO worker.py:1836 -- Connecting to existing Ray cluster at address: 127.0.0.1:6380...
   2026-10-25 20:19:02,935	INFO worker.py:2024 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8267
   42
   ```
   (colour codes elided). `grep -c`: `InvalidAuthToken` 0, `AuthenticationError` 0, `Token
   authentication` 0.
5. `RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380`: **exit 0**; one active
   node; `0.0/10.0 CPU`, `0.0/1.0 GPU`, `0B/7.65GiB memory`, `0B/2.00GiB object_store_memory`.
6. `stop`: **exit 0**; `SIGTERM to 28 process(es)` (the 19 and the task's workers), `grace period
   of 15s over; SIGKILL to: 99355 99362`, the temp dir unnamed by any process and the four ports
   free, `released the rehearsal lock`, `stopped in 19s`. `status`: **exit 0**, lock free, no
   process, nothing to signal, the four ports free.
7. The token file: `-rw------- 1 ds283 staff 64 Oct 25 12:51 /Users/ds283/.ray/auth_token`;
   `/usr/bin/stat -f '%m %Sm'`: `1792932677 Oct 25 12:51:17 2026`. Unchanged since 2.1.

### The gates

- **V ✅**: on the pins alone `start` printed the token warning and a driver attaching by address
  was refused (V0); with the fix `start` printed none and a driver in the wrapper's import order
  attached and ran a task (V1).
- **Token, V1 part ✅**: no warning or authentication error in V1's `start` or driver output; the
  `gcs_server` and `raylet` ran with `RAY_AUTH_MODE=token`; the token file unchanged at 64 bytes,
  mode 600, mtime 12:51:17; the final `grep -F -l -f` over this campaign's records exit 1. 02's
  part is 02's.

## What is new in the logs

- **The suites**: nothing. No root printed a `DeprecationWarning`, `FutureWarning`,
  `ResourceWarning` or any other Python warning at 2.1, 2.3 or 2.8, with or without `-W default`
  ([`out/01-warnings.txt`](../../../.documents/ray-sqla-align/out/01-warnings.txt)).
- **`start`**: on the pins alone, Ray's `WARNING authentication_token_setup.py:85 -- Token
  authentication is enabled for this Ray cluster…` (V0); with the fix, not there (V1). No other
  new line was looked for: V0 and V1 have no counterpart in `rrk-adoption` 04's records but its
  `ray status`, below.
- **`ray status`** reports `0.0/1.0 GPU`; `rrk-adoption` 04's (`.documents/rrk-adoption/out/04-cluster.txt`,
  Ray 2.53.0) has a CPU line and no GPU line. Not a correctness matter for 01 (below).

## The deliberate-breakage record

Each applied to the fix tree (B5 to the tree of §2.5 step 1), the module run, the file restored
(`cmp` exit 0). Each diff as applied is `out/01-B<n>.diff`; `git apply --check` exit 0 and `git
apply -R --check` exit 1 before each was applied. None committed.

| | Breakage | Module | Failed | Result |
|---|---|---|---|---|
| **B1** | the refresh (`if "ray._raylet" in sys.modules:` and the `Config.initialize("")` call) deleted from `config/ray_auth.py` | `test_ray_auth` | `test_ray_imported_first` (`AssertionError: False is not true`) | 1 failed of 9 |
| **B2** | `if "RAY_AUTH_MODE" not in os.environ:` → `if True:` | `test_ray_auth` | `test_a_mode_already_set_is_left_as_it_is` (`'token' != 'disabled'`), `test_a_mode_already_set_is_what_ray_sees` (`True is not false`) | 2 failed of 9 |
| **B3** | `plot_by_beta.py`'s comment and `import config.ray_auth` deleted | `test_ray_auth` | `test_plot_by_beta_py_imports_it_before_ray_init` (`plot_by_beta.py does not import config.ray_auth at module level`) | 1 failed of 9 |
| **B4** | the script's `export` moved after `local rc=$?` (`bash -n` exit 0) | `test_ray_auth` | `test_ray_cluster_sh_exports_the_mode_before_ray_start` (`286 not less than 282 : ray_cluster.sh exports RAY_AUTH_MODE after its ray start`) | 1 failed of 9 |
| **B5** | `"ray"` added to `runregistrykit.toml`'s `distributions` alone | `test_registry_config` | `test_roots_and_distributions` (`Tuples differ: ('datastorekit', 'raypoolkit', 'ray') != ('datastorekit', 'raypoolkit')`) | 1 failed of 5 |

Each failed exactly the tests the prompt names.

## Observations not acted on

- **`ray status` reports `0.0/1.0 GPU`** on ChamPBH's cluster under Ray 2.59.0; `rrk-adoption` 04's
  `ray status` under 2.53.0 has `0.0/10.0 CPU` and no GPU line. `ray_cluster.sh` passes `--num-cpus`
  and no `--num-gpus`. StochasticInstantons recorded the same. Nothing in 01 depends on it; 02's
  fingerprints will show whether a build does.
- **The stop needed `SIGKILL`** after the 15 s grace: for the `gcs_server` at V0 (95793), and for
  the `gcs_server` (99355) and 99362 at V1. 99362 is a `Python -u` process of the venv's Ray, its
  command line cut by the script's listing; it was not identified further, since doing so would
  mean a command naming the cluster's temp dir. Both times the script confirmed no process left
  and the four ports free, and released the lock.
- **No root prints any Python warning**, even under `-W default`, before or after the move. So the
  suites cannot show §5.1's "new warnings" for Ray or SQLAlchemy here; 02's launch logs are where
  Ray's lines will be compared with `rrk-adoption` 04's.

## State handed on

- Branch `ray-sqla-align` at `668d30f`, on top of `551b54c`. **Not pushed.**
- **The venv holds `ray` 2.59.0 and `SQLAlchemy` 2.0.55**, shared with `jordan-normalization`, which
  now runs on Ray 2.59.0 without the token module until the merge (README §4.2). Going back is
  `./venv/bin/pip install ray==2.53.0 SQLAlchemy==2.0.46` (README §4.1); the freeze before is in
  `out/01-pip.txt`.
- The roots: CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 78, tools 102,
  RunDrivers 61.
- ChamPBH's cluster is stopped and the lock free (`status` exit 0 after V1's `stop`). The token
  file is unchanged.
- `var/ray-sqla-align-01/` holds the working outputs, the per-root outputs and the breakages'
  saved copies; nothing in `var/` was deleted. No store was opened and no run registered.
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged. 02 is held until this prompt's review.
