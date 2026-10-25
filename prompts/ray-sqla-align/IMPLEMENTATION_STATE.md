# ray-sqla-align campaign — implementation state

**Last updated:** 2026-10-25 · **Status: closed — 2 of 2 landed and reviewed.** U0–U4 ruled
by the user the same day; P1–P12 stood as proposed, the user having ruled none otherwise. Gates
V, FP and Token hold. The orchestrator's review of 02 closed the campaign (README §2). What remains
is the user's: the merge of `ray-sqla-align` into `main` and the push, then `main` into
`jordan-normalization` before that campaign's 09 (README §4.2), and the retirement of 02's two
stores.

The plan is [`README.md`](README.md), written on 2026-10-25 on branch `ray-sqla-align`, cut from
`main` at `a144c83`.

The campaign moves ChamPBH's `ray` from 2.53.0 to 2.59.0 and `SQLAlchemy` from 2.0.46 to 2.0.55,
the versions SecondaryGWKit and StochasticInstantons moved to on 2026-10-25. Every other pin stays.
Because Ray 2.59 starts `ray start --head` in token mode when `~/.ray/auth_token` exists, but never
a driver attaching by address, the move alone would leave every ChamPBH driver refused by its
cluster (README §0.2); so every Ray start names token mode, as SGK's and SI's do. Manifests and
driver logs record `ray`, `SQLAlchemy`, `numpy` and `scipy` too. The rehearsal's stores must
fingerprint to `2dd4b39b…932f`, `rrk-adoption` 04's.

**Campaign:** [`README.md`](README.md) ·
**Code:** `requirements.txt`, `runregistrykit.toml`, `utilities.py`, `config/ray_auth.py` (new),
`main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`,
`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`, `tools/tests/test_ray_auth.py` (new),
`tools/tests/test_registry_config.py`, `ObjectFactories/tests/test_package_provenance.py`,
`CLAUDE.md` ·
**Verification:** 01's log and outputs; `.documents/ray-sqla-align-verification.md` (written by 02);
outputs in `.documents/ray-sqla-align/out/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section: no issue is
open)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are corrected.
> See `CLAUDE.md`.

### Decisions

**Made by the user, 2026-10-25 (README §6.1):**

| Decision | Decided |
|---|---|
| **U0** — versions | `ray` 2.59.0 and `SQLAlchemy` 2.0.55, SGK's and SI's |
| **U1** — the shape | a campaign of its own on a branch from `main`, merged into `main` and then into `jordan-normalization` |
| **U2** — token authentication | kept, and named (`RAY_AUTH_MODE=token`), as SGK and SI do; SI's concluded records folded in |
| **U3** — recorded distributions | as SI did: the two lists held equal by a test, then `ray`, `SQLAlchemy`, `numpy`, `scipy` added to both |
| **U4** — prompts | two: 01 the change, 02 the rehearsal on 01's clean commit |

The planner's proposals, P1–P12, are in README §6.2.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Move `ray` and `SQLAlchemy`, with token mode named](01-move-ray-and-sqlalchemy-with-token-mode.md) | the pins and the venv; V0; the distributions; `config/ray_auth.py`, its importers and the script; the test; V1; `CLAUDE.md` | Opus | ✍️ 2026-10-25 | ✅ 2026-10-25, reviewed | `668d30f` | [01](logs/01-move-ray-and-sqlalchemy-with-token-mode.md) |
| 02 | [Rehearse registered runs under Ray 2.59](02-rehearse-registered-runs-under-ray-2.59.md) | `reference`, `resumed-a` (`SIGTERM`), `resumed-c` and `plot_by_beta.py` on 01's code; gates FP and Token | Opus | ✍️ 2026-10-25 | ✅ 2026-10-25, reviewed | `e04fbb8` | [02](logs/02-rehearse-registered-runs-under-ray-2.59.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

The review of 02 closes the campaign (README §2).

**Orchestrator notes** are written just before dispatch, under `orchestrator/`:
[`prompt-01.md`](orchestrator/prompt-01.md) (`551b54c`), used for 01;
[`prompt-02.md`](orchestrator/prompt-02.md) (`b16c435`), used for 02.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **B1** | 01 | the baselines: the freeze, the provenance of seven distributions, the five roots, the token file's `ls -l` and mtime | ✅ 01 |
| **P1** | 01 | `requirements.txt:79` `ray==2.59.0`, `:92` `SQLAlchemy==2.0.55`; the venv upgraded; the freeze moved by those two lines alone | ✅ 01 |
| **V0** | 01 | on the pins alone: `start` prints Ray's token warning; a driver attaching by address is refused | ✅ 01 |
| **D1** | 01 | the toml's `distributions` held equal to `utilities.RECORDED_DISTRIBUTIONS` (B5 caught); the four names added to both | ✅ 01 |
| **T1** | 01 | `config/ray_auth.py`, imported by `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` | ✅ 01 |
| **T2** | 01 | `ray_cluster.sh` names the mode before `ray start`; its `actors:` line and usage say so | ✅ 01 |
| **T3** | 01 | `tools/tests/test_ray_auth.py`, nine checks, no Ray; B1–B4 each caught | ✅ 01 |
| **V1** | 01 | with the fix: no warning at `start`; `gcs_server` and `raylet` in token mode; a driver in the wrapper's import order attaches and runs a task | ✅ 01 |
| **C1** | 01 | `CLAUDE.md`: the bullet "Ray's token mode"; the `tools/tests` count | ✅ 01 |
| **R1** | 02 | `reference`: `done`/0; fingerprint `2dd4b39b…932f` | ✅ 02 |
| **R2** | 02 | `resumed-a`: `SIGTERM`ed, `killed`/143, the store partial | ✅ 02 (`<s>` 90 s; β = 10 stored unvalidated, β = 25 absent) |
| **R3** | 02 | `resumed-c`: `done`/0, only the missing rows computed; fingerprint `2dd4b39b…932f` | ✅ 02 (scalar 5 / 2 / 2 / 2) |
| **R4** | 02 | `compare_stores` equal; manifests `git_dirty` false, naming `ray` 2.59.0 and `SQLAlchemy` 2.0.55 | ✅ 02 |
| **R5** | 02 | `plot_by_beta.py` on the reference: `histories.csv` `d7c62c37…c522`; the store unchanged | ✅ 02 |
| **W1** | 02 | `.documents/ray-sqla-align-verification.md` §1 | ✅ 02 |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| **V** | on the pins alone the cluster is in token mode and refuses a driver attaching by address; with the fix, `start` prints no warning and a driver in the wrapper's import order attaches and runs a task | 01 | ✅ 01 (V0, V1) |
| **FP** | 02's two stores, under `ray` 2.59.0 and `SQLAlchemy` 2.0.55, each fingerprint to `2dd4b39b…932f`; `compare_stores` equal but for bookkeeping; `histories.csv` `d7c62c37…c522` | 02 | ✅ 02 |
| **Token** | no `start` output, launch log or reader log holds Ray's token warning or an authentication error; the cluster's `gcs_server` and `raylet` run with `RAY_AUTH_MODE=token`; the token file unchanged at mode 600; no record holds the token | 01 (V1), 02 | V1 part ✅ 01; 02's part ✅ 02 |

If FP fails, the bisection (`ray` alone, or `SQLAlchemy` alone) is the user's call (README §2.2).

## 3. Active and unresolved issues

None open.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-25)

Asked by the user on 2026-10-25, after SGK's `py313-venv` and SI's `ray-sqla-align` closed, in the
middle of `jordan-normalization` (whose prompt 04 had landed and been reviewed, at `58c1c93`).
Measured at `a144c83` (README §0.4):
- the tree clean, no untracked file; no run registered; the lock free;
- a dry run of the two moved pins installs those two packages and nothing else;
- ChamPBH starts Ray only by `ray start --head` in `ray_cluster.sh` and attaches by address in its
  three drivers: SI's case, which SI measured as a refusal on its pins alone;
- the token file exists (SGK's, 12:51, mode 600); no ChamPBH run has read it;
- `main`'s computational code is unchanged since `rrk-adoption` 04's rehearsal, so its fingerprint
  `2dd4b39b…932f` and `histories.csv` `d7c62c37…c522` are the references.

The user ruled U0–U4 the same day, U2 asking that SI's concluded records be folded into the plan:
V0 and V1 (the refusal measured before the fix, and the fix on the real cluster before the commit),
the `ps -E` check of the cluster's environment, the refresh exercised by the wrapper's `import ray`,
the breakages B1–B4, and the token never printed are SI's. U4 split the work into two prompts so
that the rehearsal runs on a clean tree. No issue opened or assigned.

### 5.1 After prompt 01 (2026-10-25)

Dispatched at `551b54c` (the orchestrator's note); landed in `668d30f`. Log:
[`logs/01-move-ray-and-sqlalchemy-with-token-mode.md`](logs/01-move-ray-and-sqlalchemy-with-token-mode.md);
outputs in `.documents/ray-sqla-align/out/01-*`.
- **The pins.** The dry run installed `SQLAlchemy-2.0.55 ray-2.59.0` alone; the freeze moved by
  those two lines alone; `pip check` clean; the three git packages unmoved, none editable.
- **The roots**, before → on the pins → on the fix: CosmologyModels 18, ComputeTargets 61 + 115,
  ObjectFactories 78 and RunDrivers 61 throughout; tools 93 → 93 → **102** (T3's nine). All OK. No
  root printed any Python warning at any point, with or without `-W default`.
- **V0**, on the pins alone: `start` exit 0 with Ray's `Token authentication is enabled` warning;
  the driver attaching by address refused, exit 1 after 21 s, 21 `InvalidAuthToken` lines and
  `AuthenticationError … RAY_AUTH_MODE is currently 'disabled'`; stopped, lock free.
- **D1**: the equality test caught B5; both lists name `datastorekit, raypoolkit, ray, SQLAlchemy,
  numpy, scipy`; the `>> packages:` line names `ray 2.59.0` and `SQLAlchemy 2.0.55`.
- **T1–T3**: `config/ray_auth.py`, its import in the three drivers, the script's export; T3's nine
  tests pass standalone in about 2 s, and each of B1–B4 fails exactly the test the prompt names.
- **V1**, on the fix: `start` exit 0, no warning; `RAY_AUTH_MODE=token` in the `gcs_server`'s and
  the `raylet`'s environments; the driver, importing `ray` first, attached and printed `42`;
  `ray status` answered (`0.0/10.0 CPU`, `0.0/1.0 GPU`); stopped, lock free; the token file
  unchanged (64 bytes, mode 600, 12:51:17).
- No issue opened. Not pushed. The venv holds the new pins (README §4.1 to go back).

**Orchestrator review of prompt 01 (2026-10-25, 20:25–20:31 GMT).** The ten checks of
[`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) §3, made at `668d30f`; all pass.
1. **Scope.** `668d30f`'s parent is `551b54c`; its 32 files are §3 item 7's list (the code and
   `CLAUDE.md`, 19 outputs under `out/01-*`, the log and the board). Nothing under
   `ComputeTargets/`, `CosmologyModels/`, `PRyM/`, `RunDrivers/`, `StoreFixtures/`,
   `config/datastore.py`, `config/sharding.py` or `prompts/INDEX.md` moved; nothing of `var/` is
   committed; the tree clean; nothing pushed.
2. **The venv**, measured here: `pip freeze` against the orchestrator's freeze before dispatch
   differs in the `ray` and `SQLAlchemy` lines alone; the git packages unmoved, none editable;
   `pip check` clean; a dry run of `requirements.txt` installs nothing.
3. **The token file**: 64 bytes, mode 600, `/usr/bin/stat` `1792932677`, as at the gate. The
   recursive `grep -F -l -f` over this campaign's records and `git grep -F -l -f` over `668d30f`'s
   tree both exit 1.
4. **The fix, by reading.** `config/ray_auth.py` as §2.6 says: the licence header, the
   constant, set only when absent, the refresh only when `ray._raylet` is loaded, no Ray import, the
   comment's points. The script: the export before `local t0=$SECONDS` with the `:-token` default
   and its comment, the `actors:` line, the usage paragraph where the note's ruling 8 puts it, and
   no hunk in the lock half, the stop, the ports or `ray start`'s arguments; `bash -n` 0; `--help`
   carries the sentence. The importers: two lines each, before the repository's first import. In
   the two plot scripts that is three third-party imports after `import ray` rather than straight
   after it (the log's deviation 15, `IMPLEMENTATION CHOICE`). Accepted: nothing between them
   touches Ray's config, the README's P4 is met in its "before the first import of the
   repository's own modules", and T3 checks the property that matters.
5. **The distributions.** Both lists name `datastorekit, raypoolkit, ray, SQLAlchemy, numpy,
   scipy`, each with a comment naming the campaign and U3; the equality test and
   `test_default_names` as §2.5 says. The `>> packages:` line here names `ray 2.59.0` and
   `SQLAlchemy 2.0.55`, the four PyPI installs `(no recorded origin)`.
6. **T3 and the breakages.** The module's nine methods read against §2.6: fresh interpreters with
   `RAY_AUTH_MODE` removed, no Ray started, nothing written, every `subprocess.run` with a timeout.
   B1–B4 replayed here from the committed diffs (`git apply --check` 0): B1 fails
   `test_ray_imported_first`; B2 the two `disabled` tests; B3
   `test_plot_by_beta_py_imports_it_before_ray_init`; B4 the script test; each reverted. B5's
   committed diff is against the tree before the four names, so it was replayed in kind: `"scipy"`
   removed from the toml alone fails `test_roots_and_distributions`; restored. The tree clean
   afterwards.
7. **V0**, from `out/01-v0.txt`: `start` 0 with Ray's `Token authentication is enabled` warning;
   the driver refused, exit 1, 21 `InvalidAuthToken`, one `AuthenticationError`, nothing from
   `cluster_resources()`; `stop` 0; `status` free.
8. **V1**, from `out/01-v1.txt`: `start` 0 with no warning; the `actors:` line carries the
   variable; `RAY_AUTH_MODE=token` for the `gcs_server` (99355) and the `raylet` (99403, a
   `raylet` binary in `status`'s listing), each control 1; the driver, `ray` imported first, exit
   0 and `42`, its three counts 0; `ray status` 0; `stop` 0; `status` free. **Gate V holds, and
   so does gate Token's V1 part.** `stop` needed `SIGKILL` after the grace for the `gcs_server`,
   as it did under Ray 2.53.0 (`rpk-v030-adoption` 01, `out/01-cluster.txt`), and, newly, for
   99362, a `Python -u` of the venv's Ray started with the head; the script confirmed nothing
   left and released the lock both times. Recorded, no issue: the stop's rule is unchanged and
   held.
9. **The records.** `CLAUDE.md`'s bullet "Ray's token mode" after "The rehearsal lock", which is
   unchanged; the `tools/tests` count 102 and the date sentence. The board and the log as the
   prompt's §6 and the note's ruling 12 say; every deviation classed. `OPEN_ISSUES.md` untouched,
   no issue having opened. The message in `CLAUDE.md`'s form.
10. **The roots** at `668d30f`, serially, no cluster up: CosmologyModels 18 OK (68 s),
    ComputeTargets 61 OK (96 s) + 115 OK (105 s), ObjectFactories 78 OK, tools **102** OK,
    RunDrivers 61 OK; loads 3 to 22. `black --check` clean on the eight changed `.py` files.
    `ray_cluster.sh status`: lock free, no process, the ports free. `runregistrykit list`: no runs.

The orchestrator's rulings that read the prompt otherwise than it was written: the token file's
mtime through `/usr/bin/stat` (the shell's `stat` is GNU's), and the three driver checks as three
methods, so `tools/tests` is 102. The SHA replaces "this commit" here and in the log. 02 is no
longer held; it is dispatched at the user's word. Until `main` is merged into it,
`jordan-normalization` runs on Ray 2.59.0 without the token module and must start no Ray
(README §4.2).

### 5.2 After prompt 02 (2026-10-25)

Dispatched at `b16c435` (the orchestrator's note); landed in `e04fbb8`. Log:
[`logs/02-rehearse-registered-runs-under-ray-2.59.md`](logs/02-rehearse-registered-runs-under-ray-2.59.md);
record: [`.documents/ray-sqla-align-verification.md`](../../.documents/ray-sqla-align-verification.md)
§1; outputs in `.documents/ray-sqla-align/out/02-*`.
- **The Gate** held at 20:53: the tree clean at `b16c435`, its code 01's; the freeze 01's; the
  cluster stopped and the lock free; no run; the token file 64 bytes, mode 600, 12:51:17.
- **The roots**, before `start` and after `stop`: CosmologyModels 18, ComputeTargets 61 + 115,
  ObjectFactories 78, tools 102, RunDrivers 61, all OK.
- **The cluster**: `start` exit 0, no token warning; `gcs_server` (10995) and `raylet` (11031)
  `RAY_AUTH_MODE=token`; `ray status` `0.0/10.0 CPU`, `0.0/1.0 GPU`; `stop` exit 0 (`SIGKILL` to
  the `gcs_server` and one `Python -u` after the grace, as at V1), `status` free.
- **R1** `reference` `done`/0 (3 min 36 s); **R2** `resumed-a` `killed`/143 at `<s>` = 90 s, the
  handler 0 s after the signal, β = 10 stored unvalidated; **R3** `resumed-c` `done`/0, the prune
  report naming β = 10, scalar 5 / 2 / 2 / 2; `results_store_id` `null`, `null`, the resumed
  store's.
- **FP**: both stores `2dd4b39b…932f` (status, sidecar and `store fingerprint`), each class as 04's;
  `compare_stores` `RESULT: equal`, its output 04's; `histories.csv` `d7c62c37…c522`; the reference
  store unchanged by the plot. **Gate FP holds.**
- **Token**: the four strings 0 in `start.txt`, the three launch logs and the plot log; the token
  file unchanged; the final grep over the records exit 1. **Gate Token's 02 part holds.**
- **R4**: every manifest `git_head` `b16c4355…`, `git_dirty` false, `script_sha256` `685b615b…`,
  seven packages with `ray` 2.59.0 and `SQLAlchemy` 2.0.55.
- **New in the logs**: the `>> packages:` line's four entries; Ray's accelerator `FutureWarning`
  gone; the prune report in `resumed-c`. Nothing bearing on correctness.
- No issue opened. Not pushed. The two stores and three runs stay under `var/` for the user.

**Orchestrator review of prompt 02 (2026-10-25, 21:30–21:45 GMT).** The eleven checks of
[`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) §3, made at `e04fbb8`; all pass.
1. **Scope.** `e04fbb8`'s parent is `b16c435`; its 37 files are the note's list exactly (the
   document, 34 outputs under `out/02-*`, the log and the board). No `.py`, `.sh`, `.toml` or
   requirements file, `CLAUDE.md`, `prompts/INDEX.md` or `.documents/OPEN_ISSUES.md` moved;
   nothing of `var/` is committed; the tree clean; nothing pushed.
2. **The venv and the token**, measured here: `pip freeze` `cmp`-equal to the orchestrator's freeze
   before dispatch; `pip check` clean. The token file 64 bytes, mode 600, `/usr/bin/stat`
   `1792932677`. The note's ruling-17 grep and `git grep -F -l -f` over `e04fbb8` both exit 1.
3. **The cluster.** `ray_cluster.sh status` exit 0: the lock free, no process, the four ports free.
   The lock module's `status`: free.
4. **The runs, from the registry.** `list`: three runs, `reference` `done`, `resumed-a` `killed`,
   `resumed-c` `done`, all finished. Each `manifest.json` and `status.json` under `var/runs/` is
   `cmp`-equal to its committed copy: exits 0, 143, 0; `results_store_id` `null`, `null`,
   `9919583b…0114` (the resumed store's); `git_head` `b16c4355…`, `git_dirty` false,
   `script_sha256` `685b615b…`, which is `main.py`'s; seven packages with `ray` 2.59.0,
   `SQLAlchemy` 2.0.55, numpy 2.3.5, scipy 1.17.0. `fingerprint_sidecar` "not written … (there is
   none)" for the first two and `written` for `resumed-c`.
5. **The order and the signal.** In each launch log the three `** registered_run:` lines are lines
   1–3 and `>> packages:` line 4. `resumed-a`'s log: the handler's line `** SIGTERM at 2026-10-25
   21:08:36 +0000: recorded killed, exit code 143; …`, the `SystemExit(1)` line, the marker 143 and
   the launcher's 143; `interrupt_when.py`'s `sent SIGTERM` at 21:08:36 UTC, so 0 s (±1 s). No
   `*** SIGTERM received` banner and no alarm line.
6. **The delay.** `compute_times.py` re-run here on the reference store (`mode=ro`): 6.65, 6.75,
   7.66, 42.45 and 107.07 s, as logged; ruling 6 gives `(42.45 + 107.07)/2 + 20 = 94.76`, rounded
   to 90 s, the gap 64.62 s. On the resumed store now β = 10 and 25 carry `resumed-c`'s timestamps
   (21:11:46–47) and the three small β `resumed-a`'s (21:07:16–17), as the account of the prune and
   `resumed-c`'s 5 / 2 / 2 / 2 says.
7. **The stores, re-measured.** `compare_stores.py`: exit 0, `problems: none`, `RESULT: equal`.
   `store fingerprint` without `--write`: both `2dd4b39b61b9…0932f`, each matching its sidecar
   (the reference's taken "by a person", the resumed's by `resumed-c`). `store show` and
   `stage_counts.py` on the three logs as logged.
8. **`histories.csv`**: `d7c62c37d8ad…c522`. The reference store's primary and sidecar `shasum`s
   equal to the ones `02-plot.txt` records before and after the plot.
9. **Token, 02's part.** The four strings `grep -c` 0 in `start.txt`, the three launch logs and the
   plot log, re-run here. `02-cluster.txt`: `RAY_AUTH_MODE=token` for the `gcs_server` (10995) and
   for 11031, a `raylet` binary by its own `ps -o comm=`, each control 1. **Gate FP holds, and so
   does gate Token.**
10. **The records.** The document's figures checked against the outputs above. "What is new in the
    logs" re-derived here for `resumed-c` against 04's by ruling 16's method: only in 04's, Ray's
    accelerator `FutureWarning` (two lines) and the old `>> packages:` line; only in 02's, the new
    `>> packages:` line and the six lines of the prune report. That is the log's account. The board
    in `### 5.2` (ruling 18); the log in §5.1's form, every deviation classed. Its deviation 23
    (`UNINTENDED DRIFT`, GNU `date -r` in a timing line of `02-plot.txt`, the times then taken
    from the epochs) touches a record's wall time only. Accepted.
11. **The suites** at `e04fbb8`, serially, no cluster up: CosmologyModels 18 OK (79 s),
    ComputeTargets 61 OK (114 s) + 115 OK (124 s), ObjectFactories 78 OK, tools 102 OK,
    RunDrivers 61 OK; loads 3 to 6. No `.py` file committed, so `black` has nothing to check.

The orchestrator's rulings that read the prompt otherwise than it was written: an end wait
repeatable once (none timed out), `S` and `L` absolute, the token grep widened to the verification
document, and this section numbered `5.2`. The SHA replaces "this commit" here and in the log.
**The campaign is closed.** Not pushed. The merge of `ray-sqla-align` into `main` and the push,
then `main` into `jordan-normalization` before its 09, are the user's to direct (README §4.2);
the two stores `var/datastores/ray-sqla-align-02-{reference,resumed}.db` and the three run
directories stay for the user to retire.
