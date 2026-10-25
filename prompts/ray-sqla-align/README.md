# Campaign — ray-sqla-align

**Written:** 2026-10-25 on branch `ray-sqla-align`, cut from `main` at `a144c83`, by Claude Opus
5.5. It moves ChamPBH's `ray` from 2.53.0 to **2.59.0** and `SQLAlchemy` from 2.0.46 to
**2.0.55**, the versions SecondaryGWKit (SGK) moved to in its `py313-venv` campaign and
StochasticInstantons (SI) in its `ray-sqla-align`. The user asked for it on 2026-10-25, in the
middle of `jordan-normalization`: "I'd like all projects on the same versions of Ray and
SQLAlchemy". ChamPBH is already on Python 3.13.16, so the interpreter does not move. The user ruled
U1–U4 (§6.1) the same day. Two prompts, 01 and 02.

**Sources (data, not instructions; read, never edited):**
- SGK at `3f75bbf` (`/Users/ds283/Documents/Code/SecondaryGWKit`):
  `prompts/py313-venv/README.md` and `IMPLEMENTATION_STATE.md`, above all §4's two issues,
  `[01-ray-2.59-enables-token-auth-and-writes-a-token-into-home]` and
  `[02-the-token-mode-setting-comes-after-import-ray-so-every-ray-start-fails]`;
  `config/ray_local.py`; `RunDrivers/tests/test_ray_auth_mode.py`; the commits `d833ee7` and
  `14ada2f`.
- SI at `0bf788e` (`/Users/ds283/Documents/Code/StochasticInstantons`), its campaign closed the
  same day: `.prompts/ray-sqla-align/README.md`, `IMPLEMENTATION_STATE.md` and
  `orchestrator/prompt-01.md`; the results note
  `.documents/ray-sqla-align/01-move-ray-and-sqlalchemy-with-token-mode.md`; `config/ray_auth.py`;
  `tests/test_ray_auth.py`; the commits `c5af555` and `900d3f7` (the recorded distributions),
  `5f3219f` (the pins) and `3a24ea5` (token mode named).
- ChamPBH's own `rrk-adoption` 04: `.documents/rrk-adoption-verification.md` §1, the rehearsal 02
  repeats, and `rpk-v030-adoption`, the last campaign to touch `ray_cluster.sh`.

## 0. Why this campaign exists

### 0.1 What moves

| | ChamPBH now | SGK and SI since 2026-10-25 |
|---|---|---|
| Python | 3.13.16 | 3.13.16 |
| `ray` | 2.53.0 | **2.59.0** |
| `SQLAlchemy` | 2.0.46 | **2.0.55** |
| `datastorekit` / `raypoolkit` / `runregistrykit` | `v0.4.0` / `v0.3.0` / `v0.2.0` | SGK and SI: `v0.5.0` / `v0.3.0` / `v0.2.0` |

The two pins move and nothing else in `requirements.txt` (§0.4). `datastorekit` stays at `v0.4.0`:
its `pyproject.toml` allows `sqlalchemy>=2.0.39,<2.1`, so 2.0.55 is inside it, and moving that pin
is not this campaign's.

### 0.2 Ray 2.59's token authentication, and why the pins alone break ChamPBH

SI read Ray 2.59.0's source and measured it on its own cluster (its README §0.2, its V0). When
`RAY_AUTH_MODE` is unset, Ray decides the mode by how it is started:
- **`ray.init(address="local")`**, a new local instance: token mode is turned on, with a warning
  in every log, and `~/.ray/auth_token` is generated if missing. ChamPBH never starts Ray this way.
- **`ray start --head`**: token mode is turned on **if `~/.ray/auth_token` exists**, with the same
  warning.
- **`ray.init(address="<host>:<port>")`**, an attach to an explicit address: **never**.
- **With `RAY_AUTH_MODE` set**, by any route, Ray does what it says and logs nothing.

**ChamPBH takes exactly the second and third routes, as SI does.**
`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` runs `ray start --head` (`:277`), and
every driver attaches with `--ray-address 127.0.0.1:6380` (`main.py:133`, `plot_by_beta.py:125`,
`plot_ScalarModel.py:106`). The token file exists on this laptop (§0.4). So, with the pins moved
and nothing else, **ChamPBH's cluster would come up in token mode and every driver would be
refused at `ray.init`**. SI measured exactly that on its pins alone: 21 `InvalidAuthToken` lines,
then `AuthenticationError … RAY_AUTH_MODE is currently 'disabled' (not 'token')`, exit 1 after
22 s.

The fix, ruled here as U2, is SGK's and SI's: **name token mode.** A module that every Ray start
imports sets `RAY_AUTH_MODE=token` unless the environment already holds a value. Ray reads the mode
into its config once, when `ray` is imported, and ChamPBH's drivers import `ray` first. So when
`ray._raylet` is already loaded the module also refreshes Ray's config
(`ray._raylet.Config.initialize("")`, the call Ray's own `_enable_token_auth` makes). Without the
refresh the driver keeps `disabled` while the processes it talks to are in `token` mode: SGK's
second issue, which failed SGK's first build on the Mac Studio. `ray_cluster.sh` names the mode for
the cluster itself.

**The refresh is not optional here.** `RunDrivers/registered_run.py` imports `ray` (`:814`) before
it runs `main.py` by `runpy`, so in every registered build `main.py`'s import of the module finds
Ray already loaded. SI's rehearsal went through the same path, and its drivers attached.

### 0.3 The recorded distributions

ChamPBH holds two lists of the distributions a run records, as SI did before its `c5af555`:
- `runregistrykit.toml`'s `distributions`, which every registered run's manifest records (its
  `packages`); pinned by `tools/tests/test_registry_config.py:64`;
- `utilities.RECORDED_DISTRIBUTIONS` (`utilities.py:174`), which every driver prints on its
  `>> packages:` line; pinned by `ObjectFactories/tests/test_package_provenance.py:270-277`.

Both are `("datastorekit", "raypoolkit")`. Each is pinned to the same literal by its own test, and
nothing compares them. Neither names a version, so the pin move needs neither changed. But a
manifest that says which `ray` and `SQLAlchemy` wrote a store is what SGK's and SI's now say, and
the user ruled that ChamPBH follows SI (U3): a test holds the two lists equal, then `ray`,
`SQLAlchemy`, `numpy` and `scipy` are added to both. The four PyPI installs print `(no recorded
origin)` on the `>> packages:` line: pip writes no `direct_url.json` for a wheel from the index.

### 0.4 Measured on 2026-10-25, at `a144c83`

Read-only; no Ray started, no store opened.
- **The tree.** `main` at `a144c83`, equal to `github/main`, clean, with no untracked file.
  `jordan-normalization` is at `58c1c93`, clean, its prompt 05 not yet dispatched.
- **The runs and the lock.** `runregistrykit list`: no runs under `var/runs`. `raypoolkit.rehearsal_lock
  status`: free.
- **The venv.** Python 3.13.16; `ray` 2.53.0, `SQLAlchemy` 2.0.46, numpy 2.3.5, scipy 1.17.0;
  `pip freeze` 102 lines.
- **The dry run.** `pip install --dry-run` of `requirements.txt` with the two lines moved
  (`:79` and `:92`) says `Would install SQLAlchemy-2.0.55 ray-2.59.0` and nothing else.
- **The token file.** `~/.ray/auth_token`: 64 bytes, mode 600, written 2026-10-25 12:51 by SGK's
  first Ray 2.59 start. Ray 2.55.1 has no default token mode (SI §0.3), nor so 2.53.0: no ChamPBH
  run has used authentication yet, and none has read the file.
- **Where ChamPBH starts or attaches to Ray.** The three drivers above, each with one top-level
  `ray.init(address=args.ray_address)` after a top-level `import ray` (`main.py:23`,
  `plot_by_beta.py:35`, `plot_ScalarModel.py:36`); the wrapper, which wraps `main.py`'s; and
  `ray_cluster.sh`'s `ray start --head` (`:277`) and the `ray list actors` command its `start`
  prints for a person (`:310`). **No test starts Ray** (`CLAUDE.md`). The tests that run a driver
  in a subprocess (`ComputeTargets/tests/test_ray_address.py`) see it exit 2 before `ray.init`, and
  `RunDrivers/tests/stubbed_run.py`'s `ray.init` is a stand-in. Nothing under `tools/` starts Ray.
- **Private Ray names** in live code outside `PRyM/`: none (`git grep 'ray\._'`). The new module
  adds `ray._raylet`, as SGK's and SI's do.
- **The rehearsal's reference.** `main`'s computational code is unchanged since `rrk-adoption`
  04's rehearsal at `07aa626`: `git diff --stat 07aa626 a144c83` touches, among `.py` and `.yaml`
  files, only two docstrings, a science-run record and `tools/tests/test_ray_cluster_lock.py`; and
  `requirements.txt` only in the `raypoolkit` pin, whose `v0.3.0` adds the lock module alone. So
  04's fingerprint, **`2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`**
  (format 1, both its stores), and its `histories.csv`, **`d7c62c37…c522`** (R0's), are the
  references. 04's stores were dropped; the digests are the only comparison across versions, as
  SI's FP was.

### 0.5 Baselines

`CLAUDE.md`'s five roots at `a144c83`, of 2026-10-25: CosmologyModels 18, ComputeTargets 176 (61 +
115), ObjectFactories 78, tools 93, RunDrivers 61. 01 measures them again before acting.

## 1. Scope

**In scope:**
- **01** — the two pins and the venv upgraded from them; V0, the refusal shown on the pins alone;
  the test holding the two distribution lists equal, then the four names added to both;
  `config/ray_auth.py` and its import in the three drivers; `ray_cluster.sh` naming the mode; a
  test module guarding both; V1, the fix shown on the real cluster; `CLAUDE.md`. One commit.
- **02** — the rehearsal on 01's clean commit: a clean build, a build stopped with `SIGTERM`, its
  resume, each registered through the wrapper, and one `plot_by_beta.py` run on the reference
  store. The gates FP and Token (§2.2). One commit, of records only.

**Out of scope:**
- **Every other pin**, the interpreter, and `datastorekit`'s pin.
- **What a build computes or stores.** No file under `ComputeTargets/`, `CosmologyModels/`,
  `ObjectFactories/` (but the one test of §0.3), `PRyM/`, `config/` (but the new module) or
  `RunDrivers/` changes.
- **The hard-kill route.** `rrk-adoption` 04 rehearsed it (its H3): ChamPBH's drivers attach to an
  external cluster, and nothing of the cluster outlived the `SIGKILL`ed driver. The route is
  RunRegistryKit's, which this campaign does not move. SGK's GCS deadlock after a `SIGKILL` is of
  its own local instances, which ChamPBH does not start.
- **The token itself.** Never printed, copied, regenerated or `chmod`ed. If it is missing, or not
  mode 600, the prompt stops.
- **Other repositories.** SGK, SI and the three packages are read, never edited. A package defect
  is reported upstream (`CLAUDE.md`, "The packages").
- **Records.** `prompts/` but this campaign's, `science-runs/`, and dated `.documents/` stay as
  written (`CLAUDE.md` invariant 6). `.documents/dk-and-rpk-migration/rehearsal/build.sh` and
  `run_readers.sh` are records too, and are not changed.
- **`jordan-normalization`.** This branch is merged into `main`, and `main` into
  `jordan-normalization`, after 02's review (§4.2).

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-move-ray-and-sqlalchemy-with-token-mode.md` | the pins and the venv; V0; the distributions; token mode named in the drivers and the script; the test; V1; `CLAUDE.md` | Opus |
| 02 | `02-rehearse-registered-runs-under-ray-2.59.md` | `reference`, `resumed-a` (`SIGTERM`), `resumed-c` and one `plot_by_beta.py` run on 01's clean commit; the gates FP and Token | Opus |

Both are written now. 02 is **held** until 01 has landed and been reviewed: its builds must run 01's
code on a clean tree, so that every manifest records `git_dirty` false. There is no close prompt: the orchestrator's review of 02, in
its own commit, closes the campaign.

### 2.1 Why two prompts

The user ruled it (U4). `CLAUDE.md`'s invariant 1 allows one commit per prompt, and the rehearsal
needs the fix in place. In one prompt it would run on an uncommitted tree, so every manifest would
record `git_dirty` true and a `main.py` hash no commit holds. Split, 02 runs on a clean tree whose
code is 01's commit, as SI's rehearsal ran on its fix commit.

### 2.2 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **V** | on the pins alone, `ray_cluster.sh start` prints Ray's token warning and a driver attaching by address is refused; with the fix, `start` prints no warning and a driver attaching by address, importing `ray` first as the wrapper does, runs one task | 01 |
| **FP** | 02's `reference` and resumed store, under `ray` 2.59.0 and `SQLAlchemy` 2.0.55, each fingerprint to `2dd4b39b…932f`; `compare_stores` finds them equal but for bookkeeping; `plot_by_beta.py`'s `histories.csv` is `d7c62c37…c522` | 02 |
| **Token** | no output of `start` and no launch or reader log of 02 holds `Token authentication is enabled`, `Generated new authentication token`, `InvalidAuthToken` or `AuthenticationError`; the cluster's `gcs_server` and `raylet` run with `RAY_AUTH_MODE=token`; `~/.ray/auth_token` unchanged (64 bytes, mode 600, its 12:51 mtime); no committed record contains it | 01 (V1), 02 |

**If FP fails**, nothing is changed to make it pass: the stores, the logs and `compare_stores`'s
output are recorded, and the user decides how to bisect (`ray` alone, or `SQLAlchemy` alone; each
has wheels for 3.13 at both versions).

A launch log free of the warning shows nothing by itself: a driver attaching by address never logs
it (SI's correction 4). Token's evidence is `start`'s output, the cluster's environment, and the
drivers attaching at all, which V0 shows they cannot on the pins alone.

## 3. Datastores

- **01** opens none. V0 and V1 start the cluster and run a one-line driver; neither builds a store.
- **02** builds two throwaway stores, `var/datastores/ray-sqla-align-02-reference.db` and
  `var/datastores/ray-sqla-align-02-resumed.db` (16 shards each), with their sidecars, and reads the
  first with `plot_by_beta.py`. They are kept for the user to retire with `store retire`, as
  SGK's and SI's were. No other store is opened.

## 4. The interfaces

### 4.1 Rollback

Each prompt is one commit, reverted by `git revert`. **The venv must be put back too**, since the
install is not in `git`. 01 saves the freeze before, and going back is

```bash
./venv/bin/pip install ray==2.53.0 SQLAlchemy==2.0.46
```

A revert of 01 while the cluster runs, or the lock is held, stops the cluster with the script's
`stop` first. Reverting 01's code without the venv leaves Ray 2.59 refusing the drivers (§0.2).

### 4.2 What 01 and 02 hand on

- **01 to 02.** The commit whose code 02's builds run; the venv at the new pins; the roots'
  counts; the freeze.
- **To `main`.** After 02's review, `ray-sqla-align` is merged into `main` (which has not moved since
  `a144c83`) and pushed, as the user directs.
- **To `jordan-normalization`.** `main` is then merged into `jordan-normalization` before that
  campaign's prompt 09, its rehearsal on the cluster; 05–08 start no Ray.
  - **The venv** is shared by both branches in this checkout. Once 01 has installed the pins,
    `jordan-normalization` runs on Ray 2.59.0 before the merge, with no token module. Its suites
    start no Ray, so they are unaffected; **any Ray start on it before the merge would be refused**
    (§0.2). That is the reason the merge comes before its 09.
  - **Expected conflicts:** `prompts/INDEX.md` (both branches add a row and change the header);
    `CLAUDE.md`'s count lines (both move counts); possibly `main.py`'s imports, which
    `jordan-normalization` rewrites. `plot_ScalarModel.py` is `plot_ScalarHistory.py` there, and
    its one-line import should follow the rename. The review trial-merges and records what it
    finds.
  - **09's orchestration note** states the token rule, and that a Ray CLI command against the
    cluster needs `RAY_AUTH_MODE=token`.
- **To the production machine.** Pull, and reinstall the venv from `requirements.txt`. Ray 2.59
  writes `~/.ray/auth_token` there at its first local start, or reads it at `ray start` if it
  exists; it is set to mode 600 by hand (SGK's rule). That is the user's.

## 5. The rules this campaign runs under

The project-wide ones in `CLAUDE.md`, plus, from `dk-and-rpk-migration` and `rpk-v030-adoption`:
- **deliberate breakage**: each test this campaign adds or changes catches the breakages its
  prompt names; the log records each as a diff, exactly as applied; none is committed;
- **no test needs Ray**, and none touches the real `HOME`, the real lock, the token or a real
  ChamPBH cluster;
- **a package defect** is reported upstream and never worked around here; **no change in a sibling
  repository**;
- **no `ray stop`**; the cluster only through `ray_cluster.sh`;
- **a held lock is a stop, never a wait** (exit 3: stop and ask);
- **each lock or script command is a shell command of its own**, its output redirected to a file,
  never piped, and no shell command that runs the script or a lock command names `/tmp/ray-cpbh`;
- **no polling** (`CLAUDE.md`, run-registry rule 4): one check after each launch, then a wait on
  the launch log's last line by a single foreground call, never a `sleep` loop or `pgrep`.

And:
- **Existing tests are not modified**, but the two of §0.3 that pin the distributions, as U3 rules.
- **The token is never printed or copied.** No command reads `~/.ray/auth_token` but `ls -l` (or
  `stat`) and the final `grep -F -l -f`, which lists file names only. The environment of a Ray
  process is read only through `grep -o 'RAY_AUTH_MODE=[a-z]*'`, never printed whole.
- **A Ray CLI command against ChamPBH's cluster** runs with `RAY_AUTH_MODE=token` in its
  environment once the fix is in, and is not run during V0.
- **The real-Ray checks are named here**, so that they are within `CLAUDE.md`'s "what stays": 01's
  V0 and V1, each a start, a one-line driver and a stop, about a minute; 02's three pilot builds of
  about 3–6 min each and one `plot_by_beta.py` run, as `rrk-adoption` 04's were.

### 5.1 The log template

- the subject, commit and result;
- **What shipped**;
- **Deviations from the prompt**, each classified `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE`
  or `UNINTENDED DRIFT`;
- **Verification performed**, with every count against the baselines taken before;
- **What is new in the logs**: a `DeprecationWarning`, a `FutureWarning` or a Ray line that
  `rrk-adoption` 04's logs do not have at the same point is recorded and explained, and any that
  bears on correctness opens a §3 issue. A new line is not a stop by itself;
- **The deliberate-breakage record**;
- **Observations not acted on**;
- **State handed on** (§4.2).

## 6. Decisions

### 6.1 Made by the user (2026-10-25)

| | Decided |
|---|---|
| **U0** — versions | `ray` 2.59.0 and `SQLAlchemy` 2.0.55, SGK's and SI's ("all projects on the same versions of Ray and SQLAlchemy"); ChamPBH already on Python 3.13 |
| **U1** — the shape | a campaign of its own on a branch from `main`, closed by its review, merged into `main` and then into `jordan-normalization` (the planner's recommendation, accepted) |
| **U2** — token authentication | kept, and named (`RAY_AUTH_MODE=token`), as SGK and SI do, with SI's concluded records folded into this plan |
| **U3** — recorded distributions | as SI did: a test holding `runregistrykit.toml`'s list equal to `utilities.RECORDED_DISTRIBUTIONS`, then `ray`, `SQLAlchemy`, `numpy` and `scipy` added to both |
| **U4** — prompts | two: 01 the change, 02 the rehearsal on 01's clean commit (§2.1); this amends U1's "one prompt" |

### 6.2 Proposed by the planner (P1–P12)

Accepted as proposed unless the user rules otherwise.
- **P1 — the name** `ray-sqla-align`, as SI's campaign for the same move.
- **P2 — the venv upgraded in place**, as SI's (its C2): the interpreter does not move, and the dry
  run moves two packages. The freeze before is saved (§4.1). SGK rebuilt its venv because its
  interpreter moved.
- **P3 — `config/ray_auth.py`**, SI's module, under SI's name: ChamPBH's cluster has its own temp
  dir in the script, and no driver starts a local instance, so the module holds only the mode. It
  sets `RAY_AUTH_MODE` only when the environment has none, refreshes Ray's config when
  `ray._raylet` is loaded, imports nothing from Ray, and its comment says why, citing SGK's second
  issue and V0.
- **P4 — its importers**: `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py`, each at module
  level, straight after its `import ray` and before the first import of the repository's own
  modules, with a one-line comment. The wrapper needs none: it runs `main.py` in-process. No test
  starts Ray, so no test harness imports it.
- **P5 — `ray_cluster.sh`** exports `RAY_AUTH_MODE="${RAY_AUTH_MODE:-token}"` in `cmd_start`
  before its `ray start`, with a comment; the `actors:` line it prints carries
  `RAY_AUTH_MODE=token`; the usage text says a Ray CLI command against the cluster needs it. The
  lock half, the stop, the ports and `ray start`'s arguments are untouched. As SI's C5.
- **P6 — the test**, `tools/tests/test_ray_auth.py`, beside `test_ray_cluster_lock.py` and
  `test_registry_config.py`: it guards repository-level configuration and the script, and `tools`
  is a fast root. `unittest`, in the style of the root; fresh interpreters for the module; `ast`
  for the drivers; text for the script. No Ray started, nothing written, the real `HOME` untouched.
  It holds SI's nine checks that apply here (SI's tenth is its `conftest.py`).
- **P7 — the distributions in 01**, not before it as SI's were: SI did them first because its
  planner had overstated a protection, which ChamPBH does not have. In 01 the equality test lands
  first and is shown to catch a one-sided change (B5), then the four names are added.
- **P8 — V0 and V1 in 01.** V0, on the pins alone, turns §0.2 from a reading of Ray's source into a
  measurement on ChamPBH's cluster; V1, after the fix and before the commit, keeps a fix that does
  not work from being committed. Each is about a minute under the lock.
- **P9 — 02 is `rrk-adoption` 04's H1, H2 and H4**, the clean build, the `SIGTERM` and the resume,
  with the same pilot grid and the wrapper, and its `plot_by_beta.py` run. No `SIGKILL` (§1). The
  `SIGTERM` checks that the wrapper still records `killed`/143 under Ray 2.59's signal handlers;
  the plot run is the only real-Ray check of a plot script's import.
- **P10 — outputs.** While a prompt works, what it saves goes under `var/ray-sqla-align-NN/`
  (ignored), so that 02's tree stays clean for its builds. For the commit, small text outputs are
  copied to `.documents/ray-sqla-align/out/NN-*`, after the token check. 02 writes the verification
  document `.documents/ray-sqla-align-verification.md`; 01's log and outputs are its record.
- **P11 — issues.** None opened or assigned at planning. `.documents/OPEN_ISSUES.md` holds no issue
  this campaign touches.
- **P12 — dispatch at the user's word.** Each prompt takes the shared lock, which SGK and SI use
  too; a held lock is a stop.

## 7. Baselines for 02

02 compares against `rrk-adoption` 04 (§0.4): its fingerprint and `histories.csv` digests; its stage
counts, 5 / 5 / 5 / 5 for `reference`; its wall times (5 min 40 s, 4 min 30 s to the kill, 3 min
5 s) as a guide to the load, not a gate; and its launch logs, for §5.1's "What is new in the logs".
