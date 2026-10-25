# ray-sqla-align campaign — implementation state

**Last updated:** 2026-10-25 · **Status: 01 landed, awaiting review — 1 of 2 landed, 0 of 2
reviewed.** U0–U4 ruled by the user the same day; P1–P12 stand as proposed unless the user rules
otherwise. 02 is held until 01's review.

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
| 01 | [Move `ray` and `SQLAlchemy`, with token mode named](01-move-ray-and-sqlalchemy-with-token-mode.md) | the pins and the venv; V0; the distributions; `config/ray_auth.py`, its importers and the script; the test; V1; `CLAUDE.md` | Opus | ✍️ 2026-10-25 | ✅ 2026-10-25, awaiting review | this commit | [01](logs/01-move-ray-and-sqlalchemy-with-token-mode.md) |
| 02 | [Rehearse registered runs under Ray 2.59](02-rehearse-registered-runs-under-ray-2.59.md) | `reference`, `resumed-a` (`SIGTERM`), `resumed-c` and `plot_by_beta.py` on 01's code; gates FP and Token | Opus | ✍️ 2026-10-25 · ⏸ until 01's review | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

The review of 02 closes the campaign (README §2).

**Orchestrator notes** are written just before dispatch, under `orchestrator/`.

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
| **R1** | 02 | `reference`: `done`/0; fingerprint `2dd4b39b…932f` | ⬜ |
| **R2** | 02 | `resumed-a`: `SIGTERM`ed, `killed`/143, the store partial | ⬜ |
| **R3** | 02 | `resumed-c`: `done`/0, only the missing rows computed; fingerprint `2dd4b39b…932f` | ⬜ |
| **R4** | 02 | `compare_stores` equal; manifests `git_dirty` false, naming `ray` 2.59.0 and `SQLAlchemy` 2.0.55 | ⬜ |
| **R5** | 02 | `plot_by_beta.py` on the reference: `histories.csv` `d7c62c37…c522`; the store unchanged | ⬜ |
| **W1** | 02 | `.documents/ray-sqla-align-verification.md` §1 | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| **V** | on the pins alone the cluster is in token mode and refuses a driver attaching by address; with the fix, `start` prints no warning and a driver in the wrapper's import order attaches and runs a task | 01 | ✅ 01 (V0, V1) |
| **FP** | 02's two stores, under `ray` 2.59.0 and `SQLAlchemy` 2.0.55, each fingerprint to `2dd4b39b…932f`; `compare_stores` equal but for bookkeeping; `histories.csv` `d7c62c37…c522` | 02 | ⬜ |
| **Token** | no `start` output, launch log or reader log holds Ray's token warning or an authentication error; the cluster's `gcs_server` and `raylet` run with `RAY_AUTH_MODE=token`; the token file unchanged at mode 600; no record holds the token | 01 (V1), 02 | V1 part ✅ 01; 02's part ⬜ |

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

Dispatched at `551b54c` (the orchestrator's note); landed in this commit. Log:
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
