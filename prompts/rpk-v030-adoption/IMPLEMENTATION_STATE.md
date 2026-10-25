# rpk-v030-adoption campaign — implementation state

**Last updated:** 2026-10-25 · **Status: PLANNED — 01 written, not dispatched.** U1 and U2 ruled by
the user; P1–P8 proposed.

The plan is [`README.md`](README.md), written on 2026-10-25 on branch `rpk-v030-adoption`, cut from
`main` at `ac187c8`, from RayPoolKit's `v0.3.0` contract and its switch-over checklist for ChamPBH.

The campaign moves ChamPBH's `raypoolkit` pin from `v0.2.0` to `v0.3.0` and replaces the lock half
of `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` with the package's
`raypoolkit.rehearsal_lock`. The lock moves to `~/.raypoolkit/rehearsal-lock`; the cluster, its
temp dir `/tmp/ray-cpbh` and its address `127.0.0.1:6380` do not change.

**Campaign:** [`README.md`](README.md) ·
**Code:** `requirements.txt`, `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`,
`tools/tests/test_ray_cluster_lock.py`, `CLAUDE.md` ·
**Verification:** 01's outputs, `.documents/rpk-v030-adoption/out/01-*.txt`, and its log ·
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
| **U1** — when and where | move to `v0.3.0` now, as a campaign of its own on a branch from `main`, merged into `jordan-normalization` before its rehearsal |
| **U2** — the other projects | each moves in a campaign of its own; no day held in common, and nothing here keeps the projects' locks consistent meanwhile |

The planner's proposals, P1–P8, are in README §6.2.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Switch to the `raypoolkit` rehearsal lock](01-switch-to-the-raypoolkit-rehearsal-lock.md) | the pin; the script's lock half replaced by the module; a test of its lock paths with no Ray; `CLAUDE.md`; the script on the real cluster under the new lock | Opus | ✍️ 2026-10-25 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

The review of 01 closes the campaign (README §2).

**Orchestrator notes** are written just before dispatch, under `orchestrator/`.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **P1** | 01 | `requirements.txt:80` at `v0.3.0`; the venv installed from it | ⬜ |
| **S1** | 01 | the script's lock half replaced by `raypoolkit.rehearsal_lock`; its cluster half unchanged; no live line names the old lock | ⬜ |
| **T1** | 01 | `tools/tests/test_ray_cluster_lock.py`: the script's lock paths, with a stub `ray` and a fake `gcs_server`, no Ray | ⬜ |
| **C1** | 01 | `CLAUDE.md`'s bullet "The rehearsal lock"; the `tools/tests` count | ⬜ |
| **R1** | 01 | the script's `start`, `status` and `stop` on the real cluster under the new lock | ⬜ |
| **B1** | 01 | the baselines: `pip freeze`, provenance, the five roots before and after | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| **E** | RayPoolKit has tagged `v0.3.0` with a contract and a checklist for ChamPBH | — | ✅ holds (2026-10-25): `v0.3.0` on `67a75b9`; `docs/rehearsal-lock.md`; `docs/adoption/rehearsal-lock/champbh.md` (at `127b2b4`, read at `feae4e5`) |
| **R** | on the real cluster, the module lists the `gcs_server`, `raylet` and client server, refuses `release` and a second `acquire` while it runs, and the script's `stop` releases it with nothing left | 01 | ⬜ |

The other projects' moves are not a gate here (U2).

## 3. Active and unresolved issues

None.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-25)

Proposed in conversation on 2026-10-25 and accepted by the user (U1); U2 the same day: every project
is switching, each in its own campaign. Measured at `ac187c8` (README §0.2): the checklist still
describes the tree (the script's blob `eaae1e7`, the acceptance grep's 5 lines); no run in the
registry; no Ray process; neither lock location exists; a dry run of the moved pin installs
`raypoolkit-0.3.0` alone; the module, run from the tag under this venv against a scratch `HOME`,
behaves as the contract says.

**Trial merge** (README §4.2): the planning commit, with 01's `CLAUDE.md` edits simulated (a bullet
after "Ray on this machine", the `tools/tests` count changed), merged by `git merge-tree` against
`jordan-normalization` at `807f175`: `CLAUDE.md` merges cleanly; `prompts/INDEX.md` conflicts, as
expected (the header's count and the rows both branches add).
