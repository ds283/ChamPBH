# rpk-v030-adoption campaign — implementation state

**Last updated:** 2026-10-25 · **Status: 01 landed, awaiting review.** U1 and U2 ruled by the user;
P1–P8 proposed.

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
| 01 | [Switch to the `raypoolkit` rehearsal lock](01-switch-to-the-raypoolkit-rehearsal-lock.md) | the pin; the script's lock half replaced by the module; a test of its lock paths with no Ray; `CLAUDE.md`; the script on the real cluster under the new lock | Opus | ✍️ 2026-10-25 | ✅ 2026-10-25 | this commit | [01](logs/01-switch-to-the-raypoolkit-rehearsal-lock.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

The review of 01 closes the campaign (README §2).

**Orchestrator notes** are written just before dispatch, under `orchestrator/`.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **P1** | 01 | `requirements.txt:80` at `v0.3.0`; the venv installed from it | ✅ the dry run added `raypoolkit-0.3.0` alone; `raypoolkit` 0.3.0 at `67a75b97…`, not editable; the `pip freeze` diff is that one line; `pip check` clean |
| **S1** | 01 | the script's lock half replaced by `raypoolkit.rehearsal_lock`; its cluster half unchanged; no live line names the old lock | ✅ the cluster half, values, port check and sourcing guard identical to `HEAD` by `diff`; the checklist's grep 5 → 0; 10 lines name the module |
| **T1** | 01 | `tools/tests/test_ray_cluster_lock.py`: the script's lock paths, with a stub `ray` and a fake `gcs_server`, no Ray | ✅ 9 tests, 11.5 s; M1–M6 each caught, as the note expected |
| **C1** | 01 | `CLAUDE.md`'s bullet "The rehearsal lock"; the `tools/tests` count | ✅ the bullet after "Ray on this machine"; `tools/tests` 93; merges cleanly with `jordan-normalization` (`git merge-file`) |
| **R1** | 01 | the script's `start`, `status` and `stop` on the real cluster under the new lock | ✅ 2026-10-25; the module's and the script's listings the same 18 pids; gate R |
| **B1** | 01 | the baselines: `pip freeze`, provenance, the five roots before and after | ✅ 18 / 61 + 115 / 78 / 84 / 61 before, 18 / 61 + 115 / 78 / 93 / 61 after, all OK |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| **E** | RayPoolKit has tagged `v0.3.0` with a contract and a checklist for ChamPBH | — | ✅ holds (2026-10-25): `v0.3.0` on `67a75b9`; `docs/rehearsal-lock.md`; `docs/adoption/rehearsal-lock/champbh.md` (at `127b2b4`, read at `feae4e5`) |
| **R** | on the real cluster, the module lists the `gcs_server`, `raylet` and client server, refuses `release` and a second `acquire` while it runs, and the script's `stop` releases it with nothing left | 01 | ✅ holds (2026-10-25, 01): `gcs_server` 89972, `raylet` 89987 and client server 89974 listed; `release` 1, a second `acquire` 3 and a second `start` 3; `stop` 0, the lock released, nothing left |

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

### 5.1 After prompt 01 (2026-10-25)

Landed in this commit; log [`logs/01-switch-to-the-raypoolkit-rehearsal-lock.md`](logs/01-switch-to-the-raypoolkit-rehearsal-lock.md),
outputs `.documents/rpk-v030-adoption/out/01-*.txt`. The note's six rulings were applied as the log
classifies them. No §4 stop condition was met, and no issue was opened, so §3 and §4 stay empty and
`.documents/OPEN_ISSUES.md` is unchanged.

- **The venv holds `raypoolkit` 0.3.0** at `67a75b97…` (`requested_revision` `v0.3.0`, not
  editable). `datastorekit` and `runregistrykit` are unchanged.
- **The script** calls `raypoolkit.rehearsal_lock` for `acquire`, `set-pid`, `release` and `status`.
  `stop` reads the owner file at the module's `default_lock_dir()`. The script's own stale-cluster
  check is gone, and its cluster half is unchanged.
- **The roots:** 18 / 61 + 115 / 78 / 84 / 61 before, 18 / 61 + 115 / 78 / 93 / 61 after.
- **On the real cluster:** the module's listing and the script's are the same 18 pids. `gcs_server`
  starts at column 104 of the real `gcs_server`'s 150-column `describe_pid` line, which the log
  records under "Observations not acted on", with no issue (ruling 1). `stop` SIGKILLed the
  `gcs_server` once the 15 s grace was over, as in earlier campaigns' transcripts.
