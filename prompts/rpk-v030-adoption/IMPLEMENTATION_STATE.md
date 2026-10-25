# rpk-v030-adoption campaign — implementation state

**Last updated:** 2026-10-25 · **Status: CLOSED 2026-10-25 — 1 of 1 landed (01 `95ad91d`),
reviewed; gate R holds.** U1 and U2 ruled by the user; P1–P8 stood as proposed, the user having
ruled none otherwise. Merging into `main`, and `main` into `jordan-normalization`, is the user's
(README §4.2).

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
| 01 | [Switch to the `raypoolkit` rehearsal lock](01-switch-to-the-raypoolkit-rehearsal-lock.md) | the pin; the script's lock half replaced by the module; a test of its lock paths with no Ray; `CLAUDE.md`; the script on the real cluster under the new lock | Opus | ✍️ 2026-10-25 | ✅ 2026-10-25 | `95ad91d` | [01](logs/01-switch-to-the-raypoolkit-rehearsal-lock.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

The review of 01 closes the campaign (README §2).

**Orchestrator notes** are written just before dispatch, under `orchestrator/`:
[`prompt-01.md`](orchestrator/prompt-01.md) (`5478c6e`), used for 01.

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

Landed in `95ad91d`; log [`logs/01-switch-to-the-raypoolkit-rehearsal-lock.md`](logs/01-switch-to-the-raypoolkit-rehearsal-lock.md),
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

**Orchestrator review of prompt 01 (2026-10-25).** Against `95ad91d`, by note 01's eight checks.
All pass.
- **Scope.** The eleven files of the prompt's §3 item 7 and nothing else; `git status` clean.
- **The pin and the venv.** `requirements.txt`'s diff is the one line. `raypoolkit` 0.3.0 at
  `67a75b97…`, `requested_revision` `v0.3.0`, not editable; `datastorekit` 0.4.0 at `c8570e84…` and
  `runregistrykit` 0.2.0 at `0bae3a3c…` unchanged; `pip check` clean; `--help`'s first line as the
  contract says.
- **The script.** Its diff read whole. Extracted from both trees and compared with `cmp`, the cluster
  half (`temp_dir_regex` through `stop_cluster`, 113 lines), the cluster's values, `say`/`err`,
  `ray start`'s arguments and the sourcing guard are identical. Every lock call passes the values
  from the variables; `cmd_stop` refuses before `stop_cluster`, also when `default_lock_dir()`
  cannot be asked (ruling 4); `bash -n` and `--help` exit 0.
- **The test.** Read against §2.4 and rulings 1–4. Run alone: 9 tests OK in 11.4 s (load 7.6);
  afterwards no process names a test temp dir, and `~/.raypoolkit` is empty. It reads the owner
  file through `rehearsal_lock.parse_owner`, a public name of the module that the contract's §2 does
  not list; a rename upstream would fail the test loudly, the safe side, so it is left as it is.
- **The breakages.** M1–M6 extracted from the committed `01-test.txt`, each checked, applied,
  checked in reverse, run and reverted in `bash`: M1 fails tests 3, 4 and 6; M2 test 5; M3 test 9;
  M4 test 7; M5 and M6 test 1, as note 01 expected. The script and `requirements.txt` hashes were
  restored and `git status` was clean.
- **The grep.** The checklist's item 8.2 at `95ad91d`: 0 lines; the script names the module on 10.
- **The real cluster.** `01-cluster.txt` read against §2.8: the module listed the `gcs_server`
  (89972), the `raylet` (89987) and the client server (89974), the same 18 pids as the script's
  listing; `release` 1, a second `acquire` 3 and a second `start` 3 while it ran; `stop` 0 and the
  lock released. Now: the script's `status` shows the lock free, no processes, the ports free;
  neither location exists. **Gate R holds.**
- **The records and the roots.** `CLAUDE.md`'s bullet after "Ray on this machine" (unchanged),
  naming no old location; `tools/tests` 93 and the date sentence. The board's items and gate R; the
  log in §5.1's form, 14 deviations classified, none `UNINTENDED DRIFT`. The five roots at
  `95ad91d`, serially (load 22 → 6): 18 / 61 + 115 / 78 / 93 / 61, all OK; `black --check` clean.

**The `describe_pid` cut** (the log's first observation): the real `gcs_server` is found with 46
columns to spare on this machine, and the cut is in the cluster half, which RayPoolKit's phase 2
may take over. Nothing fails today, so no issue is opened. Residue fixed in this commit: "this
commit" replaced by `95ad91d` here and in the log; note 01 listed; the header closed. The campaign
is closed. Merging `rpk-v030-adoption` into `main`, and `main` into `jordan-normalization` before
its 09, is the user's (README §4.2).

**Merged (2026-10-25).** At the user's request `main`, unmoved since `ac187c8`, fast-forwards to the
commit that adds this paragraph, and `main` is then merged into `jordan-normalization` (README
§4.2). Neither branch is pushed by that request; the branch `rpk-v030-adoption` is kept.
