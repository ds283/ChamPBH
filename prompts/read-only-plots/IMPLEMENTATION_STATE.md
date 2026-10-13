# read-only-plots campaign — implementation state

**Last updated:** 2026-10-13 · **Status: LIVE — 4 of 4 prompts written, 0 landed. U1–U6 and P1–P8
ruled by the user on 2026-10-13, all as proposed (README §6).**
The plan is [`README.md`](README.md), written on 2026-10-13 on branch `read-only-plots`, cut from
`main` at `17fad8b`.

The campaign makes `--ray-address` required, with `auto` refused, and opens both plot scripts on
read-only pools. Their shared store-reading setup moves into an importable module, so that a test
can drive it on a stand-in store. If U5 is so ruled, a rehearsal shows that a real plotting run
changes no file of the store it reads.

**Campaign:** [`README.md`](README.md) ·
**Code:** `config/argument_parser.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, a new
`plot_inputs.py` ·
**Verification:** `.documents/read-only-plots-verification.md` (created by 03, if U5 is "rehearse") ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.12

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Require the Ray address](01-require-the-ray-address.md) | `--ray-address` without a default; `auto` refused (U1, U2) | Sonnet | ✍️ 2026-10-13 | ⬜ | — | — |
| 02 | [Read-only plot pools](02-read-only-plot-pools.md) | `plot_inputs.py`; both plot scripts `read_only=True`; misses per U3, U4; closes `[08-plot-scripts-open-read-write-pools]` | Opus | ✍️ 2026-10-13 | ⬜ | — | — |
| 03 | [Rehearse a read-only plotting run](03-rehearse-a-read-only-plotting-run.md) | one pilot build and the readers, checksums before and after (U5) | Opus | ✍️ 2026-10-13 | ⬜ | — | — |
| 04 | [Close](04-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-13 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–04 are written once the user has ruled on README §6.
*Written 2026-10-13 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **A1** | 01 | `--ray-address` with no default; `ray_address_problem()` refusing a missing value and `auto` | ⬜ |
| **A2** | 01 | the check before each driver's `ray.init`, exit 2; `main.py --inventory` exempt (U1) | ⬜ |
| **A3** | 01 | `ComputeTargets/tests/test_ray_address.py`: the function, the parser, the drivers by subprocess and `ast` | ⬜ |
| **A4** | 01 | `CLAUDE.md`'s "Ray on this machine"; `architecture-summary.md` §5.1 | ⬜ |
| **O1** | 02 | `plot_inputs.py`: the shared setup as functions of a pool; potentials and couplings one at a time (P1, P2) | ⬜ |
| **O2** | 02 | a missing parameter stops the run (U4); a missing potential or coupling is skipped with a warning (U3) | ⬜ |
| **O3** | 02 | both plot scripts on `plot_inputs.py`, `read_only=True`; `--drop` refused (P3) | ⬜ |
| **O4** | 02 | `ComputeTargets/tests/test_plot_inputs.py`, on read-only stand-in stores; every file unchanged | ⬜ |
| **O5** | 02 | `[08-plot-scripts-open-read-write-pools]` closed; the text tests (P6) | ⬜ |
| **H1** | 03 | one pilot build under real Ray | ⬜ |
| **H2** | 03 | the readers: every store file byte-identical; `histories.csv` equal to R0's | ⬜ |
| **H3** | 03 | under real Ray: `auto` refused, a missing `T_stop` stops, `--drop` refused | ⬜ |
| **H4** | 03 | `.documents/read-only-plots-verification.md` §1 | ⬜ |
| **Z1** | 04 | `architecture-summary.md`: `plot_inputs.py` and read-only plot pools | ⬜ |
| **Z2** | 04 | `CLAUDE.md` | ⬜ |
| **Z3** | 04 | the handover, `.documents/review-remediation-verification.md` §4.13 | ⬜ |
| **Z4** | 04 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Ray address | every driver refuses a run that starts Ray without `--ray-address`, and refuses `auto`, before `ray.init` | 01 | ⬜ |
| Read-only plots | both plot scripts open `read_only=True`; on a stand-in store their lookups find every row, skip a missing potential or coupling, stop on a missing parameter, and change no file | 02 | ⬜ |
| R | a real plotting run on a real store changes no file of it (U5) | 03 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

- `[08-plot-scripts-open-read-write-pools]` (on the [`dk-and-rpk-migration`](../dk-and-rpk-migration/IMPLEMENTATION_STATE.md)
  board §3, which carries the **Assigned** line): to 02.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-13)

Planned at the user's request, after `dk-and-rpk-migration` closed and its branch was merged
(`78769a0`). The user had already ruled that the science rebuild waits for `jordan-normalization`,
so this campaign does not touch it.
- **The measurement** (README §0.3) was read-only at `17fad8b`. A subagent read both plot scripts,
  their helpers, every factory's `build` and the package at `v0.3.0`; the orchestrator checked the
  load-bearing lines.
- **Rows 1–9 can be refused under `read_only=True`.**
  - Rows 1–5, 8 and 9 are the parameters; each is found if the plot run is given the build's
    options.
  - Row 6 is the potentials, over every stored M × Λ, which `main.py` never stored as a product.
  - Row 7 is the couplings: a β whose coupling an interrupted build never stored.
- **The history lookups can never be refused**: their `build`s return an unavailable object on a
  miss.
- **Precedent.** StochasticInstantons made `--ray-address` required at its `d6d550b`, and
  SecondaryGWKit assigned the same change to its `handover` campaign (its `0fbbc4e`).
- **The issue.** `[08-plot-scripts-open-read-write-pools]` is assigned to 02 (P7). Its row moves to
  a new `.documents/OPEN_ISSUES.md` §1.12. The count stays 27.

### 5.1 Ruled, and prompts 01–04 written (2026-10-13)

The user accepted U1–U6 and P1–P8 as proposed:
- **U1:** `--ray-address` required whenever Ray starts; `main.py --inventory` exempt.
- **U2:** `auto` refused.
- **U3:** a missing potential or coupling skipped with a warning.
- **U4:** a missing parameter row stops the run.
- **U5:** rehearse (03).
- **U6:** the package's refusal of another `VERSION_LABEL` accepted.

The user also kept `CLAUDE.md`'s "Ray on this machine" rule, added by `dk-and-rpk-migration`'s
orchestrator. Prompts 01–04 were written the same day, from the measurement of README §0.3 and a
read of the drivers' start-up code at `bacca64`.
- **The store fixture.** `StoreFixtures/standin_pool.py` patches `ray.get`, so a module that calls
  `ray.get` runs inside it. Its `open_pool` takes `read_table_config=` and `read_only=` as keyword
  arguments.
- **The rehearsal scripts.** `build.sh` and `run_readers.sh` read `REHEARSAL_ROOT`, so 03 runs them
  unchanged under `read-only-plots/`.

