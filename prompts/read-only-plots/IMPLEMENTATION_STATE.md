# read-only-plots campaign — implementation state

**Last updated:** 2026-10-13 · **Status: PLANNED — 0 of 4 prompts written; awaiting the user's
rulings on README §6.1 (U1–U6) and §6.2 (P1–P8).**
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
| 01 | Require the Ray address | `--ray-address` without a default; `auto` refused (U1, U2) | Sonnet | ⬜ | ⬜ | — | — |
| 02 | Read-only plot pools | `plot_inputs.py`; both plot scripts `read_only=True`; misses per U3, U4; closes `[08-plot-scripts-open-read-write-pools]` | Opus | ⬜ | ⬜ | — | — |
| 03 | Rehearse a read-only plotting run | one pilot build and the readers, checksums before and after (U5) | Opus | ⬜ | ⬜ | — | — |
| 04 | Close | documents, handover, index, board | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–04 are written once the user has ruled on README §6.

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
