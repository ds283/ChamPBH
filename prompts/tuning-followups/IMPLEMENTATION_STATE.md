# tuning-followups campaign — implementation state

**Last updated:** 2026-10-27 · **Status: LIVE — 2 of 2 prompts written, 0 landed; R1–R4 and U1–U4
ruled by the user 2026-10-27, P1–P12 standing as proposed (README §6). Next: the orchestrator note
for 01.**
The plan is [`README.md`](README.md), written on 2026-10-27 on branch `tuning-followups`, cut from
`jordan-normalization` at `4ad15fe`.

The campaign carries out two of the user's rulings of 2026-10-27, before the science rebuild:
- `--retry-failed-scalar`, so that a stored scalar failure (a tuning that hit its cap) can be
  recomputed, with the `ScalarHistory` lookup ordered as `BBNData`'s so that a success and its old
  failures coexist;
- quiet tune lines: a history prints one only after 120 s since its last, from its 10th
  integration, or on a guard or reset step.

**Campaign:** [`README.md`](README.md) ·
**Code:** `config/argument_parser.py`, `pipeline_selection.py`, `main.py`,
`ObjectFactories/ScalarHistory.py`, `tools/bbn_from_store.py`, `ComputeTargets/ScalarHistory.py`,
`tools/history_and_bbn.py`, `tools/surfing_scan.py` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.17 (assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Retry failed scalar histories](01-retry-failed-scalar.md) | `--retry-failed-scalar`; the first and second passes; the ordered lookup; `bbn_from_store`; tests; notes | Opus | ✍️ | ⬜ | — | — |
| 02 | [Quiet tune lines](02-quiet-tune-lines.md) | the print rule (120 s, 10 integrations, guard, reset); the tools; tests; notes; §4.17; `CLAUDE.md` | Sonnet | ✍️ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Written 2026-10-27, after the user ruled U1–U4. The review of 02 closes the campaign (README §2,
U4).

### 1.1 Items

| Prompt | Items |
|---|---|
| 01 | F1 the flag · F2 the first pass · F3 the second pass · F4 the ordered lookup · F5 `bbn_from_store` · F6 the summary line · F7 the tests · F8 the notes |
| 02 | Q1 the rule and its constants · Q2 the tools · Q3 the tests · Q4 the notes · Q5 §4.17 · Q6 `CLAUDE.md` |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Retry | with the flag, a stored scalar failure is recomputed and the success is what every lookup returns; without it, nothing is recomputed; a key with a success and older failures, or two failures, looks up without raising | 01 | ⬜ |
| Quiet | a tuning under 10 integrations and 120 s prints no tune line; past either it prints by the rule; a guard or reset step prints; the stored values bitwise unchanged | 02 | ⬜ |

## 3. Active and unresolved issues

None opened.

### 3.1 Assigned to this campaign

Assigned 2026-10-27 at planning, from the user's rulings of that day. Each entry stays on its own
board, which carries the **Ruled** and **Assigned** lines; the prompt named closes it there, in that
board's §4, and deletes its row from `.documents/OPEN_ISSUES.md` §1.17.

| Issue | Its board | Prompt | Why here |
|---|---|---|---|
| `[08-a-history-that-did-not-converge-is-never-retried]` | `jordan-normalization` | 01 | R1: `--retry-failed-scalar` |
| `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` | `jordan-normalization` | 02 | R2: quiet tune lines supersede it |

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-27)

Asked by the user on 2026-10-27, after `jordan-normalization` closed (10 landed `d638738`, reviewed
`d049296`), with three rulings of the same day recorded in `4ad15fe`. Read at `4ad15fe` (README
§0.2, §0.3):
- `sqla_ScalarHistoryFactory.build` looks up with `one_or_none()` and no ordering, so a retried
  history stored beside its failure would make every later lookup of that key raise
  `MultipleResultsFound`; `BBNData`'s factory orders its lookup (`run-integrity` 03), which is why
  `--retry-failed-bbn` works. Every lookup reaches the factory's `build`, the vectorized ones
  included (`datastorekit/SQL/Datastore.py:565`). `tools/bbn_from_store.py` reads `ScalarHistory`
  by SQL with no `failure` filter.
- The tune lines exist for the heartbeat (`jordan-normalization` P12): without them the silence at
  a block's end is a whole history, since RayPoolKit prints only after a `ray.wait` returns. Three
  standalone tools call `tune_history` directly and show the lines at the terminal; two tests parse
  or count them.

No issue opened; two assigned (§3.1). `.documents/OPEN_ISSUES.md`: 25 open, two of them now in
§1.17.

### 5.1 Rulings and prompts (2026-10-27)

The user ruled U1–U4 the same day: U1 "don't care. `tuning-followups` is fine"; U2 "yes, quiet by
default. Keep one line per integration, initially" (the three standalone tools pass
`progress="every"`); U3 "I think we don't need a rehearsal"; U4 "OK" (two prompts, the review of 02
closes). P1–P12 stand. Prompts 01 and 02 written.

R3 was carried out at the user's word ("If you can add the issue directly to RayPoolKit and commit
it there"): RayPoolKit `cf77234`, a board with no prompts, `client-reports`, holding
`[00-a-pass-blocks-until-process-batch-size-tasks-complete]`. The package documents the wait as
its behaviour, with a default of 1 (RayPoolKit `README.md:161`); ChamPBH passes 3 to its three
titled queues (`main.py:330`, `:581`, `:861`). Changing ChamPBH's `process_batch_size` is not in
this campaign; it is recorded on `rrk-adoption`'s board for the user.
