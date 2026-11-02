# reflection-restart campaign — implementation state

**Last updated:** 2026-11-02 · **Status: LIVE — 0 of 4 landed.** Planned 2026-11-02 at the user's
request; U1–U5 ruled by the user the same day, U6 (the fix) to be ruled after 01 and 02, P1–P10 standing
(README §6). The plan is [`README.md`](README.md), on branch `reflection-restart`, cut from `main` at
`cc7fe75`.

The campaign takes the defect diagnosed in `.documents/post-reflection-step-2026-11-02/`: after a
reflection the restarted Radau's first outward step can be uncapped, and Radau can accept a wrong step
whose interpolant crosses φ = 0. It measures four fixes, audits the 2026.8.0 store for stored successes
that took such a step, implements the fix the user rules, and says what of the run must be rebuilt.
Under U3 as ruled, every stored success with a bad step is regenerated, whatever its observables do.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/ScalarHistory.py` (`integrate_scalar_history`), `tools/` (the scanner) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.21 (assigned)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | Measure the fixes | the scanner; F1–F4 measured; why Radau accepts the bad step | Opus | ⬜ | ⬜ | | |
| 02 | Audit the 2026.8.0 store | A, A2, B, C re-integrated at their stored normalization; bitwise reproduction; bad steps per history | Sonnet | ⬜ | ⬜ | | |
| — | the user's ruling U6 | the fix, from 01 and 02 | — | | | | |
| 03 | The fix | the ruled fix; tests; a reflecting golden history; labels; documents | Opus | ⬜ | ⬜ | | |
| 04 | Verdict on 2026.8.0 | the 42 and a sample recomputed; the verdict per block and figure; the issue closed; the handover; the close | Opus | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed. 02 runs on the
tree before 03.

Orchestrator notes, one prompt at a time, in `orchestrator/`.

### 1.1 Items

None yet.

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Signature | the scanner reproduces the diagnosis's three bad steps and its 845-step count on the nine histories | 01 | ⬜ |
| Measure | each of F1–F4 measured: the three failures, signature steps over all outward steps, golden digits, cost at each M; why Radau accepts the bad step | 01 | ⬜ |
| Audit | every scanned history reproduces its stored samples bitwise (or the exceptions listed); bad steps counted per history over A, A2, B, C | 02 | ⬜ |
| Fix | the three failures succeed; no signature step in the nine; golden histories as the ruling predicts; a reflecting golden pinned | 03 | ⬜ |
| Verdict | the 42 recomputed; the sample's observables against the store; what is rebuilt, per block and figure | 04 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

- `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` — assigned 2026-11-02 from
  the `inelastic-reflection` board (§3 there, which holds its measurements and impact). Closed by 04.

## 4. Resolved issues

None.

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-01, after `inelastic-reflection` 12): CosmologyModels
26, ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68. Since then
`745c8ee` touched `ComputeTargets/tests/test_model_interface.py` and `c95650a`
`ObjectFactories/tests/test_alpha_figures.py`, so prompt 01 measures the counts again before it changes
anything.
