# reflection-restart campaign — implementation state

**Last updated:** 2026-11-02 · **Status: CLOSED 2026-11-02 — 3 of 3 landed (01, 03, 04; 02 withdrawn).** Planned 2026-11-02 at the
user's request; U1–U5 ruled by the user the same day, P1–P10 standing (README §6). Re-planned the same day
after 01, at the user's word (README §6.3): the signature narrowed to the first step of a restarted Radau;
02 withdrawn, since `quanta-diagnostics` rebuilds the whole store; U3–U5 lapsed. U6 ruled the same day: F3 plus the guard
(README §6.3). Prompt 03 landed the same day: F3 and the guard are the production code, `VERSION_LABEL`
2026.9.0. The handover was restated by the user the same day (README §6.4): the rebuild of record follows
`one-loop-drain` 08 at 2026.10.0, and no store of record is built at 2026.9.0. Prompt 04 written the same
day, and landed the same day: the 42 recomputed (41 succeed), the issue closed, the fixed-α turning-point failures
diagnosed (verdict (ii): a limit of the wall search that predates this campaign, met through F3's new step grid), the
handover written; the campaign closed. `reflection-restart` is merged into `one-loop-drain` at the user's word (that
campaign's U1); 04 merged nothing. The plan is [`README.md`](README.md), on branch `reflection-restart`, cut from `main` at
`cc7fe75`.

The campaign takes the defect diagnosed in `.documents/post-reflection-step-2026-11-02/`: after a
reflection the restarted Radau's first outward step can be uncapped, and Radau can accept a wrong step
whose interpolant crosses φ = 0. It measures four fixes, implements the fix the user rules, and hands over
to the rebuild of record, which rebuilds the whole store (README §6.3).

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
| 01 | [Measure the fixes](01-measure-the-fixes.md) | the scanner; F1–F4 measured; why Radau accepts the bad step | Opus | ✅ | ✅ | `d4703df` | [log](logs/01-measure-the-fixes.md) |
| ~~02~~ | ~~Audit the 2026.8.0 store~~ | **withdrawn 2026-11-02** (README §6.3): the whole store is rebuilt | — | — | — | | |
| — | the user's ruling U6 | F3 plus the guard (README §6.3), 2026-11-02 | — | ✅ | ✅ | | |
| 03 | [The fix](03-the-fix.md) | F3 and the guard, the probe switch removed; tests; a reflecting golden history; labels; documents | Opus | ✅ | ✅ | `b81eab8` | [log](logs/03-the-fix.md) |
| 04 | [The close](04-close.md) | the 42 recomputed with the fix; the fixed-α turning-point failures of 2197 and 2188 investigated on two trees; the issue closed; the handover to the rebuild (README §6.4); the close | Opus | ✅ | ✅ | this commit | [log](logs/04-close.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed · ~~struck~~ withdrawn.

Prompts are written one at a time, each after the previous one lands and is reviewed. (02, which was to
run on the tree before 03, is withdrawn: README §6.3.)

Orchestrator notes, one prompt at a time, in `orchestrator/`:
- [`prompt-01.md`](orchestrator/prompt-01.md) (`3b8a553`) — used for 01.
- [`prompt-03.md`](orchestrator/prompt-03.md) (`89a0760`) — used for 03.
- [`prompt-04.md`](orchestrator/prompt-04.md) (`741dcce`) — used for 04.

**Orchestrator review of prompt 01 (2026-11-02)**, by orchestrator note 01 §3, on `d4703df`. Passed; seven
checks:
1. **Scope.** The commit's parent is `3b8a553`, the note's. It holds ruling 7's 21 files and nothing else
   (`CLAUDE.md`'s test counts only), and nothing is pushed or left untracked. In
   `ComputeTargets/ScalarHistory.py` the diff holds only the switch, its values, the `first_step` argument
   F3 needs and `probe_retakes`. F4 rebuilds Radau from `N = solver.t` at the top of the loop, which is the
   start of the discarded step, before `accepted_steps` is counted.
2. **Nothing left running.** Neither recorded pid (4986, 2091) answers `kill -0`. `runregistrykit list`
   is unchanged: 23 runs, all finished.
3. **Bitwise at None.** Run by the orchestrator: `test_golden_history`, `test_store_fingerprint_golden` and
   `test_tuning`, 48 OK in 144 s. Two reflection-store histories were re-integrated at their stored
   normalization by the orchestrator's own probe, not the scanner. β = 2.9, M = 4×10⁻³¹, α = 0.08 has 5926
   samples, and β = 2.0, M = 10⁻¹⁵, α = 0.01 has 5437. Both are bitwise in raw N, φ, π, Σ and g*_ρ.
4. **Signature.** The scanner was re-run on β = 1.14, M = 10⁻¹⁵, α = 0.01 (failing) and on β = 1.2,
   M = 10⁻¹⁵, α = 0.03 (clean). It counts 270 and 174 first outward steps, as `scan_first_steps.txt` does.
   It finds one bad step in the first and none in the second. The bad step has N₀ = 37.67089767,
   h = 1.61×10⁻⁴, h·ω = 1.07×10³ and π ratio 8.79×10⁻⁵, as printed there.
5. **The variants.** The switch's code was read against prompt §1.2 and rulings 3–4. β = 1.5, M = 10⁻¹²,
   α = 0.05 was re-run under F3 and under F4, and both succeed. Under F3: 7183 accepted steps, and no
   flagged first step. Under F4: 7123 steps, 59 retakes, and the bad step itself among the discarded steps.
   These are the record's numbers.
6. **§1.4.** `measure/why_radau_accepts.py 1.5 1e-12 0.05` was re-run, and its output is identical to the
   committed `out/` file. SciPy 1.17.0's `radau.py` (lines 485–487) refines the error estimate only
   `if rejected and error_norm > 1`, as §1.5 says.
7. **The records.** The log is in §5.1's form, with ruling 1–7 deviations classed. The verification
   document §1 and the `out/` files are ASCII text. Row 01, the items and the gates are on this board, and
   the issue is in §3 and in the index at §1.22.

Residue fixed in the review commit: "this commit" → `d4703df` in the log and here. The count in
`prompts/INDEX.md` (26 → 27), which the log noted as outside the agent's files. Dated notes on 845 → 835 in
the diagnosis README and in the campaign README §2.1.

**Orchestrator review of prompt 03 (2026-11-02)**, by orchestrator note 03 §3, on `b81eab8`. Passed; seven
checks. The prompt stopped once, before its commit, on two failures in `test_release.py`. The user ruled
both changes in ("Please allow both"): F3 clamped to the solver's bound, and `BETA_5_FAILED_CHECK_N`
re-pinned (log 03, D16 and D17).
1. **Scope.** The commit's parent is `89a0760`, the note's. Its 36 files are ruling 8's, plus
   `test_release.py` at the user's ruling and `CLAUDE.md`'s counts. Nothing is pushed or left untracked.
   `grep -i probe ComputeTargets/ScalarHistory.py` finds only the Jacobian's "probe" comments.
   `FINGERPRINT_FORMAT` is 6.
2. **Nothing left running.** The replay's pid 28532 does not answer `kill -0`, and no scan or unittest
   process is alive. `runregistrykit list` is unchanged: 23 runs, all finished.
3. **The goldens.** Run by the orchestrator: `test_golden_history`, `test_store_fingerprint_golden`,
   `test_tuning`, `test_golden_reflecting_history`, `test_restart_fix`, `test_release` and
   `test_fingerprint_format_guard`, 87 OK in 199 s. `fix/compare_goldens.py` reports "differences not
   allowed: 0".
4. **The replay.** Three tasks were re-run by the orchestrator on `b81eab8`: the β = 1.5 failure (tuned),
   the store task β = 2.9, M = 4×10⁻³¹, α = 0.08, and the tuned golden. Each is equal to 01's F3 line in
   outcome, both SHA-256 digests, sample count, steps, RHS evaluations, compute steps and reflections. The
   store comparison equals 01's F3 comparison, the golden hook says bitwise, and `restart_retakes` is 0.
5. **The guard.** The code was read against prompt §1.2 and rulings 5–6:
   - `new_solver` alone arms the mark, for a restart with π > 0 or a retake.
   - The mark is cleared at the first accepted step, and the step's start is copied before the step.
   - The clamp `min(first_step, N_bound − N)` applies to F3's value only.

   A probe of the orchestrator's (scratchpad) then ran the whole tuned β = 1.5, M = 10⁻¹², α = 0.05
   history through the scanner:
   - with `_restart_first_step` replaced to return SciPy's choice, it succeeds with one retake (the
     signature discarded once, kept never);
   - with the guard's predicate also replaced to return False, it fails with the original
     `m_eff^2 = -1 e^-225.0798555`.
6. **The labels.**
   - `config/version.py` has a 2026.9.0 entry, which says lookups are version-keyed and a 2026.8.0 store
     is read with its own tree.
   - `main.py` registers and maps stepping 1 beside stepping 0.
   - Both plot scripts select stepping 1 only.
   - `test_kinematic_cap_loop`'s label test follows.
7. **The records.**
   - The log is in §5.1's form, D1–D17 classed, with the note's rulings and the user's two among them.
   - The verification document gains §2 and its diff only adds lines. `fix/out/` is ASCII text.
   - The notes in `numerical-strategies.md` and `architecture-summary.md` are dated.
   - Row 03, the items and the Fix gate are on this board, and no issue was opened.

Residue fixed in the review commit:
- "this commit" → `b81eab8` in the log, here and in verification §2;
- the board's stale sentence on 02;
- `prompts/INDEX.md`'s row;
- two comments in `test_release.py` that gave the release step's end as 40.29089 only.

### 1.1 Items

| Item | What | Prompt | State |
|---|---|---|---|
| 01-a | the scanner `tools/outward_step_scan.py` (every outward Radau step, P1's flag, the store comparison, the batch) and its 22 tests in `tools/tests/test_outward_step_scan.py` | 01 | ✅ |
| 01-b | the probe switch `_PROBE_OUTWARD_FIX` (None, F1, F2, F3, F4, F3+F4) and `_PROBE_STIFFNESS_C` in `ComputeTargets/ScalarHistory.py`, `IntegrationResult.probe_retakes`; 10 tests in `ComputeTargets/tests/test_probe_outward_fix.py`; removed by 03 | 01 | ✅ |
| 01-c | the measurement (`.documents/reflection-restart/measure/`, `out/`) and `.documents/reflection-restart-verification.md` §1 | 01 | ✅ |
| 03-a | F3 (`_restart_first_step`) and the guard (`_outward_step_flagged` on the first accepted step of a Radau restarted with π > 0, retaken at h/2, failing below h_floor) in `integrate_scalar_history`, at the reflection, the drain and the release; `IntegrationResult.restart_retakes`, the payload's `"restart_retakes"`, `extra_data`'s `restart_retakes` only above 0; F3's step at most the distance to the solver's bound (the user's ruling after 03's stop, "Please allow both"); 14 tests in `ComputeTargets/tests/test_restart_fix.py`; `test_release.py`'s `BETA_5_FAILED_CHECK_N` re-pinned 40.29089 → 40.29068 at the same ruling | 03 | ✅ |
| 03-b | the probe switch removed (01-b's module code and its 10 tests); `tools/outward_step_scan.py` on the production code (`--variant` gone, a variant refused, `restart_retakes` and the signature's counts), its tests 22 → 24 | 03 | ✅ |
| 03-c | the labels: `VERSION_LABEL` 2026.9.0, `Radau+kinematic-cap-stepping1` (registered in `main.py`, selected by the plot scripts), `FINGERPRINT_FORMAT` 6 unchanged; the tests that name them | 03 | ✅ |
| 03-d | the golden records `golden_history_rr03.json` and `golden_history_rr03_tuned.json` (old records' every field but the label and the added `restart_retakes`), and the reflecting golden `golden_history_rr03_reflecting.json` (β 1.5, M 10⁻¹², α 0.05) with its module and test | 03 | ✅ |
| 03-e | the replay of 01's 66 F3 tasks (equal in all 66), `.documents/reflection-restart/fix/` and `out/`, verification §2, the dated notes in `numerical-strategies.md` and `architecture-summary.md` | 03 | ✅ |
| 04-a | the 42 recomputed tuned on the fixed tree (`var/reflection-restart/04/the-42.jsonl`): 41 succeed, 1 fails (4389, the effective minimum not found, opened §3); no `log_m_eff_squared` failure, no signature step, no retake (verification §3.1) | 04 | ✅ |
| 04-b | the fixed-α turning-point failures: 2197 and 2188 at `89a0760` (success, bitwise to the store) and on this tree (the probe's failure, to every character); F3 off with the guard live bitwise `89a0760`'s; the failing call alone, the roots at 1.51 and 1.58 quanta, the nine's at 0.11–16.54; the margins along each history; the nine on the fixed tree (8 fail at 0.65–6.70 quanta, 2386 succeeds); verdict (ii) (verification §3.2–§3.6) | 04 | ✅ |
| 04-c | `.documents/reflection-restart/close/` and `out/`, verification §3; the issue closed (§4, the `inelastic-reflection` pointer); the fixed-α issue narrowed (§3.1); the handover notes (the run record's README and `fig6b/PLAN.md` §3, `review-remediation-verification.md` §4.19, the diagnosis README) | 04 | ✅ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Signature | the scanner reproduces the diagnosis's three bad steps and its 845-step count on the nine histories | 01 | ✅ 2026-11-02: 835 = 835 (the diagnosis's sum; 845 was its slip, orchestrator note 01 ruling 1), every printed line identical, no accounting difference (verification §1.2) |
| Measure | each of F1–F4 measured: the three failures, signature steps over all outward steps, golden digits, cost at each M; why Radau accepts the bad step | 01 | ✅ 2026-11-02 (verification §1.4–§1.6); its proviso, `[01-p1-flag-fires-on-legitimate-steps]`, closed by the user's ruling of the signature (§4) |
| ~~Audit~~ | withdrawn with 02 (README §6.3) | — | — |
| Fix | the three failures succeed; no signature step (as ruled, README §6.3) in the nine; golden histories as the ruling predicts; a reflecting golden pinned | 03 | ✅ 2026-11-02: the three succeed (7183, 49 765, 27 665 steps, no retake); the replay of 01's 66 F3 tasks equal to 01's F3 in every field, 66 of 66, the guard never firing; no signature step kept or discarded; the α = 0 goldens equal but for the label and `restart_retakes` = 0; the reflecting golden pinned (verification §2) |
| Close | the 42 recomputed and their outcome stated; the issue closed; the handover written | 04 | ✅ 2026-11-02: the 42 recomputed, 41 succeed and 1 fails in the effective minimum (opened §3), no `log_m_eff_squared` failure and no signature step; the issue closed (§4); the handover written; the fixed-α question answered, verdict (ii) (verification §3.7) |

## 3. Active and unresolved issues

None open on this board (the one opened by prompt 01 closed 2026-11-02, §4). Prompt 03 opened none (log 03 §7).
*(2026-11-02, prompt 04: opened one, below. The campaign closed with it open and not assigned.)*

### `[04-a-recomputed-history-does-not-find-the-effective-minimum]`

**Opened:** 2026-11-02 by prompt 04, from the 42 recomputed (prompt §1.1: a failure of a kind not open as an issue is
opened; verification §3.1).

- **What.** 4389 (shard0015 of `science-2026.8.0`; β = 1.13, M = 10⁻¹² M_P, α = 0.01), one of the 42 histories that
  failed in `log_m_eff_squared` in the 2026.8.0 run, recomputed tuned on the fixed tree (`b81eab8`'s computing code),
  passes that failure and fails after its seed integration (14 318 steps, 59 reflections, to N = 45.724): "the
  effective minimum was not found: effective_minimum (ExponentialModel(M=2.4353e+06GeV,n=1,beta=1.13)): V_eff' does
  not change sign on the bracket [8.20971e-05, 0.0328388] (values 3.724e-121, 3.86555e-121) (tuning integration 0)".
  Its stdout also prints a negative G (`ODEPolicy`, "negative value of G = -0.00070779") and the
  not-relativistic-at-birth margin warning. Record: `var/reflection-restart/04/the-42.jsonl`.
- **Impact.** One history of block C (Figure 6b's M_crit comparison) above the surfing threshold β = 1.1074 still
  fails on the fixed code, for a reason other than the defect this campaign fixed. The run's own effective-minimum
  failures (A 8, B 16, C 25 at β ≤ 1.1074, and one in D at M = 0.5, β = 20; run record "Failed histories") are none of
  them in block C above the threshold, and the user left the kind undiagnosed in the non-surfing region (`PLAN.md`
  §8). Whether other C histories above the threshold would meet it under the rebuild's code is not known.
- **Next step.** For the user: diagnose it (V_eff' positive across the bracket after the seed integration), before or
  with the rebuild of record. Not fixed here (prompt §2). Not assigned.

### 3.1 Assigned to this campaign

- `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` — assigned 2026-11-02 from
  the `inelastic-reflection` board (§3 there, which holds its measurements and impact). Closed by 04, with
the handover saying the whole store is rebuilt (README §6.3). *(2026-11-02: closed by prompt 04, this commit;
  the entry is in §4.)*
- `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]` — assigned 2026-11-02 from the
  `one-loop-drain` board (§3 there, on branch `one-loop-drain` at `c07839e`, which holds its measurements and
  logs), at the user's ruling (that campaign's README §6.3, ruling 6). 2197 and 2188 (4×10⁻³¹ M_P, α = 10⁻³)
  fail on `2d50b95` at their stored normalization, after about 115 s in `classical_turning_point`
  (ln E = −306.7), though both completed on `0209aac`. **For 04:** find whether 03 (F3 changes the
  trajectory after every restart) causes it, by running both at `b81eab8^` and `b81eab8`; then record, and
  either fix it within this campaign's scope or say where it goes. The campaign that found it plans to move
  laboratory-M histories to a one-loop drain, under which both complete, but it keeps the fixed α as a law.
  *(2026-11-02, prompt 04 written: §1.2 there. The planner found that the 2026.8.0 store already holds nine
  "not monotone" failures of `classical_turning_point` at laboratory M, made before this campaign's code;
  04 §0.2 lists them, with a reading to be tested.)*
- **Narrowed (2026-11-02) by prompt 04** (this commit; verification §3.2–§3.6): **verdict (ii)**, 03 moves a correct
  trajectory onto a limit of the wall search that was already there.
  - **What F3 does.** At `89a0760` both succeed, bitwise to the store; on this tree both fail with the probe's message
    to every character. With `_restart_first_step` returning SciPy's choice and the guard live, this tree gives
    `89a0760`'s history bit for bit, and the guard discards nothing: F3 alone moves them.
  - **The cause.** The failing call, `classical_turning_point(−276.63072859401797, φ, ln E)`, raises alone at both
    trees in 0.1 ms; the wall search is unchanged since the store's tree `75208cd`. Its root, ln V − ln V_floor =
    ln(1 + E/V_floor), lies at **1.51** (2197) and **1.58** (2188) quanta of ulp(ln V_floor), inside the region
    `_log_V_minus_floor` treats as −∞ (`WALL_ROUNDING_QUANTA` = 16). Every finite residual is then positive, the outer
    end stays −∞, and the further halvings reach the region's edge, 2⁻⁵ e-folds apart, where two probes round to the
    same 17 quanta and "not monotone" is raised. At laboratory M such a root is any E/V_floor below about 9.7×10⁻¹³.
  - **Where it is met.** The wall is assessed at every step start with π < 0. Its least margin, on both trees, is at
    the first step start of an infall, just after the plateau turn, where ρ_kin is as small as the step grid puts that
    start near the apex; the infall's later calls carry its energy, which runs on smoothly. Before 03 the least
    margins were **208.6** (2197) and **1257.5** (2188) quanta; on this tree, before the failing call, 2.83×10⁶ and
    6.39×10⁸, and then one start falls within 1.6. F3 changes every flight's step grid, and so where those starts
    fall.
  - **The nine** of the store (made before this campaign's code) have roots at 0.11–16.54 quanta (1001's is the
    largest), each call raising alone. Recomputed tuned on this tree, eight fail the same way at new φ and ln E, at
    0.65–6.70 quanta, and 2386 (β 0.5, M 10⁻³⁰, α 0.15) succeeds.
  - **What it means for a fixed-α build at laboratory M.** On 2026.9.0's code a history at fixed α fails wherever an
    infall's first step start falls within 17 quanta of the floor; which histories do is a matter of the step grid. Of
    the eleven measured here, ten fail on the fixed tree, two of them stored successes. The rate over A, A2 and B is
    not measured.
  - **Next:** for the user; proposed: `one-loop-drain` 01, which lands the drain law in the step loop on the merged
    tree, keeps the fixed α as a selectable law under which these histories run, and owns the related inside-wall
    stop of 3873; the rebuild of record (its 08) runs at laboratory M. The forms the measurements point to: a model
    hook for ln(V − V_floor) in closed form (normalization + ln expm1((M/φ)ⁿ) for the exponential); or no assessment
    where E/V_floor is below the rounding width (such a start is far from the wall, φ/M ≈ 7×10²³ at the two failures,
    where no decision is made); or a root inside the −∞ region reported rather than raised. Not fixed here (prompt
    §2).

## 4. Resolved issues

### `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]`

**Opened:** 2026-11-02 on the `inelastic-reflection` board, after that campaign's close, from the diagnosis
`.documents/post-reflection-step-2026-11-02/`. Its opening entry, with its measurements and impact, stays there (§3 of
[that board](../inelastic-reflection/IMPLEMENTATION_STATE.md)), with a dated pointer to this entry. **Assigned**
2026-11-02 to this campaign (§3.1).

**Closed:** 2026-11-02 by prompt 04 (this commit).
- **The fix**: F3 and the guard (prompt 03, `b81eab8`, ruled by the user as U6, README §6.3): every Radau restarted with
  π > 0 starts at `min(global_max_step, max(h_floor, f φ/π))`, at most the distance to its bound, and its first
  accepted step is tested by P1's flag and retaken at half its h if flagged. `VERSION_LABEL` 2026.9.0.
- **The Fix gate** (03): the three reproduced failures succeed; the replay of 01's 66 F3 tasks is equal in every field;
  no signature step; the goldens as ruled; the reflecting golden pinned (verification §2).
- **The 42** (04): recomputed tuned on the fixed tree, 41 succeed and 1 fails (4389, the effective minimum not found,
  opened §3); none fails in `log_m_eff_squared`, none keeps or discards a signature step, none retakes (verification
  §3.1). Not stored (README §6.3).
- **The handover**: no store of record is built at 2026.9.0; the whole store is rebuilt by the user after
  `one-loop-drain` 08, at 2026.10.0, on a tree that holds this fix once `reflection-restart` is merged into
  `one-loop-drain`; the 2026.8.0 store is superseded by that rebuild (the run record's README, `fig6b/PLAN.md` §3,
  `review-remediation-verification.md` §4.19).

Its row is deleted from `.documents/OPEN_ISSUES.md` §1.21. The precedent of an issue opened on one board and closed by
another campaign is `[08-a-history-that-did-not-converge-is-never-retried]` (`jordan-normalization`, closed by
`tuning-followups` 01). There the entry moved to its home board's §4 with a **Resolved** line; here, as prompt 04 §1.3
asks, the closing entry is on the closing campaign's board and the opening entry gets a dated pointer.

### `[01-p1-flag-fires-on-legitimate-steps]`

**Opened:** 2026-11-02 by prompt 01, from its measurement (log 01 §5; verification §1.1, §1.4).
**Closed:** 2026-11-02, by the user's ruling of the signature (README §6.3): "P1 only seems to need to fire
on the first step of a restarted Radau." Counted on 01's raw output (`var/reflection-restart/01/`) by the
orchestrator: P1's flag (either condition) on the first accepted step of an instance begun after a
reflection, a drain or a release holds for exactly the three bad steps under None, and for no step under F3,
over the 66 histories of its batch, the three goldens among them. 02, which needed it, is withdrawn; 03's Fix gate counts it.

- **What.** P1's flag (README §6.2), which the scanner applies and the probe's F4 acts on, fires on steps that
  are not the bad step, in every history measured, reflecting or not:
  - **the ratio condition** (π_end/π_start < ½, π > 0 throughout) on the last outward step before an apex,
    where the field decelerates legitimately (−π′ > 0): about one per outward flight, with π ratios running
    continuously up to 0.4999, against the least unflagged 0.5001. Under the production code (None) these are
    every ratio-only flag of the batch; none is a first step of a restarted instance;
  - **the phi condition** (the interpolant below (1 − f) φ_start) on a step that turns at an apex and falls back
    by the f φ the acceleration cap allows (φ_min/φ_start = 0.89983 at β = 2.9, α = 0.08, in five stored
    successes at M = 4×10⁻³¹, 10⁻¹⁵, 10⁻¹², 10⁻³³ and 10⁻³⁰).
  The three bad steps of the diagnosis are the only flagged steps that are the first step of a restarted Radau
  instance and carry the phi condition (with −π′ < 0 and h·ω ≈ 1060–1240).
- **Impact.** (a) P1's count of "signature steps" does not measure the defect: under None the batch's histories
  hold thousands of ratio-only flags. (b) F4 acts outside restarts: it retakes the legitimate steps, so it moves
  the golden histories (α = 0, no reflection) and every history it touches, which a restart-only fix would not.
  (c) 02's audit, which counts the signature per history, would list every reflecting success as affected
  unless it counts a narrower signature.
- **Next step.** For the user, before 02 is written: rule the signature 02 counts (for example P1's phi
  condition on the first step of a restarted instance, which isolates the three bad steps in all 01 measured,
  or that with a bound on h·ω), and, if F4 is still wanted, the flag it acts on. The scanner records each row's
  conditions, start reason, first-step flag, −π′ and h·ω, so a narrower signature needs no new run of 01's
  histories. Not fixed here (orchestrator note 01, ruling 4: the flag is not tuned).

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-01, after `inelastic-reflection` 12): CosmologyModels
26, ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68. Since then
`745c8ee` touched `ComputeTargets/tests/test_model_interface.py` and `c95650a`
`ObjectFactories/tests/test_alpha_figures.py`, so prompt 01 measures the counts again before it changes
anything.

**Measured by prompt 01, 2026-11-02, before it changed anything** (at `3b8a553`): CosmologyModels 26,
ComputeTargets 498 (122 + 376), ObjectFactories 130, tools 123, RunDrivers 68, all passing (log 01 §4.1).
**After prompt 01** (11:39–11:55): CosmologyModels 26, ComputeTargets 508 (122 + 386), ObjectFactories 130, tools 145,
RunDrivers 68, all passing; ComputeTargets gains 10 (`test_probe_outward_fix.py`), tools 22 (`test_outward_step_scan.py`).

**Measured by prompt 03, 2026-11-02, before it changed anything** (at `89a0760`): CosmologyModels 26, ComputeTargets
508 (122 + 386), ObjectFactories 130, tools 145, RunDrivers 68, all passing. **After prompt 03** (15:34–15:49, after
the user's ruling on its stop): CosmologyModels 26, ComputeTargets 514 (122 + 392), ObjectFactories 130, tools 147,
RunDrivers 68, all passing; ComputeTargets loses 10 (`test_probe_outward_fix.py`) and gains 14
(`test_restart_fix.py`) and 2 (`test_golden_reflecting_history.py`), tools gains 2 (log 03 §4.1).

**Measured by prompt 04, 2026-11-02, before anything else** (at `741dcce`, 22:28–22:44 UTC, load 3–5 on 10 cores):
CosmologyModels 26, ComputeTargets 514 (122 + 392), ObjectFactories 130, tools 147, RunDrivers 68, all passing (log 04
§4.1). Prompt 04 changes no code or test file, so they were not run again and the counts stand.
