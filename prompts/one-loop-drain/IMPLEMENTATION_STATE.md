# one-loop-drain campaign — implementation state

**Last updated:** 2026-11-05 · **Status: LIVE — 3 of 11 landed (01, 02, 02b); the measured law adopted (README §8.10); 02c written, not dispatched; U15 open.** Planned
2026-11-02 as `quanta-diagnostics` at the user's request, after a scoping session the same day
([`source/scoping_2026-11-02.md`](source/scoping_2026-11-02.md)); U1–U7 and P1–P16 ruled the same day,
all as proposed (README §6). **Re-planned the same day around the drain law and renamed `one-loop-drain`**
(branch and folder), at the user's rulings of README §6.3, after the premise was corrected
([`source/margins-correction_2026-11-02.md`](source/margins-correction_2026-11-02.md)) and a one-loop drain
probed ([`source/drain-probe_2026-11-02.md`](source/drain-probe_2026-11-02.md)). **README §8 is the plan;** U8–U12 ruled the same day, all as proposed (README §8.6).
The branch was cut from `reflection-restart` at `3b8a553`. Under U1 as extended by ruling 6, no prompt is
written until `reflection-restart` 04 has landed and been merged here; under U2, the rebuild of record
waits for this campaign's 08. *(2026-11-02: `reflection-restart` 04 landed and that campaign closed; its branch was
merged here (`1561b60`). The user ruled the wall-search fix into 01 (README §8.8), and the fixed-α issue came
back to §3 for it. Prompt 01 written the same day.)* *(2026-11-03: prompt 01 landed, with the user's ruling (b) on
the inside-wall stop (§3, §4); the Law gate holds, and two issues closed.)* *(2026-11-03: prompt 02 landed; the Measure
gate holds, one issue opened (§3), and U13 and U14 are open for the user with its measurements.)* *(2026-11-04: the user ruled U13: both gap
prescriptions kept, `zero` the default (README §8.6, P26). U14 is open.)* *(2026-11-04: prompt 02b inserted and written at the user's request, to
measure A ∈ {0.05, 0.5, 1.5, 2, 5, 10} before U14 is ruled (README §8.4, §8.6).)* *(2026-11-04: prompt 02b landed; the Wide
gate holds, no issue opened, one extended (§3), and U14 is open for the user with the measurements at nine A.)* *(2026-11-04: the user ruled U14: A ∈ {0.05, 0.1,
0.5, 1, 1.5, 2, 5, 10} for the rebuild of record (README §8.6).)* *(2026-11-04: the small-A failures diagnosed, an issue
opened on the one-loop fraction at small ℓ, and the side-campaign `crossover` started at the user's request, to land
before 03 (README §8.9).)* *(2026-11-05: `crossover` closed. The user adopted its measured law in place of 2.7/ℓ and both
gap prescriptions (U13 re-ruled), kept U14, and ruled one versioned drain-model key, the removal of `zero` and `edge`,
and a tabulated S(L). 02c and 02d inserted before 03, 03 revised, 02c written; U15 (n ≠ 1) open for the user (README §8.10).)*

The campaign replaces the fixed-α drain by a one-loop law, α_eff = min(α_cap, A·2.7/ℓ)·w_gate at a
one-loop turnaround and zero at R ≤ 1, with the fixed law kept beside it, bitwise. It measures the law
across the grid before the user rules the gap prescription and the A values of record; stores the
law's model-free instrumentation (a row per bounce, the masses per sample, the regime shares and margins
per history); derives the spectrum-dependent measures in a tool outside the integrator; rehearses both
laws; and writes the figure tools.

**Campaign:** [`README.md`](README.md) ·
**Code:** `ComputeTargets/ScalarHistory.py`, `CosmologyConcepts/Models/AbstractModel.py` (the wall search, 01), `ComputeTargets/quanta_spectrum.py` (new),
`ObjectFactories/ScalarHistory.py`, `config/`, `main.py`, `plot_*.py`, `extract_common.py`, `tools/` ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.23

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

The plan as first ruled had seven prompts (README §2), none written; the re-plan replaced them (README §8.4).

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The drain law](01-the-drain-law.md) | the law and A as inputs; the regime in the loop; α_eff by regime; the gap prescription as a module attribute; the `"drain"` payload key; the tool route's options; the 3873 failure; the wall-search fix (README §8.8) | Opus | ✅ | ✅ | `b99f6a4` | [01](logs/01-the-drain-law.md) |
| 02 | [Measure the law](02-measure-the-law.md) | U11's thirty histories by the tool route, re-tuned, at three A and both gap prescriptions, with two exact shortcuts (S1, S2) spot-checked; regime shares, the gap's reach, the R·S(L) bound, Y_p, D/H, ΔN_eff against the fixed law; then U13–U14 | Opus | ✅ | ✅ | `3f3f0cf` | [02](logs/02-measure-the-law.md) |
| 02b | [Measure over a wider A](02b-measure-wide-A.md) | inserted 2026-11-04 at the user's request: 02's thirty histories at A ∈ {0.05, 0.5, 1.5, 2, 5, 10}, both gap prescriptions; 02's lines reused after a reproduction check; S2 generalised; the first impact's drained fraction and the cap; the handover for U14 at nine A | Opus | ✅ | ✅ | `cc1f449` | [02b](logs/02b-measure-wide-A.md) |
| 02c | [The measured law](02c-the-measured-law.md) | inserted 2026-11-05 (README §8.10): the adopted law in `ScalarHistory.py`, replacing 2.7/ℓ and both gap prescriptions; S(L); U15 as ruled; the `"drain"` record names the model version and records L and ln R; `--gap-prescription` removed; the reproduction of crossover's adopted-law maps | Opus | ✍️ | ⬜ | | |
| 02d | Measure the measured law | inserted 2026-11-05: 02b's sample re-tuned under the adopted law at U14's eight A; replaces 02's and 02b's one-loop measurements | Opus | ⬜ | ⬜ | | |
| 03 | The store | the keys, `ScalarHistoryBounce`, the sample and history columns, one versioned drain-model key (README §8.10, decision 1; it replaces the gap-prescription key of U13/P26), `FINGERPRINT_FORMAT` 7, `VERSION_LABEL` | Opus | ⬜ | ⬜ | | |
| 04 | Drivers and exports | the law's options in `main.py` and the plot scripts; `histories.csv`, `fields.csv`, `bounces.csv` | Sonnet | ⬜ | ⬜ | | |
| 05 | Derivation tool | `quanta_spectrum.py` and `tools/quanta_measures.py`: n_g, n̄_j, δ_stim, E, phase slip, backreaction ratio, the R·S(L) bound | Opus | ⬜ | ⬜ | | |
| 06 | Rehearse | the fixed law base against tip, bitwise; the one-loop law interrupted and resumed, by value | Opus | ⬜ | ⬜ | | |
| 07 | Figure tools | the grid figure and the per-history figure | Sonnet | ⬜ | ⬜ | | |
| 08 | Close | the verification document, the law and measures for the paper, the handover for the rebuild, the indexes | Opus | ⬜ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed.

Orchestrator notes, one prompt at a time, in `orchestrator/`:
- [`prompt-01.md`](orchestrator/prompt-01.md) (`b1415a3`) — used for 01.
- [`prompt-02.md`](orchestrator/prompt-02.md) (`b1d6bc5`) — used for 02.
- [`prompt-02b.md`](orchestrator/prompt-02b.md) (`62d3f2f`) — used for 02b.

**Orchestrator review of prompt 01 (2026-11-03)**, by orchestrator note 01 §3, on `b99f6a4`. Passed; seven
checks:
1. **Scope.**
   - The commit's parent is `b1415a3`, the note's. It holds ruling 9's 36 files and nothing else, and nothing is
     pushed. Only `var/` outputs are untracked.
   - The diff over `config`, `main.py`, the plot scripts, `ObjectFactories` and `tools/outward_step_scan.py` is
     empty.
   - In `AbstractModel.py` the diff adds the predicate, its cache and the one early return in
     `classical_turning_point`. `_search_log_field`, `WALL_ROUNDING_QUANTA` and `WALL_SEARCH_HALVINGS` are
     unchanged.
   - `VERSION_LABEL` is 2026.9.0 and `FINGERPRINT_FORMAT` is 6.
2. **Nothing left running.** No pid under `var/one-loop-drain/01/` answers `kill -0`. `runregistrykit list` is
   unchanged: 23 runs, all finished.
3. **The fixed law.**
   - Run by the orchestrator: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` and
     `test_store_fingerprint_golden`, 50 OK in 154 s.
   - Three of `reflection-restart` 04's 42, re-run through the scanner on `b99f6a4`: `shard0015#3298`,
     `shard0003#3819` and `shard0015#4389`. Each equals `the-42.jsonl` in outcome, both digests, `n_samples`,
     `accepted_steps`, `RHS_evaluations`, `reflections` and `restart_retakes`, and 4389 fails with the same
     reason.
4. **The fix.**
   - Read against P21: the early return sits after `r_0 >= 0` and before the search, and the docstring carries
     the argument.
   - In a scratch probe, the old behaviour was taken with `_turning_point_unresolvable` patched to False, and
     compared with the new on `probe 2197` and `stored shard0000#1001` (16.5 quanta): the new returns None, the
     old raises.
   - At 2197's ln V_floor, with ln E at the least finite value (predicate False), old and new return the same
     root, bitwise. One float below it (predicate True), the new returns None and the old raises.
5. **The law.**
   - `drain_alpha_effective` reads `min(DRAIN_ALPHA_CAP, A * DRAIN_C1 / ell) * w_gate`, as ruling 3 asks.
   - `unresolved_reflection_refused` shares `_wall_and_kinetic` and `_g2_refuses` with `_reflection_guards`, and
     is called only where `unresolved_turnaround` fired.
   - Re-run by the orchestrator: 2197 at A = 1, gap zero, `gate_weight` patched
     (`law/one_loop_run.py ... probe`). It was compared with the probe's `2197-A1.json` by the orchestrator's own
     comparison: all 10 candidates' (N, α_eff, ΔK) equal, and the dark-radiation values, `accepted_steps`,
     `reflections` and the parking N equal.
6. **The tool route.**
   - `--help` shows `--drain-law` and `--gap-prescription`.
   - The pinned fixed-law tests (`test_history_and_bbn`, `test_history_and_bbn_alpha`) pass, 7 OK in 38 s.
   - One run, `1.185 4e-31 --drain-law one_loop --alpha 1 --fixed-normalization …`, prints
     `drain_law=one_loop A=1 gap=zero` and a `drain` line (10 one-loop candidates, `unresolved_declined=0`).
     r_χ today is 8.21841×10⁻³, the probe's.
7. **The records.**
   - The log follows `dk-and-rpk-migration` README §5.1's six sections, with every deviation classed and the
     user's ruling (b) quoted.
   - The verification document is new, with §1 only, and `law/out/` holds text only.
   - The numerical-strategies and architecture-summary notes are dated 2026-11-03.
   - Row 01 and the Law gate are landed. Both issues are on §4 with their measurements.
   - `.documents/OPEN_ISSUES.md` reads 30 open, last updated 2026-11-03, with 4 rows under §1.23 and 30 in all.
   - `CLAUDE.md`'s counts are the measured 26, 566 (163 + 403), 130, 156 and 68.

   The review substituted `b99f6a4` for "this commit" on the board, in the log and in the verification
   document.

Noted, not acted on: the deliberate-breakage record's B5 found that one test of `TestTheLawsInputs`
(`test_1_integrate_history_refuses_before_any_integration`) does not catch A = 0 being accepted, since its mock
model raises a ValueError of its own. The others of its class do.

**Orchestrator review of prompt 02 (2026-11-03)**, by orchestrator note 02 §3, on `3f3f0cf`. Passed; seven
checks:
1. **Scope.**
   - The commit's parent is `b1d6bc5`, the note's. It holds ruling 10's 24 files and nothing else, and nothing is
     pushed. Only `var/` outputs are untracked.
   - The diff over the code paths and `CLAUDE.md` is empty, and `measure/out/` holds text only.
   - The agent amended its own commit once before handing back, to correct three phrases of verification §2.9.
     Nothing had landed on top of it, so the rule of one commit per prompt holds.
2. **Nothing left running.** No pid under `var/one-loop-drain/02/` answers `kill -0`. Each of the six batches has
   its `batch.end`, and no `run_sample.py` process is alive. `runregistrykit list` is unchanged: 23 runs, all finished.
3. **The sample is complete.** `sample.jsonl` has 180 lines with 180 distinct ids. Together they cover 30 histories
   × 3 A × 2 prescriptions, with nothing missing and nothing extra: 110 `run`, 65 `S1` and 5 `S2`. The
   orchestrator re-derived every S1 and S2 decision with its own script, from the per-integration regime counts of
   every integration (the 18 that raised included). It agrees with the agent's at every task.
4. **A shortcut, re-run.** The orchestrator ran two derived tasks as children with prefix `review-`:
   `W-2183-A1-edge` (S1) and `L-M4e-15-A0.01-zero` (S2). It compared their partial and result files with their
   sources' by its own comparison. Both are equal in every value but the task id, the argv and the record's own
   `gap_prescription` or `A`: per integration, every regime and injection, the tuning record, both digests and
   the BBN values.
5. **The tool, re-run.** `tools/history_and_bbn.py 2.0 4e-31 --drain-law one_loop --alpha 1 --gap-prescription zero`,
   run by the orchestrator, gives stdout equal to `tasks/R-865-A1-zero.log` once `wall=` and `PRyM_time=` are
   masked. The line's full-precision values, written in the tool's formats, appear in its `history`, `tuning`,
   `dissipation`, `drain` and `bbn` lines.
6. **The numbers.**
   - Recomputed from `sample.jsonl`:
     - the largest Δ(ΔN_eff today), edge − zero, over the 10 pairs both run and successful, is +0.09955 at
       L-M5e-16, A = 1;
     - 2197's ΔN_eff today is 0.061211 at A = 1 and 0.011413 at A = 0.1, and it fails at A = 0.01 under both
       prescriptions;
     - 17 histories meet a gap candidate at A = 0.01 under `zero` (P 2, W 10, R-632, and the ladder's four from
       5 × 10⁻²⁰ to 5 × 10⁻¹⁶);
     - 29 failures: 18 by the step budget and 11 because the effective minimum was not found.
   - **The R·S(L) bound.** L-M1e-16 at A = 1 was re-tuned by the orchestrator's own `tune_history` call, and the
     bound recomputed from the payload by its own arithmetic: 54 classical candidates, all in range, a sum of
     1.680039880463449 × 10⁻⁴⁵, a share 9.108874548736619 × 10⁻⁷ and an r_χ of 1.785765712567109 × 10⁻⁸. Each is
     equal to the line's, and so is the samples' SHA-256.
   - One fixed-law reference (serial 2142: β 2.0, α 1.5 × 10⁻³, shard 0002) equals the store in all 11 fields,
     and there are 261 references in all.
7. **The records.**
   - The log follows §5.1's seven sections, with rulings 1–10 and D1–D11 classed.
   - Verification §1 is unchanged (the diff only adds), and §2.1–§2.10 follow prompt §2's six items. The handover
     (§2.9) states measurements and the options they bear on, and makes no choice.
   - On the board: row 02, its items, the Measure gate, U13 and U14 "open for the user", one issue on §3, and
     **Last updated**. `.documents/OPEN_ISSUES.md` reads 31 open, with 31 rows, 5 of them under §1.23.
   - `CLAUDE.md` is unchanged, and the suites' counts stand.

   The review substituted `3f3f0cf` for "this commit" on the board, in the log and in the verification document.

Noted, not acted on:
- Verification §2.5 gives the R·S(L) bound's range as 3.3 × 10⁻⁸ to 9.5 × 10⁻⁴ of K where a one-loop or gap
  candidate drains. Where only the parking leftover drains it is larger (0.142 of K at L-M5e-16 under `zero`), as
  the same section states.
- L-M5e-16 at A = 1 under `edge` took 747 s, against 16 s under `zero`. The gap drain at the first impact leads into
  a classical tail of 9962 candidates.

**Orchestrator review of prompt 02b (2026-11-04)**, by orchestrator note 02b §3, on `cc1f449`. Passed; seven
checks, each with what it found:
1. **Scope.**
   - `cc1f449` has 25 files, all of ruling 10's set: `measure-02b/` (8 scripts, 14 text outputs), verification §2b, the
     log and the board. Its parent is `62d3f2f`, the note.
   - The tree is clean, and nothing is pushed.
   - The diff over the code paths, `.documents/one-loop-drain/measure/`, `CLAUDE.md` and `.documents/OPEN_ISSUES.md`
     is empty. `measure-02b/out/` holds text only.
   - `var/one-loop-drain/02/` holds no file newer than the note's commit, and its `sample.jsonl` still has 180 lines.
   - The agent amended its own commit once before handing back (`f02d738` → `cc1f449`), with nothing on top. This was
     accepted.
2. **Nothing left running.** None of the seven batches' pids answers `kill -0`, each has `batch.end`, and the registry
   still lists 23 finished runs.
3. **The sample is complete.**
   - 360 lines, one for each (history, A, prescription), with no duplicates: 171 run (167 succeeding, 4 failing),
     159 by S1 and 30 by S2.
   - The orchestrator re-derived every decision from the per-integration regime counts with its own script, S2's
     from 02's A = 1 lines. Every derivation is valid, and no run was one a shortcut would have spared.
4. **A shortcut, re-run.** `W-2179-A5-edge` (S1) and `L-M4e-15-A2-zero` (S2) were run by 02b's driver in the
   orchestrator's own process, with the prefix `review-`.
   - The partial files are bitwise equal to their sources, `W-2179-A5-zero` and 02's `L-M4e-15-A1-zero`, apart from
     `gap_prescription` and `A`.
   - The result files differ only in `id`, `argv` and, against 02, the fields that 02b adds (`first_impact`,
     `cap_one_loop_by_ell`, `cap_gap_edge`).
5. **The tool, re-run.** `tools/history_and_bbn.py 1.0925 4e-31 --drain-law one_loop --alpha 5.0 --gap-prescription
   zero` (W-2179) gives 13 lines. These are identical to the driver's log for `W-2179-A5-zero` once `wall=` and
   `PRyM_time=` are masked, the `history`, `drain`, `dissipation` and `bbn` lines among them.
6. **The numbers.** Recomputed from the two `sample.jsonl` files and the partials with the orchestrator's own script,
   all equal to the report:
   - the first-impact α_eff at A = 10: 0.837–0.962 across P and W, and 0.99 (capped) for P-2188;
   - 2197's ΔN_eff today: 0.5639, 0.2771, 0.1188, 0.0917, 0.0612, 0.0333, 0.0114 and 0.0086 at A = 10 down to
     0.05, and a failure at 0.01;
   - 4 tunings failing at A = 0.05: R-632 and L-M5e-20, under both prescriptions;
   - the cap count from the kept ℓ, equal to the count by α_eff = 0.99 and to the recorded count: 3, 4 and 2 for
     P-2188 at A = 10, R-632 at A = 5 and W-2187 at A = 10.
7. **The records.**
   - The log follows §5.1's seven sections, with rulings 1–10 and D1–D8 classed.
   - Verification §1 and §2 are unchanged (the diff only adds 346 lines), and §2b follows prompt §2's seven items.
     The U14 handover (§2b.11) states measurements and options, and makes no choice.
   - On the board: row 02b, its items, the Wide gate, U14 "open for the user", and the dated paragraph on the §3
     issue.
   - `CLAUDE.md` and the suites' counts are unchanged.

   The review substituted `cc1f449` for "this commit" on the board, in the log and in the verification document. It
   also corrected the board's status line from "3 of 10" to "3 of 9" (01, 02, 02b and 03–08), and changed the index
   row of the §3 issue from "for the user with U13/U14" to "for the user with U14", since U13 is ruled.

Noted, not acted on:
- R-632 (β = 1.0) gives Y_p 0.4345 and D/H 10.4 × 10⁻⁵, with ΔN_eff at 1 MeV of 0 and parking at N ≈ 36, at A = 1 as
  well as at 5 and 10. So these values belong to that history, not to A.
- Ruling 5's projection for A = 0.05 costed every remaining task at P's 26 s, and was low by about nine times: the
  ladder's long classical tails took 711–942 s each. It held under the three-hour limit regardless.

**Open for the user, with 02's measurements (2026-11-03;
[verification §2.9](../../.documents/one-loop-drain-verification.md)):**
- **U13**, the gap prescription of record. The prescriptions differ only where the cascade enters the gap: on the
  ladder (8 × 10⁻¹⁸ to 5 × 10⁻¹⁶ M_P at every A, 5 × 10⁻²⁰ at A ≤ 0.1), at laboratory M only at A = 0.01, and for β = 1.0 at
  A ≤ 0.1. Where both succeed (the ladder), `edge` raises ΔN_eff today by up to 0.0995 and Y_p by up to 1.3 × 10⁻³.
  At laboratory M no pair could be compared. **Ruled by the user, 2026-11-04:** "Please keep both, but make `zero` the default in production"
  (README §8.6; P26 proposes the key).
- **U14**, the A values of the rebuild of record. At laboratory M, ΔN_eff today falls roughly in proportion to A. A = 0.01
  cannot be built in the transition window on this code (§3). **Ruled by the user, 2026-11-04,** after 02b: "In that case, it seems to me that our original list `A \in { 0.05, 0.1, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0 }` covers the required region adequately. I see no reason to change it, unless you have an objection."
  (README §8.6). The orchestrator raised no objection.
  **With 02b's measurements (2026-11-04; [verification §2b.11](../../.documents/one-loop-drain-verification.md)):** at
  laboratory M every P and W history builds, one-loop only, at A = 10, 5, 2, 1.5, 0.5 and 0.05. Their ΔN_eff today is
  0.029–0.645 at A = 10, 0.024–0.335 at 5 and 0.0025–0.0094 at 0.05 (2197: 0.564, 0.277, 0.0086), and ΔN_eff at 1 MeV is
  close to proportional to A. At A = 10 the first impact drains 84–99% of the kinetic energy and the field parks by
  N = 18.8–24.7. R-632 (β = 1.0) fails at A ≤ 0.1, and the ladder's 5 × 10⁻²⁰ M_P at A = 0.1 and 0.05. Open for the user.

### 1.1 Items

| Item | Prompt | State |
|---|---|---|
| `drain_law` on `integrate_scalar_history`, `integrate_history`, `tune_history` (P22) | 01 | ✅ |
| the regime in the loop, α_eff by regime, the constants (P17, P18, P23, P24, U9, U10) | 01 | ✅ |
| `_GAP_PRESCRIPTION`, temporary (P20, P25) | 01 | ✅ (02c removes it, README §8.10) |
| the `"drain"` payload key, `DrainRecord`/`DrainCandidate` | 01 | ✅ |
| the wall-search fix, `_turning_point_unresolvable` (P21) | 01 | ✅ |
| the declined unresolved reflection (the user's ruling (b), 2026-11-03) | 01 | ✅ |
| `tools/history_and_bbn.py --drain-law`, `--gap-prescription` (P25) | 01 | ✅ |
| the measure scripts, `.documents/one-loop-drain/measure/` (driver, task list, fixed-law references, decisions, spot and tool checks, analyses) | 02 | ✅ |
| U11's 30 histories × A ∈ {1, 0.1, 0.01} × {zero, edge}: 110 run, 65 by S1, 5 by S2; spot and tool checks bitwise | 02 | ✅ (verification §2) |
| the handover for U13 and U14 | 02 | ✅ (verification §2.9; U13 and U14 ruled 2026-11-04) |
| the wide-A scripts, `.documents/one-loop-drain/measure-02b/` (02's scripts imported; the reproduction check, the first impact and the cap) | 02b | ✅ |
| 02's 30 histories × A ∈ {10, 5, 2, 1.5, 0.5, 0.05} × {zero, edge}: 171 run, 159 by S1, 30 by S2; the reproduction, spot and tool checks bitwise; the handover for U14 at nine A | 02b | ✅ (verification §2b; U14 ruled 2026-11-04) |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Law | fixed law: golden histories bitwise, payload unchanged but the new key; one-loop law: 2197 and 2188 reproduce the probe's table, 3016 and 3233 drain nothing; 3873 explained | 01 | ✅ 2026-11-03 (verification §1.10): the goldens bitwise, the 42 equal 04's, the probe's table bitwise at A = 1 and 0.1, 3016/3233 drain nothing, 3873 explained and completing after ruling (b) |
| Measure | the sample at three A and both gap prescriptions, re-tuned; regime shares and the gap's reach across M and β; the prescriptions' spread in Y_p, D/H, ΔN_eff; the cost | 02 | ✅ 2026-11-03 (verification §2.10): 180 tasks, 81 of 110 runs succeeding; the gap reached at laboratory M only at A = 0.01, where the tunings fail; the spread measured on the ladder; 1 h 58 min, at most six workers |
| Wide | 02's sample at 02b's six A, the reproduction check bitwise; regime shares, the first impact and the cap across nine A; the cost; the handover for U14 | 02b | ✅ 2026-11-04 (verification §2b.12): 360 tasks, 167 of 171 runs succeeding; every tuning at A = 0.5–10 succeeds, and P and W stay one-loop down to A = 0.05; at A = 10 the first impact drains 84–99% of the kinetic energy at laboratory M; 43 min, at most six workers |
| Measured | the fixed law unchanged bitwise; the law equal to crossover's reference implementation at synthetic inputs; crossover's adopted-law maps reproduced candidate by candidate | 02c | ⬜ |
| Remeasure | the sample at U14's eight A under the adopted law, re-tuned; the failures stated; against 02/02b | 02d | ⬜ |
| Store | the round trip exact, bounce rows in order; the fixed law and α = 0 write no bounce row; fingerprint format 7 beside 6 | 03 | ⬜ |
| Export | the new export columns equal the stored record; existing `histories.csv` columns byte-identical; the drivers refuse a mismatched law | 04 | ⬜ |
| Derive | the derived measures against direct quadrature (10⁻⁶); peak-collapsed against the scoping probe's method | 05 | ⬜ |
| Bitwise | the fixed law's tip build equal to the base's in every pre-existing column | 06 | ⬜ |
| R | the one-loop build, interrupted and resumed, equals the straight build by value (bounce rows included) and 02's tool route | 06 | ⬜ |
| Figures | both tools render from 06's exports; their tests pass | 07 | ⬜ |

## 3. Active and unresolved issues

Opened by the planner on 2026-11-02, from the scoping (`source/scoping_2026-11-02.md`); two more
opened the same day, after the plan was ruled, from the drain probe (`source/drain-probe_2026-11-02.md`).
At the re-plan the same day (README §8) one of those was split in two (§4), and the rest re-pointed at
the new prompts.

- **`[00-weighted-margins-hide-the-least-relativistic-generations]`** — the stored worst margins and
  failing shares (`inelastic-reflection` 09) run over the generations holding at least 10⁻³ of
  K(N_final) only, and the generations are not stored. In the low-α transition window at laboratory M
  the others hold 80–98% of ρ_χ. Measured on two histories (serials 2188, 2197; β 1.06, 1.185, α 0.001):
  tracker margin over all generations 7.3 and 1.6 against a stored 68.3 and 69.0; reflection margin
  −17.5 and −20.4 against 17.9 and 17.5, with 0.92 and 0.84 of ρ_χ below zero against a stored failing
  share of 0. **Impact:** the stored record understates exposure at later bounces wherever the
  below-weight share is large (up to 0.98 at 4×10⁻³¹, 0.65 at β = 20); the expansion history is not
  affected (matter-like excess ≤ 5×10⁻⁶ of ρ_χ). **Next:** closed by 02, when the all-generation values
  and the generations are stored.
  **Re-planned 2026-11-02** (README §8.3): the all-generation margins and the bounce rows are stored under
  the one-loop law only (ruling 2), by 03, which closes this; under the fixed law the record stays as it
  is, and 08 documents that.
- **`[00-later-bounces-act-on-quanta-already-present]`** — a bounce is non-adiabatic for quanta with
  p ≪ ω_t, which the wall's mass makes of most of ρ_χ in the transition-window histories (the wall's
  mass is e²²–e⁵² above the between-bounce mass). It rescatters them and stimulates production into
  occupied modes; the (k, −k) pairs are correlated, so coherent build-up is possible. The ledger treats
  the quanta as passive radiation. **Impact:** unmeasured; the drain fraction and the spectrum's peak
  at later bounces could change. The user ruled it documented and quantified, not modelled (2026-11-02).
  **Next:** quantified by 04 and 05 (δ_stim, δ_resc under the stated spectrum and random phases),
  documented by 07, and left open as a known limitation.
  **Narrowed 2026-11-02** (source `drain-probe_2026-11-02.md` §§1, 3): the premise above is wrong in
  one respect. A bounce acts on a mode by where it sits against the bounce's band, not by p/m: a mode
  with p ≫ k_ex follows adiabatically whatever its mass, so only generations with ln(Q_g/Q_k) ≲ O(1)
  are rescattered or stimulated. Parametric resonance is excluded wherever production is
  non-negligible: the phase slip per bounce is 10⁶–10³⁰ rad in every producing train probed (two
  laboratory-M histories under a one-loop drain, three at 10⁻¹² M_P at stored α and without a drain),
  against |β|² ≲ 1. The random-phase recursion n_{j+1} = n_j + |β_j|²(1 + 2n_j) is then the complete
  model; under a one-loop drain it is negligible (n ~ αe^{−4ℓ} ≪ 1). At the stored fixed α the late
  classical generations still have n ≫ 1, which is `[00-fixed-alpha-drain-overstates-late-reflections]`.
  **Re-planned 2026-11-02** (README §8.3): quantified by the derivation tool (05) under the one-loop law,
  on 02's and 06's histories: δ_stim, with rescattering bounded by it; documented by 08, and closed there
  if negligible under the one-loop law, otherwise left open as ruled. Under the fixed law it stays a
  documented limitation.
- **`[00-fixed-alpha-drain-overstates-late-reflections]`** — opened 2026-11-02 (source
  `drain-probe_2026-11-02.md`). A fixed α cannot follow the drain along the cascade or with M. At
  α = 10⁻³ at laboratory M it underestimates the early one-loop drains (one loop: 0.08–0.26) and asks
  the late classical bounces (R < 1) for occupations n ≫ 1 in their own band: in 2197, 61% of ρ_χ is born
  at R < 1 with n > 1, against |β|² ≲ O(1) from one bounce (a fraction ~R e^{4ℓ} of E; R·S(L) on the
  classical wall). Under α_eff = min(1, A·2.7/ℓ), zero at R ≤ 1, 2197 and 2188 park after 10–95 one-loop
  bounces and r_χ rises 1–10× (2197's ΔN_eff 0.0064 → 0.061 at A = 1); at 10⁻¹² M_P nothing is drained
  and ΔN_eff → 0 (3233: 0.335 → 0). **Impact:** the stored ρ_χ of the low-α laboratory-M histories is
  dominated by an unphysical late drain, and the stored large-M ΔN_eff rests on drains far above what
  a bounce can deliver; observable effects are below current bounds in the histories probed. **Next:**
  the user's decisions, README §6.3 (1–4).
  **Re-planned 2026-11-02:** this campaign's object (README §8). The one-loop law lands in 01; 03 closes
  this when the gap prescription of record (U13) is fixed and the law's keys are stored. *(2026-11-04: U13
  ruled, both kept and `zero` the default; 03 closes this when the law's keys, the prescription among them, are
  stored.)*
- **`[00-the-parking-leftover-has-no-spectrum]`** — the energy drained at the parking switch (up to
  0.10 of ρ_χ at 4×10⁻³¹, β 1.06–1.107; ≤ 0.03 elsewhere) has no peak estimate, so no margin and no
  wall-exposure measure covers it; it is reported only as `unassessed_share`. **Impact:** not measured.
  **Next:** out of this campaign's scope; recorded for a later one.
  **2026-11-02, at the re-plan:** under the one-loop law the parking leftover was 0.008–0.23% of K in the
  four laboratory-M probe runs, and at 10⁻¹² M_P it is all of ρ_χ (3016: r_χ 1.0×10⁻⁶ today). 02 measures
  its share across the sample (README §8.3); the spectrum stays out of scope.

- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`** — opened 2026-11-03 by prompt 02 (`3f3f0cf`;
  `.documents/one-loop-drain-verification.md` §2.4, §2.5; `var/one-loop-drain/02/sample.jsonl`). The tool
  route was run tuned on `b99f6a4`'s code, under the one-loop law. There, 29 of 110 tunings fail, each in or just after
  its seed integration:
  - 18 by the step budget (2 × 10⁶ accepted steps, at N = 43.0–47.7 after 13 573–14 638 reflections);
  - 11 by "the effective minimum was not found" after the seed integration (V_eff' of one sign on the bracket, values
    3.3–3.9 × 10⁻¹²¹; the kind of `[04-a-recomputed-history-does-not-find-the-effective-minimum]`).

  They are every laboratory-M history of the transition window at A = 0.01 (P and W, β 1.06–1.1875), under both
  prescriptions, but P-2188 under `edge`, and R-632 (β = 1.0) at A = 0.1 and 0.01. They include also the ladder's
  5 × 10⁻²⁰ M_P at A = 0.1, which succeeds at A = 1 and 0.01. In each the cascade reaches the gap and R ≤ 1, and a P or W
  integration at A = 0.01 meets 9992–13 497 classical candidates. **Impact:** A = 0.01 cannot be built at laboratory M in
  the transition window on this code, so U14 has no A = 0.01 values there, and U13 cannot be compared at laboratory M.
  `main.py` tunes through the same `tune_history`, so a rebuild at those (β, A) on this code takes the same path (not
  run). **Next:** U13 and U14 ruled 2026-11-04; A = 0.01 is not among U14's values, so the rebuild of record does not meet
  the P and W failures; it will meet β = 1.0's at A = 0.05 and 0.1 (and the ladder's 5 × 10⁻²⁰ M_P, if that M is built). Not
  diagnosed, and not acted on (prompt 02 §1.5).
  *(2026-11-04, prompt 02b (`cc1f449`; verification §2b.5): measured at A = 0.05. Every P and W history succeeds there,
  one-loop only, in 24–26 s. Four tunings fail, of the same two kinds and at the two histories that failed at A = 0.1:
  R-632 (β = 1.0) under both prescriptions, by "the effective minimum was not found" after the seed integration (37
  one-loop, 58–98 gap and 1488–4193 classical candidates; values 3.27795 × 10⁻¹²¹); and the ladder's 5 × 10⁻²⁰ M_P under
  both, by the step budget at N = 46.07–46.44 after 14 020–14 116 reflections. At A = 0.5–10 every tuning succeeds.)*
  *(2026-11-04, diagnosed by the orchestrator from 02's and 02b's partial files, at the user's request: one cause. **The
  field never parks before today.**
  - **Per bounce, the one-loop law drains no less than the fixed law did.** At A = 0.01 a one-loop bounce drains
    0.027/ℓ, from about 8 × 10⁻⁴ at the first impact up to 0.33.
  - **But it stops draining.** Once ℓ reaches 0 the chain has no solution, so the cascade is in the gap and then at
    R ≤ 1, where `zero` drains nothing. The cascade then goes on essentially elastic.
  - **For 2197:**
    - the stored fixed α = 10⁻³ drains 6648 times and parks at N = 45.16;
    - the one-loop law at A = 0.1 drains 95 times and parks at N = 38.45, and at A = 0.05 drains 134 times and
      parks at N = 39.09;
    - at A = 0.01 it drains 183 times, the last at N = 40.24, then meets 508 gap and 13 260 classical candidates,
      and does not park by N_final = 46.0.
  - **Whether a history fails is a race** between parking and ℓ reaching 0. At A ≥ 0.05 at laboratory M the field
    parks while still one-loop (ℓ_min 0.31–1.27 at A = 0.05).
  - **Both kinds of failure follow from this:**
    - the step budget is the 10⁴ undrained bounces before today;
    - "the effective minimum was not found" is a field still bouncing today. 2197 at A = 0.01 is at
      φ₀ ≈ 7 × 10⁻¹¹ against a minimum near 8.6 × 10⁻¹⁶, and `effective_minimum` brackets only (0.05 φ₀, 20 φ₀)
      (`AbstractModel.py` :537, called from `_tuning_state`, `ScalarHistory.py` :4937).
  - **β = 1.0 and the ladder at 5 × 10⁻²⁰ M_P are the same case.** R-632 at A = 0.1 drains 34 times to N = 39.87,
    then meets 200 gap and 4515 classical candidates, and does not park.
  - **This need not be a numerical defect.** If R ≤ 1 bounces are close to elastic, a small-A field may really be
    unparked today, and the tuning, which assumes the field at its minimum today, then has no solution. The drain
    near ℓ → 0 is itself in doubt (`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]`).)*
  *(2026-11-05, under the law the user adopted (README §8.10), measured by crossover's `measured_law_map.py
  --law=adopted` at each history's old-law normalization, not re-tuned (`source/crossover/README.md` "The adopted
  law at small A").
  - **At A = 0.05** every history of 02b's set that tuned under the old law still parks. The P and W histories park
    0.3–0.4 e-folds later, mostly in the gap's first bounces.
  - **The two failures remain:** R-632 (β = 1.0) and the 5 × 10⁻²⁰ M_P ladder.
  - **Their floor:** R-632 parks at A = 0.5 but not at 0.2; the ladder parks at A = 0.2 but not at 0.1.
  - The user keeps A = 0.05 regardless (U14 stands). **Next:** 02d measures it re-tuned.)*

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

- **`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]`** — opened 2026-11-04 by the orchestrator, at the user's
  request, after 02b (`cc1f449`).
  - **What the law rests on.** The one-loop law drains A · 2.7/ℓ, with ℓ = ln(k_ex/m_ta).
    - `wall_backreaction.tex` derives 2.7/ℓ in the Born regime ("The scheme solved in the Born regime") as the
      production c(V'')²/8π², which has no logarithm, divided by the dressing (V'')²ℓ/32π².
    - It verifies the law to 5% for ln(k_ex/k_IR) ≳ 4. It finds the scheme self-consistent only for
      k_IR ≲ 0.1 k_ex (ℓ ≳ 2.3), and for k_IR ≳ 0.2 k_ex the drag vanishes and the field runs on to the classical
      wall.
    - The scheme's modes are massless, with the mass entering only as the infrared cutoff.
  - **What it is asked to do.** The law rises as the cascade loses energy and ℓ falls, up to α_cap.
  - **What the user and the orchestrator believe instead (2026-11-04).** The fraction should fall as ℓ → 0:
    - once k_ex ≈ m_ta, the modes near k_ex have ω ≈ m_ta ≈ 1/Δt and respond nearly adiabatically;
    - in the limit, the classical exponential wall is reflectionless (the note's heavy-pulse result), and
      production at R ≤ 1 is about R·S(L) ≈ 10⁻⁴ R;
    - so the rise of 2.7/ℓ below ℓ ≈ 4 is an extrapolation of the Born scheme past its range, not a prediction.
  - **How much of the drained energy K comes from that range** (02 and 02b's partials, share of K from one-loop
    drains):

    | history | A | ℓ_min | ℓ < 4 | ℓ < 2.3 |
    |---|---|---|---|---|
    | 2197 | 0.1 | 1.8 | 0.04 | 0.001 |
    | 2197 | 0.05 | 0.52 | 0.19 | 0.02 |
    | 2197 | 0.01 | 0.083 | 0.70 | 0.44 |
    | W-2187 | 0.05 | 0.31 | 0.30 | 0.04 |
    | W-2187 | 0.01 | 0.11 | 0.78 | 0.50 |

    At A ≥ 0.5 at laboratory M, and at β = 2 at every A, no drain has ℓ < 4: 2197 has ℓ_min ≥ 4.9 and R-865
    ℓ_min ≥ 8.5.

  **Impact:** at laboratory M in the transition window, the drain law at A ≤ 0.1 relies partly on the extrapolation,
  and so do ΔN_eff, Y_p and D/H there. The parking time does too: a fraction that falls as ℓ → 0 would leave the
  cascade more energy when it reaches the near-elastic regime. **Next:** the side-campaign `crossover` (README §8.9),
  which the user ruled to land before 03. Its scripts and outputs are in `source/crossover/`.
  *(2026-11-04, crossover stage 1 (`source/crossover/README.md`): **the measurements do not support the belief
  above within the one-loop range.**
  - The note's Born scheme was generalized to the cascade's own bounces (2197 at A = 0.05, L 102–5.7, ln R
    138–19), with the infrared end at the mode mass. It reproduces the note's table.
  - The fraction lies below 2.7/ℓ: 0.92 of it at ℓ = 9.5, 0.65 at ℓ = 2.4. But it still rises as ℓ falls: 0.73 at
    ℓ = 2.4 and 0.92 at ℓ = 1.5, where the field no longer comes back out.
  - Massive mode functions give the same production to 0.2%.
  - At ℓ ≈ 1 the scheme fails, and the classical wall is not elastic there: a test field reaching it would produce
    R·S(L) ≈ 10³–10⁵ times E.
  - S(L) stays at 10⁻³–10⁻², so the nearly elastic classical reflection holds only once R·S(L) < 1. At A = 0.01 at
    laboratory M, that is the gap's last 179 of 508 bounces and the classical tail.
  - So the region in doubt is from the chain's failure to R·S(L) ≈ 1, where `zero` drains nothing. Stage 2 is to
    cover it.)*
  *(2026-11-04, crossover stage 2 (`source/crossover/README.md` "Stage 2"; `out/stage2_*.txt`): the one-loop
  scheme was solved self-consistently with massive modes, renormalized against the instantaneous vacuum, with no
  Born reduction.
  - **Validation.** It equals `born.py` to 0.07% where both apply (k_IR = 10 m_ta) and S(L) with the loop off to
    10⁻⁵. Energy is conserved to ≤ 10⁻⁷ (6.6 × 10⁻⁵ at one grid corner). Every convergence variant moves the fraction
    by ≤ 2 × 10⁻³.
  - **Along 2197 at laboratory M** (the same at A = 0.05 and 0.01), the fraction of E absorbed in one bounce is:
    - 0.25, 0.42, 0.57, 0.72, 0.81 and 0.92 at ℓ = 9.5, 4.9, 3.0, 2.0, 1.5 and 1.0;
    - **0.97 at ℓ ≈ 0.5**, 0.88 at the chain's last bounce and 0.83 at gap entry;
    - then, through the gap, 0.52, 0.28, 0.16, 0.06 and 0.003 at R·S = 70, 8.6, 1, 0.1 and R = 1.
  - It is 0.88–0.96 of `born.py`'s and 0.88–0.33 of 2.7/ℓ (ℓ = 9.5–1.0).
  - **≥ 90% for ℓ ≈ 1.1 down to 0.18; never ≥ 99%** (largest 0.97 on the curve, 0.986 off it).
  - **So the fraction does not fall as ℓ → 0 within the chain; it peaks near ℓ = 0.5 and falls only through the
    gap.** The nearly elastic regime (< 0.1, v_out/v_in > 0.95) begins between R·S ≈ 1 and 0.1, the gap's last 90–180
    bounces at A = 0.01.
  - At β = 2 (R-865, ℓ ≥ 17.75 at A = 0.05) stage 2 agrees with `born.py` to 2.5%.
  - **The fraction is a function of ℓ and V₀/M⁴.** On one M's curve it is a function of ℓ alone, the same for every
    β and A.
  - The scheme stays outside its own domain throughout: ε = 10²–10⁸ at the turn, and the Hartree shift is
    23–90 k_ex².
  - The linearized (Hartree) mode equation diverges and could not be compared. The scheme's soft 1/k² tail carries
    ≤ 0.5% of the quanta's energy.
  - **Next:** the user re-rules U13 (`zero`, `edge`, a terminal drain, or a measured function; the evidence on each
    is in the README's "What stage 2 hands back"), and so whether 2.7/ℓ itself is replaced.)*
  *(2026-11-04, the measured law on the cascades, at the user's request (`source/crossover/README.md` "The measured
  law on the cascades"; `measured_law_map.py`, in-process patch, no production change):
  - **The law tried.** f = 1 − exp(−2.69/(ℓ + 0.12)) for ℓ ≥ 1, 0.9 below that, stage 2's gap rows and R·S(L) in
    the classical regime; α_eff = min(0.99, A f) w_gate, at the old-law normalizations.
  - **At A = 1.** 2197 parks at N = 31.99, ℓ_min = 11.8. R-632 (β = 1.0) parks at N = 37.88, right after its first
    plateau drain at ℓ = 0.79. Neither reaches the gap.
  - **At A = 0.05.** 2197 parks at its 14th gap bounce. R-632 does not park: 51 gap and 792 classical candidates by
    N_final = 45.08.
  - Parking is ρ_kin/ρ_R < 10⁻⁶, a race against radiation; the energy left at gap exit is about V₀ on this curve
    whatever the prescription.
  - **So the gap prescription matters only at small A, and the small-A failure is a property of small A.**)*
  *(2026-11-04, other M, at the user's request (`source/crossover/README.md` "Other M"; `stage2_mscan.py`,
  `fit_law.py`):
  - **Near M_crit** (the β = 2 ladder at 8 × 10⁻¹⁸–5 × 10⁻¹⁶ M_P, bounces at L ≈ 75–106):
    - one-loop bounces absorb 0.03–0.25 more than the laboratory law;
    - the gap absorbs 0.73 at ln R = 6.7, falling to 0.06 at 2.4, where the laboratory law (in ln R·S) gives 0.003.
  - **At 10⁻³ M_P** every bounce is the test field (frac = R·S, 10⁻⁴⁰ or less).
  - **A law in ℓ, L and R alone fits every curve:**
    - one-loop: 1 − exp[−2.69/(ℓ + 0.97 − 0.41 ln L)], within 0.074 of 27 points, with a cap of about 0.95 and 0.9
      below ℓ = 1;
    - gap: y = 0.020 (ln R − 1.56)^1.55 (1 + 0.84 ln L), f = 1 − e^{−y}, within 0.062 of 34 points;
    - classical: R·S(L).
  - It replaces the laboratory-calibrated law of `measured_law_map.py`. U13 and the drain law remain the user's to
    rule.)*
  *(2026-11-05, the adopted law at small A (`source/crossover/README.md` "The adopted law at small A";
  `measured_law_map.py --law=adopted`). The user adopted the ℓ ≥ 1 law with its ln L shift, the 0.9 plateau and
  the gap fit, and asked whether A = 0.05 still works.
  - **At A = 0.05,** at each history's old-law normalization (a proxy for tuning), every history that tuned under
    the old law still parks. The P and W histories park 0.3–0.4 e-folds later, mostly in the gap's first bounces.
  - **The same two still fail:** R-632 (β = 1.0) and the 5 × 10⁻²⁰ M_P ladder.
  - **Their floor:** R-632 parks at A = 0.5 but not at 0.2; the ladder parks at A = 0.2 but not at 0.1.
  - **Why:** at A = 0.05 no bounce drains more than 0.045. The old law parked small-A fields through the
    divergence of 2.7/ℓ.)*
  **Closed (2026-11-05)** by the user's adoption of the measured law (README §8.10), which replaces 2.7/ℓ and both
  gap prescriptions. The fraction at small ℓ is now measured, not extrapolated: 0.80–0.98 below ℓ ≈ 2, falling
  through the gap with R and L (`source/crossover/README.md`, stage 2 onwards). The code change is prompt 02c's, and
  the re-measurement of the sample 02d's.

- **`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`** — opened 2026-11-02, when
  `[00-reflection-restart-tree-fails-probe-histories]` was split (§4; source `drain-probe_2026-11-02.md` §5).
  On `reflection-restart` at `2d50b95`, at the stored normalization, 2197 and 2188 at their stored
  α = 10⁻³ fail after about 115 s in `classical_turning_point` (ln E = −306.7: a residual not monotone,
  alternating −inf and repeated values, which looks like float cancellation in V − V_floor), though both
  completed on `0209aac`. Logs: `source/probe/drain/results/2197-base-stored-alpha.log`,
  `2188-base-stored-alpha.log`. **Impact:** not investigated; histories of the low-α transition window
  may fail on the fixed tree where they succeeded before, and `reflection-restart` 03's F3 changes the
  trajectory after every restart, so it may be that prompt's regression. Under the one-loop law both
  histories complete on the same tree. **Next:** `reflection-restart` 04.
  **Assigned (2026-11-02):** to `reflection-restart` 04, by the user's ruling 6 (README §6.3), to find
  whether its 03 caused this; that campaign's board §3.1 carries it.
  **Narrowed (2026-11-02) by `reflection-restart` 04** (`ac8ce15`; its board §3.1 holds the measurements, its
  verification document §3.2–§3.6 the evidence): verdict (ii), 03's F3 moves a correct trajectory onto a limit
  of the wall search that predates it. The search cannot find a root of V(φ_t) − V_floor = E that lies below
  17 quanta of ln V's rounding (`WALL_ROUNDING_QUANTA` = 16 treats the region as −∞), and raises "not monotone"
  or "the outer end is still −inf". The step loop meets it at an infall's first step start, near the apex,
  where E/V_floor falls below about 10⁻¹² and the field is far out in the tail (φ/M ≈ 7×10²³ at the two
  failures). The roots are at 1.51 and 1.58 quanta in 2197 and 2188, and at 0.11–16.54 in the nine
  "not monotone" failures of the 2026.8.0 store, made before `reflection-restart`. Eight of those nine still
  fail on the fixed tree.
  **Assigned (2026-11-02):** to this campaign's 01, by the user: "Add the wall-search fix to `one-loop-drain`
  01." The fixed law runs these histories, and 01 already owns the related inside-wall stop of 3873. The form of
  the fix is README §8.8, P21. **Next:** 01.
  **Closed 2026-11-03 by prompt 01** (`b99f6a4`; `.documents/one-loop-drain-verification.md` §1.3–§1.4): the
  rule of README §8.8 P21, `_turning_point_unresolvable`, makes `classical_turning_point` return None where ln E lies
  below the least finite ln(V − V_floor); only calls that raised change. All 19 measured failing inputs now return
  None with the predicate holding, and each raises with a copy of the method before the fix. 2197 and 2188 at their
  stored normalization, α = 10⁻³, complete (131 and 46 assessments answered); the nine of `reflection-restart` 04
  §0.2, tuned, all succeed; the 42 replay equal 04's; the predicate costs under 10⁻³ of the wall time.

- **`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`** — opened 2026-11-02, when
  `[00-reflection-restart-tree-fails-probe-histories]` was split (§4; source `drain-probe_2026-11-02.md` §5).
  3873 (10⁻¹² M_P, β 1.11), run on `2d50b95` at its stored normalization with nothing drained, stops at
  N = 43.76 at the check "reflection requested inside the wall" (W/(π²/2) = 1.485); at its stored
  α = 0.03 it passes. Log: `source/probe/drain/results/3873-A1.log`. The check dates from `fc97233`
  (2026-10-01, the integrator audit), not from `reflection-restart` 03, as the source record and the first
  form of this entry said. **Impact:** under the one-loop law every large-M history drains nothing, so all
  of them take the path on which 3873 stops; how many stop is not known. **Next:** 01 (README §8.4).
  **Narrowed 2026-11-03 by prompt 01** (verification §1.7): the branch is the unresolved turnaround. In the wall's
  linear tail (nothing drained, E/V₀ ≈ e^{−9.2}) W/ρ_kin = 0.5 at 3φ_t and exceeds 1 inside 2φ_t. φ_t crossed the
  10⁻⁸ M_P threshold only at φ = 1.67 φ_t, so the rule fired late and G2 refused. The same mechanism is behind 21 of
  the 2026.8.0 store's fixed-law failures (M = 10⁻¹⁵–10⁻¹² M_P, β 0.5–1.0). **Ruled by the user, 2026-11-03**, on
  option (b): "I see. I had assumed that was an energy balance that meant the field and moved beyond its reflection
  point, but I see that it's not. In which case, (b) is correct." **Closed 2026-11-03 by prompt 01** (`b99f6a4`):
  where the unresolved reflection would fail G2 (G2's own expression), that step start makes no unresolved
  decision and the floor rule is tested as usual; every such state raised before, so nothing that succeeded moves.
  3873 under the one-loop law at A = 1 now completes (19 declined step starts); 3016 and 3233 are unchanged; 3460
  and 4397, two of the 21, complete tuned under the fixed law (4 and 13 declined in the accepted integration).

- **`[00-reflection-restart-tree-fails-probe-histories]`** — opened 2026-11-02 from the drain probe;
  **closed the same day by splitting** at the user's ruling 6 (README §6.3) into
  `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, assigned to
  `reflection-restart` 04, and `[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`, for this
  campaign's 01 (§3). Its text said the inside-wall check was added by `reflection-restart` 03; it dates
  from `fc97233`.

## 5. Baselines

The suites as `CLAUDE.md` records them (2026-11-01, after `inelastic-reflection` 12): CosmologyModels 26,
ComputeTargets 497 (122 + 375), ObjectFactories 129, tools 123, RunDrivers 68. `c95650a` added one to
ObjectFactories, and `reflection-restart` will add more before 01 starts, so 01 measures the counts again
on its own tree before it changes anything.

**2026-11-02, at the re-plan:** on `reflection-restart` after its 03 (`b81eab8`), CosmologyModels 26,
ComputeTargets 514 (122 + 392), ObjectFactories 130, tools 147, RunDrivers 68. `reflection-restart` 04
may add more, so 01 still measures them on its own tree first.

**2026-11-03, prompt 01:** measured first at `b1415a3`: 26, 514 (122 + 392), 130, 147, 68, all passing. After 01:
26, 566 (163 + 403), 130, 156, 68, all passing (log 01 §4).

**2026-11-03, prompt 02:** measured first at `b1d6bc5`: 26, 566 (163 + 403), 130, 156, 68, all passing. After 02 (no code
changed): the same counts, all passing (log 02 §4).

**2026-11-04, prompt 02b:** measured first at `62d3f2f`: 26, 566 (163 + 403), 130, 156, 68, all passing. After 02b (no code
changed): the same counts, all passing (log 02b §4).
