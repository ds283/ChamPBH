# one-loop-drain campaign — implementation state

**Last updated:** 2026-11-07 · **Status: LIVE — 10 of 11 landed (01, 02, 02b, 02c, 02d, 03, 04, 05, 06, 07); the measured law in the code (02c), measured on the sample (02d), stored (03), exported by the drivers (04), its spectrum-dependent measures derived from the exports (05), and rehearsed under Ray (06: Bitwise and R hold); 07: the figure tools (`tools/plot_quanta_grid.py`, `tools/plot_quanta_history.py`) render from 06's and 05's exports, the gate Figures holds; 08 (the close) written.** Planned
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
and a tabulated S(L). 02c and 02d inserted before 03, 03 revised, 02c written; U15 (n ≠ 1) open for the user (README §8.10).)* *(2026-11-05: stage 2 on the n = 2 wall supports
L = ln(1 + E/V₀); the user ruled U15 (b), the law at every n with that L (README §8.10). 02c's gate is met.)* *(2026-11-05: prompt 02c
landed; the Measured gate holds. Two rulings of the user during it: the gap fraction is 0 where 1 + q ln L ≤ 0, and L moves
into the model class with no default; one stop ruled (the maps' headers). No issue opened or closed.)* *(2026-11-05: prompt 02d
landed; the Remeasure gate holds. 230 of 240 tunings succeed under the measured law; R-632 fails at A = 0.1 and 0.05 and the
5 × 10⁻²⁰ M_P ladder at A = 0.05, which extends the §3 issue; C-3873 times out at seven A. Its numbers replace 02's and 02b's
as the campaign's measurement of the one-loop law. No issue opened or closed.)* *(2026-11-06: the campaign reviewed before 03 (README §8.11):
ready, with three stale items noted for 04–06. The planner proposed P27–P32 for the store, the defaults unless the user
says otherwise: the drain-model key on `alpha_value` (U8), with `fixed` where no model is named. Prompt 03 written.)* *(2026-11-06: prompt 03
landed; the Store gate holds. The drain-model key, the bounce table, the history's drain record and the sample masses are stored,
`FINGERPRINT_FORMAT` 7 and `VERSION_LABEL` 2026.10.0; two issues closed (§4), none opened.)* *(2026-11-06: the campaign reviewed before 04 (README §8.12). The planner proposed P33–P40 for the drivers and exports, the defaults unless the user says otherwise: `--A-values` beside `--alpha-values`, the one-loop products under `<label>/<drain_model>/A=<A>/`. Prompt 04 written.)* *(2026-11-06: prompt 04 landed; the Export gate holds. `--A-values` is in `main.py` and the plot scripts, the one-loop products go under `<label>/<drain_model>/A=<A>/`, histories.csv gains `drain_model` and the drain record, fields.csv the sample masses, and a new bounces.csv holds the bounce rows; the fixed law's exports are byte-identical to the dispatch tree's. No issue opened or closed.)* *(2026-11-06: the campaign reviewed before 05 (README §8.13). The planner proposed P41–P49 for the derivation tool, the defaults unless the user says otherwise, and put U16 to the user: whether U4's spectrum shape stays for the one-loop generations, which now carry most of the drained energy. Prompt 05 written on the proposal, U4 as ruled; it is not dispatched until the user answers U16.)* *(2026-11-06: U16 ruled by the user: U4 stays for every generation, the associated piece at scale 1 the default; the alternatives withdrawn (README §8.13). Prompt 05 ready to dispatch.)* *(2026-11-06: prompt 05 landed; the Derive gate holds. U17 ruled by the user at dispatch: the associated piece's 1/x runs to x_p and falls as e^{−κ(x − x_p)} above. `ComputeTargets/quanta_spectrum.py` holds U4's shape (`U4-U17-v1`), its constants, moments and kernels; `tools/quanta_measures.py` derives n_g, n̄, δ_stim, E, the phase slip and the backreaction ratio from 04's exports. Run on 02d's 230 successes: every task re-integrated equal to 02d, δ_stim ≤ 1.3 × 10⁻³. No issue opened or closed; one narrowed (§3).)* *(2026-11-06: the campaign reviewed before 06 (README §8.14). The planner proposed P50–P57 for the rehearsal, the defaults unless the user says otherwise: the base `1561b60` built from a scratch worktree, the workers' code probed, three runs per store with nine of 02d's one-loop tasks, the gates Bitwise and R made exact. Prompt 06 written.)* *(2026-11-07: prompt 06 landed; the gates Bitwise and R hold. The fixed law built at `1561b60` and at the tip agrees in 2 524 629 values; the one-loop build interrupted by `SIGTERM` and resumed equals the straight one, and both equal 02d's tool route bit for bit, Y_p and D/H included; the readers and the tool behave under Ray as 04 and 05 said. No issue opened or closed.)* *(2026-11-07: the campaign reviewed before 07 (README §8.15). The planner proposed P58–P65 for the figure tools, the defaults unless the user says otherwise: `tools/plot_quanta_grid.py` (heat maps over β and M per A, ten panels) and `tools/plot_quanta_history.py` (six panels on N), a shared `tools/quanta_figures.py`, tests on constructed exports, a demonstration on 06's and 05's exports written under `var/one-loop-drain/07/`, and the plot scripts' footer "Reflections (elastic model)" relabelled. The shard warning recorded in DatastoreKit at the user's request (`befa7f3`). Prompt 07 written.)* *(2026-11-07: prompt 07 landed and was reviewed (`867b267`, `c8b87d6`); the gate Figures holds. No issue opened or closed.)* *(2026-11-07: the campaign reviewed before 08 (README §8.16). Every gate holds and none of the three open issues is assigned here. The planner proposed P66–P73 for the close, the defaults unless the user says otherwise: no code; the law and the measures for the paper as a new §8 of the verification document; reading guides in `numerical-strategies.md` §6.12 and `architecture-summary.md` §7.2; `CLAUDE.md` by description only; the handover for the rebuild of record as `review-remediation-verification.md` §4.20; no issue closed, two given dated lines and one narrowed; the inventory run read-only on the reference store and on a copy of the base store; the merge the user's. Prompt 08 written.)*

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
| 02c | [The measured law](02c-the-measured-law.md) | inserted 2026-11-05 (README §8.10): the adopted law in `ScalarHistory.py`, replacing 2.7/ℓ and both gap prescriptions; S(L); U15 (b), L = ln(1 + E/V₀) at every n; the `"drain"` record names the model version and records L and ln R; `--gap-prescription` removed; the reproduction of crossover's adopted-law maps | Opus | ✅ | ✅ | `18add05` | [02c](logs/02c-the-measured-law.md) |
| 02d | [Measure the measured law](02d-measure-the-measured-law.md) | inserted 2026-11-05: 02's thirty histories re-tuned under the measured law at U14's eight A, every task run (no shortcuts); regime shares, the law's edges, parking, the first impact and the cap, ΔN_eff, Y_p, D/H, against 02/02b and the fixed law; the cost of the classical drains; replaces 02's and 02b's one-loop measurements | Opus | ✅ | ✅ | `98b1601` | [02d](logs/02d-measure-the-measured-law.md) |
| 03 | [The store](03-the-store.md) | the drain-model key on `alpha_value` (U8, README §8.10 decision 1, P27), `ScalarHistoryBounce` under the one-loop law (P29), the history columns (regime shares, counts, ℓ_min, the all-generation margins; P30), the sample masses at every law (U5, P31), the guard (P28), `FINGERPRINT_FORMAT` 7, `VERSION_LABEL` 2026.10.0; closes two §3 issues | Opus | ✅ | ✅ | `fe4c917` | [03](logs/03-the-store.md) |
| 04 | [Drivers and exports](04-drivers-and-exports.md) | `--A-values` in `main.py` and the plot scripts, beside `--alpha-values` (P33, P34); the one-loop paths and labels (P35); `drain_model` and the drain record in `histories.csv` (P36), the sample masses in `fields.csv` (P37), a new `bounces.csv` (P38); the raw-SQL readers of `alpha_value` and the inventory report's label (P39); the registered run's scope (P40) | Sonnet | ✅ | ✅ | `64a6274` | [04](logs/04-drivers-and-exports.md) |
| 05 | [Derivation tool](05-derivation-tool.md) | `ComputeTargets/quanta_spectrum.py` (U4's shape, its moments and the kernels G and Ω; P41) and `tools/quanta_measures.py` reading 04's exports alone (P42): K(N) rebuilt from the bounce rows (P43); n_g, n̄_j, δ_stim, E(N), the phase slip and the backreaction ratio (P44); three outputs for 07 (P45); the associated piece at 0, 1 and 10 (P46); run on 02d's 230 successes (P47); the Derive gate (P48); the R·S(L) bound dropped (README §8.13) | Opus | ✅ | ✅ | `9830738` | [05](logs/05-derivation-tool.md) |
| 06 | [Rehearse](06-rehearse.md) | registered builds under Ray (P50–P57): the fixed law at the base `1561b60` against the tip, bitwise, with the workers' code probed; the one-loop law interrupted and resumed, against the straight build and 02d's tool route; the readers, `--A-values` and the derivation tool on the exports | Opus | ✅ | ✅ | `1416b80` | [06](logs/06-rehearse.md) |
| 07 | [Figure tools](07-figure-tools.md) | `tools/plot_quanta_grid.py` (P59) and `tools/plot_quanta_history.py` (P60) from 04's exports and 05's outputs, sharing `tools/quanta_figures.py` (P58, P61); tests on constructed exports (P62); rendered on 06's and 05's exports, the PNGs committed (P63); the footer label (P64) | Sonnet | ✅ | ✅ | `867b267` | [07](logs/07-figure-tools.md) |
| 08 | [Close](08-close.md) | the law and the measures for the paper (verification §8, P67); the reading guides in `numerical-strategies.md` §6.12 and `architecture-summary.md` §7.2 (P68); `CLAUDE.md` by description (P69); the handover for the rebuild, `review-remediation-verification.md` §4.20 (P70); the issues (P71); the inventory checks (P72); the board and index closed (P73) | Opus | ✅ | ⬜ | | |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

Prompts are written one at a time, each after the previous one lands and is reviewed.

Orchestrator notes, one prompt at a time, in `orchestrator/`:
- [`prompt-01.md`](orchestrator/prompt-01.md) (`b1415a3`) — used for 01.
- [`prompt-02.md`](orchestrator/prompt-02.md) (`b1d6bc5`) — used for 02.
- [`prompt-02b.md`](orchestrator/prompt-02b.md) (`62d3f2f`) — used for 02b.
- [`prompt-02c.md`](orchestrator/prompt-02c.md) (`1647f7a`, amended in the review commit, its §5) — used for 02c.
- [`prompt-02d.md`](orchestrator/prompt-02d.md) (`f4da906`) — used for 02d.
- [`prompt-03.md`](orchestrator/prompt-03.md) (`d3069cb`) — used for 03.
- [`prompt-04.md`](orchestrator/prompt-04.md) (`8df05c1`) — used for 04.
- [`prompt-05.md`](orchestrator/prompt-05.md) (`88c6ae5`) — used for 05.
- [`prompt-06.md`](orchestrator/prompt-06.md) (`41d18c4`) — used for 06.
- [`prompt-07.md`](orchestrator/prompt-07.md) (`d4031dc`) — used for 07.

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

**Orchestrator review of prompt 02c (2026-11-05)**, by orchestrator note 02c §3, on `18add05`. Passed; seven
checks:

During the work the user made three rulings, each forwarded in their words and recorded in README §8.10 and in note
§5:
- the gap's f = 0 where 1 + q ln L ≤ 0, in place of the note's ruling 9, so no issue was opened;
- L moved into the model class with no default, which the orchestrator raised after the agent's stop;
- ruling 4's stop (the maps' headers) ruled the exception.

1. **Scope.**
   - `git show --stat 18add05` lists 28 files: prompt §5's, ruling 11's, and `AbstractModel.py` and
     `ExponentialModel.py` by the user's ruling. Its parent is `1647f7a`.
   - The tree is clean, and no remote branch contains `18add05`.
   - In `source/crossover/`, only `cascade_map.py` (the `_GAP_PRESCRIPTION` guard) and `measured_law_map.py` (the
     patch's `*args`) change. `law/`, `measure/` and `measure-02b/` change only by their new READMEs.
   - Nothing under `config/`, `ObjectFactories/`, `main.py`, the plot scripts, `PRyM/` or `thirdparty/` changes, and
     `VERSION_LABEL`, `FINGERPRINT_FORMAT` and the stepper label are untouched.
   - The removed names appear in the tree only in absence tests and dated comments.
2. **Nothing left running.** The two batch pids (20401, 86349) are dead, both `batch.end` files exist, and
   `runregistrykit list` shows 23 runs, all finished.
3. **The law, read.**
   - `drain_fraction` and `drain_alpha_effective` follow README §8.10 and `fraction_adopted` term for term,
     including the evaluation order of the gap's product.
   - The S table's 21 pairs equal `stage1a_S_of_L.txt`'s columns L and S(L) exactly (loaded by `np.loadtxt`).
   - The continuation and the clamp are where the rulings put them.
   - `ExponentialModel._drain_L` is the stable form of note ruling 2. At x = −30, 0, 30, 102 and 800 it gives
     9.36 × 10⁻¹⁴, ln 2, 30.000000000000092, 102 and 800. `AbstractModel._drain_L` raises and is not abstract.
4. **The reference, independently.** A scratch script compared the production law with `fraction_adopted` imported
   from `source/crossover/` (not the test's copy), at 10⁴ random points over the three assessed regimes, four M,
   ℓ in (0, 40), L in (0.01, 300) and ln R in (−50, 30), with α_eff at three A and two w_gate:
   - 8717 points are bitwise equal;
   - 252 are classical points above L = 134, equal bitwise to exp(ln R)·(S(134)·(134/L)²);
   - 1031 are gap points with 1 + q ln L ≤ 0, where the reference is ≤ 0 and production is 0.0;
   - none departs otherwise, and no one-loop point departs at all.
5. **One map, re-run.** W-2187 at A = 0.05, unpatched `cascade_map.py` in a process of my own (17.7 s, load 5.0):
   - byte-identical to the agent's `var/one-loop-drain/02c/maps/W-2187-A0.05.txt`;
   - its 199 rows equal crossover's;
   - its header shows the ruled last-digit difference: `parking_N` 39.02200520613764 against 39.022005206137614.
6. **The tool, re-run.** §2.4's command (26.6 s): its `drain` line equals the agent's, and its `history` line too,
   apart from the wall time. That is 192 one-loop and 11 gap candidates, ℓ_min 0.0977756, and
   `model=one-loop-measured-v1`.
7. **The records.**
   - The log follows §5.1's seven sections, with its fourteen deviations classed, among them the note's rulings and the user's three.
   - Verification §1, §2 and §2b are unchanged (the diff only adds lines), and §2c follows prompt §2's four items.
   - On the board: row 02c, its items, the Measured gate and **Last updated**. No issue moved, so
     `.documents/OPEN_ISSUES.md` stays at 31.
   - The suites, re-run serially in the review: 26 (70 s), 162 (142 s) + 432 (481 s), 130 (91 s), 156 (206 s), 68
     (27 s), all passing, at 1-minute loads of 11–51 at their starts. These equal `CLAUDE.md`'s new counts.

   The review substituted `18add05` for "this commit" on the board, in the log, and in the three dated READMEs. It
   recorded the user's three rulings in README §8.10 and added §5 to note 02c.

Noted, not acted on:
- The fitted constants and S(L) are measured on the exponential family too (log 02c §6). Moving L into the model does
  not make them model-independent. If another potential is added, the law's applicability to it is a question for
  the user, beside `_drain_L`.
- Classical candidates now drain, each taking a restart: 83 on the ladder at A = 0.2. 02d measures the cost on the
  whole sample.

**Orchestrator review of prompt 02d (2026-11-05)**, by orchestrator note 02d §3, on `98b1601`. Passed; seven
checks. No stop was raised during the work.

1. **Scope.**
   - `git show --stat 98b1601` lists 25 files, all ruling 10's. Its parent is `f4da906`.
   - The tree is clean, and no remote branch named `one-loop-drain` exists.
   - The diff over the code paths, over `law/`, `law-02c/`, `measure/` and `measure-02b/`, and over `CLAUDE.md` is
     empty.
   - `measure-02d/out/` holds text only.
   - `var/one-loop-drain/02/`, `02b/` and `02c/` hold no file newer than the note.
2. **Nothing left running.** No pid of the six batches answers `kill -0`, every `batch.end` exists, and
   `runregistrykit list` shows the same 23 finished runs.
3. **The sample is complete.** `sample.jsonl` has 240 lines with 240 distinct ids, equal to `tasks.json`'s, every one
   run and untagged. Outcomes: 230 success, 3 failure, 7 timeout.
   - The failures are R-632 at A = 0.1 and 0.05 and L-M5e-20 at A = 0.05.
   - The timeouts are C-3873 at every A but 1.
4. **The law against its record, independently.** A scratch script of mine read all 245 partial files (the 240
   tasks and the checks'), 616 270 candidates.
   - `drain_alpha_effective` of each candidate's own fields and the record's A equals the stored α_eff at every one,
     by `repr`.
   - So does `fraction_adopted`, imported from `source/crossover/measured_law_map.py` with L passed exactly (M_MP =
     2L, φ_t = 2), the clamp applied, and the continuation set aside. No candidate reached either edge, so the
     reference agrees bitwise everywhere.
5. **One task, re-run.** I re-ran C-3016 at A = 0.05 (330 classical drains) by 02d's driver, in a process of my own,
   57 s at a load of about 58. Its result equals the batch's line under ruling 8: every field but the excluded
   walls and RSS. Its partial file equals the batch's (330 injections and 330 candidates, by `repr`), and so does
   its stdout, but for the wall times.
6. **The tool and the numbers.**
   - **The tool.** `tools/history_and_bbn.py 1.0775 4e-31 --drain-law one_loop --alpha 5.0` (W-2187, 21 s). Its
     `history`, `dissipation`, `drain` and `bbn` lines equal the driver's for W-2187-A5 once `wall=` and
     `PRyM_time=` are masked.
   - **The cap.** Recomputed for P-2188 at A = 10 from its partial: 3 one-loop candidates, as the line says.
   - **2197's ΔN_eff today** at A = 10 … 0.05 is 0.5554, 0.2727, 0.1170, 0.09026, 0.06009, 0.03280, 0.01131 and
     0.008558, as §2d.7 states.
   - **The failures at A = 0.1 and 0.05** are three, as stated.
   - **The clamp** is met by no gap candidate. Over the last returned integration of every task, failed tunings
     included, there are 350 gap candidates and the least L is 0.679. That is R-632's failed tunings at A = 0.1 and
     0.05, above the clamp's 0.3045. The 2.578 of §2d.5 is over the 230 accepted integrations, as it says.
7. **The records.**
   - The log follows §5.1's seven sections. Every deviation is classed: the note's ten rulings, and D1–D7, two of
     them unintended drift, corrected before the commit.
   - Verification §1–§2c are unchanged (the diff only adds §2d, after §2c), and §2d.1–§2d.12 follow prompt §2's
     eight items. The handover states measurements and makes no choice.
   - On the board: row 02d, its two items, the Remeasure gate, the dated paragraph extending the §3 issue, the
     suites, and **Last updated**. `.documents/OPEN_ISSUES.md`'s row matches it; the count stays at 31.
   - `CLAUDE.md` is unchanged. The suites were not re-run in the review. The agent ran them before and after, at 26,
     594 (162 + 432), 130, 156 and 68, all passing, and 02d changes no code or test.

   The review substituted `98b1601` for "this commit" on the board, in the log and in verification §2d.

Noted, not acted on:
- **C-3873 costs its tuning, not its integrations.** It needs 24–28 integrations against 16 under the old law, and
  it timed out at seven A. A rebuild of record that includes M = 10⁻¹² M_P, β = 1.11 will meet the same cost through
  `tune_history` (log 02d §6).
- **The failed R-632 tunings reach the gap at L ≈ 0.68,** lower than any accepted integration (2.578). That is still
  above the clamp, but nearer to it than §2d's ranges suggest.
- **The projection's P ratio (3.12) was set by the load,** not by the law. P's A = 0.05 tasks ran at a load of about
  30 and its A = 0.5 tasks at about 6. The projection (1.30 h) still bounded the batch (1.00 h).

**Orchestrator review of prompt 03 (2026-11-06)**, by orchestrator note 03 §3, on `fe4c917`. Passed; eight checks.
No stop was raised. The agent was cut off once by an API usage limit, while writing its tests, and was resumed from
its own working tree; nothing was done twice.

1. **Scope.**
   - `git show --stat fe4c917` lists 39 files, ruling 12's and the extras §0 named. Its parent is `d3069cb`, the
     note's.
   - The tree is clean, and no remote branch named `one-loop-drain` exists.
   - I read the whole diff of `ComputeTargets/ScalarHistory.py`. No line of the step loop, the law, the
     classification, the gate, the margins' core or the tuning's iteration changes.
     - `history_margins` is now `relativistic_margins(*margin_inputs(...))`, its old gathering body moved unchanged
       into `margin_inputs`.
     - `check_drain_law` moved verbatim to `CosmologyConcepts/drain_model.py`.
     - `integrate_history` and `tune_history` change only after the integration.
   - `config/fingerprint.py` adds format 7 and leaves formats 1–6 as they were. The old golden files are untouched.
   - The one change under `tools/` is `outward_step_scan.py`'s `store_rows`, which mirrors `store()`; ruling 11
     allows it, and its tests forced it.
2. **Nothing left running.** No pid under `var/one-loop-drain/03/` answers `kill -0`, every `.done` exists, and
   `runregistrykit list` shows the same 23 finished runs.
3. **The suites, re-run by me** serially, at loads of 3.7–10.3 at their starts: 26 (65 s), 622 (190 in 145 s + 432
   in 468 s), 149 (118 s), 156 (163 s) and 68 (24 s), all passing. These are the counts the log states, and none fell.
4. **The fixed law, independently.** A scratch script of mine was run in a worktree at `d3069cb` (since removed)
   and at `fe4c917`. It integrated the golden history at α = 0 and at α = 0.3, and the reflecting setup (β = 1.5,
   M = 10⁻¹² M_P) at α = 0.2. In all three payloads every key of the parent is equal by `repr`. The only new keys are
   `bounces` (None), `log_m_eff` and `log_m_tracker`.
5. **A one-loop round trip, independently.** P-2197 at A = 1, at its seed realization, which equals 02d's accepted
   tuning bitwise, was stored on a temporary store in my scratchpad and read back.
   - **Rows:** 9 bounce rows, 5 candidates (all reflections) and 4 plateau events, as candidates plus events
     predict. Ordinals run 0–8, ordered by N, the candidates in index order.
   - **ln R:** `log_R` is equal to `DrainCandidate.log_R` and to `PeakEstimate.log_R` by `hex`.
   - **K before:** the `K_before` chain equals my own float walk (K₀, plus each draining candidate's ΔK, plus the
     leftover after the cut candidate) bitwise. The walk ends at the ledger's K(N_final), which is
     `BounceRecord.K_final`.
   - **Shares:** the four regime shares, the parking share, the counts and ℓ_min equal `drain_values` by `repr`. The
     shares plus K₀/K_final sum to 1 exactly.
   - **Margins:** the nine all-generation values equal the core's, run with `MARGIN_WEIGHT_SHARE` patched to 0, by
     `repr`.
   - **Read-back:** every column is equal through the conversion definition. In Planck units every float field also
     reads back bitwise equal to the payload's value. The drain record reads back equal, and the `bounce_rows`
     column is 9.
   - **Masses:** none of them is NULL at the 4 995 samples, and they read back equal.
   - **`_do_not_populate`:** `drain_history` is readable and equal, and `bounces` raises.
6. **The key.**
   - One value, 0.5, gives two `alpha_value` rows under the two models, and a lookup with no model finds the fixed row.
   - Each of these is refused with a `ValueError`, and the table's row count is unchanged:
     - A = 0 and A = ∞ under the one-loop model;
     - the unknown model `one-loop-v0`;
     - α = 1 under the fixed model.
   - `store()` raises before storing in both directions: a one-loop payload under a fixed key, and a payload with no
     drain record under a one-loop key.
7. **§2.4's numbers.** From the six `result.json` files I recomputed each ratio (added = bounce key + masses +
   record + max(0, full − base write and read)). Each equals the file's, and the median is 0.0104, the largest
   0.254 (P-2197). W-2187's 199 stored candidate rows, read from its store by SQL, equal the last line of its 02d
   partial file on all eight `DrainCandidate` fields.
8. **The records.**
   - The log follows the template, and classes every deviation and each of §0's rulings.
   - Verification §1–§2d are unchanged; §3 follows prompt §2.
   - On the board: row 03, its items, the Store gate, the two issues moved to §4 with their closure lines, and
     **Last updated**.
   - `.documents/OPEN_ISSUES.md` holds 29 rows, its count says 29, and it is dated 2026-11-06.
   - `CLAUDE.md`'s counts are those measured, and its "Redshift" sentence names the drain model.
   - `architecture-summary.md` and `numerical-strategies.md` have their dated notes and remove nothing.

   The review substituted `fe4c917` for "this commit" on the board, in the log and in verification §3. It also
   corrected `prompts/INDEX.md`'s row, whose open count (6, of 32) was stale before 03.

Noted, not acted on:
- **The masses are most of the added cost at laboratory M.** P-2197's ratio, 0.254, is the only one near 0.25, and
  its integration takes only 2.6 s. The log's §6 notes that the tracker's mass after N_late is computed twice. Reusing
  the margins' values is a possible saving, if a rebuild's cost calls for it.
- **The guard checks the model, not A**, and a hand-built one-loop payload without `"bounces"` stores no row (log
  §6). The stored path makes neither.
- **The wall-side ln T_J path of ruling 7** was exercised on stored data only at C-3233 (73 candidates); P-2197's
  candidates are all reflections.
- **For 04:** the raw-SQL readers of `alpha_value` and the inventory report's α label (log §6).

**Orchestrator review of prompt 04 (2026-11-06)**, by orchestrator note 04 §3, on `64a6274`. Passed; seven checks.
No stop was raised. Before dispatch the orchestrator put to the user a question about one column of bounces.csv,
`log_T_Jordan_GeV`, which it took to be the one converted column that could not equal its stored column bit for bit.
The user called the question misconceived (2026-11-06): which columns come back exactly depends only on the units
(M_P = 1 in Planck units is an accident), and the units system exists so that they can change. Note 04's ruling 1
therefore states the export's equality as an identity of the store's own conversions, the same for every column. The
orchestrator's figure of about half the cells changing came from random values; on stored temperatures in Planck units
ln T and ln GeV are within a factor two, the subtraction is exact, and the agent measured 0 of 55 000 cells differing.

1. **Scope.**
   - `git show --stat 64a6274` lists 34 files, prompt §5's and ruling 9's, with the changed test modules the log names.
     Its parent is `8df05c1`, the note's.
   - The tree was clean, `git worktree list` showed the main checkout alone, and nothing is pushed.
   - Under `ComputeTargets/` and `ObjectFactories/` only tests change. `FINGERPRINT_FORMAT` is 7, `VERSION_LABEL`
     2026.10.0, and `tools/history_and_bbn.py` is unchanged.
2. **The suites, re-run by me** serially at loads of 5.4–36.3 at their starts: 26 (68 s), 651 (219 in 149 s + 432 in
   476 s), 163 (131 s), 173 (215 s) and 72 (28 s), all passing. These are the counts the log states, and none fell.
3. **The exports, independently.** A scratch script of mine stored P-2197 at A = 1 and the fixed-law histories at
   α = 0.1 and α = 0 on a temporary store, exported them with the agent's functions, and checked every new cell against
   the raw column read by SQL, by ruling 1's identity: 69 drain cells of histories.csv (blank under the fixed law), 31 138
   mass cells of fields.csv (no NULL among them) and 270 cells of bounces.csv (9 rows, `bounce_rows` 9, ordinals 0–8).
   `write_bounces_csv` under `Mpc_units`, on a constructed row, equals `bounce_row_columns` cell by cell.
4. **The fixed law, byte for byte.** A second script of mine, run in a scratch worktree at `8df05c1` (since removed) and
   at `64a6274`, wrote histories.csv and fields.csv (the base's `write_csv_file` by `ast`, unedited) for the same three
   histories. Projected onto the base's columns, every file of the tip is byte-identical to the base's, P-2197's
   included.
5. **The drivers.** I read the diffs of `main.py`, `plot_inputs.py`, both plot scripts and `extract_common.py`. My probe:
   `--A-values` is None by default and refuses 0, −0, −1, nan, inf and text with exit 2, naming the option; 10 and
   1000 parse. `alpha_label` gives `α = 0.1`, `alpha = 0.1` and `$\alpha$ = 0.1` under `fixed` and
   `A = 1 (one-loop-measured-v1)` otherwise. `alpha_path` gives `alpha=0` for −0 and
   `one-loop-measured-v1/A=1.2346`, and refuses a call with no model or an unknown one. `plot_inputs.parameters` returns
   the fixed rows, then the A rows, each with its model, and refuses an A stored only as an α (`--A-values`) and an α
   stored only as an A (`--alpha-values`).
6. **The tools.** My probe of `bbn_from_store._find_query` on an imitation store with the same value under both
   models finds each law's own history; without the `drain_model` column it finds the fixed one and, under the one-loop
   model, nothing. `_law_alpha` accepts `--alpha 10` under `one_loop`, refuses it under `fixed` with today's message,
   and requires `--alpha` under `one_loop`. `describe_scope` adds `A 0.5, 1` only with `--A-values`. The inventory
   report's and `outward_step_scan.py`'s counts I checked by reading the code and the tests that pin them
   (`test_inventory_alpha`, `test_inventory_report`, `test_one_loop_tools`), not by a probe of my own.
7. **The records.**
   - The log follows the template, and classes every deviation and each of note 04's rulings.
   - Verification §1–§3 are unchanged; §4 follows prompt §2.
   - On the board: row 04, its items, the Export gate and **Last updated**.
   - `CLAUDE.md`'s counts are those measured, and "The plot scripts" names `--A-values` and the one-loop path.
   - `architecture-summary.md` has its dated note.

   The review substituted `64a6274` for "this commit" on the board and in the log. It corrected the log's §7, whose
   first sentence said every exported cell is the stored column exactly, which holds only in some units, and it updated
   `prompts/INDEX.md`'s row.

Noted, not acted on:
- **The `--A-values` refusal names the option twice** (`argument --A-values: --A-values: under the one-loop drain law
  alpha carries A, …`), because `check_drain_model`'s message begins with its context. The message is right;
  rewording it would change the rule's one statement.
- **`plot_ScalarHistory.py` kept `pandas` and `nan`**, which the moved code no longer uses (log §6). Removed in a
  housekeeping commit after this review.
- **Two copies of `_law_alpha`** (`tools/history_and_bbn.py`, `tools/bbn_from_store.py`; log §6).

**Orchestrator review of prompt 05 (2026-11-06)**, by orchestrator note 05 §3, on `9830738`. Passed; seven checks.
Before dispatch the orchestrator found that U4's associated piece, 49 x_*³/x at every x, makes I diverge, and put its
upper end to the user, who chose the 1/x piece to x_p, falling above it as e^{−κ(x − x_p)} (U17, README §8.13; note 05,
ruling 1). The agent stopped once on a usage limit after §1.6's run had ended, and was resumed from its transcript;
nothing ran in between. No stop condition was raised.

1. **Scope.**
   - `git show --stat 9830738` lists 27 files, ruling 12's. Its parent is `88c6ae5`, the note's.
   - The tree was clean, and nothing is pushed.
   - Under `ComputeTargets/` only `quanta_spectrum.py` and its test are new, and nothing under `ObjectFactories/`
     changes. `FINGERPRINT_FORMAT` is 7 and `VERSION_LABEL` 2026.10.0. `plot_ScalarHistory.py`'s diff is the helper's
     two lines and its import.
   - No pid recorded under `var/one-loop-drain/05/` answers `kill -0`, and nothing under `var/one-loop-drain/0[1-4]*/`
     is newer than the note.
2. **The suites, re-run by me** serially, 15:27–15:45, at loads of 4.6–17.7 at their starts: 26 (67 s), 665 (219 in
   155 s + 446 in 478 s), 163 (130 s), 191 (210 s) and 72 (25 s), all passing. These are the counts the log states,
   and none fell.
3. **The spectrum, independently.** A scratch script of mine, with its own shape and SciPy's adaptive quadrature on
   panels split at x_p, at y and at x_p e^u:
   - κ = 3.9657820823215935, equal to the module's bit for bit;
   - I and J within 8 × 10⁻¹⁶ at c_a = 0, 1 and 10;
   - G within 4.7 × 10⁻¹² and Ω within 2.25 × 10⁻⁹ at random points inside the tables, between nodes, at their ends
     and a decade beyond, at the three scales; the quadrature's own error estimate is below 2 × 10⁻¹³.
4. **The measures, independently.** A second script of mine reads bounces.csv, fields.csv and histories.csv and
   computes K(N), ln n_g, n̄, δ_stim, E with both masses, the phase slip, k_ex/H and the backreaction ratio by its own
   arithmetic. E uses a fixed Gauss–Legendre quadrature over the full spectrum, without the tables.
   - **P-2197 at A = 1:** every measure within 4.2 × 10⁻⁸ of the tool's; K(N) equal, the leftover after the last
     candidate included.
   - **The worst Ω of my fixed quadrature.** It has no break at x_p e^u, and on the larger histories it reached
     2 × 10⁻⁶ in n̄. Against my adaptive quadrature the module's Ω is within 6.3 × 10⁻¹¹ over the whole u range of
     C-3016-A0.05 (0.01 to 21). With the module's Ω in my sum, n̄ and δ_stim agree to 5 × 10⁻¹⁵. That holds on
     C-3016-A0.05 (parking share 0.999993) at the three scales, on L-M1e-16-A0.5 (11 158 generations, the largest
     δ_stim) and on C-3873-A1 (no parking), and E agrees to 10⁻¹⁵ on each.
   - **One phase slip differs by 5.6 × 10⁻⁵** (L-M1e-16-A0.5, ordinal 7434: slip 2.44 rad against θ ≈ 10¹⁰, with
     exponents up to 130 in magnitude). That is within the formula's own conditioning, about 130 ε θ/slip ≈ 10⁻⁴.
   - **The bracketed leftover,** which no history of 02d has, I probed on `KHistory` with constructed rows. K steps at
     each drain, the leftover is placed at the cut candidate, and E is blank where K = 0 or the mass is None.
5. **§2.2, independently.** My own `ast` extraction of `excess`, run on a fresh `one_loop_payload` of P-2197, against
   the tool's `matter_excess` with `PEAK_SPECTRUM` on the agent's export of the same integration:
   - 3 081 samples compared for each mass, worst 2.38 ε (field) and 2.52 ε (tracker), every one inside the note's
     tolerance;
   - 1 914 matched blanks where K = 0;
   - my own cancellation-free peak E within 6 × 10⁻¹⁶ of the tool's.
6. **The run.**
   - 230 results, none in error. Each one's candidates equal 02d's, its fields and bounce rows read back equal, and its
     temporary store is deleted.
   - The merged histories.csv's 230 rows are the parts' rows, in `write_histories_csv`'s order.
   - Each A's directories number its 02d successes (27, 28, 29, 30, 29, 29, 29, 29).
   - The tool, re-run at c_a = 1 on a copy of the export root, wrote 461 files byte-identical to the agent's (52.7 s,
     exit 0).
   - The headline figures of the report are what the files give: δ_stim 2.2 × 10⁻⁷⁵ to 1.34 × 10⁻³ (median
     3.5 × 10⁻¹⁷); n̄ > 1 in 33 histories, up to 9.0 × 10³¹; E_field ≤ 5.4 × 10⁻²⁰ and E_tracker ≤ 7.2 × 10⁻¹⁶; slips
     below 1 rad in 15; the backreaction maximum 1.8 × 10³; the K check's largest difference 1.46 × 10⁻¹¹; δ_stim's
     ratios 5.6 × 10⁻⁸ to 1 at c_a = 0 and 1 to 10 at c_a = 10.
7. **The records.**
   - The log follows the template, and classes every deviation and each of note 05's rulings. Its §7 gives 07 the
     format and 06 the command.
   - The agent widened ruling 6's K-check tolerance to cover the conditioning of K_final = K_last/(1 − parking_share),
     which is 1.4 × 10⁵ ε at a parking share of 0.999993. That is right, and logged as STRUCTURALLY REQUIRED.
   - Verification §1–§4 are unchanged; §5 follows prompt §2 and §1.6.
   - On the board: row 05, its items, the Derive gate, the narrowing of `[00-later-bounces-act-on-quanta-already-present]`
     and **Last updated**. `.documents/OPEN_ISSUES.md`'s row matches it, at 29 open.
   - `CLAUDE.md`'s counts are those measured.
   - `architecture-summary.md` and `numerical-strategies.md` have their dated notes, U17's end in the latter.

   The review substituted `9830738` for "this commit" on the board and in the log, added note 05 to the list of notes,
   and updated `prompts/INDEX.md`'s row.

Noted, not acted on:
- **The probe's cancellation was not exercised** by §2.2 (log §6): on P-2197 every y is below e^{−20}, where the probe
  takes its 0.5 x² branch.
- **The random-phase reading fails where n̄ > 1** (33 histories) and where the slip is below 1 rad (15; log §6). These
  are measurements (P49), recorded in the issue's narrowing, for 08 to document.

**Orchestrator review of prompt 06 (2026-11-07)**, by orchestrator note 06 §3, on `1416b80`. Passed; seven checks.
Before dispatch the orchestrator ruled in note 06 (§0, rulings 1–9): the probe's environment and the workers' working
directory; the candidates matched by `candidate_index` through `to_column`, not by ordinal or `repr`; the values to their
digits; R7's small network; the commands in full; the worktree; the interrupt delay. No stop condition was raised.

1. **Scope.**
   - `git show --stat 1416b80` lists 114 files, all of prompt §7's: the verification document, `rehearse-06/` (eleven
     scripts and 100 small text files in `out/`, 1.3 MB, no PDF or PNG), the log and the board. Its parent is `41d18c4`,
     the note's.
   - The tree was clean, nothing is pushed, and `git worktree list` shows the main checkout only. No code, test,
     `CLAUDE.md`, `.gitignore` or `PRyM/` changed, and nothing under `var/` is tracked. Only the board has lines
     removed: five, each replaced (the status, the history line, row 06 and the two gate rows).
2. **The suites, re-run by me** serially, ending 10:07, at loads of 129 to 20 at their starts from other
   work: 26 (78 s), 665 (219 in 208 s + 446 in 663 s), 163 (210 s), 191 (323 s) and 72 (33 s), all passing. These are the
   counts the log states, and none fell.
3. **The cluster, the registry and the code.**
   - `tools/ray_cluster.sh status` and the lock's `status`: free, no process, the four ports free.
   - `runregistrykit list`: the ten runs of this prompt, nine `done` and `resumed-a` `killed`; nothing `running` or stale.
   - My own `shasum -a 256` of `git show 1561b60:main.py` is `92117838…acc0` and of the tip's `2b8956ff…c0e9`; the three
     base manifests record `git_head` `1561b60` and the first, the seven others `41d18c4` and the second, the base's with
     the worktree as `cwd`.
   - The three probe outputs as committed: driver, task and actor import `ComputeTargets/ScalarHistory.py` and
     `ObjectFactories/__init__.py` from the worktree at 2026.9.0 before the base build, and from the main checkout at
     2026.10.0 before and after it.
   - The sidecars hold no format field of their own (log §2, ruling 1); the formats are the fingerprints', 6 for the
     base (taken by the base's code from the worktree) and 7 for the reference and resumed stores, which I re-took.
4. **Bitwise, independently.** A read-only script of mine (`sqlite3` `mode=ro`, sixteen shards each, the key tables from
   shard 0) matched the base's 17 histories to the reference's 17 under `fixed` by (M, n, β, φ*, α) through `float.hex`,
   and their samples by ln(1 + z): 101 335.
   - Every shared column of the six tables equal, by `float.hex` (the `_serial` columns through the rows they name;
     `extra_data` as a string): 1 925 365 values in `ScalarHistoryValue`, 506 675 in `AdiabaticHistoryValue`, 90 952 in
     `BBNDataValue`, 102 in `AdiabaticHistory` and 170 in `BBNData`, the agent's counts exactly; 1 105 in
     `ScalarHistory`, which the agent counts as 935 + 107 + 323 by splitting `extra_data` and the key rows. 0 differ.
   - 03's rules for the new columns: the 23 drain-record columns NULL at all 17, no bounce row, both masses filled and
     finite at all 202 670 sample slots. I did not recompute the masses; the agent did, with `sample_masses`.
5. **R, independently.**
   - `tools/compare_stores.py` on the reference and resumed stores, run by me: `RESULT: equal`, `problems: none`,
     `ScalarHistoryBounce` 2 562 matched and none on either side alone. `store fingerprint` on each: both
     `7c7641d2…dd04`, format 7.
   - My own script against 02d, on both stores: each of the nine tasks (argv the tool's defaults) matched once by A
     under `one-loop-measured-v1`, β and M; the 15 tuning keys through `_tuning_unit` (135), the drain values (117),
     1 285 candidate rows × 11 by `candidate_index` through `to_column` with `Planck_units` (14 135; all equal to the raw
     values too, these units being M_P = 1), the dissipation values (216), `bounce_rows` against the rows present; Y_p
     and D/H bitwise equal in 18 of 18. 0 differ.
6. **The readers and exports.**
   - `histories.csv` of `plot-reference` has **54 rows, not 26**: the 26 histories in P36's sort and 28 marker rows
     (`no ScalarHistory row in the store`, `plot_by_beta.py:882`), one per (row of α or A, M, β) of the run's grid that
     no history fills. The grid is not a rectangle here, and the markers are the plot script's behaviour since before this
     campaign. The note's "26 rows" took the prompt's expectation over; the agent classes it rightly as drift of the
     expectation, not of the code. Each of the 26 has its fields.csv; the nine one-loop ones have a bounces.csv, and
     nothing under `alpha=*` has one.
   - The nine bounces.csv against 05's, by `cmp`: identical.
   - The tool at c_a = 1, re-run by me on a copy of the export's CSV files: its 19 files byte-identical to the agent's,
     and its log equal but for the wall times.
   - R4 refuses `--A-values 0.5`, naming the option and the value, and no `A=0.5` directory exists. R7's re-solves print
     the stored Y_p and D/H to their ten digits, the stored values being those of the stores (my own query).
   - The 54 store files' `shasum`s equal those the agent recorded before and after each reader, one hash per file
     throughout.
7. **The records.**
   - The log follows §5.1's form and classes every deviation, each of note 06's rulings among them. Its §7 gives 07 the
     exports and 08 the stores, the gates and the costs.
   - Verification §1–§5 are unchanged (369 lines added, none removed); §6 follows prompt §3, the commands in §6.8.
   - The board: row 06, its two items, the gates Bitwise and R, the §5 suite counts and **Last updated**.
     `.documents/OPEN_ISSUES.md` is unchanged, nothing being opened.
   - The token grep over the commit finds nothing; the file is named, never quoted.

   The review substituted `1416b80` for "this commit" on the board and in the log, added note 06 to the list of notes,
   and updated `prompts/INDEX.md`'s row.

Noted, not acted on (log §6), for 07 and 08:
- **The marker rows.** `tools/quanta_measures.py` counts a one-loop marker row as a "failed history" (9 here), its
  blank `normalization` being the test it documents; `Delta_N_eff.pdf` lists α = 0.15 at laboratory M with no point.
  The rebuild's grid is a rectangle (U14), so neither shows there. 07's figure tools should skip a marker row.
- **"Reflections (elastic model)"** in a one-loop history's figure footer predates the drain laws; 07's to relabel.
- **The shard warning.** `plot_by_beta.py` and `plot_ScalarHistory.py` accept `--shards` but never pass it to
  `ShardedPool`, which takes its default of 10 and warns on a 16-shard store. A read-only open uses the store's own
  shards, so nothing is wrong but the line; it predates this campaign.
- **The disk.** The raylet warned that the volume holding `/tmp/ray-cpbh` was over 95 % full (39 GB free at the end). A
  rehearsal on this machine could meet Ray's refusal to create objects.
- **The workers' working directory** is the cluster's whatever tree a driver runs from. Harmless at both commits, whose
  data files read by relative path are identical; worth a line in 08's handover.

**Orchestrator review of prompt 07 (2026-11-07)**, by orchestrator note 07 §3, on `867b267`. Passed; seven checks.
Before dispatch the orchestrator ruled in note 07 (§0, rulings 1–10): the four counts in order, fixed before marker; values
matched by `isclose` and cells by their strings; the expected spectrum model the code's `SPECTRUM_MODEL`; each norm named,
with ranges for a log panel with no positive value or one; the history figure's undrawn counts; the six picks drawn one tool
call each; the committed names; 05's helper used unedited. No stop condition was raised. The agent (Sonnet) amended its own
commit once before reporting, to correct two sentences of its log.

1. **Scope.**
   - `git show --stat 867b267` lists 40 files, all of prompt §6's: the three new modules and their three test modules,
     §1.4's two-line label change and its two assertions, `CLAUDE.md` (the `tools/tests` count and its dated sentence only),
     the verification document (§7, 55 lines added, none removed), the architecture summary, the board, the log, and
     `figures-07/` (four scripts, `out/`, twelve PNGs, 2.9 MB). Its parent is `d4031dc`, the note's.
   - The tree was clean and nothing is pushed. `quanta_measures.py`, `quanta_exports.py`, `test_quanta_measures.py`, the
     plot scripts, `PRyM/`, `.gitignore` and the README are untouched. Nothing under `var/` is tracked and no PDF is
     committed.
2. **The suites, re-run by me** serially, between 16:14 and 16:51, at one-minute loads of 3 to 6 at their starts: 26
   (64 s), 665 (219 in 145 s + 446 in 466 s), 163 (126 s), 259 (255 s) and 72 (23 s), all passing. These are the
   counts the log states. `tools/tests` rose by the 68 tests added, and none fell. (A first pass cut its summary lines;
   the counts above are from a second pass, run serially as the first was.)
3. **The code against the prompt and the rulings.**
   - The grid's ten panels are in the prompt's order with its columns and files. Their norms are `Normalize(0, 1)` for
     (a), `LogNorm` over the finite positive values for (b), (c), (h)–(j), `Normalize` for (d), and `Normalize(−m, m)` for
     (e)–(g), as ruling 4 says, with its degenerate ranges.
   - The blank (hatched), zero (solid grey), failed (a cross in every panel) and no-quanta-row (a cross in (h)–(j)) cells
     are each drawn and counted.
   - The history figure's six panels draw the prompt's columns. Panel 1 uses ln(m a) = `log_m_*_Mp` + `raw_N`, 05's frame.
     Panel 2 counts its undrawn candidates by ruling 5's three causes.
   - Ruling 1's order is `quanta_figures.classify`, and ruling 3's checks are in `check_model_rows` and the two `run`s. Paths
     are formed by `alpha_path`, `history_dir`, `plots_dir` and `to_output`; none is formatted inline.
   - The 68 tests read the figures' artists by gid, and the norms' types and limits. They cover prompt §1.5's list.
4. **The demonstration, re-run by me**, with `--output` in my scratchpad:
   - both tools on 06's tree (2 grid pages, 9 histories), the grid tool on 05's (8 pages), and the history tool on the
     A = 0.1, M = 5 × 10⁻²⁰ pick;
   - every exit 0, and every printed line the agent's but for the wall times;
   - the 20 PNGs the same size as the agent's, byte for byte in length;
   - no file of either input tree modified since 15:00, before the agent's demonstration began.
5. **The gate Figures, independently.** A script of mine read the printed lines committed in `out/`. Every grid page's
   drawn count equals histories.csv's drawn rows at that A and quanta_histories' rows at that A and scale (12 pages over
   both trees and the three scales). Every history's generations equal quanta_histories.csv's (13 figures): 25 checks, 0
   failures. Ruling 6's picks, recomputed, are the agent's four: (A, β, M) = (0.1, 2, 5 × 10⁻²⁰) for `generations`,
   `n_bar_max` and `phase_slips_below_1`; (5, 2, 5 × 10⁻¹⁶) for `drained_share_gap`; (10, 1, 10⁻¹²) for
   `drained_share_classical`; (2, 2, 8 × 10⁻¹⁸) for `backreaction_max`.
6. **The figures, by eye.** I opened all twelve committed PNGs.
   - **The grid pages.** They hold the 02d sample's shape at every A. The β = 1.11 column appears only at A = 1, where
     C-3873 is the only history that built. The 5 × 10⁻²⁰ row is missing at A = 0.05, where that ladder fails.
     Hatching, solid grey and the cream "no history" background read apart.
   - **The history figures.** In the A = 10, β = 1, M = 10⁻¹² history, panel 2's first drained fraction is 1 (K_before =
     0 at its first candidate, by the data), and its r_χ axis reads 1.7 × 10⁻⁵⁶, that history's own value.
   - **What reads weakly.** The cream background is close to the white at the centre of the margins' diverging map; a
     near-zero margin and an empty cell are told apart by the legend more than by eye (log D2 chose the cream for this).
     A panel-2 open marker at 1 hides under its filled `alpha_eff` marker where α_eff ≈ 1. Neither is acted on.
7. **The records.**
   - The log follows §5.1's form and classes every deviation, note 07's ten rulings among them, with nine choices of its
     own (D1–D9). Its §6 gives 08 the command lines, exits, figures and costs.
   - Verification §1–§6 are unchanged, and §7 follows prompt §6.
   - The architecture summary has the dated note. The board has row 07, its two items, the gate Figures, the §5 counts
     and **Last updated**. `.documents/OPEN_ISSUES.md` is unchanged, nothing being opened.

   The review substituted `867b267` for "this commit" on the board and in the log, added note 07 to the list of notes,
   and updated `prompts/INDEX.md`'s row. **Residue fixed:** `figures-07/check_figures.py` and `pick_histories.py` were
   not `black`-clean. They are formatted in the review commit, and their outputs are byte-identical before and after.

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
| the measured law: `drain_fraction`, its constants, the S table and `drain_S_of_L`; `drain_alpha_effective(regime, ell, L, log_R, A, w_gate)`; the gap's f = 0 where 1 + q ln L ≤ 0 (the user's ruling) | 02c | ✅ |
| L in the model class (the user's ruling): `AbstractModel._drain_L` with no default, `RealizedModel.drain_L`, `ExponentialModel._drain_L` = ln(1 + E/V₀); `check_drain_law_model` at the three entry points | 02c | ✅ |
| `_GAP_PRESCRIPTION` and its names, `DRAIN_C1`, `DRAIN_ELL_EDGE` and `--gap-prescription` removed; `DrainRecord.model`; `DrainCandidate.L` and `.log_R` | 02c | ✅ |
| the four adopted-law maps reproduced and the control byte-identical (`.documents/one-loop-drain/law-02c/`); the tool route | 02c | ✅ (verification §2c) |
| the re-measure scripts, `.documents/one-loop-drain/measure-02d/` (self-contained copies of 02's driver and task list with 02b's `first_impact`; the law checked against its record at every candidate; the cap and the law's edges by regime) | 02d | ✅ |
| 02's 30 histories × U14's eight A, re-tuned under the measured law, every task run: 230 succeed, 3 fail, 7 time out (C-3873); the tool and determinism checks bitwise; α_eff equal to `drain_alpha_effective` at all 616 270 candidates; the handover for 03 and the user | 02d | ✅ (verification §2d) |
| the drain-model key: `CosmologyConcepts/drain_model.py` (the leaf module), `alpha_value`'s pair and its own factory (P27, U8, decision 1) | 03 | ✅ |
| `ScalarHistoryBounce` (P29), the bounce key and the pure builder, the gatherer split, `ledger_K_before` | 03 | ✅ |
| the history's drain record, `DrainHistoryValues`, the all-generation margins (P30) | 03 | ✅ |
| the sample masses at every law (U5, P31), the guard (P28), `compute_scalar_history`'s `drain_law` | 03 | ✅ |
| `FINGERPRINT_FORMAT` 7, its golden; `VERSION_LABEL` 2026.10.0 (U12); the registry, drop group, sharding, batch size | 03 | ✅ |
| the Store gate's scripts, `.documents/one-loop-drain/store-03/` (the fixed law key by key; the cost on six of 02d's histories) | 03 | ✅ (verification §3) |
| `--A-values` (type `drain_strength_A`, no default), the rows (fixed, then one-loop), `plot_inputs.parameters(…, A_values)`, the drivers' loops and lines (P33, P34) | 04 | ✅ |
| `alpha_path(base, value, drain_model)`, `alpha_label(value, drain_model, spelling)`, `model_of_law`; the model never defaulted (P35; note 04 rulings 2, 3) | 04 | ✅ |
| histories.csv: `drain_model` and `DRAIN_CSV_COLUMNS`, the sort, `plot_Delta_N_eff` by (model, value) (P36) | 04 | ✅ |
| fields.csv: `build_fields_frame`, `log_m_eff_Mp`, `log_m_tracker_Mp` (P37); bounces.csv: `write_bounces_csv` (P38) | 04 | ✅ |
| `tools/bbn_from_store.py --drain-law`, `tools/outward_step_scan.py` fixed rows only with a count, the inventory report's labels, `describe_scope` (P39, P40) | 04 | ✅ |
| the Export gate's scripts, `.documents/one-loop-drain/export-04/` (the store, the two trees' exports, the comparison, the new-column check) | 04 | ✅ (verification §4) |
| `ComputeTargets/quanta_spectrum.py`: U4's shape with U17's end (`SPECTRUM_MODEL = "U4-U17-v1"`), the constants with their sources, κ fitted at L = 90, I and J in closed form (P41; note 05 rulings 1, 2) | 05 | ✅ |
| the kernels G and Ω, tabulated once per c_a, the asymptotes beyond the tables, the quadrature references; `PEAK_SPECTRUM` for the gate (P5; rulings 3, 4) | 05 | ✅ |
| `tools/quanta_measures.py`: the histories found by `alpha_path` and `extract_common.history_directory` (ruling 9), K(N) rebuilt and checked (P42, P43; ruling 6), n_g, n̄, δ_stim, E, the phase slip, the backreaction ratio (P44); the three outputs with the model on every row (P45; ruling 10); `--assoc-scale` (P46) | 05 | ✅ |
| the Derive gate's scripts, `.documents/one-loop-drain/derive-05/` (the kernels, the probe, the run of 02d's 230 successes and its report; P47, P48) | 05 | ✅ (verification §5) |
| the rehearsal's scripts, `.documents/one-loop-drain/rehearse-06/` (the code probe, the stored-values reader, the three comparisons, 11's helpers adapted; P51, P53, P54) | 06 | ✅ |
| ten registered runs on the isolated cluster: the base at `1561b60` from a worktree (17 fixed-law histories), the reference and the resumed store at the tip (26 each, 9 of them 02d's one-loop tasks), run a of the resumed build interrupted with `SIGTERM` and resumed; the readers R1–R7; the three stores kept (P50, P52, P55–P57) | 06 | ✅ (verification §6) |
| `tools/quanta_figures.py` (regime colours, the four row classes, the model check, the `--output` mapping), `tools/plot_quanta_grid.py` (ten heat maps per one-loop A) and `tools/plot_quanta_history.py` (six panels sharing N per history); 68 tests on constructed exports; "Reflections: N" (P58–P62, P64) | 07 | ✅ |
| the figure scripts, `.documents/one-loop-drain/figures-07/` (the six picks, the demonstration, the count check, 12 committed PNGs): 06's exports, 2 grid pages and 9 history figures; 05's, 8 grid pages, A = 1 at c_a = 0 and 10, 4 history figures; 58 checks, 0 failures (P63) | 07 | ✅ (verification §7) |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Law | fixed law: golden histories bitwise, payload unchanged but the new key; one-loop law: 2197 and 2188 reproduce the probe's table, 3016 and 3233 drain nothing; 3873 explained | 01 | ✅ 2026-11-03 (verification §1.10): the goldens bitwise, the 42 equal 04's, the probe's table bitwise at A = 1 and 0.1, 3016/3233 drain nothing, 3873 explained and completing after ruling (b) |
| Measure | the sample at three A and both gap prescriptions, re-tuned; regime shares and the gap's reach across M and β; the prescriptions' spread in Y_p, D/H, ΔN_eff; the cost | 02 | ✅ 2026-11-03 (verification §2.10): 180 tasks, 81 of 110 runs succeeding; the gap reached at laboratory M only at A = 0.01, where the tunings fail; the spread measured on the ladder; 1 h 58 min, at most six workers |
| Wide | 02's sample at 02b's six A, the reproduction check bitwise; regime shares, the first impact and the cap across nine A; the cost; the handover for U14 | 02b | ✅ 2026-11-04 (verification §2b.12): 360 tasks, 167 of 171 runs succeeding; every tuning at A = 0.5–10 succeeds, and P and W stay one-loop down to A = 0.05; at A = 10 the first impact drains 84–99% of the kinetic energy at laboratory M; 43 min, at most six workers |
| Measured | the fixed law unchanged bitwise; the law equal to crossover's reference implementation at synthetic inputs; crossover's adopted-law maps reproduced candidate by candidate | 02c | ✅ 2026-11-05 (verification §2c.6): the goldens bitwise; the law bitwise equal to `fraction_adopted` on the grid but for the two ruled departures; every candidate row of the four maps equal, the control byte-identical, the headers' last digits the U15 (b) form of L (ruled); the tool route agrees |
| Remeasure | the sample at U14's eight A under the adopted law, re-tuned; the failures stated; against 02/02b | 02d | ✅ 2026-11-05 (verification §2d.12): 240 tasks, every one run; 230 succeed, R-632 fails at A = 0.1 and 0.05 and the 5 × 10⁻²⁰ M_P ladder at 0.05 (02's two kinds), C-3873 times out at seven A; every P, W and B history builds at all eight A; at laboratory M the law moves ΔN_eff today by at most 0.011 from the old law; 3 h 08 min, six workers |
| Store | the round trip exact, bounce rows in order; the fixed law and α = 0 write no bounce row; fingerprint format 7 beside 6 | 03 | ✅ 2026-11-06 (verification §3.5): the fixed law's payloads equal the parent's key by key but for the three new keys, the goldens pass; the round trip exact at three one-loop histories and constructed extremes (13 400 rows); format 7 beside 1–6, only `alpha_value`'s spec digest moved; median added time 0.0104 of the integration on six of 02d's histories |
| Export | the new export columns equal the stored record; existing `histories.csv` columns byte-identical; the drivers refuse a mismatched law | 04 | ✅ 2026-11-06 (verification §4.6): 168 + 52 886 + 2 130 new cells obey ruling 1's rule (0 differ from the raw column in the store's units), the rule exercised under `Mpc_units`; histories.csv (fixed-law records) and fields.csv (every history) byte-identical to the dispatch tree's on a seven-history store, the store's SHA-256 unmoved; the refusals tested |
| Derive | the derived measures against direct quadrature (10⁻⁶); peak-collapsed against the scoping probe's method | 05 | ✅ 2026-11-06 (verification §5.8): G and Ω within 2.3 × 10⁻⁹ of quadrature on the grid and beyond at c_a = 1, 0, 10, every asymptote within 10⁻⁷ at its end, every measure within 10⁻⁶ of a direct computation on constructed generations; peak-collapsed E within 2.5ε of the probe's `excess` on P-2197 at every compared sample, K(N) equal to the ledger's bitwise; 230 of 230 of 02d's tasks re-integrated with every candidate equal by `repr`, exported and measured at the three scales, the K check within 1.5 × 10⁻¹¹; 27 min, five workers |
| Bitwise | the fixed law's tip build equal to the base's in every pre-existing column | 06 | ✅ 2026-11-07 (verification §6.4): the base's 17 histories matched once each under `fixed`; 2 524 629 values in the shared columns of the six tables and the key rows, every one equal; the drain record NULL, no bounce row and the sample masses as `sample_masses` gives them at all 17; no exception; the workers probed to run their own tree's code |
| R | the one-loop build, interrupted and resumed, equals the straight build by value (bounce rows included) and 02d's tool route (README §8.14) | 06 | ✅ 2026-11-07 (verification §6.5): `compare_stores` equal with 2 562 bounce rows matched, both fingerprints `7c7641d2…dd04` (format 7); the nine one-loop histories equal to 02d's tool route in the tuning, the drain record, 14 135 candidate values, the dissipation values and Y_p and D/H, bit for bit; the exports byte-identical but for three timing columns, the nine bounces.csv byte-identical to 05's |
| Figures | both tools render from 06's exports; their tests pass | 07 | ✅ 2026-11-07 (verification §7.2): 68 tests added (`tools/tests` 191 → 259, the others unchanged); both tools exit 0 on 06's exports (2 grid pages, all 9 one-loop histories) and on 05's (8 grid pages, c_a = 0 and 10 at A = 1, 4 histories); each figure's drawn count and each history's generations equal quanta_histories.csv's, by script (58 checks, 0 failures) |

## 3. Active and unresolved issues

Opened by the planner on 2026-11-02, from the scoping (`source/scoping_2026-11-02.md`); two more
opened the same day, after the plan was ruled, from the drain probe (`source/drain-probe_2026-11-02.md`).
At the re-plan the same day (README §8) one of those was split in two (§4), and the rest re-pointed at
the new prompts.

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
  **Narrowed 2026-11-06** (prompt 05; verification §5.7): measured by the derivation tool on 02d's 230 one-loop histories, under
  U4's spectrum with U17's end and random phases. δ_stim, the fractional change of the final ρ_χ that stimulated emission would
  make at first order, is 2.2 × 10⁻⁷⁵ to 1.3 × 10⁻³ (median 3.5 × 10⁻¹⁷; at most 9.0 × 10⁻⁵ in groups P and W at laboratory M,
  1.3 × 10⁻³ on the L ladder, 8.0 × 10⁻⁴ in group C), with rescattering bounded by it (about 2n); the associated piece's scale
  moves it by at most a factor 10 (c_a = 10) or down to 5.6 × 10⁻⁸ of itself (c_a = 0). The overlap n̄_j is below 0.01 in every P,
  W, B and R history, but above 1 in 33 histories (the L ladder and group C at 10⁻¹² M_P, up to 9 × 10³¹ at bounces that drain
  almost nothing), and the phase slip at a producing bounce falls below 1 rad in 15 of them, where the random-phase model's
  premise does not hold. Left open as ruled; 08 documents it.
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
  *(2026-11-05, prompt 02d (`98b1601`; `.documents/one-loop-drain-verification.md` §2d.4; `var/one-loop-drain/02d/sample.jsonl`):
  measured re-tuned under the measured law at U14's eight A, every task run. Three tunings fail, of 02's two kinds, each in
  its seed integration:
  - R-632 (β = 1.0) at A = 0.1 and 0.05, by "the effective minimum was not found" (V_eff' values 3.27795–3.27799 × 10⁻¹²¹),
    after 44 one-loop, 88 gap and 3269 classical candidates (A = 0.1) and 43, 52 and 800 (A = 0.05), without parking. It
    builds at A ≥ 0.5, parking at N = 38.65 at A = 0.5 after 3 gap drains.
  - The ladder's 5 × 10⁻²⁰ M_P at A = 0.05, by the step budget at N = 46.07 after 14 115 reflections (8 one-loop, 2 gap,
    14 105 classical candidates). **At A = 0.1 it now builds**, where 02 failed by the step budget: it parks at N = 47.60
    (N_final 50.08) after 13 342 drains, 13 333 of them classical, in 1115 s.
  - **Every P, W and B history builds at all eight A**, and every other R and L history too; the P and W histories park at
    N = 38.4–39.4 at A = 0.05, ten of them after 5–22 gap drains.
  - C-3873 times out (3600 s) at seven A in its tuning (24–27 integrations), which is not one of 02's kinds and is reported
    beside this, not in it.
  So under the measured law the issue holds at A = 0.1 and 0.05 for R-632, and at A = 0.05 for the 5 × 10⁻²⁰ ladder;
  neither failure points at the law's code, the wall search or the inside-wall guard. Not diagnosed further (prompt 02d
  §1.5).)*

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

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
  **Closed 2026-11-06 by prompt 03** (`fe4c917`; `.documents/one-loop-drain-verification.md` §3): under the one-loop law
  every generation is stored, as a row of `ScalarHistoryBounce` with its share, weight and three margins, and the history
  stores the all-generation worst margins, their N and failing shares (`all_worst_*`, `all_*_failing_share`), taken from the
  generations the core returns with the weight ignored. Under the fixed law the record stays as it is; 08 documents that.

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
  *(2026-11-05: the user adopted the measured law in place of 2.7/ℓ and both prescriptions, and ruled one versioned
  drain-model key (README §8.10, decision 1).)*
  **Closed 2026-11-06 by prompt 03** (`fe4c917`; `.documents/one-loop-drain-verification.md` §3): the law's key is stored.
  `alpha_value` is the pair (value, `drain_model`), `"fixed"` or `"one-loop-measured-v1"`, so a history built under the
  one-loop law is keyed apart from the fixed law's, and a later version of the law is a new key value.

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

**2026-11-05, prompt 02c:** measured first at `1647f7a`: 26, 566 (163 + 403), 130, 156, 68, all passing. After 02c: 26, 594
(162 + 432), 130, 156, 68, all passing (log 02c §4); two tests of the removed gap prescriptions deleted.

**2026-11-05, prompt 02d:** measured first at `f4da906`: 26, 594 (162 + 432), 130, 156, 68, all passing. After 02d (no code
changed): the same counts, all passing (log 02d §4).

**2026-11-06, prompt 03:** measured first on a worktree of `d3069cb`: 26, 594 (162 + 432), 130, 156, 68. After 03:
26, 622 (190 + 432), 149, 156, 68, all passing (log 03 §4).

**2026-11-06, prompt 04:** measured first on a worktree of `8df05c1`: 26, 622 (190 + 432), 149, 156, 68 (the last two with one worktree-root failure each, as in 03). After 04: 26, 651 (219 + 432), 163, 173, 72, all passing (log 04 §4).

**2026-11-07, prompt 06:** measured first at `41d18c4`: 26, 665 (219 + 446), 163, 191, 72, all passing. After 06 (no code
changed): the same counts, all passing (log 06 §4), at one-minute loads of 107–200 from other work.

**2026-11-07, prompt 07:** measured first at `d4031dc`: 26, 665 (219 + 446), 163, 191, 72, all passing. After 07: 26, 665 (219 + 446), 163, 259, 72, all passing (log 07 §4); 68 tests added to `tools/tests`.
