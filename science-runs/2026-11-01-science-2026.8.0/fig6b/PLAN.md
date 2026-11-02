# fig6b — plan for Figure 6 around M_crit

**Written 2026-11-02**, at the user's request, as a plan only. The figure is not to be built until
`[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` (`inelastic-reflection`
board §3; [`.documents/post-reflection-step-2026-11-02/`](../../../.documents/post-reflection-step-2026-11-02/README.md))
is fixed and the histories it failed are recomputed. The user's words: "I'll wait until we've fixed
the issue before building the plot."

Figure 6b is the block-C half of the run plan's comparison Figure 6 ([`../PLAN.md`](../PLAN.md) §5.2:
"one band per M tier … C (around M_crit), and D as α = 0 lines at 10⁻³, 0.1 and 0.5. Where a history's
relativistic margins fail, it is marked from the stored columns"). [Figure 6a](../fig6a/NOTES.md) is
the D half. The user ruled on 2026-11-02 that 6b shows the dissipation-mode (α > 0) histories the run
already has: "For 6b I meant the histories with `alpha > 0` that we already have".

## 1. The data

Block C (`C-Mcrit`) of the main store: M = 10⁻¹⁵, 10⁻¹³ and 10⁻¹² M_P, the coarse β grid (62 values,
0.5–20), the main α grid (0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.15), φ* = 5 M_P: 434 histories at each
M, 1302 in all. These are the briefing notes' M_crit (median 1.2×10⁻¹⁵ M_P) and the top of its range
(maximum 8.2×10⁻¹³). The figures below are from `../fig6/inputs/histories.csv` as of `cc7fe75`.

| M | with yields | failed, β ≤ 1.1074 | failed, β > 1.1074 | excluded by Y_p or D/H | log₁₀ 𝓡 at the first impact, α = 0.08, β = 1.2 / 2.0 / 2.9 |
|---|---|---|---|---|---|
| 10⁻¹⁵ | 402 | 26 | 6, all `log_m_eff_squared` | 102 | −1.6 / +0.2 / +1.6 |
| 10⁻¹³ | 410 | 23 | 1, `log_m_eff_squared` | 110 | −9.6 / −7.8 / −6.4 |
| 10⁻¹² | 379 | 28 | 27, all `log_m_eff_squared` | 93 | −13.6 / −11.8 / −10.4 |

- **The failures at β ≤ 1.1074** are the non-surfing region's (the effective minimum not found,
  PRyMordial's spline floor, `integrate_scalar_history`), as at the laboratory M; 8 of them are
  `log_m_eff_squared` too (C's 42 = 34 + 8). They are not this plan's concern.
- **The 42 `log_m_eff_squared` failures** are the issue's. Until they are recomputed they would be
  holes at single (β, α), densest at 10⁻¹² in the transition window.
- **The yields do not depend on M at fixed α.** Against the laboratory M (4×10⁻³¹) at the same (β, α),
  |ΔY_p| differs by 0.0009 percentage points at the median and 0.03 at most, and |Δ(D/H)| by 0.003 and
  0.023, against tests of ±2.45 % and ±2.37 %. This is the diagnostic's finding
  ([`../diagnostic/NOTES.md`](../diagnostic/NOTES.md)): in this code α, not M, sets the dark radiation.
- **The relativistic margins fail almost everywhere here.** A birth or reflection margin fails in 402 of
  402 histories with yields at 10⁻¹⁵, 410 of 410 at 10⁻¹³ and 379 of 379 at 10⁻¹², against 293 of 1914
  at the laboratory M (`birth_failing_share`, `reflection_failing_share` > 0). The tracker margin fails
  nowhere. PLAN.md §1.1 expected the margins to fail toward 10⁻¹².

## 2. The questions before building it

The figure as PLAN.md §5.2 describes it would show three copies of Figure 6, one per M, with the margins
marked nearly everywhere. Four things are for the user.

1. **α > 0 where 𝓡 < 1.** At 10⁻¹³ and 10⁻¹² the test-field ratio at the first impact is 10⁻⁶ to 10⁻¹⁴, so
   the impacts are elastic and produce no particles; the code warns "the reflections are expected to be
   nearly elastic in this regime" for these histories. A history there with α > 0 is a dissipation the
   physics at its own M does not supply (the reflection figure's row 3 makes the same point). Only at
   10⁻¹⁵ does 𝓡 cross 1 inside the β range (near β = 2). Options: (a) draw all three M as computed, with
   the caveat in the caption; (b) draw only 10⁻¹⁵, where α > 0 is plausible over part of the β range;
   (c) shade or mark the (β, M) where 𝓡 < 1.
2. **What it adds to Figure 6.** Since the yields match the laboratory M's to 0.03 percentage points, the
   three M's bands would lie on Figure 6's. Options: (a) one panel per M, Figure 6's layout; (b) Figure 6's
   band at the laboratory M with the three M as points over it, so that the agreement is the message;
   (c) a difference panel, ΔY_p(M) − ΔY_p(4×10⁻³¹), on the scale of 10⁻² percentage points.
3. **The margins.** Every history here fails a relativistic margin. Marking each failing point, as PLAN.md
   §5.2 asks, marks all of them. Options: (a) say so once in the caption; (b) colour by the failing share
   (`reflection_failing_share`); (c) leave the margins to the notes.
4. **Rows.** Figure 6's four rows (ΔY_p, Δ(D/H), ΔN_eff today, the parking temperature), or Figure 6a's
   three (ΔY_p, Δ(D/H), a verdict strip). A log₁₀ 𝓡 row against β would show where each M's impacts stop
   producing particles.

The recommendation, for the user to rule on: option 1(c), 2(b), 3(a), and Figure 6's first two rows plus a
log₁₀ 𝓡 row, so that the figure says in one place that at fixed α the yields do not depend on M and that
the particle production behind α switches off between 10⁻¹⁵ and 10⁻¹³.

## 3. Before it is built

1. The issue is fixed, in its own campaign, and the fix is checked against the three reproduced failures
   (β = 1.5 at 10⁻¹²; β = 1.14 and 1.09 at 10⁻¹⁵, all at the α of the note).
2. The 42 failed histories are recomputed in the main store, by block C's command with
   `--retry-failed-scalar` (tuning-followups prompt 01), as a registered run on the isolated cluster.
   Whether the fix also changes the stored digits of C's successful histories, and so whether all of C
   is recomputed rather than the 42, is for that campaign to measure: if it does, C's successes and the
   new histories would come from different code.
3. `../run_figure_exports.sh` is re-run, so that `../fig6/inputs/histories.csv` carries the recomputed
   rows. Figures 6 and 6a read the same file; their rows are at other M and must stay cell for cell the
   same, which the export's comparison should confirm.
4. The questions in §2 are ruled.

*Added 2026-11-02, by `reflection-restart` prompt 04: steps 1–2 as they now stand.*
- **Step 1 is done.** The fix is F3 and the guard (`reflection-restart` 03, `b81eab8`, `VERSION_LABEL` 2026.9.0). Its
  Fix gate holds: the three reproduced failures succeed (`.documents/reflection-restart-verification.md` §2.1).
- **The 42, recomputed with the fix** as a measurement (verification §3.1): 41 succeed, none fails in
  `log_m_eff_squared`. One fails, 4389 (β = 1.13, M = 10⁻¹², α = 0.01), in the effective minimum after its seed
  integration; it is open as an issue (`reflection-restart` board §3).
- **Step 2 does not happen as written.** The 42 cannot be recomputed into the 2026.8.0 store: lookups are
  version-keyed, so a 2026.9.0 history is not a 2026.8.0 one, and C's successes and the recomputed holes would come
  from different code. No store of record is built at 2026.9.0 either.
- **So the figure's store is the rebuild of record's.** The user rebuilds the whole store after `one-loop-drain` 08,
  at 2026.10.0, on a tree that holds the fix once `reflection-restart` is merged into `one-loop-drain`. The
  prerequisites of this plan are therefore that rebuild, step 3's export re-run on it, and step 4. Its design (§1–§2)
  is not changed here.

Then `fig6b.py` is written beside this file, as `fig6a.py` is: reading only the export and the store's
sidecar, needing no Ray, writing a PDF, a PNG and a provenance record, with notes.
