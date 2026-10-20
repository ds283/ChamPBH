# fig6_draft — the draft BBN-constraint figure

**Collected and reviewed 2026-10-20.** The figure is `BriefingNotes/figs/fig6_draft.pdf` in the
Chameleon PBHs paper repository. It was committed there in `6dd117d` (2026-10-04 16:43), with
briefing notes written by Claude Science, and it is included by `Paper1.tex` (as a placeholder) and
by `BriefingNotes/bbn_constraints_explainer.tex` (`fig:fig6`). The PDF says Matplotlib 3.11.1,
2026-10-04 10:23.

**Source.** Claude Science's provenance bundle for that artifact, `script-bda3ef12.zip` (artifact
version `bda3ef12-8940-4d2a-9ff2-1a3f3c071618`), as exported on 2026-10-20. Taken from it:
- `fig6_draft.py`: lines 1141–1273 of the bundle's `run.py`, unchanged. The lines before them are
  Claude Science's replay layer for its host SDK, which the figure code does not call, and they are
  not kept;
- `inputs/6c1a4d00-a0a-figstyle_kernel.py` (Claude Science's figure-style helpers, run by the script
  with `exec`) and `inputs/9924a7a7-5a5-sigma_of_T.csv` (Σ = 1 − 3w against T), unchanged and under
  their bundle names, since the script opens them by those relative paths;
- `environment.yml`, unchanged. It calls itself frozen, but it pins only Python (3.11.16) and leaves
  Matplotlib, NumPy and pandas (`<3`) unpinned.

The bundle's `operon_tape.json`, the session's recorded host calls, is **not** kept: the figure does
not need it, and it holds the Claude Science conversation.

**Inputs not in the bundle.** The script reads `plot_by_beta.py`'s output for φ* = 5, 2 and 1 by an
absolute path, `HIST_ROOT = '/Users/ds283/ChamPBH-stores-bt02'`. Those three files are copied
unchanged from that store's `science-out-phi{5,2,1}/QCD_Cosmology/histories.csv` (written
2026-10-04 00:12–00:17, before the figure). They are kept under
`inputs/ChamPBH-stores-bt02/` in the same layout, so that setting `HIST_ROOT` to that path is the one
change a re-run needs. `DB_STEM` is defined and never used. **The script has not been re-run here.**

## The review (2026-10-20, from reading the code and the inputs, not by running it)

On these inputs:
- **the title's figure holds**: 71.1% of the 83 histories with 1.07 ≤ β < 1.5 at M = 10⁻³ M_P have
  Y_p outside the 2σ range, and none above β = 1.5;
- **the surfing threshold** comes out at β = 1.10745 at T = 0.1817 GeV. That agrees with ChamPBH's
  analytic QCD threshold, 1.107448 at 0.18189 GeV (`.documents/surfing-temperature-verification.md:134`),
  so `sigma_of_T.csv` matches ChamPBH's equation of state;
- **the Standard Model baselines** (Y_p = 0.24688, D/H = 2.4583 × 10⁻⁵) are rebuilt from the rows,
  and the script asserts that every row agrees to 10⁻⁹;
- **the beige band's edge** splits cleanly between β = 1.06 and 1.07, the largest and smallest β at
  M = 10⁻³ whose delivery temperature is below and above 10 MeV;
- **no point leaves the axes unmarked**: values above the clip are drawn as triangles, and none falls
  below the lower limit;
- **no history at M = 10⁻³ or 10⁻⁵ is marked as a solver failure.**

To fix before a publication version (none of them changes this figure):
1. **The Y_p test is two-sided**, |Y_p − 0.245| < 2 × 0.003, where the paper excludes only above the
   upper limit, 0.251. No history here has Y_p < 0.239, so the two agree on these inputs.
2. **One black triangle is mislabelled.** The label is "no BBN result (ρ_NP/ρ_R ≫ 1)". Nine of the ten
   are histories below PRyMordial's spline floor (β = 0.50–0.90). The tenth, β = 0.95, returned
   Y_p = 0.512, which failed the output check. The selection also takes every M at φ* = 5; on these
   inputs all ten are at M = 10⁻³.
3. **The blue band ends at the analytic threshold, 1.107.** The surfing-temperature scan found the
   first captured history in (1.10, 1.15] (that document, §1.3). The caption's "nominal" covers this.
4. **Not self-contained as exported.** The bundle lists two inputs; the three `histories.csv` it also
   needs are kept here instead.

The numbers are those of the `2026.6.0` store with the `+bt02` refresh, which the
`jordan-normalization` rebuild replaces. The figure must be redrawn from that rebuild's output.
