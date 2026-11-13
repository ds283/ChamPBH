# relativistic — do the quanta produced at reflections stay relativistic?

**Of 2026-11-09.** The plan is [`PLAN.md`](PLAN.md), with the user's rulings in its §6. `relativistic.py` was written by
a Claude Code session at the user's request ("Let's produce whatever figures we can, and leave it at that"). This is
the first instance of a **standard analysis** (`science-runs/README.md`, "Standard analyses"): a later run repeats it
from this folder (below, "Repeating it for a later run").

**Ruled by the user, 2026-11-09** (PLAN.md §6):
- The fixed law: "I'm not sure we want to go to the trouble of doing these estimates carefully for the fixed law. We
  know that's wrong, and it's there for comparison purposes. Let's produce whatever figures we can, and leave it at
  that." It is drawn only where the store has a measure for it, and its gap is not opened as an issue.
- No reference line in rel-lab: "I don't see anything obvious to include here."
- The rest of the plan: "So far this sounds fine."

**Accepted by the user, 2026-11-13**, of the figures and of the session's choices below: "Your choices seem fine."
Committed on the `electroweak-eos` branch at the user's request, which needs this folder in order to update it.

```bash
./venv/bin/python science-runs/2026-11-08-science-2026.10.0/relativistic/relativistic.py
```

It needs no Ray and no store. It reads:
- `../fig6/inputs/histories.csv`, the run's `plot_by_beta.py` export of the main store (the margins, the failing
  shares and the parking share are its columns);
- `inputs/quanta_histories.csv`, `-assoc0.csv` and `-assoc10.csv`, unchanged copies of
  `var/science-2026.10.0/out/main/QCD_Cosmology/`'s, written by `tools/quanta_measures.py` in the post-build script
  (`4ade10b`) at c_a = 1, 0 and 10;
- `inputs/samples/A=<A>/beta=<β>/M=0.00097413eV_n=1/`: unchanged copies of `bounces.csv` (the export), and of
  `quanta_samples.csv` and `quanta_bounces.csv` (the measures, c_a = 1), for the three histories of rel-history;
- `../registry/stores/science-2026.10.0.manifest.json`, the main store's sidecar.

It writes `rel-lab`, `rel-M` and `rel-history` (`.pdf`, `.png`) and `relativistic.provenance.json`, which records the
script's and every input's SHA-256, the git commit and whether the tree was dirty, the store's digest, and the
numbers below by (M, A) and by (M, α).

**The measures.** A margin is ln(p/m) for one generation of quanta: at its birth, at the worst later wall it meets,
and against the tracker's mass; the integrator warns below ln 10. "All-generation" means every generation, not only
those above the ledger's weight. E is the matter-like excess of ρ_χ over its radiation value, Σ_g ΔK_g G(m a/Q_g)/K
(`ComputeTargets/quanta_spectrum.py`, the U4 spectrum with U17's end, `U4-U17-v1`), at the field's mass or the
tracker's; r_χ E is the same excess as a share of ρ_R. It is the measure that bounds the physics: a generation that
fails its margin but carries no energy does nothing.

## What it shows

- **rel-lab** — M = 4×10⁻³¹ M_P, the 186 β, in Figure 6's four columns and colours: the band over A = 0.05–3, A = 1
  black, A = 3, 5 and 10 green long dashes, the fixed law blue dash-dot. Six rows: (a) the largest r_χ E_field over
  the history, (b) E_field today, (c) the worst birth margin, (d) the worst reflection margin, (e) the share of K
  that fails at a wall, (f) the parking share. The fixed law appears in row (e) only, as its weighted failing share
  (the generations above the ledger's weight; the store writes nothing else for it).
- **rel-M** — A = 1 at every M of the store, one line per M: B's six and 10⁻³⁰ in viridis, 4×10⁻³¹ black (as A = 1 in
  rel-lab), M_crit's two in reds. Rows (a), (d), (e) and (f) of rel-lab. Crosses mark the 18 histories at 10⁻¹² that
  fail the K check, for which the tool writes nothing.
- **rel-history** — three histories at 4×10⁻³¹: A = 0.05 at β = 1.0775 (the largest share of K failing at a wall of
  any A = 0.05 history), A = 1 at β = 2.0, A = 10 at β = 2.0. Top: r_χ E_field(N) and r_χ E_tracker(N) against 1 + z_E.
  Bottom: each generation's birth, worst-wall and tracker margins against 1 + z_E at its birth, sized by its share of
  K.

A zero share is drawn on the bottom axis (10⁻⁶) of rows (e) and (f).

## What it says

From `relativistic.provenance.json` (one-loop law, all A; margins over the stored histories, E over the measured):

| M / M_P | stored | measured | max r_χ E_field | max E_field today | worst birth | worst reflection | worst tracker | > 1% of K failing at a wall | max parking share |
|---|---|---|---|---|---|---|---|---|---|
| 10⁻³⁰ | 278 | 278 | 1.5×10⁻³² | 1.4×10⁻⁴⁵ | −2.2 | −1.9 | 29.4 | 5 | 0.009 |
| 4×10⁻³¹ | 1838 | 1838 | 8.9×10⁻³³ | 5.0×10⁻⁴⁵ | −1.3 | −2.7 | 30.1 | 41 | 0.055 |
| 10⁻³² | 283 | 283 | 3.5×10⁻³⁴ | 1.4×10⁻⁴⁴ | −5.8 | −6.4 | 31.2 | 10 | 0.010 |
| 10⁻³³ | 284 | 284 | 6.3×10⁻³⁵ | 1.3×10⁻⁴⁰ | −6.9 | −9.8 | 32.5 | 15 | 0.011 |
| 10⁻³⁶ | 286 | 286 | 9.0×10⁻³⁷ | 5.2×10⁻⁴² | −1.9 | −18.6 | 33.2 | 29 | 0.012 |
| 10⁻⁴⁰ | 288 | 288 | 1.9×10⁻³³ | 6.4×10⁻⁴⁰ | −2.0 | −30.0 | 28.9 | 35 | 0.013 |
| 10⁻⁴² | 288 | 288 | 1.4×10⁻³¹ | 5.3×10⁻³⁹ | −2.3 | −35.6 | 28.5 | 38 | 0.013 |
| 10⁻¹⁵ (A = 1) | 24 | 24 | 1.2×10⁻¹⁹ | 8.4×10⁻⁵⁰ | −18.5 | −21.2 | −26.0 | 17 | 0.99 |
| 10⁻¹² (A = 1) | 26 | 8 | 7.1×10⁻³³ | 1.8×10⁻²⁹ | −15.1 | −15.1 | −15.1 | 2 | 1.0 |

- **Under the one-loop law the quanta stay relativistic in energy, everywhere.** The matter-like share of ρ_R never
  exceeds 1.5×10⁻³² at or below 10⁻³⁰ M_P, nor 1.2×10⁻¹⁹ at M_crit. Nothing in the figure comes within twenty decades
  of mattering to the expansion.
- **Individual generations do fail their margins**, at the walls more than at birth, and in a known window: at 4×10⁻³¹
  the 41 histories with more than 1% of K failing at a wall are 38 at A = 0.05 (β = 1.06–1.42), one at A = 0.1
  (β = 1.0775) and two at β ≤ 1.0;
  at lower M they are the non-surfing β ≤ 1.0 and the low-A transition window, growing in number and depth as M falls
  (worst reflection margin −35.6 at 10⁻⁴²). rel-history's first column shows why it does not matter: the failing
  generations are the late, soft ones, born in thousands of tiny drains near the end of the bouncing, and r_χ E stays
  below 10⁻³⁸ there. This is the finding of 2026-11-02 on the 2026.8.0 store, that the late quanta are *born*
  non-relativistic (`prompts/one-loop-drain/source/margins-correction_2026-11-02.md`), now measured over the whole
  store.
- **The tracker margin holds at every laboratory and lower M** (≥ 28.5). At M_crit it fails, as expected of the
  classical regime there (2026.8.0's tier C: birth margin about −0.7 by construction).
- **E_field and E_tracker differ by up to thirty decades while the field bounces** (rel-history): between bounces the
  field's mass is far below the tracker's. They meet once the field parks.
- **The spectrum's associated piece hardly matters.** From c_a = 1 to 0 the largest relative change of E_field max,
  r_χ E_field max or E_field today over every measured history is 3.9×10⁻⁵, and to c_a = 10 it is 3.5×10⁻⁴.

## What it does not cover

- **The fixed law** (α = 0.01, 0.1): no all-generation margins, no bounce rows and no E, by `one-loop-drain`'s design
  (README §8.1, §8.3). Its weighted failing share reaches 0.86 (α = 0.01) and 0.99 (α = 0.1) at 4×10⁻³¹, for β ≤
  1.0–1.43, and about 1 at M_crit. By the user's ruling it is a comparison only and is not covered further.
- **The parking leftover**, which has no spectrum (`[00-the-parking-leftover-has-no-spectrum]`): ≤ 5.5% of K at
  4×10⁻³¹, ≤ 1.3% at the other low M, and all of K at most of M_crit.
- **The 18 histories at 10⁻¹², A = 1, that fail the K check** (undiagnosed; the run README) and the failed histories.
- **Stimulated emission and rescattering** (δ_stim, n̄; PLAN.md P4). Seen in passing: one history (10⁻³³, A = 0.5,
  β = 0.9) has δ_stim = 7×10⁴, and 24 at M_crit have n̄ > 1. Not this analysis.

## Choices of the session, not ruled

- **The x axis is Figure 6's four columns**, not the single log axis PLAN.md §2.1 named, so that it can be read beside
  Figure 6.
- **rel-lab has six rows, not four**: the birth and reflection margins and the two shares each have a row, since
  three quantities with a band each cannot be read in one. The tracker margin, which never falls below 28.5 at low M,
  is in the table and not drawn.
- **rel-history has two rows**: a row of E as a share of ρ_χ was dropped as a copy of the r_χ E row at another scale.
  It reads `bounces.csv` as well as the two files PLAN.md named, since the margins of each generation are there.
- `plot_quanta_grid.py`'s figures are not used (PLAN.md Q4): its β axis is categorical and cannot be read when the
  dense and sparse grids share it. They stay in `var/science-2026.10.0/out/` as diagnostics.

## Repeating it for a later run

1. Copy this folder into the new run's folder, without its renders, provenance and NOTES.md.
2. The run must have exported both stores' `histories.csv` **and** each one-loop history's `fields.csv` and
   `bounces.csv` into one root, then run `tools/quanta_measures.py ROOT --assoc-scale C` for C = 1, 0, 10 (this run's
   `run_postbuild_2026.10.0.sh` is the pattern; its run script missed it).
3. Copy the new `quanta_histories*.csv` into `inputs/`, choose the three histories again by the same rules (the A =
   0.05 history with the largest `all_reflection_failing_share` at laboratory M; A = 1 and the largest A at β = 2.0)
   and copy their three files; point `HISTORIES`, `SIDECAR`, `M_LAB` and `HISTORY_PICKS` at the new run.
4. Run it, write a new NOTES.md, and update the row in `science-runs/README.md`'s "Standard analyses".
