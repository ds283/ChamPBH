# relativistic — do the quanta produced at reflections stay relativistic? (2026.11.0)

**Of 2026-11-14**, made by a Claude Code session (the run's orchestrator) at the user's request ("write the record and
commit any outstanding work, then the analyses"). **Not yet seen by the user.** It repeats the 2026.10.0 analysis
([`../../2026-11-08-science-2026.10.0/relativistic/`](../../2026-11-08-science-2026.10.0/relativistic/NOTES.md)) as
that folder's "Repeating it for a later run" says, with its plan and rulings
([`PLAN.md`](../../2026-11-08-science-2026.10.0/relativistic/PLAN.md) §6, the user's of 2026-11-09; its choices
accepted by the user on 2026-11-13) and no new choice. That NOTES.md says what the measures are, what each figure
shows and what the analysis does not cover; none of that is repeated here.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/relativistic/relativistic.py
```

It needs no Ray and no store. It reads `../fig6/inputs/histories.csv` (this run's main-store export); `inputs/
quanta_histories.csv`, `-assoc0.csv` and `-assoc10.csv`, unchanged copies of `var/science-2026.11.0/out/main/
QCD_Cosmology/`'s, written by the run script's `tools/quanta_measures.py` steps at c_a = 1, 0 and 10; the three
histories' `bounces.csv`, `quanta_samples.csv` and `quanta_bounces.csv` in `inputs/samples/`; and the main store's
sidecar. It writes `rel-lab`, `rel-M`, `rel-history` (`.pdf`, `.png`) and `relativistic.provenance.json`.

## How it was repeated

- `relativistic.py` is 2026.10.0's with the run's name, the sidecar, the plan's path in its docstring (the plan stays
  in 2026.10.0's folder; it was not copied here, since it is that analysis's dated plan) and `EW_BRACKET`, moved to
  2.980–2.985 as in this run's Figure 6. Committed in `0df8347`, the commit the provenance file records (clean tree).
- **The three histories, chosen again by the same rules**, are the same: A = 0.05 at β = 1.0775 (its
  `all_reflection_failing_share`, 0.508, is still the largest of any A = 0.05 history at laboratory M; next 1.07 at
  0.461), and A = 1 and A = 10 at β = 2.0. `HISTORY_PICKS` is unchanged.
- The beige "not at the wall" edge is computed from the data: β = 1.06875 here (1.06625 in 2026.10.0).

## What it says

From `relativistic.provenance.json` (one-loop law, all A; margins over the stored histories, E over the measured),
aggregated over A as in 2026.10.0's table, which the same aggregation reproduces from that run's provenance file:

| M / M_P | stored | measured | max r_χ E_field | max E_field today | worst birth | worst reflection | worst tracker | > 1% of K failing at a wall | max parking share |
|---|---|---|---|---|---|---|---|---|---|
| 10⁻³⁰ | 278 | 278 | 1.7×10⁻³² | 1.4×10⁻⁴⁵ | −2.2 | −1.9 | 29.4 | 5 | 0.009 |
| 4×10⁻³¹ | 1888 | 1888 | 8.9×10⁻³³ | 5.1×10⁻⁴⁵ | −1.3 | −2.7 | 30.1 | 41 | 0.049 |
| 10⁻³² | 283 | 283 | 4.6×10⁻³⁴ | 1.4×10⁻⁴⁴ | −5.8 | −6.4 | 31.2 | 10 | 0.009 |
| 10⁻³³ | 284 | 284 | 8.3×10⁻³⁵ | 1.5×10⁻⁴⁰ | −7.0 | −9.8 | 32.3 | 15 | 0.009 |
| 10⁻³⁶ | 286 | 286 | 1.0×10⁻³⁶ | 5.2×10⁻⁴² | −1.9 | −18.6 | 33.2 | 30 | 0.011 |
| 10⁻⁴⁰ | 288 | 288 | 1.9×10⁻³³ | 6.4×10⁻⁴⁰ | −2.0 | −30.0 | 28.9 | 35 | 0.012 |
| 10⁻⁴² | 288 | 288 | 1.4×10⁻³¹ | 5.3×10⁻³⁹ | −2.3 | −35.6 | 28.5 | 38 | 0.012 |
| 10⁻¹⁵ (A = 1) | 25 | 25 | 1.6×10⁻¹⁹ | 8.3×10⁻⁵⁰ | −18.4 | −20.7 | −25.1 | 16 | 0.99 |
| 10⁻¹² (A = 1) | 26 | 9 | 1.9×10⁻³³ | 1.7×10⁻²⁹ | −15.1 | −15.1 | −15.1 | 2 | 1.0 |

**Every finding of 2026.10.0's analysis stands:**
- **Under the one-loop law the quanta stay relativistic in energy, everywhere.** The matter-like share of ρ_R never
  exceeds 1.7×10⁻³² at or below 10⁻³⁰ M_P (1.5×10⁻³² in 2026.10.0), nor 1.6×10⁻¹⁹ at M_crit (1.2×10⁻¹⁹).
- **Individual generations fail their margins in the same window**: at 4×10⁻³¹ the 41 histories with more than 1% of K
  failing at a wall are 38 at A = 0.05 (β = 1.06–1.42), one at A = 0.1 (β = 1.0775) and one at A = 0.5 (β = 1.0),
  as in 2026.10.0 (whose note counts two at β ≤ 1.0); at lower M they grow in number and depth as M falls (worst
  reflection margin −35.6 at 10⁻⁴²). rel-history's three histories are within 0.4% of 2026.10.0's in every recorded
  number (A = 0.05: r_χ E_field max 4.14×10⁻⁴¹ against 4.13×10⁻⁴¹).
- **The tracker margin holds at every laboratory and lower M** (≥ 28.5) and fails at M_crit.
- **The associated piece hardly matters**: from c_a = 1 to 0 the largest relative change is 3.9×10⁻⁵, to c_a = 10
  3.5×10⁻⁴, both 2026.10.0's to the digits shown.

## What moved since 2026.10.0

- **50 more histories at laboratory M** (the five new β 2.950–2.975 at ten A), and the largest parking share there
  falls from 0.055 to 0.049; at the other low M it is unchanged or falls by up to 20% (0.011 to 0.009 at 10⁻³³).
- **M_crit**: 10⁻¹⁵ has one more stored history (β = 1.2, which converged here, the run README's "Failed histories"),
  and 10⁻¹² one more measured history, since 17 fail the K check where 2026.10.0 had 18 (β = 3.0 passes here); the
  crosses in rel-M mark them. These change the M_crit rows' extremes: max r_χ E_field 1.2×10⁻¹⁹ → 1.6×10⁻¹⁹ at 10⁻¹⁵,
  7.1×10⁻³³ → 1.9×10⁻³³ at 10⁻¹².
- At 10⁻³⁶ one more history has more than 1% of K failing at a wall (30 against 29).

## What it does not cover

As 2026.10.0's NOTES.md says: the fixed law (its weighted failing share at laboratory M reaches 0.86 at α = 0.01 and
0.99 at α = 0.1, as there), the parking leftover (≤ 4.9% of K at laboratory M, ≤ 1.2% at the other low M, all of K at
most of M_crit), **the 19 histories that fail the K check** (17 in the main store at 10⁻¹², A = 1, and R-Mcrit's two;
undiagnosed, the run README) and the failed histories, and stimulated emission and rescattering. `plot_quanta_grid.py`'s
figures are not used, for the reason there (PLAN.md Q4, its categorical β axis).
