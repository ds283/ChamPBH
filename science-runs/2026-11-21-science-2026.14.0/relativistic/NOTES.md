# relativistic — do the quanta produced at reflections stay relativistic? (2026.14.0, one set per wall)

**Of 2026-11-22**, written by a Claude Code session at the user's request ("please write the notes"), from the figures
of `4ec8e68`. **Reviewed by the user on 2026-11-22** ("I've reviewed all those notes"). It repeats 2026.11.0's analysis for each wall, with 2026.10.0's plan and
rulings (that run's `relativistic/PLAN.md` §6, the user's of 2026-11-09) and no new choice. Below the line is 2026.11.0's
note, unchanged. That note and 2026.10.0's say what the measures are and what each figure shows.

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/relativistic/relativistic.py --wall {exp,pl1,pl2}
```

It needs no Ray and no store. For each wall it reads:
- `../fig6/inputs/<wall>/histories.csv`;
- `inputs/<wall>/quanta_histories.csv`, `-assoc0.csv` and `-assoc10.csv`, unchanged copies of that wall's
  `tools/quanta_measures.py` output at c_a = 1, 0 and 10;
- the three picked histories' files in `inputs/<wall>/samples/`;
- the wall's store's sidecar.

It writes `rel-lab-<wall>`, `rel-M-<wall>`, `rel-history-<wall>` (`.pdf`, `.png`) and
`relativistic-<wall>.provenance.json`. Each provenance file records `6bc8eed` and a clean tree.

**The port** (2026-11-20, `relativistic.py`'s docstring):
- the one-loop rows are the wall's own drain model;
- the join key gains `model` and `n`;
- `EW_BRACKET` is per wall, set to (2.980, 2.985) on every wall on 2026-11-22 (`../fig6/NOTES.md`);
- `HISTORY_PICKS` follow 2026.10.0's rules within each wall. They give the same three picks on every wall: A = 0.05 at
  β = 1.0775 (the largest share of K failing at a wall), A = 1 at β = 2.0 and A = 10 at β = 2.0;
- the power wall's figures say that x_* and the associated piece are carried over from the exponential, unmeasured on
  that wall (power-law-wall U5).

## What it says

From each `relativistic-<wall>.provenance.json`, one-loop law, all A, aggregated over A as in 2026.11.0's table. The
margins are taken over the stored histories and E over the measured ones. "> 1%" counts the histories with more than 1%
of K failing at a wall.

| wall | M / M_P | stored | measured | max r_χ E_field | max E_field today | worst birth | worst reflection | worst tracker | > 1% | max parking share |
|---|---|---|---|---|---|---|---|---|---|---|
| `exp` | 10⁻³⁰ | 278 | 278 | 1.0×10⁻³² | 1.8×10⁻⁴⁶ | −0.10 | 0.40 | 29.8 | 0 | 0.009 |
| | 4×10⁻³¹ | 1888 | 1888 | 4.9×10⁻³³ | 6.8×10⁻⁴⁶ | 0.19 | 0.87 | 30.2 | 2 | 0.053 |
| | 10⁻³² | 281 | 281 | 2.6×10⁻³⁴ | 1.6×10⁻⁴⁶ | 0.06 | 2.18 | 31.6 | 0 | 0.009 |
| | 10⁻³³ | 281 | 281 | 4.4×10⁻³⁵ | 1.7×10⁻⁴⁶ | 1.97 | 2.66 | 32.5 | 0 | 0.010 |
| | 10⁻³⁶ | 281 | 281 | 3.8×10⁻³⁷ | 2.0×10⁻⁴⁶ | 4.17 | 4.36 | 34.8 | 0 | 0.013 |
| | 10⁻⁴⁰ | 276 | 276 | 2.7×10⁻³⁸ | 1.8×10⁻⁴⁶ | 7.38 | 7.81 | 36.0 | 0 | 0.012 |
| | 10⁻⁴² | 274 | 274 | 4.2×10⁻³⁸ | 1.5×10⁻⁴⁶ | 9.00 | 9.13 | 35.8 | 0 | 0.014 |
| | 10⁻¹⁵ (A = 1) | 25 | 25 | 1.0×10⁻¹⁹ | 5.3×10⁻⁵⁰ | −0.67 | −3.48 | −6.4 | 16 | 0.99 |
| | 10⁻¹² (A = 1) | 26 | 9 | 1.2×10⁻³³ | 8.6×10⁻³⁰ | −0.67 | −0.99 | −2.4 | 2 | 1.0 |
| `pl2` | 10⁻³⁰ | 278 | 278 | 2.3×10⁻³⁴ | 8.3×10⁻⁴¹ | −0.20 | −0.30 | 32.1 | 0 | 0.011 |
| | 4×10⁻³¹ | 1892 | 1892 | 1.7×10⁻³⁴ | 3.3×10⁻⁴⁰ | −0.20 | −0.29 | 32.2 | 3 | 0.033 |
| | 10⁻³² | 286 | 286 | 5.2×10⁻³⁵ | 1.8×10⁻⁴⁰ | −0.20 | −0.20 | 32.8 | 0 | 0.012 |
| | 10⁻³³ | 289 | 289 | 2.4×10⁻³⁵ | 2.0×10⁻³⁹ | −0.20 | −0.55 | 33.2 | 0 | 0.011 |
| | 10⁻³⁶ | 294 | 294 | 2.4×10⁻³⁶ | 3.7×10⁻³⁸ | −0.20 | −0.49 | 34.3 | 0 | 0.012 |
| | 10⁻⁴⁰ | 284 | 284 | 5.5×10⁻³⁸ | 2.6×10⁻³⁸ | 3.47 | 3.70 | 36.3 | 0 | 0.014 |
| | 10⁻⁴² | 284 | 284 | 1.2×10⁻³⁸ | 1.6×10⁻³⁸ | 5.34 | 5.49 | 37.1 | 0 | 0.013 |
| `pl1` | 10⁻³⁰ | 258 | 258 | 1.2×10⁻³⁵ | 7.6×10⁻⁴⁶ | 0.06 | 0.06 | 33.9 | 0 | 0.011 |
| | 4×10⁻³¹ | 1858 | 1858 | 7.1×10⁻³⁵ | 3.3×10⁻⁴⁵ | 0.06 | 1.08 | 33.0 | 2 | 0.040 |
| | 10⁻³² | 264 | 264 | 5.4×10⁻³⁶ | 2.9×10⁻⁴⁵ | 0.06 | −0.07 | 34.1 | 0 | 0.013 |
| | 10⁻³³ | 265 | 265 | 3.6×10⁻³⁶ | 3.6×10⁻⁴⁵ | 0.06 | 0.08 | 34.4 | 0 | 0.011 |
| | 10⁻³⁶ | 269 | 269 | 1.1×10⁻³⁶ | 9.9×10⁻⁴⁴ | 0.06 | −0.06 | 35.1 | 0 | 0.014 |
| | 10⁻⁴⁰ | 274 | 274 | 2.1×10⁻³⁷ | 3.8×10⁻⁴² | 0.06 | 0.00 | 35.9 | 0 | 0.012 |
| | 10⁻⁴² | 276 | 276 | 1.0×10⁻³⁷ | 6.1×10⁻⁴¹ | 0.06 | −0.07 | 36.3 | 0 | 0.012 |

No history fails the K check at any M of any wall, except 17 at 10⁻¹² on `exp`, as in 2026.11.0.

- **Under the one-loop law the quanta stay relativistic in energy, on every wall.** The matter-like share of ρ_R never
  exceeds:
  - 1.0×10⁻³² at or below 10⁻³⁰ M_P on `exp`;
  - 2.3×10⁻³⁴ on `pl2`;
  - 7.1×10⁻³⁵ on `pl1`;
  - 1.0×10⁻¹⁹ at M_crit (`exp` only).

  The conclusion is the same on all three walls.
- **Few generations fail their margins.** The worst margins sit near zero on the power walls at most M and on `exp` from
  10⁻³⁰ to 10⁻³², and they are positive below that on `exp` (2.0 to 9.1). On every wall the histories with more
  than 1% of K failing at a wall are 2 or 3, all at laboratory M, and none at any other low M. The tracker margin holds at
  every laboratory and lower M on every wall (≥ 29.8), and fails at M_crit.
- **E today is larger on the power walls but still negligible.** It is up to 3.7×10⁻³⁸ on `pl2` and 6.1×10⁻⁴¹ on `pl1`,
  against 6.8×10⁻⁴⁶ on `exp`. Times r_χ it bounds nothing.
- **The associated piece matters more on the power walls.** The largest relative change in E is:

  | | `exp` | `pl2` | `pl1` |
  |---|---|---|---|
  | c_a = 1 → 0 | 5.6×10⁻⁵ | 1.8×10⁻³ | 1.8×10⁻² |
  | c_a = 1 → 10 | 5.0×10⁻⁴ | 1.6×10⁻² | 0.14 |

  On the power walls x_* and the associated piece are carried over from the exponential and not measured there (U5). A
  14% change in a share of order 10⁻³⁵ changes no conclusion.
- **rel-history's three histories**:

  | | `exp` | `pl2` | `pl1` |
  |---|---|---|---|
  | A = 0.05, β = 1.0775: generations | 197 | 198 | 206 |
  | A = 0.05, β = 1.0775: worst birth / reflection margin | 1.11 / 1.13 | 1.05 / 1.08 | 1.06 / 1.08 |
  | A = 0.05, β = 1.0775: r_χ E_field max | 5.5×10⁻⁴² | 1.1×10⁻³⁷ | 2.1×10⁻⁴¹ |
  | A = 1, β = 2: generations | 46 | 48 | 37 |
  | A = 10, β = 2: generations | 8 | 11 | 13 |

  At A = 1 and 10 every margin is above 19.

## Resonance: band overlap and phase slip (added 2026-11-23)

**Added on 2026-11-23** by a Claude Code session at the user's request ("Add those resonance numbers to the relativistic
NOTES"). The note above did not report these measures, although `tools/quanta_measures.py` writes them into the same
`quanta_histories*.csv` this analysis reads. **Not yet seen by the user.**

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/relativistic/resonance.py > science-runs/2026-11-21-science-2026.14.0/relativistic/resonance.txt
```

`resonance.py` reads only `inputs/<wall>/quanta_histories*.csv`, one-loop rows, and prints `resonance.txt`, the source
of every number below. The measures are `quanta_measures.py`'s (`one-loop-drain` 05):
- **n̄_max**, the largest over a history's generations j of n̄_j = Σ_{g earlier} A_g Ω(ln Q_g − ln Q_j): the occupation
  the earlier quanta leave in the band where generation j is born, Q = k_ex a being the comoving peak. It is small when
  Q moves by more than a band's width between generations;
- **δ_stim** = Σ_j 2 n̄_j ΔK_j / K_final, the fractional change of the final ρ_χ that stimulated emission would make at
  first order with random phases;
- **the phase slip** of the comoving peak's mode between consecutive bounces, taken massless (the mass only adds phase,
  so this understates it). Parametric resonance needs slips below about 1 rad over many consecutive bounces.

**From 10⁻³⁰ to 10⁻⁴² M_P, on every wall, the measures rule out resonance.**

| wall | histories | δ_stim, median by M | δ_stim, max | n̄_max, max | n̄_max > 1 | slip < 1 rad | least slip |
|---|---|---|---|---|---|---|---|
| `exp` | 3559 | 6×10⁻³⁹ (10⁻³⁰) to 3×10⁻⁶³ (10⁻⁴²) | 1.4×10⁻⁶ | 1.9×10⁻³ | 0 | 0 | 3.7×10²¹ rad |
| `pl2` | 3607 | 7×10⁻⁴² to 2×10⁻⁶⁵ | 1.2×10⁻⁵ | 3.2 | 2 | 0 | 2.7×10²⁰ rad |
| `pl1` | 3464 | 5×10⁻⁴⁵ to 2×10⁻⁶⁰ | 2.9×10⁻⁵ | 1.1 | 1 | 0 | 1.2×10²⁰ rad |

- **The bands do not overlap where the field is at the wall.** At β ≥ 1.06875 (Figure 6's wall edge) n̄_max is at most
  4.2×10⁻⁴ (`exp`), 3.6×10⁻³ (`pl2`) and 9.5×10⁻³ (`pl1`). Every history with n̄_max > 0.1 (none on `exp`, 6 on `pl2`,
  7 on `pl1`) is at β = 0.5–1.0, below the wall edge, and even there δ_stim is at most 2.6×10⁻⁵.
- **The phases are never coherent.** No history at these M has a slip below 1 rad at any bounce; the least is 10²⁰ rad.
- **On the exponential the overlap falls steeply with M**: the largest δ_stim goes from 1.4×10⁻⁶ at 4×10⁻³¹ M_P to
  1.1×10⁻¹⁹ at 10⁻⁴². On the power walls it stays at 10⁻⁷–10⁻⁵ down to 10⁻³⁶ (n = 2) or 10⁻⁴² (n = 1). At the wall
  (β ≥ 1.06875) it is at most 1.2×10⁻⁵ (`pl2`) and 2.9×10⁻⁵ (`pl1`), both at laboratory M.
- **The associated piece hardly matters.** From c_a = 0 to 10 the largest δ_stim moves by less than 3% on `exp` and
  `pl2` and from 2.8×10⁻⁵ to 3.6×10⁻⁵ on `pl1`; the counts of slips below 1 rad do not move. On the power walls the
  spectrum shape, and so Ω, is carried over from the exponential and not measured there (U5).

**At M_crit (`exp`, block C, A = 1) the conditions are not ruled out.** n̄_max > 1 in 19 of 25 histories at 10⁻¹⁵ M_P
and 5 of 9 measured at 10⁻¹², up to 3×10¹⁷, and slips fall below 1 rad in 11 and 4 histories. δ_stim is still at most
2.9×10⁻⁵ there: the large overlaps sit at bounces that drain almost nothing. This is the regime of
`[00-later-bounces-act-on-quanta-already-present]` (`one-loop-drain`, open), where the random-phase reading of δ_stim
fails, as it did there on the low-M ladder and at 10⁻¹² M_P.

These are measurements, not bounds. δ_stim is a first-order, random-phase estimate; stimulated emission and rescattering
themselves are still not modelled (below, "What it does not cover").

## What moved since 2026.11.0 (the exponential)

Between the trees k_ex was redefined (power-law-wall 07b–07d), the spectrum became tabulated (R8) and the one-loop law
became v2 (`../changes/NOTES.md`). This analysis does not separate their effects.
- **The margins hold far more widely.**
  - The worst reflection margin at 10⁻⁴² is +9.1, against −35.6 in 2026.11.0. At 10⁻³⁶ it is +4.4 (−18.6) and at
    laboratory M +0.87 (−2.7).
  - Histories with more than 1% of K failing at a wall: 2 at laboratory M (41), and none at 10⁻³⁰–10⁻⁴² (5 to 38 at
    each M).
  - rel-history's A = 0.05 history, whose worst birth and reflection margins were −0.97 and −2.72, now has +1.11 and
    +1.13.
- **The matter-like share falls at low M.** Max r_χ E_field at 10⁻⁴² is 4.2×10⁻³⁸, against 1.4×10⁻³¹. At laboratory M it
  is 4.9×10⁻³³ (8.9×10⁻³³). Max E today at 10⁻⁴² is 1.5×10⁻⁴⁶ (5.3×10⁻³⁹).
- **M_crit**: max r_χ E_field 1.0×10⁻¹⁹ at 10⁻¹⁵ (1.6×10⁻¹⁹). The 17 histories at 10⁻¹² that fail the K check are as
  before.
- The largest parking share at laboratory M is 0.053 (0.049). The stored counts at B's M fall by up to 14 (288 → 274 at
  10⁻⁴²). This goes with the B-block status changes at β ≤ 1.0 that `../changes/changes.txt` lists, where most
  failures now come at the effective minimum rather than at PRyMordial's floor.

## What it does not cover

Everything 2026.11.0's note lists, on every wall:
- the fixed law;
- the parking leftover;
- the histories that fail the K check, and the failed histories;
- stimulated emission and rescattering.

In addition, x_* and the associated piece are unmeasured on the power walls (U5), and `pl1` has no history at
β = 10, 15 or 20.

---

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
