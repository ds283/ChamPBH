# fig6 — Figure 6 at the laboratory mass, 2026.14.0, one figure per wall

**Of 2026-11-22**, written by a Claude Code session at the user's request ("please write the notes"), from the figures
of `4ec8e68`. **Reviewed by the user on 2026-11-22** ("I've reviewed all those notes"). It repeats 2026.11.0's Figure 6 for each of the run's three walls, with that
figure's rulings (the user's of 2026-11-09, which 2026.11.0's note below the line restates) and no new choice. Below the
line is 2026.11.0's note, unchanged.

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/fig6/fig6.py --wall {exp,pl1,pl2} [--points]
```

A wall is one potential at one n (PLAN.md P10): `exp`, the exponential at n = 1 under `one-loop-exponential-v2`; `pl1`
and `pl2`, the inverse power law at n = 1 and 2 under `one-loop-inverse-power-v1`. Each figure reads
`inputs/<wall>/histories.csv`, a byte-for-byte copy of that wall's `plot_by_beta.py` export, and the wall's store's sidecar
in `../registry/stores/`. It writes `fig6-<wall>.pdf`, `.png` and `.provenance.json`. Each provenance file records
`6bc8eed` and a clean tree.

## The port and the bracket

- **The port, 2026-11-20** (PLAN.md §5, P9). There is one figure per wall (`--wall`, the `WALLS` table). The one-loop
  rows are the wall's own drain model, in place of the retired `one-loop-measured-v1`, and rows are filtered by the
  export's `model` and `n` columns. 2026.11.0's input was edited as text, with no float re-parsed, to give it a `model`
  column and rename its drain model. On it, `--wall exp` reproduces every count in 2026.11.0's `fig6.provenance.json`.
- **`EW_BRACKET`, set 2026-11-22**, after the build, at the user's request ("fix m_trend and set the brackets").
  `ew_candidate` is the same on all three walls: β = 2.980 → 2.985, with the median T_deliver going from 1.953 to
  20.65 GeV (×10.58). At laboratory M the delivery temperature at each of these β is the same in every row of every wall,
  whatever the A or α (min = max). So the bracket is (2.980, 2.985) for `exp`, `pl1` and `pl2`, in `fig6.py`'s `WALLS`
  and `relativistic.py`'s, and `fig6b.py` reads the `exp` value. On `pl1` this is not what `power-law-wall` 07d's
  surfing scan found, which was no electroweak edge at n = 1; that scan was at 10⁻³ M_P, another M.

## What it says

Counts for β ≥ 1.06, as in 2026.11.0's table. Each A or α has 187 β on `exp` and `pl2`. `pl1` has 184, because every
history at β = 10, 15 and 20 fails there, at every A and α. Those failures are an overflow of `(ln V)''` in
`InversePowerModel.d2_logV_dphi2` at φ ≈ 10⁻¹⁵⁶ to 10⁻¹⁹⁸ M_P, and they are the ✕ at the right of `fig6-pl1`. In a
"largest β" row, a dash means none is excluded.

| | A = 0.05 | 0.1 | 0.2 | 0.5 | 1 | 1.5 | 2 | 3 | 5 | 10 | α = 0.01 | α = 0.1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| excluded, `exp` | 77 | 77 | 73 | 64 | 61 | 52 | 48 | 35 | 86 | 93 | 77 | 64 |
| excluded, `pl2` | 77 | 77 | 74 | 70 | 60 | 52 | 53 | 33 | 63 | 92 | 77 | 64 |
| excluded, `pl1` | 78 | 78 | 75 | 68 | 67 | 65 | 52 | 34 | 41 | 91 | 77 | 64 |
| (2026.11.0, exponential) | 77 | 78 | 70 | 68 | 61 | 49 | 39 | 37 | 88 | 93 | 77 | 64 |
| largest β excluded, `exp` | 1.92 | 1.94 | 1.80 | 1.52 | 1.50 | 1.48 | 1.34 | 1.1775 | 1.72 | 1.84 | 1.94 | 1.50 |
| largest β excluded, `pl2` | 1.74 | 1.96 | 1.82 | 1.70 | 1.52 | 1.42 | 1.39 | 1.1825 | 1.50 | 1.84 | 1.94 | 1.50 |
| largest β excluded, `pl1` | 1.74 | 1.94 | 1.82 | 1.68 | 1.68 | 1.50 | 1.49 | 1.1875 | 1.20 | 1.82 | 1.94 | 1.50 |
| max ΔN_eff today, `exp` | 0.009 | 0.012 | 0.019 | 0.037 | 0.068 | 0.096 | 0.124 | 0.185 | 0.315 | 0.599 | 0.014 | 0.082 |
| max ΔN_eff today, `pl2` | 0.009 | 0.011 | 0.017 | 0.032 | 0.057 | 0.082 | 0.105 | 0.152 | 0.258 | 0.501 | 0.014 | 0.082 |
| max ΔN_eff today, `pl1` | 0.008 | 0.011 | 0.015 | 0.028 | 0.050 | 0.071 | 0.092 | 0.132 | 0.221 | 0.439 | 0.014 | 0.082 |

**The three figures look alike because most of what they draw does not depend on the wall.**

- **Everything before the first impact is the same on every wall.** T_deliver is equal at every β, so these coincide on
  all three walls: the beige region's edge (β = 1.06875), the analytic QCD threshold, the electroweak bracket, and the β
  with no BBN result (0.5, 0.75 and 0.9).
- **The fixed law does not see the wall.** Joined on (M, β, α), over the histories ok on both walls at every M of the main
  blocks (670 against `pl1`, 696 against `pl2`):
  - `exp` and `pl1` agree exactly in ΔN_eff today and at 1 MeV, T_deliver, the parking temperature, the reflections and
    the injected fraction, and to 0.0023 percentage points in ΔY_p and 0.032 in Δ(D/H);
  - `pl2` agrees with both to 10⁻¹² relative in ΔN_eff, and to 0.008 and 0.035 points in the yields.

  The exclusion counts are 77 and 64 on all three walls. Only ln R at the first impact differs, which the fixed law does
  not read.
- **Under the one-loop law the walls differ only in how much each impact drains.** The law reads ℓ on the exponential
  and ln R on the power wall. At laboratory M, A = 1 and β = 1.2, the first impact drains:
  - f ≈ 0.078 on `exp` (ℓ = 32.4, falling branch 2.514/(ℓ − 1.186));
  - f ≈ 0.064 on `pl2` (ln R = 309, falling branch 19.61/(ln R − 10.90));
  - f ≈ 0.055 on `pl1` (ln R = 511, falling branch 27.97/(ln R − 14.98)).

  Over the 1877 (`pl2`) and 1847 (`pl1`) one-loop histories at laboratory M ok on both walls, against `exp` (5–95% range
  in brackets):
  - ΔN_eff today is lower by a median factor 0.85 (0.82–0.98) on `pl2` and 0.74 (0.69–0.96) on `pl1`;
  - the injected fraction is lower by the same factors;
  - the reflections are 6% and 13% more;
  - the parking temperature is lower by a median 0.12 and 0.22 dex;
  - the share of the drained energy taken in the one-loop regime agrees to 0.1% (5–95%); it is at least 0.96 on
    every wall.
- **The visible difference is at A = 5, where the dark radiation is present during BBN.**

  | A = 5 | `exp` | `pl2` | `pl1` |
  |---|---|---|---|
  | parked above 10 keV and excluded | 82 | 52 | 19 |
  | excluded | 86 | 63 | 41 |
  | largest β excluded | 1.72 | 1.50 | 1.20 |
  | max ΔN_eff today | 0.315 | 0.258 | 0.221 |

  ΔN_eff today passes 0.30 at A = 5 only on `exp` (12 β, 1.1225–1.15). At A = 10 the walls nearly agree: 93, 92 and 91
  excluded, with ΔN_eff above 0.30 at 73, 70 and 60 β, up to 0.60, 0.50 and 0.44. At A = 3, 4, 1 and 0 histories park
  above 10 keV and are excluded. On every wall ΔN_eff today never excludes a history the BBN tests did not already
  exclude. At laboratory M a power wall at a given A drains like the exponential at a somewhat smaller A: `pl1` at A = 5
  peaks at ΔN_eff 0.22, between the exponential's A = 3 (0.19) and A = 5 (0.32).
- **In the band (A ≤ 3) the exclusions reshuffle but do not move.**
  - β excluded at some A in the band: 97, 96 and 99 (`exp`, `pl2`, `pl1`), up to β = 1.94, 1.96 and 1.94.
  - β excluded at every A in the band: 24, 28 and 28, all below β = 1.19.
  - Most per-A counts are within 6 of the exponential's. The exceptions are on `pl1`: A = 1.5 (65 against 52) and A = 1
    (67 against 61, up to β = 1.68 against 1.50).

  These exclusions come from the yields in the window β = 1.06–1.5, where the shifts are jagged in β. Single histories
  there differ between `pl1` and `exp` by up to 2.1 percentage points in ΔY_p and 11.9 in Δ(D/H); the medians are 0.07
  and 0.24. Shifts that change by this much where the shifts are jagged move histories across a test's edge in both
  directions. This is the kind of reshuffle 2026.11.0's note describes between trees.
- **Negative shifts.** There are 261, 279 and 305 one-loop histories with a negative shift (`exp`, `pl2`, `pl1`). None
  goes below −0.44%, −0.52% and −0.49% in ΔY_p or −0.36%, −0.41% and −0.41% in Δ(D/H), all well inside the tests.

## What moved since 2026.11.0 (the exponential)

Between the trees the exponential's one-loop law changed (`one-loop-measured-v1` became `one-loop-exponential-v2`), as
did k_ex and the spectrum tables (`../changes/NOTES.md`). The fixed law is unchanged to the bit. Under the one-loop law,
ΔN_eff today falls by a median 4.9% at laboratory M, and the largest ΔN_eff falls by about 5% at every A (0.071 → 0.068
at A = 1, 0.63 → 0.599 at A = 10).
- The counts move by up to 9 (A = 2: 39 → 48) and the largest excluded β by up to 0.18 (A = 0.5: 1.70 → 1.52). All of
  these are in the window's reshuffle: `changes.txt` lists 26 histories that become excluded and 20 that cease to be,
  each near a test's edge.
- At A = 5, ΔN_eff passes 0.30 at 12 β, against 17. 179 one-loop histories park above 10 keV and are excluded (4 at A
  = 3, 82 at A = 5, 93 at A = 10), against 186 (8, 85, 93).

## Observations, not acted on

- **On the power walls the one-loop law is used far beyond the ln R it was measured on.**
  `.documents/power-law-wall/law-03/fit_fn.txt` fits the falling branch on the points with ln R > 28 and checks it on
  the grid to ln R = 100 (n = 1) and 60 (n = 2). This run's one-loop histories at laboratory M reach their first impact
  at ln R = 344–635 (`pl1`) and 209–383 (`pl2`), 3–6 times beyond the last checked point, on the falling branch's
  1/(ln R − c) tail. Later impacts reach lower ln R (the least is 13.4 on `pl1` and 11.4 on `pl2`, `log_R_min_drain`),
  so a history's drain is not all on the tail. How much of the walls' difference comes from impacts beyond ln R = 100
  (or 60) is not measured here. Figure 6 does not mark this. The exponential's law reads ℓ, which is 32–39 at the first impact at A = 1 (M-trend
  panel (b)), inside the ℓ ≤ 45 one-loop-drain 02d measured. No campaign board records the power walls' extrapolation;
  it is the user's to rule on.

  **Corrected later on 2026-11-22.** The range stated above is wrong. It came from `fit_fn.txt`'s grid check rows,
  which stop at ln R = 100 and 60. The falling branch was fitted on stage 2's curve points
  (`.documents/power-law-wall/wall-02/stage2_scan.txt`), which reach ln R = 554.5 at n = 1 (the 10⁻³³ M_P curve) and
  344.7 at n = 2. So this run's first impacts at laboratory M go about 15% beyond the measured range at n = 1 (to 635)
  and 11% at n = 2 (to 383), not 3–6 times. Over every M of the run they reach 740 and 490. At the top of the measured
  range the law is 4–5% below the measured f at n = 1 (0.0513 against 0.0538 at ln R = 545.6) and 2% at n = 2 (0.0597
  against 0.0611 at 329.7). The user ruled on 2026-11-22: "For now we'll accept the `ln R` extrapolation". The run's
  README, "What stays open", says what a test would take.
- `pl1` has no history at β = 10, 15 or 20 at any M: all 180 fail (`../m-trend/NOTES.md`). On the other walls those β surf the electroweak kick,
  where the shifts are small.

---

# fig6 — Figure 6 at the laboratory mass, 2026.11.0

**Of 2026-11-14**, made by a Claude Code session (the run's orchestrator) at the user's request ("write the record and
commit any outstanding work, then the analyses"). **Not yet seen by the user.** It repeats the 2026.10.0 Figure 6
([`../../2026-11-08-science-2026.10.0/fig6/`](../../2026-11-08-science-2026.10.0/fig6/NOTES.md)) on this run's
store, with that folder's rulings (the user's of 2026-11-09: the band over A = 0.05–3, A = 1 black, A = 3, 5 and 10
as green long-dashed lines, the fixed law at α = 0.01 and 0.1 as blue dash-dot lines, Planck's one-tailed
ΔN_eff < 0.30, no point for each history) and no new choice. That folder's NOTES.md says what each element is and
where each number comes from; it is not repeated here.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/fig6/fig6.py [--points]
```

It reads only `inputs/histories.csv`, an unchanged copy of `var/science-2026.11.0/out/main/QCD_Cosmology/histories.csv`
(SHA-256 `fb6a8dbb…`, written by the run script's `plot_by_beta.py` step, 09:12–09:18 UTC on 2026-11-14), and the
main store's sidecar in `../registry/stores/`. It writes `fig6.pdf`, `fig6.png` and `fig6.provenance.json`.

## What changed in the script

`fig6.py` is 2026.10.0's with three changes, listed in its docstring and committed in `ad7df2d`, the commit its
provenance file records (clean tree):
- the run's name in the docstring, the title and the PDF metadata;
- the sidecar it cites, `science-2026.11.0.manifest.json`;
- **`EW_BRACKET`**, the grey "surfs the electroweak kick" region, which marks where the delivery temperature jumps at
  laboratory M. In this run's export it jumps between β = 2.980 (1.95 GeV) and 2.985 (20.6 GeV) at every A and α,
  where 2026.10.0's jumped between 3.000 and 3.005; the script's constant is set to the new bracket. This is the move
  `electroweak-eos` measured (its verification §2.4) and the reason PLAN.md §0.1 moved the dense grid's fine segment.

The beige "not at the wall" region's edge is computed by the script from the data (the largest β delivered below
10 MeV); it reads β < 1.069 here.

## What it says

Counts for β ≥ 1.06 (187 histories at each A or α: 2026.10.0's 182 and the five new β 2.950–2.975), as in 2026.10.0's
table. The same counting reproduces that table exactly from that run's input. (The provenance file's own counts include
β = 1.0, so they are one larger.)

| | A = 0.05 | 0.1 | 0.2 | 0.5 | 1 | 1.5 | 2 | 3 | 5 | 10 | α = 0.01 | α = 0.1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| excluded | 77 | 78 | 70 | 68 | 61 | 49 | 39 | 37 | 88 | 93 | 77 | 64 |
| (2026.10.0) | 76 | 76 | 69 | 68 | 61 | 48 | 39 | 37 | 88 | 93 | 75 | 64 |
| largest β excluded | 1.92 | 1.94 | 1.80 | 1.70 | 1.50 | 1.38 | 1.23 | 1.18 | 1.74 | 1.84 | 1.94 | 1.50 |
| max ΔN_eff today | 0.009 | 0.013 | 0.020 | 0.039 | 0.071 | 0.10 | 0.13 | 0.20 | 0.34 | 0.63 | 0.014 | 0.082 |

The largest β excluded and the largest ΔN_eff are 2026.10.0's at every A and α. **Every finding of 2026.10.0's
NOTES.md stands**, with these counts:
- the exclusions fall with A up to A = 3 (β ≤ 1.92 to β ≤ 1.18), then rise at A = 5 and 10 (1.74, 1.84), from the dark
  radiation present during BBN;
- 186 one-loop histories park above 10 keV and are excluded (8 at A = 3, 85 at A = 5, 93 at A = 10; 2026.10.0: 184,
  with 6 at A = 3); the fixed law has none;
- ΔN_eff today passes 0.30 only at A = 5 (β = 1.115–1.155, 17 histories, up to 0.335) and A = 10 (β = 1.095–1.50, 73, up
  to 0.63), and never excludes a history the BBN tests did not already exclude;
- the band alone (A ≤ 3) has 97 β excluded at some A, up to 1.94 (96 in 2026.10.0); 19 β are excluded at every A, all
  below 1.2 (19); 105 at some A, up to 1.94 (104); above β ≈ 1.94 nothing is excluded;
- A = 1 (61, up to 1.50) is close to α = 0.1 (64, up to 1.50), and A = 0.05–0.1 match α = 0.01 (77–78, up to 1.92–1.94);
- 257 one-loop histories have a negative shift (253), none below −0.46% in ΔY_p or −0.36% in Δ(D/H), all well inside
  the tests.

## What moved since 2026.10.0

Joined on (row, β) at laboratory M, over the 182 β both runs hold:

- **The five new β (2.950–2.975), 60 histories, are none of them excluded.**
- **13 histories change class**, all at β = 1.0625–1.23, in the transition window where the shifts are jagged in β:
  10 become excluded (α = 0.01 at β = 1.0625 and 1.08; A = 0.05, 0.1, 0.2 at 1.0625; A = 0.1 at 1.08; A = 0.5 at
  1.0825 and 1.085; A = 1.5 at 1.23; A = 3 at 1.12) and 3 cease to be (A = 0.5 at 1.0675 and 1.1375; A = 3 at 1.0675).
  Each sits near a test's edge in one run or the other, and the largest moves are in Δ(D/H) at β = 1.0625 (+1.5 to +1.6
  pp, from 1.5–2.2% to 3.2–3.7%), where the field reaches the wall at about 0.3 MeV.
- **Away from the edge** (β < 2.9), the median move is 0.005 pp in ΔY_p and 0.009 pp in Δ(D/H); the largest are 0.23 pp
  and 1.9 pp, both in the transition window.
- **At the electroweak edge** the grey region moves left by 0.02 in β, and β = 2.985–3.000 now deliver above 10 GeV;
  their shifts were already well inside the tests and stay so (over β = 2.950–3.040, |ΔY_p| ≤ 0.41%,
  |Δ(D/H)| ≤ 1.0%, ΔN_eff ≤ 0.076, against 0.40%, 0.99% and 0.075 in 2026.10.0).

The comparison across the whole store is PLAN.md P5's `changes/` analysis.
