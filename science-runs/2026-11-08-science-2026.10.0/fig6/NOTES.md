# fig6 — Figure 6 at the laboratory mass, under the one-loop drain law

**Of 2026-11-09**, accepted by the user the same day. It follows the `2026.8.0` Figure 6
([`../../2026-11-01-science-2026.8.0/fig6/`](../../2026-11-01-science-2026.8.0/fig6/NOTES.md)), whose design the
user ruled on 2026-11-01 and 2026-11-02, and moves it to the one-loop drain law. `fig6.py` was written by a Claude
Code session at the user's request ("start with 1", 2026-11-09). The choices marked *ruled* are the user's, in
their words below.

**Ruled by the user, 2026-11-09:**
- "A=1 as the black line is fine."
- "Fixed `alpha=0.01` and `alpha=0.1` drawn as lines would be very helpful."
- "Let's try the separate band over `A=0.05 to 3`, as you propose, to see what it looks like." The band spans
  A = 0.05–3 and A = 5 and 10 are lines of their own, since their exclusions come from the dark radiation present
  during BBN (below). Labelled a trial by the user.
- "Use your recommendations for the Planck `Delta N_eff`": the one-tailed ΔN_eff < 0.30 (below).

```bash
./venv/bin/python science-runs/2026-11-08-science-2026.10.0/fig6/fig6.py [--points]
```

It reads only `inputs/histories.csv`, an unchanged copy of
`var/science-2026.10.0/out/main/QCD_Cosmology/histories.csv` (SHA-256 `196f720b…`, written by the run's
`plot_by_beta.py` step at 03:29 UTC on 2026-11-09), and the main store's sidecar in `../registry/stores/`. It
needs no Ray and no store. It writes `fig6.pdf`, `fig6.png` and `fig6.provenance.json`, which records the
script's and the input's SHA-256, the git commit and whether the tree was dirty, the store's digest, and the
exclusion counts by A and by α.

## What it shows

M = 4×10⁻³¹ M_P, φ* = 5 M_P: ΔY_p and Δ(D/H) relative to PRyMordial + ΛCDM, ΔN_eff today and T_J at parking,
against β, in the four columns of 2026.8.0. Carried over unchanged: the baseline (Y_p = 0.24688,
D/H = 2.4583×10⁻⁵), the differential two-sided tests (±2.45% on Y_p, ±2.37% on D/H), the BBN window in the
parking row, the beige
not-at-the-wall region (β < 1.066), the electroweak cut (3.000–3.005) and the analytic QCD threshold. The
delivery temperatures, which place the first two, equal 2026.8.0's to 1.4×10⁻¹⁴ at all 186 β.

- **The band** (*ruled*) spans A = 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2 and 3 at each β. All of them were
  built at 182 of the 186 β; the other four are non-surfing (β ≤ 1.0). The band and every line are split at the
  wall's edge, so the non-surfing samples are not joined to the surfing ones.
- **A = 3, 5 and 10** (*ruled*) are green long-dashed lines, lime, sea green and leaf green: A = 3 is the band's largest A,
  and A = 5 and 10 lie outside the band. Above β ≈ 1.2 they set
  the upper edge of a band over all ten A at most β (108 of 126 in ΔY_p, 94 in Δ(D/H), 124 in ΔN_eff), and at
  β = 1.49–1.84 they are the only A excluded (8 β).
- **A = 1**, the law at its computed strength, is the black line (*ruled*). No point is drawn for each history
  (*ruled*; `--points` draws them, to `fig6-points.*`).
- **The fixed law** at α = 0.1 (medium blue) and 0.01 (steel blue) is drawn beside it as dash-dot lines (*ruled*).
- **The Planck line** is dark grey, dashed, so that it is not read as one of the fixed law's blue lines (a
  choice of the session, not ruled).
- **ΔN_eff today** (*ruled*) is drawn against Planck 2018's one-tailed bound, in place of 2026.8.0's 2 × 0.17 =
  0.34. Planck 2018 results VI (Aghanim et al., A&A 641, A6 (2020), arXiv:1807.06209; `Planck:2018vyg` in the
  paper), §7.5.2: "Restricting ourselves to the more physically motivated models with ΔNeff > 0, the one-tailed
  Planck TT,TE,EE+lowE+lensing+BAO constraint is ∆Neff < 0.30 at 95 %." The same section gives N_eff = 2.92
  +0.36/−0.37 (95%, TT,TE,EE+lowE; eq. 66b) and 2.99 +0.34/−0.33 (95%, TT,TE,EE+lowE+lensing+BAO; eq. 67b), the
  abstract's 2.99 ± 0.17 at 68%. Dark radiation only adds to N_eff, so the one-tailed bound applies. **For the
  paper:** its text says ΔN_eff ≲ 0.3 "from the cosmic microwave background alone"; Planck's 0.30 includes
  lensing and BAO, and no one-tailed CMB-only bound is quoted there.
- **Crosses:** β = 0.5, 0.75 and 0.9, where no A gave a BBN result (BBN's starting temperature just below
  PRyMordial's 0.0002 MeV floor). **Triangles:** β = 1.0, ΔY_p = +76% and Δ(D/H) = +323%, above the axes.

## What it says

Counts for β ≥ 1.06 (182 histories at each A or α), from `fig6.provenance.json`:

| | A = 0.05 | 0.1 | 0.2 | 0.5 | 1 | 1.5 | 2 | 3 | 5 | 10 | α = 0.01 | α = 0.1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| excluded | 76 | 76 | 69 | 68 | 61 | 48 | 39 | 37 | 88 | 93 | 75 | 64 |
| largest β excluded | 1.92 | 1.94 | 1.80 | 1.70 | 1.50 | 1.38 | 1.23 | 1.18 | 1.74 | 1.84 | 1.94 | 1.50 |
| max ΔN_eff today | 0.009 | 0.013 | 0.020 | 0.039 | 0.071 | 0.10 | 0.13 | 0.20 | 0.34 | 0.63 | 0.014 | 0.082 |

- **The exclusions fall with A up to A = 3, then rise.** From A = 0.05 to 3 a stronger drain parks the field
  earlier and the excluded range shrinks, from β ≤ 1.92 to β ≤ 1.18. At A = 5 and 10 it grows again, to 1.74 and
  1.84, for a different reason (below).
- **At large A the exclusion comes from dark radiation during BBN.** Above β = 1.5, among histories that park
  above 10 keV, the shifts follow ΔN_eff at 1 MeV: correlation 0.997 for D/H and 0.965 for Y_p, at about +13.7%
  in D/H and +5.5% in Y_p per unit ΔN_eff, the response of BBN to any extra radiation. D/H fails the test from
  ΔN_eff(1 MeV) ≈ 0.17. At A = 5 and 10 the first impacts drain enough of the field's energy early to reach it.
- **This reverses a 2026.8.0 finding.** There, no history that parked above 10 keV was excluded. Here 184 one-loop
  histories park above 10 keV and are excluded (6 at A = 3, 85 at A = 5, 93 at A = 10), all by the BBN tests;
  the fixed law at α = 0.01 and 0.1 still has none. Late parking is still how the small-A histories are
  excluded.
- **ΔN_eff today passes the Planck line (0.30) only at A = 5 and 10**: at A = 5 for β = 1.115–1.155 (17 histories,
  up to 0.335), at A = 10 for β = 1.095–1.50 (73, up to 0.63). It never excludes a history the BBN tests did not
  already exclude. In the non-surfing region, β = 1.0 (ΔN_eff 0.335–0.34) crosses it at every A ≥ 0.5 and both α (A ≤ 0.2 has no result there);
  it is excluded by the BBN tests regardless.
- **The band alone (A ≤ 3)** has 96 β excluded at some A, up to β = 1.94.
- **19 of the 182 β are excluded at every A**, all below 1.2; **104 at some A**, up to β = 1.94. Above
  β ≈ 1.94 nothing is excluded, as in 2026.8.0.
- **A = 1 against the fixed law:** 61 excluded up to β = 1.50, close to α = 0.1 (64, up to 1.50); A = 0.05–0.1
  match α = 0.01 (75–76, up to 1.92–1.94).
- **Negative shifts:** 253 one-loop histories have ΔY_p < 0 or Δ(D/H) < 0, none below −0.45% and −0.37%, all well
  inside the tests, as in 2026.8.0.

## The drafts of 2026-11-09, in order

1. The band over all ten A, every history a point, A = 1 black, the fixed law orange and blue (dotted).
2. At the user's word, the band over A = 0.05–3 with A = 5 and 10 as dark-red lines, and the Planck line at 0.30.
3. At the user's request ("one more draft version without the individual dots ... These make the plot very
   busy and in some cases the dots are visually confusing relative to the dotted lines"), the same without the
   points. The user: "that last version, without points for the individual histories, is easier to read."
4. At the user's request: "lines for A=3, A=5, A=10 in different contrasting shades of green", "both the fixed
   law lines ... in contrasting shades of blue", long dashes for the green lines and dash-dot for the blue, "to
   help distinguishing which lines serve which purpose, and which lines should be grouped together to interpret
   the bands".
5. At the user's request, the darkest line of each group (A = 10, α = 0.1) brightened "while keeping the
   colour-space distance between the other lines in each group, as much as possible": greens A = 3 `#71d926`,
   A = 5 `#30c26d`, A = 10 `#2d6619`; blues α = 0.1 `#2662d9`, α = 0.01 `#539fc6`, found by a search over
   green (95–145°) and blue (200–220°) hues. In CIELAB, A = 10 and α = 0.1 are now ΔE 62 and 84 from black
   (43 and 45 before); the closest greens are ΔE 39 apart (33–40 before) and the two blues 57 (44 before).
   This is the figure now in `fig6.*`.

Every count above holds for each draft; only the drawing changed.

## Ruled

The user, 2026-11-09, of the fifth draft: "Yes, that's what I want. Please commit this version." The split
(band over A ≤ 3, A = 3, 5 and 10 as green lines) stands, and this is the figure of record.
