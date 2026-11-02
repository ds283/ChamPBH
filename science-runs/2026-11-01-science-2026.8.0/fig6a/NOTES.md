# fig6a — Figure 6 at large M, natural mode

**Draft of 2026-11-02**, for the user to review. Made by a Claude Code session at the user's request
("a Fig. 6a that is a more direct successor to the Fig. 6 from `2026.6.0` and shows the constraints
for different 'large' `M` values near `M = Mp`, for `alpha=0`").

```bash
./venv/bin/python science-runs/2026-11-01-science-2026.8.0/fig6a/fig6a.py
```

It reads `../fig6/inputs/histories.csv`, the export of
[`../run_figure_exports.sh`](../run_figure_exports.sh) that Figure 6 and the reflection figure also
read, and the main store's sidecar in `../registry/stores/`. It needs no Ray and no store. It writes
`fig6a.pdf`, `fig6a.png` and `fig6a.provenance.json` (the script's and input's SHA-256, the commit
and whether the tree was dirty, the store's digest, and, for each M, the β that pass both tests and
the reason each history without a result has none).

## What it shows

M = 10⁻³, 0.1 and 0.5 M_P, α = 0 (natural mode: elastic reflections, no dark radiation), φ* = 5 M_P,
the 62 β of the run's tier B grid at each. ΔY_p and Δ(D/H) are relative to PRyMordial + ΛCDM, and the
tests are Figure 6's: differential and two-sided, |ΔY_p| ≤ 2.45% and |Δ(D/H)| ≤ 2.37% (blue band).

- **The y axes are symmetric-log**, linear inside ±3%, since the shifts run from −90% to +580%. The
  band therefore takes more of the axis than its size would on a linear one.
- **The bottom row** is the verdict for each M at each β: the M's marker where a history passes both
  tests, a grey cross where it is excluded, a black triangle where BBN gave no result (β = 0.5 and
  0.75 at every M, 0.9 at 10⁻³: the BBN temperature falls below PRyMordial's spline floor; 0.9 at
  0.1: Y_p = 0.505, outside the output check), and a bar where there is no history (M = 0.5, β = 8,
  10 and 15: the normalization did not converge in 30 integrations; β = 20: the effective minimum
  overflows in tuning, the overflow that stopped block D until `745c8ee` made it a recorded failure).
- **Grey line:** Figure 6's α = 0.08 line at the laboratory mass, for reference.
- **Regions** as in Figure 6: beige where the field has not reached the wall before BBN
  (T_deliver < 10 MeV, β ≤ 1.0 at all three M, which share the bracket 1.0–1.08 exactly), the dashed
  analytic QCD surfing threshold 1.1074, and grey past the electroweak threshold. Here it is drawn at
  3.020, the value measured at M = 10⁻³ by the `surfing-temperature` campaign; on this grid the
  delivery temperature jumps between β = 3.0 and 3.25 at all three M.

## What it says

- **At M = 0.1 and 0.5 every history with yields is excluded**, by both tests: ΔY_p from −14% to
  −55% at 0.1 and −34% to −60% at 0.5 for β ≥ 1.08, Δ(D/H) from −41% to −90%.
- **At M = 10⁻³, four β pass** (1.16, 1.30, 1.35, 1.40). In the window 1.07–1.50, 14 of 18 are
  excluded, with shifts of both signs (8 of 18 have ΔY_p > 0); above 1.5 all 40 are excluded by both
  tests, with ΔY_p from −3.3% to −11% and Δ(D/H) from −4% to −26%, growing with β.
- **The sign is the normalization's.** As shown in the session that made this figure (and in no
  document yet), the shift in the expansion rate at 1 MeV is mostly the factor
  A²(φ_1MeV)/A²(φ_ref) = e^{2β(φ_1MeV − φ_ref)}. At large M today's minimum φ_ref sits well out from
  the wall (0.035, 0.24 and 0.45 M_P at β = 1.5), while at BBN the field is near the wall, so the
  factor is −9%, −51% and −73% at β = 1.5; the remainder (−5.8% there) is the same at all three M. This
  is the reversal the `jordan-normalization` brief predicted at M = 10⁻³, β = 1.5 (Y_p −3.6%); the run
  gives −3.77%.

## Changes from the 2026.6.0 draft

- **M.** 10⁻³ is kept and 0.1 and 0.5 added. 10⁻⁵ is dropped: the 2026.8.0 run has no history there
  (its plan's tier B is 10⁻³, 0.1 and 0.5 in natural mode).
- **The tests** are Figure 6's, differential and two-sided (the user's ruling of 2026-11-01), where the
  draft compared Y_p and D/H with the measurements' absolute 2σ ranges.
- **The bottom row.** The draft's "fraction within 2σ" was a running mean over a ±0.025 window of a
  grid sampled every 0.01 at M = 10⁻³; tier B samples every 0.01–0.05 in the window and more sparsely
  above, so each β is drawn as its own verdict instead.
- **The axes** are symmetric-log, where the draft clipped at +12% and +25%: the large-M shifts are
  negative and large, and would all be off the draft's axes.
- **The surfing band.** The draft's blue "reaches the wall without surfing" band between the wall edge
  and the analytic threshold is not drawn; the threshold is a dashed line, as in Figure 6.
