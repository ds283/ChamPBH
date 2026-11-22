# lnR-test — the power walls' one-loop law beyond its measured ln R

**Of 2026-11-22**, run by a Claude Code session at the user's request ("run the ln R test"), after the user had accepted
the extrapolation for now ("For now we'll accept the `ln R` extrapolation"). **Not yet seen by the user.** Measurements,
not bounds.

```bash
PYTHONPATH=. ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.py > science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.txt
LNR_TEST_START_CAP=1e30 PYTHONPATH=. ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.py > science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test-cap1e30.txt
```

## What it does

The power walls' law f_n(ln R) is `one-loop-inverse-power-v1`, `InversePowerModel._drain_fraction`. It was fitted on
`power-law-wall` 02's stage 2, whose curve points reach ln R = 554.5 (n = 1) and 344.7 (n = 2)
(`.documents/power-law-wall/wall-02/stage2_scan.txt`). This run's one-loop histories reach a first impact at ln R up to
740.2 (n = 1) and 490.0 (n = 2).

`lnR_test.py` runs stage 2 by the same code, `powerscan.run_point_power` under `prompts/power-law-wall/source/wall/`,
which is not edited. It compares each fraction with the production law's constants, imported from
`InversePowerModel`. The points:
- **repeats:** the two largest recorded points again (n = 1 at ln R = 545.6393, n = 2 at 329.7256);
- **new, on the 4×10⁻³¹ M_P curve:** n = 1 at 600, 650, 700 and 750; n = 2 at 380, 420, 460 and 500;
- **new, on the 10⁻³³ M_P curve:** n = 1 at 650 and n = 2 at 460, since stage 2's fraction should depend on ln R and n
  alone.

It takes about 20 s on six processes.

**Two limits of the stage-2 code, not of the physics:**
- `stage2.solve` searches outwards for a start where the lowest mode is adiabatic. It refuses a start more than 10¹⁵
  times beyond the entry point, and at n = 1 that refuses ln R = 650 and 700 ("no adiabatic start"). The second run
  loads `stage2.py` with that cap at 10³⁰, by a text substitution at import, with nothing else changed. Every point the
  original cap allows is identical under the raised one, apart from the seconds column.
- `stage2.solve` forms R = e^{ln R}, which overflows above ln R ≈ 709. So ln R = 750 cannot be run (`OverflowError`).

## What it found

From `lnR_test.txt` and `lnR_test-cap1e30.txt`. "law − f" is relative to stage 2's f.

| n | ln R | curve | stage 2 f | law | law − f | ℓ there | |ΔE/E| |
|---|---|---|---|---|---|---|---|
| 1 | 545.64 (repeat) | 4×10⁻³¹ | 0.053796 | 0.051338 | −4.57% | 44.63 | 1.7×10⁻⁹ |
| 1 | 600 | 4×10⁻³¹ | 0.048968 | 0.046681 | −4.67% | 49.17 | 1.6×10⁻⁹ |
| 1 | 650 (cap 10³⁰) | 4×10⁻³¹ | 0.045234 | 0.043086 | −4.75% | 53.34 | 1.6×10⁻⁹ |
| 1 | 650 (cap 10³⁰) | 10⁻³³ | 0.045234 | 0.043086 | −4.75% | 53.34 | 1.6×10⁻⁹ |
| 1 | 700 (cap 10³⁰) | 4×10⁻³¹ | 0.042030 | 0.040005 | −4.82% | 57.52 | 1.4×10⁻⁹ |
| 1 | 750 | — | — | 0.037335 | — (R overflows) | — | — |
| 2 | 329.73 (repeat) | 4×10⁻³¹ | 0.061095 | 0.059654 | −2.36% | 40.61 | 3.6×10⁻⁹ |
| 2 | 380 | 4×10⁻³¹ | 0.053071 | 0.051743 | −2.50% | 46.91 | 3.5×10⁻⁹ |
| 2 | 420 | 4×10⁻³¹ | 0.048051 | 0.046804 | −2.60% | 51.92 | 2.7×10⁻⁹ |
| 2 | 460 | 4×10⁻³¹ | 0.043900 | 0.042726 | −2.67% | 56.94 | 2.5×10⁻⁹ |
| 2 | 460 | 10⁻³³ | 0.043900 | 0.042726 | −2.67% | 56.94 | 2.5×10⁻⁹ |
| 2 | 500 | 4×10⁻³¹ | 0.040409 | 0.039301 | −2.74% | 61.95 | 2.4×10⁻⁹ |

- **The repeats reproduce `stage2_scan.txt`** in every printed digit (f 0.053796 and 0.061095).
- **The law's extrapolation holds its shape.** Beyond the measured range the law stays below stage 2 by the offset it
  already had at the top of that range. That is −4.6% to −4.8% at n = 1 (to ln R = 700) and −2.4% to −2.7% at n = 2
  (to 500), drifting by about 0.1 percentage point per 50 in ln R. The fall of f with ln R follows the law's
  1/(ln R − c) tail.
- **The fraction depends on ln R and n alone out here too.** The 10⁻³³ M_P curve gives the 4×10⁻³¹ curve's f at the
  same ln R, in every digit, at n = 1 (650) and n = 2 (460).
- **Coverage of this run.**
  - n = 2: the test reaches 500, beyond every first impact of the run (at most 490.0).
  - n = 1: it reaches 700. 50 of the run's 3339 successful one-loop histories have a first impact beyond that, up to
    740.2: β = 3.0, 3.5 and 5.0 at 10⁻⁴⁰ and 10⁻⁴² M_P, every A. The stage-2 code cannot reach them while it forms R
    itself.

## What it means for the run, and what it does not cover

- The extrapolation to the run's ln R adds nothing new: the law is low by a near-constant fraction across the run's
  range, as it was where it was fitted.
  - The power walls' per-impact drain fraction is therefore low by about 4.7% at n = 1 and 2.6% at n = 2 everywhere on
    the falling branch.
  - That is far smaller than the differences between the walls `../fig6/NOTES.md` records (median ΔN_eff at laboratory
    M ×0.74 and ×0.85 against the exponential).
  - How a uniformly low f moves each history's ΔN_eff and yields is not measured here: f enters each impact of a history
    whose later impacts depend on the earlier ones.
- **Not tested:**
  - n = 1 above ln R = 700 (50 histories, all surfing the electroweak kick, where the shifts are small);
  - the rising branch and the gap, which this run's first impacts do not reach, though later impacts do (the least
    drain ln R is 13.4 at n = 1 and 11.4 at n = 2);
  - the exponential's own extrapolation in ℓ below 4×10⁻³¹ M_P (P4).
- Whether the law should be refitted to remove its offset is the user's to decide. A refit would change
  `one-loop-inverse-power-v1` and so the power-law store.
