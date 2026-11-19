# The power wall: S, the one-loop fraction and the spectrum (power-law-wall prompt 02)

**Campaign:** [`power-law-wall`](../../README.md) · **Prompt:** [`02-measure-the-wall.md`](../../02-measure-the-wall.md) ·
**Log:** [`logs/02-measure-the-wall.md`](../../logs/02-measure-the-wall.md) · **Outputs:**
[`.documents/power-law-wall/wall-02/`](../../../../.documents/power-law-wall/wall-02/) · **Record:**
[`.documents/power-law-wall-verification.md`](../../../../.documents/power-law-wall-verification.md) §2

Research. Nothing here changes production code. It carries the method of crossover
([`../../../one-loop-drain/source/crossover/`](../../../one-loop-drain/source/crossover/README.md)) to the wall of
V = V₀[1 + (M/φ)ⁿ] at n = 1 and 2, and measures how the adopted one-loop drain law fits there. The measurements are
for the user's rulings U3–U5. Nothing here is a recommendation, a bound or a law.

## Files

| file | what |
|---|---|
| `walls.py`, `born.py`, `stage1a_classical.py`, `stage2.py`, `stage2_validate.py` | copies of crossover's (each header names its source and commit), changed only where a comment says "power-law-wall 02": `walls.PowerWall`; `stage2.solve` and `run_point` take a wall object; `stage2.V4` reads any wall's `V4`; `solve`'s start cap is relative to the entry point |
| `powerscan.py` | what the scans share: the curves, the chain at (x, R), `run_point_power`, the adopted law (`f_law`, `drain_fraction` term for term) |
| `path_map.py`, `runs_map.sh`, `map_summary.py` | the paths (§1.4): one tuned history by the tool route under the fixed law, every candidate's (ln x, ln R, φ_t, ℓ_t, ℓ, regime, ln k_ex/M_P); the twenty-five histories; their summary and the Planck-scale table |
| `reproduce_exponential.py` | the copies on crossover's `Wall` and `WallN` against crossover's recorded numbers (§1.2) |
| `validate_wall.py` | §1.2's derivatives and units, §1.3's items 1–4 and 6 |
| `chain_vs_production.py` | §1.3 item 5: the chain against `InversePowerModel.one_loop_turnaround`; `f_law` against `drain_fraction` |
| `stage1a_power.py` | S_n (§1.5), with its quadrature checks |
| `scan_curves.py` | stage 2 along the six curves and an off-curve grid (§1.6) |
| `curve_tables.py` | the curve points as Markdown tables with the adopted law beside them (`stage2_curves.md`) |
| `fit_law.py` | the adopted law against stage 2 at L_E, n, L_M and the best constant L, and the trial refit (§1.7) |
| `spectrum.py` | the test field's x_p, low-k slope and κ, and the exponential's by the same code (§1.8) |

Run from this directory with `../../../../venv/bin/python`, except `path_map.py`, `runs_map.sh` and
`chain_vs_production.py`, which run from the repository root with `PYTHONPATH=.`. Each script's docstring gives its
command and where its output goes. No script uses Ray, a store or the run registry; the scans use six processes.

## The wall and the units

`PowerWall(x, n)` is the wall measured from its floor, U = V − V₀ = V₀(M/φ)ⁿ, parametrized by x = E/V₀ at the
classical turning point, with ω_t = 1 (V''(φ_t) = 1). Its default unit of field is φ_t (orchestrator note 02,
ruling 2):

    U = E φ^{-n},   E = 1/(n(n + 1)),   φ_t = 1,   V₀ = E/x,

and `units="M"` gives walls.py's units (M = 1, φ_t = x^{−1/n}, U = cφ^{−n}, c = V₀ = φ_t^{n+2}/(n(n + 1)), E = cx),
used only where both are well scaled. x = ∞ is the wall with no floor.

**The loop term.** With field unit u and time unit 1/ω_t, the physical tadpole ½V'''⟨δφ²⟩ divided by u ω_t² is
½Ũ'''Σ̃ (ω_t/u)², where Ũ = U/(u²ω_t²) and Σ̃ = ⟨δφ²⟩/ω_t². Since R = ω_t⁴/(8π²E_phys) and Ẽ = E_phys/(u²ω_t²),
(ω_t/u)² = 8π²RẼ: the term is K Ũ''' Σ̃ with K = 4π²RẼ, born.py's G = RẼ/2 in its Born limit, in any unit of field.
So stage2.py's K = 4π²RE holds unchanged in units of φ_t, with E = 1/(n(n + 1)). §1.2's check: the stage-2 fraction in
the two units agrees to 1e-14 (below).

**What the floor does.** U and its derivatives are V₀(M/φ)ⁿ and its derivatives, so in units of φ_t they do not
depend on x: the classical bounce, the mode equation and the stage-2 system are the same at every x. x enters only
through λ = −d ln V/dφ = n s/φ with s = U/(V₀ + U), which sets k_ex = λ√E/2 and so the chain's ℓ (and the production's
k_ex and ℓ). Hence S_n and stage 2's fraction are functions of R and n alone, while ℓ and k_ex depend on x where
(φ_ta/φ_t)ⁿ approaches x.

`run_point_power` therefore sets stage 2's mode grid from the bounce's own scales (the floor-free chain's m_ta and
k_ex, or ω_t and n√E/2), not from ℓ(x): crossover's `run_point` takes them from the chain on the wall it is given,
which the floor suppresses at small x, and its grid then follows the floor (validate V6: 0.546 against 0.669 at
x = 100, n = 1, ln R = 40).

## Results (2026-11-14 to 15)

The full record, with every table, is `.documents/power-law-wall-verification.md` §2; the outputs are in
`.documents/power-law-wall/wall-02/`. In brief:

- **The copies reproduce crossover** (`reproduce_exponential.txt`): S(10) = 1.3529e-03, S of WallN(5, 2) = 2.14335e-03,
  born.run 0.44982, run_point(7.885, 23.833) 0.71709 with 311 modes and 53265 steps, all as recorded.
- **The units** (`validation.txt`, U): units of φ_t against units of M at x = 10, 10³ (10² for ℓ): S to 2.7e-11,
  ℓ to 7e-16, the stage-2 fraction to 1.2e-14 with the same step count.
- **The map** (`map/`, `map_summary.txt`): all twenty-five histories tune in one integration; every candidate is
  one-loop and every history parks one-loop. The path formula holds at all 1148 power-wall candidates to 2.8e-13. At
  laboratory M the first impact has ℓ = 41.7–44.6 (n = 1) and 38.0–40.6 (n = 2), and the classical k_ex is
  e^84–e^116 M_P (n = 1) and e^34–e^55 M_P (n = 2); the exponential's is e^−6.7 (β = 2) to e^2.4 (β = 3.05).
- **The validation** (`validation.txt`, `chain_vs_production.txt`): the classical limit to 2e-6, frac/R → S_n; the Born
  limit to 7e-4; energy to 2e-8; convergence ±1e-5 (ℓ ≳ 20), ±1e-3 (ℓ ≈ 1), ±6e-3 (the gap); the chain equals
  `InversePowerModel.one_loop_turnaround` to 1.7e-14 (and to the printed digits at all 1148 candidates); S_n and the
  fraction do not depend on x, ℓ does once x ≲ 10³(φ_ta/φ_t)ⁿ.
- **S** (`stage1a_S_n.txt`): S_1 = 1.02620e-2 and S_2 = 5.21615e-3 at every x.
- **Stage 2 along the curves** (`stage2_scan.txt`, `stage2_curves.md`): f is one function of ln R per n on every curve;
  0.054 (n = 1) and 0.061 (n = 2) at the laboratory first impacts, up to 0.974 near ℓ = 0.4–0.7, falling through the
  gap to 4e-3 near R = 1.
- **The law** (`fit_law.txt`): at the mapped points L_E is within 0.021 (n = 1) and 0.070 (n = 2), L = n within 0.005
  and 0.100; over all points every option's worst residual (0.25–0.90) is on the 1e-33 curve, where the floor lowers
  the production's ℓ and turns one-loop bounces into "gap" ones without changing f.
- **The spectrum** (`spectrum.txt`): at large x, x_p = 0.47 (n = 1) and 0.51 (n = 2), κ = 2.0 and 2.5, the rise below the
  peak k^1.3 and k^1.8; the same code gives the exponential's 0.65, 3.97 and 1.99.

No figure was made.

## Prompt 03: the law and the spectrum on this wall (2026-11-15)

**Prompt:** [`03-the-law-on-this-wall.md`](../../03-the-law-on-this-wall.md) · **Log:**
[`logs/03-the-law-on-this-wall.md`](../../logs/03-the-law-on-this-wall.md) · **Outputs:**
[`.documents/power-law-wall/law-03/`](../../../../.documents/power-law-wall/law-03/) · **Record:**
[`.documents/power-law-wall-verification.md`](../../../../.documents/power-law-wall-verification.md) §3

Two scripts, added by prompt 03 to carry out the user's rulings R6–R10 (README §6.5, §6.6). Each is still research in
the sense of this directory: its output is a record, and production reads only the literals typed from it.

| file | what |
|---|---|
| `fit_fn.py` | the drain fraction on this wall as f_n(ln R) per n (R7), fitted on 02's `stage2_scan.txt` (read, not re-run); its reference implementation `fraction_power(n, ln R)`, whose literals are production's (`InversePowerModel.DRAIN_PL_*`); and, with `gate`, the gate Law's first two items against production |
| `spectrum_table.py` | the test field's \|β_k\|² tabulated on three walls (R8; P19–P21) for `ComputeTargets/quanta_spectrum.py`, at walls.py's strict setting; modes `lowk`, `energy`, `table`, `shape` (R9) and `oldnew` |

How to run each (from this directory):

```bash
../../../../venv/bin/python fit_fn.py > ../../../../.documents/power-law-wall/law-03/fit_fn.txt
PYTHONPATH=../../../.. ../../../../venv/bin/python fit_fn.py gate > ../../../../.documents/power-law-wall/law-03/gate_law.txt
../../../../venv/bin/python spectrum_table.py lowk   >  ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt
../../../../venv/bin/python spectrum_table.py energy >> ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt
../../../../venv/bin/python spectrum_table.py table LITERALS.py >> ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt
PYTHONPATH=../../../.. ../../../../venv/bin/python spectrum_table.py shape >> ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt
PYTHONPATH=../../../.. ../../../../venv/bin/python spectrum_table.py oldnew OLD_MODULE.py >> ../../../../.documents/power-law-wall/law-03/spectrum_tables.txt
```

`table` writes the literals for `quanta_spectrum.py` to `LITERALS.py` (a scratch path; not committed). `oldnew` reads the
module as it stood before prompt 03 (`git show 7c60226:ComputeTargets/quanta_spectrum.py > OLD_MODULE.py`). The
spectrum modes use six processes; `shape` takes about twenty minutes, the others under a minute each.

### Results

The full record is `.documents/power-law-wall-verification.md` §3. In brief:

- **The fit** (`fit_fn.txt`): the orchestrator's scratch fit (note 03 §0) reproduced with its windows and weights. At the
  literals (six significant figures), over the 62 | 60 non-classical back-out points (n = 1 | 2) the largest miss is
  0.0541 | 0.0557 (rms 0.0191 | 0.0215), and over the mapped histories' points 0.0042 | 0.0030; the switch is at
  ln R_x = 23.0894 | 16.7102, where f peaks at 0.968 | 0.966; f(0⁺) = 0.0049 | 0.0037 against R S_n = 0.0103 | 0.0052
  at ln R = 0.
- **The gate** (`gate_law.txt`): `InversePowerModel`'s fraction equals `fraction_power` bit for bit at 122019
  evaluations per (n, units), under Planck and GeV units; production's α_eff at A = 1 equals min(0.99, f) bit for bit at
  all 1148 candidates of 02's twenty-two power-law map files.
- **The tables** (`spectrum_tables.txt`): from k/ω_t = 10⁻³ to x_hi = 4.0 (exponential), 6.5 (n = 1) and 5.5 (n = 2),
  153–156 nodes at 0.05 in ln x; the spline within 2.8e-6 of direct evaluations at every midpoint; x_p = 0.6468, 0.4658,
  0.5067; κ_hi = 4.191, 2.157, 2.720; the tail carries 2.2e-4, 7.4e-4, 4.5e-4 of x³s. The low-k power at the strict
  setting is 2.0000 (exponential), 1.665 (n = 1) and 1.986 (n = 2) at k/ω_t = 10⁻³; the precision floor moves from
  near 10⁻⁴ ω_t (walls.py's setting) to near 10⁻⁵ ω_t.
- **The shapes** (`shape`): on the exponential, from L = 40 to 134, x_p moves by −3.2% to +0.9%, J by −0.7% to +0.2%
  and κ_hi by −3.7% to +1.1% against L = 90's table. On the power wall the spectrum measured at E/V₀ is the table read
  at x s_t to 2.4e-6 in ln|β|²: the table's error in x is the factor 1/s_t (x_p ×2.08 at n = 1 and ×3.35 at n = 2 at
  the laboratory curve's R = 1 point), measured and not acted on (R9).
- **Old against new** (`oldnew`): on the exponential at c_a = 1, I ×1.334, J ×0.79, M₁/M₃ ×0.63, ln s(x_p) from −0.956
  to −1.975, G(1) ×0.68, Ω(0.5) ×0.53, Ω(2) ×1.25 against U4-U17-v1.
