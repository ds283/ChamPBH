# inelastic-reflection — verification

**Campaign:** [`prompts/inelastic-reflection/`](../prompts/inelastic-reflection/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/inelastic-reflection/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`inelastic-reflection/out/`](inelastic-reflection/out/) ·
**Scripts:** [`inelastic-reflection/measure/`](inelastic-reflection/measure/) (written here), and, run
unchanged, [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/) (`ray_cluster.sh`,
`interrupt_when.py`, `stage_counts.py`) and `tools/compare_stores.py`

This document records the campaign's measurement and rehearsal (prompt 11, 2026-11-01). The earlier
prompts' records are their logs.
- **§1, the measurement** (U10): 48 tuned histories with BBN, by `tools/history_and_bbn.py`, across the
  physical M, β and α, natural mode beside them, and the parking ε.
- **§2, the rehearsal** (U11, R31): `jordan-normalization` 09's run `a` at α = 0 and 0.1, built under real
  Ray on ChamPBH's isolated cluster through the wrapper, interrupted in the α = 0.1 scalar stage and
  resumed; its α = 0 histories against that prompt's 2026.7.0 store; the three drivers' first run
  with α.
- **§3** the BBN spline across injections (P14, P46), **§4** the Standard Model exchange (U7), **§5** the
  commands, **§6** what was observed and not acted on.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated subsection,
and the figures below stay as they are, correct for the tree they were taken on.

The stores, the run records, the launch logs and the figures live in the repository's `var/`, ignored by
git: the stores in `var/datastores/`, the runs in `var/runs/`, everything else in
`var/inelastic-reflection-11/`. They are kept for the user. Only small text outputs are committed, under
`inelastic-reflection/out/11-*` and `inelastic-reflection/out/grid/`. Every claim below is a line in
one of them, or in a command's output quoted here.

**Tree:** `6d1a77c` (`6d1a77c046b0fda2003df0a1b7e35abba148be3d`, the orchestrator's note for 11, on top of
`0a47ec4`, which adds the prompt), whose code is prompt 10's `8c12afa`. `main.py` SHA-256
`7ec588228a58bf2ebb594953a42a818f54b73b0a087175bd95d2fe8ed5e72226`. `ComputeTargets/ScalarHistory.py:126`
`PARKING_EPSILON = 1.0e-6`, unchanged. Gate record: [`11-gate.txt`](inelastic-reflection/out/11-gate.txt).

**The machine.** 10 CPUs, host `mps026088` (`mps026088.phys.susx.ac.uk` in the manifests), shared. The
one-minute load ran from 2.2 to 33.9 during the prompt, driven at times by macOS's own indexing and
assistant daemons (`corespotlightd`, `assistant_service`, `mediaanalysisd`, `AddressBookSourceSync`,
`system_profiler`; [`11-load.txt`](inelastic-reflection/out/11-load.txt)). No ChamPBH run or Ray process was
up at the Gate. Every launch's load is given beside it. A slow run is recorded, not called a defect.

---

## 1. The measurement (U10; prompt 11 §1.1, §1.3, §2.1–§2.5)

**Result: the gate "Measure" holds.**
- **Every point of P40 has a row**: 48 of 48, every one `exit status: 0`, every history tuned and every
  BBN solve a success.
- **The drain is independent of M**: at each of the 16 (β, α), `injected_fraction` at M = 10⁻¹² and
  4×10⁻³¹ M_P agree to **1.47×10⁻⁵** at worst (β = 1.2, α = 0.03), and are equal to the printed six
  digits at the other 15; the limit is 10⁻³.
- **ΔN_eff today rises strictly with α** at each of the 8 (M, β).
- **Every margin is reported**: the ten margin fields of the `dissipation` line are present at all 32
  dissipation points (and the 8 ε points).
- **α = 0 reproduces 2026.7.0** bitwise: §2.4, 548 922 values, no difference.

### 1.1 The scripts and their checks

Under [`inelastic-reflection/measure/`](inelastic-reflection/measure/), each with a docstring:
`points.py` (the 48 points, slugs `nat-M0.5-b2`, `dis-M4e-31-b2-a0.1`, `eps1e-08-M4e-31-b2-a0.1`; ε = 10⁻⁴
formats `:g` as `eps0.0001-…`), `eps_point.py`, `run_grid.py`, `summarise_grid.py`, and the rehearsal's
`histories.py` and `compare_jn09.py` (§2).

- **`eps_point.py 1e-6 -- 2 4e-31 --alpha 0.1` against the plain tool**, timings masked as orchestrator
  note 10's ruling 7 masks them (`wall=`, `PRyM_time=`), the `uptime` lines dropped: `diff` shows one line,
  `> epsilon=1e-06` ([`11-eps-check.txt`](inelastic-reflection/out/11-eps-check.txt)).
- **That check could not tell a working patch from none.** The script's first version imported the module
  as `import ComputeTargets.ScalarHistory as SH`. `ComputeTargets/__init__.py` exports the class
  `ScalarHistory` under the module's name, so that statement binds the class, and `SH.PARKING_EPSILON = …`
  set an attribute of the class that nothing reads. At ε = 10⁻⁶ the output is the same either way. The
  first grid's eight ε points were therefore all at ε = 10⁻⁶, bitwise the grid's own points: same parking N,
  steps, Yp and D/H. The committed script takes the module from `sys.modules` through `importlib` and asserts
  it is a module. A probe that wraps the cut's ratio test shows the patch reaching it: at β = 2,
  M = 4×10⁻³¹, α = 0.1 the history parks at candidate 16 (ratio 9.96×10⁻⁵), 48 (9.32×10⁻⁷) and 79
  (8.75×10⁻⁹) for ε = 10⁻⁴, 10⁻⁶ and 10⁻⁸. The eight ε points were run again (§1.2). The check above is
  the corrected script's; the first version's diff, the same one line, is kept in the same file. Log 11 D3.
- **`run_grid.py --only` on two points, then again**: `dis-M4e-31-b2-a0.1` and `dis-M1e-12-b2-a0.1` ran
  (18.2 s each, `status=0`), and the second invocation logged `skipped … .txt exists` for both
  ([`11-run_grid-selftest.txt`](inelastic-reflection/out/11-run_grid-selftest.txt)). Those two stayed done
  and were skipped by the grid.

### 1.2 The run

- **The SM baseline** (ruling 1), before the grid: `tools/bbn_from_store.py --sm-baseline --small-network`,
  `Yp=0.2468802117 DoH=2.458287893 He3oH=1.041932695 Li7oH=5.486373007`, 7.8 s
  ([`11-sm-baseline.txt`](inelastic-reflection/out/11-sm-baseline.txt)). `plot_by_beta.py` prints the same
  baseline to its five digits (`Yp=0.24688, D/H x1e5=2.45829`).
- **The grid**, after the cluster was stopped and with nothing else of this prompt running: launched
  detached at 03:21:45 GMT (load 5.31 / 7.40 / 9.48), checked once (pid 55694 alive, three points
  started), and waited on by one background `wait_for_line.py` on `** grid done`, which it printed at
  03:29:23: **7 min 38 s** for the 46 points not already done, three at a time, at a one-minute load of
  5.3–31.2 at the points' starts ([`11-run_grid.out`](inelastic-reflection/out/11-run_grid.out)).
- **The eight ε points ran twice.** Their first outputs (the class patch, §1.1) were moved aside to
  `var/inelastic-reflection-11/grid-eps-class-patch/`, and the same runner was launched detached with
  `--only` on the eight at 03:32:04 (load 11.06), ending `** grid done` at 03:32:56, 52 s
  ([`11-run_grid-eps-rerun.out`](inelastic-reflection/out/11-run_grid-eps-rerun.out)). No other point ran
  twice. No point reached the 900 s point timeout; no `.part` was left.
- **The cost.** The 48 points' walls add up to 1 406 s, 23.4 minutes serial (P40: about 25): natural mode
  742 s (8 points, 19–191 s), dissipation 527 s (32, 11–31 s), ε 138 s (8, 13–22 s). The slowest point is
  natural mode at M = 10⁻⁷, β = 5, 191.1 s (483 767 steps, one integration).
- **Every point's output** is [`out/grid/<slug>.txt`](inelastic-reflection/out/grid/), with its header
  (command, SHA, start, load) and footer (end, load, exit status, wall);
  [`11-grid.csv`](inelastic-reflection/out/11-grid.csv) has one row per point and 112 fields, and
  [`11-grid-tables.md`](inelastic-reflection/out/11-grid-tables.md) the tables, in full. The tables below
  are drawn from it.

### 1.3 Dissipation mode (prompt §2.1)

**Convergence and cost.** Every one of the 32 points converged **on the seed** (one integration). Residuals
|residual_H| ≤ 3.22×10⁻⁶ and |residual_φ| ≤ 4.38×10⁻⁶, inside 10⁻⁵. Walls 11.1–31.3 s, of which
PRyMordial about 8–9 s; steps 1 649–32 106. The points at M = 10⁻¹² and 4×10⁻³¹ take the same steps and
RHS evaluations at β = 2, 3 and 5; at β = 1.2 they differ by a few steps, and by 5 % at α = 0.03 (32 106
against 30 564).

**The `dissipation` line** (the same at both M to the digits shown, except ln 𝓡 at the first impact):

| β | α | inj. (all unresolved) | injected_fraction | ρ_χ/ρ_R 1 MeV | 70 keV | today | ΔN_eff 1 MeV | ΔN_eff today | park N | park T_J |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 0.03 | 176 | 0.00477057 | 0.003034 | 0.002714 | 0.002885 | 0.018281 | 0.021488 | 38.287 | 0.57 eV |
| 1.2 | 0.1 | 7 | 0.0150926 | 0.01009 | 0.008852 | 0.008817 | 0.060792 | 0.065672 | 28.364 | 11.6 keV |
| 1.2 | 0.3 | 16 | 0.0447883 | 0.03021 | 0.02604 | 0.02594 | 0.18204 | 0.19323 | 27.023 | 44.2 keV |
| 1.2 | 0.9 | 6 | 0.133561 | 0.09081 | 0.07612 | 0.07581 | 0.54722 | 0.56467 | 18.436 | 134.5 MeV |
| 2 | 0.03 | 22 | 0.00400321 | 0.002589 | 0.002283 | 0.002276 | 0.015602 | 0.016948 | 31.576 | 25.4 keV |
| 2 | 0.1 | 49 | 0.0126169 | 0.008239 | 0.007069 | 0.007041 | 0.049648 | 0.052441 | 30.349 | 83.6 keV |
| 2 | 0.3 | 24 | 0.031084 | 0.01973 | 0.01654 | 0.01647 | 0.11891 | 0.1227 | 23.877 | 37.5 MeV |
| 2 | 0.9 | 6 | 0.0426022 | 0.02359 | 0.01977 | 0.0197 | 0.14216 | 0.14669 | 21.028 | 401 MeV |
| 3 | 0.03 | 118 | 0.00295705 | 0.001933 | 0.001656 | 0.00165 | 0.01165 | 0.012287 | 35.264 | 90.0 keV |
| 3 | 0.1 | 59 | 0.00796211 | 0.005078 | 0.004256 | 0.004239 | 0.0306 | 0.031576 | 29.394 | 23.3 MeV |
| 3 | 0.3 | 27 | 0.0119612 | 0.00671 | 0.005624 | 0.005602 | 0.040434 | 0.041723 | 26.77 | 213 MeV |
| 3 | 0.9 | 6 | 0.014319 | 0.007482 | 0.006271 | 0.006246 | 0.045084 | 0.046522 | 25.02 | 1.02 GeV |
| 5 | 0.03 | 187 | 0.000839868 | 0.000432 | 0.0003621 | 0.0003606 | 0.002603 | 0.0026859 | 36.871 | 197 MeV |
| 5 | 0.1 | 60 | 0.00206365 | 0.001008 | 0.0008446 | 0.0008412 | 0.0060719 | 0.0062655 | 35.559 | 616 MeV |
| 5 | 0.3 | 19 | 0.00469875 | 0.002255 | 0.00189 | 0.001883 | 0.013588 | 0.014022 | 33.689 | 3.70 GeV |
| 5 | 0.9 | 5 | 0.00675799 | 0.003186 | 0.00267 | 0.00266 | 0.019197 | 0.019809 | 30.342 | 96.9 GeV |

- Every reflection is unresolved (`resolved_candidates=0`) at both M. Every history parks; **none is
  released** (`release_N=None`): at these M the parked tracker stays adiabatic to today.
- **ln 𝓡 at the first impact**: at M = 4×10⁻³¹, 138.2, 142.4, 144.8 and 164.2 at β = 1.2, 2, 3, 5 (no
  warning printed); at M = 10⁻¹², −31.28, −27.08, −24.65 and −5.23, and every one of those 16 points printed
  R6's "nearly elastic" warning once.
- **The margins** ([`11-grid-tables.md`](inelastic-reflection/out/11-grid-tables.md) "The margins"):
  - at M = 4×10⁻³¹ every worst margin is at least 6.54 (birth, β = 1.2, α = 0.03), every failing share 0,
    and no margin warning;
  - at M = 10⁻¹² the worst birth margin is −0.70 to −0.74 at every point, with failing shares 0.966–0.9999;
    the worst reflection margin −0.68 to +0.62, failing shares 0.040–0.9994; the tracker margin 12.7–21.2,
    share 0. Each of those 16 points printed the birth and reflection margin warnings;
  - the unassessed share (R21) is 5×10⁻⁷ to 1.6×10⁻³, the same at both M.
- **BBN.** Against the SM (Yp 0.2468802117, D/H 2.458287893): at β = 1.2, ΔYp = +2.6×10⁻³ to +7.2×10⁻³ and
  Δ(D/H) = +0.036 to +0.186 (×10⁻⁵); at β = 2, ΔYp −5.2×10⁻⁴ to +1.9×10⁻³; at β = 3 and 5, ΔYp ≤ +6.2×10⁻⁴
  and Δ(D/H) ≤ +0.015. Against natural mode at M = 10⁻⁷ (P43's second reference), ΔYp −7.2×10⁻³ to
  +2.3×10⁻³. Yp at β = 1.2 is not monotonic in α (0.25386, 0.24949, 0.24952, 0.25411 at α = 0.03, 0.1,
  0.3, 0.9); β = 1.2 at α = 0.03 injects through the whole BBN window and parks at 0.57 eV (§3).
- **Warnings counted:** 0 at every M = 4×10⁻³¹ point and every ε point; 3 at every M = 10⁻¹² point
  (nearly elastic, birth margin, reflection margin); no other `!!` line.

### 1.4 The drain's independence of M (prompt §2.2, gate Measure)

| β \ α | 0.03 | 0.1 | 0.3 | 0.9 |
|---|---|---|---|---|
| 1.2 | **1.47×10⁻⁵** (0.00477064 vs 0.00477057) | 0 | 0 | 0 |
| 2 | 0 | 0 | 0 | 0 |
| 3 | 0 | 0 | 0 | 0 |
| 5 | 0 | 0 | 0 | 0 |

|Δ`injected_fraction`|/`injected_fraction` between M = 10⁻¹² and 4×10⁻³¹. ΔN_eff today: 2.33×10⁻⁵ at
β = 1.2, α = 0.03 and 0 elsewhere. "0" means equal to the line's six printed digits (`:.6g`), so a relative
difference below about 5×10⁻⁶ is not resolved. **The limit, 10⁻³, holds** by a factor of 68 at the worst
pair, whose two histories park 0.0008 e-folds apart (38.2875 and 38.2883 to the table's digits). Log 05 §5.5
measured 3×10⁻⁹ untuned. The tuning does not move it: every point converged on its seed, whose
normalization depends on M only through φ_min (log 06 §5.6), so the tuned histories are the seeded ones.

### 1.5 ΔN_eff against α (prompt §2.3, gate Measure)

| M | β | ΔN_eff today at α = 0.03, 0.1, 0.3, 0.9 | ΔN_eff 1 MeV | rises | ΔN_eff today/α | (max − min)/mean |
|---|---|---|---|---|---|---|
| both | 1.2 | 0.021488, 0.065672, 0.19323, 0.56467 | 0.018281, 0.060792, 0.18204, 0.54722 | yes | 0.716, 0.657, 0.644, 0.627 | 0.13 |
| both | 2 | 0.016948, 0.052441, 0.1227, 0.14669 | 0.015602, 0.049648, 0.11891, 0.14216 | yes | 0.565, 0.524, 0.409, 0.163 | 0.97 |
| both | 3 | 0.012287, 0.031576, 0.041723, 0.046522 | 0.01165, 0.0306, 0.040434, 0.045084 | yes | 0.410, 0.316, 0.139, 0.052 | 1.56 |
| both | 5 | 0.0026859, 0.0062655, 0.014022, 0.019809 | 0.002603, 0.0060719, 0.013588, 0.019197 | yes | 0.0895, 0.0627, 0.0467, 0.0220 | 1.22 |

**ΔN_eff today rises strictly with α at all 8 (M, β)**, and so does ΔN_eff at 1 MeV. The values are the same
at both M to the printed digits (β = 1.2, α = 0.03 aside, §1.4). The drain is close to linear in α only at
β = 1.2 (ΔN_eff/α within 13 %); at β ≥ 2 it saturates, ΔN_eff/α falling by a factor 3.5–8 from α = 0.03
to 0.9. A measurement, not a gate. At α = 0.9 the history parks within a few e-folds of its first impact
(at 0.13–97 GeV), so most of the cascade is cut. ΔN_eff at 1 MeV is 0.85–0.97 of today's: the rest is
injected after 1 MeV, or is the factor between R22's two conventions.

### 1.6 Natural mode (prompt §2.4)

| M | β | integrations | residual_H | residual_φ | wall (s) | steps | P23 warning | Yp | D/H (×10⁵) |
|---|---|---|---|---|---|---|---|---|---|
| 0.5 | 1.2 | 9 | −1.177e-06 | +5.631e-06 | 19.2 | 2 451 | — | 0.1481088904 | 0.6881632458 |
| 0.5 | 2 | 11 | +2.859e-07 | −5.434e-07 | 29.3 | 4 311 | — | 0.1321216459 | 0.5608376718 |
| 0.5 | 3 | 12 | +2.576e-07 | −5.097e-08 | 42.4 | 6 271 | — | 0.1197554279 | 0.4540515949 |
| 0.5 | 5 | 20 | −5.959e-08 | −1.520e-07 | 118.1 | 12 834 | — | 0.09966651129 | 0.3086503576 |
| 10⁻⁷ | 1.2 | 1 | −1.028e-07 | −6.807e-07 | 53.5 | 150 847 | φ_t = 9.79e-10 M_P | 0.2567120943 | 2.598914815 |
| 10⁻⁷ | 2 | 1 | −2.502e-06 | −4.234e-08 | 131.3 | 408 566 | φ_t = 9.43e-10 M_P | 0.2465096843 | 2.461463032 |
| 10⁻⁷ | 3 | 1 | −3.999e-06 | −6.114e-07 | 156.6 | 493 615 | φ_t = 9.233e-10 M_P | 0.2467474981 | 2.469334081 |
| 10⁻⁷ | 5 | 1 | −1.936e-06 | −2.893e-07 | 191.1 | 483 767 | φ_t = 7.906e-10 M_P | 0.2466796763 | 2.452970176 |

- All 8 converged. At M = 10⁻⁷ **P23's warning was printed at every β** (one `!!` line each, no other) and
  every history reached T_CMB and tuned there, on its seed. β = 2 reproduces P40's probe exactly
  (Yp = 0.2465096843, D/H = 2.461463032, 408 566 steps, φ_t = 9.4×10⁻¹⁰ M_P), in 131.3 s against 129 s.
- At M = 0.5 nothing was printed; Yp falls from 0.148 to 0.0997 with β, the large-M yields README §8.6
  leaves to the user (the handoff's §2.2).

### 1.7 The parking ε (prompt §2.5)

At M = 4×10⁻³¹, α = 0.1, the second run (§1.2):

| β | ε | park N | park T_J | injected_fraction | ΔN_eff today | ΔN_eff 1 MeV | Yp | D/H (×10⁵) | wall (s) | steps |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 10⁻⁴ | 28.3641 | 11.56 keV | 0.0150926 | 0.0656722 | 0.0607917 | 0.2494870752 | 2.494555114 | 13.1 | 2 196 |
| 1.2 | 10⁻⁶ | 28.3641 | 11.56 keV | 0.0150926 | 0.0656722 | 0.0607917 | 0.2494870752 | 2.494555114 | 12.1 | 2 196 |
| 1.2 | 10⁻⁸ | 33.7804 | 51.3 eV | 0.0150926 | 0.0656722 | 0.0607917 | 0.2494900817 | 2.494587231 | 15.2 | 9 132 |
| 2 | 10⁻⁴ | 29.1825 | 220 keV | 0.0126408 | 0.0525816 | 0.0496484 | 0.2470928279 | 2.472660472 | 14.2 | 4 123 |
| 2 | 10⁻⁶ | 30.3493 | 83.6 keV | 0.0126169 | 0.0524405 | 0.0496484 | 0.2470934572 | 2.472544933 | 18.2 | 9 929 |
| 2 | 10⁻⁸ | 30.7533 | 57.6 keV | 0.0126166 | 0.0524386 | 0.0496484 | 0.2470845388 | 2.472522282 | 21.2 | 15 272 |
| 3 | 10⁻⁴ | 28.4775 | 54.0 MeV | 0.00798208 | 0.0316794 | 0.0307006 | 0.2473046014 | 2.468476985 | 15.2 | 5 496 |
| 3 | 10⁻⁶ | 29.3943 | 23.3 MeV | 0.00796211 | 0.0315756 | 0.0306 | 0.2473020181 | 2.468474031 | 17.2 | 11 558 |
| 3 | 10⁻⁸ | 29.6084 | 19.1 MeV | 0.00796198 | 0.0315748 | 0.0305992 | 0.2473008917 | 2.468375492 | 22.3 | 17 906 |
| 5 | 10⁻⁴ | 34.9377 | 1.10 GeV | 0.00206856 | 0.00627652 | 0.00608259 | 0.2469690156 | 2.459990118 | 14.2 | 4 294 |
| 5 | 10⁻⁶ | 35.5586 | 616 MeV | 0.00206365 | 0.00626553 | 0.00607194 | 0.2469707509 | 2.460138242 | 17.2 | 11 460 |
| 5 | 10⁻⁸ | 35.6114 | 587 MeV | 0.00206365 | 0.00626553 | 0.00607194 | 0.246974252 | 2.460043126 | 22.2 | 17 941 |

- **Against log 06 §5.5.** At β = 1.2, ε = 10⁻⁴ and 10⁻⁶ park at the same candidate and ε = 10⁻⁸ parks
  **5.416 e-folds later** (log 06: 5.4, at 51.3 eV; the same here), with the drain unchanged to the printed
  six digits (log 06: Σ + leftover the same to 2×10⁻⁸). No history is released at any ε.
- **ε = 10⁻⁴ against 10⁻⁶** at β = 2, 3, 5: the switch comes 0.62–1.17 e-folds earlier, and
  `injected_fraction` is larger by 1.9–2.5×10⁻³ relative (0.5–2.4×10⁻⁵ absolute, inside the ε ρ_R bound on
  the leftover, P19), ΔN_eff today by 1.8–3.3×10⁻³ relative.
- **ε = 10⁻⁸ against 10⁻⁶**: the switch comes 0.05–0.40 e-folds later at β = 2, 3, 5, and the drain differs
  by at most 2.4×10⁻⁵ relative.
- **BBN:** |ΔYp| ≤ 8.9×10⁻⁶ and |Δ(D/H)| ≤ 1.5×10⁻⁴ (×10⁻⁵) between any two ε at the same β, against the
  shifts from the SM of §1.3 (≥ 3.9×10⁻⁵ in Yp).
- **Cost:** a smaller ε takes more steps before the switch: 3.3–4.2× more at ε = 10⁻⁸ than at 10⁻⁴.

---

## 2. The rehearsal (U11, R31; prompt 11 §1.2, §2.6–§2.8)

**Result: the gate "R" holds.**
- **The resumed store equals the reference**: `compare_stores` `RESULT: equal`, `problems: none`; both
  stores fingerprint to **`afd2f329d79b3b3e65fd7af53dd1ef33b80f60f74e49eb1d77698da0ea6f1264`**.
- **The interruption was shown**: the signal fell in the α = 0.1 scalar stage after three of its four
  histories were stored, and the resume computed the fourth.
- **The two α are distinct histories**: eight `ScalarHistory` rows, four per α, with distinct serials and
  values (§2.3).
- **The readers behave as log 10 says** (§2.6): the inventory names α; both plot scripts write every
  product of R26 and leave the store's 18 files byte-identical; an unstored α exits 2, names
  `--alpha-values` and `0.3`, and writes nothing.
- **Every stored tuned value equals the standalone tool's, bitwise**: 8 histories × 15 keys × 2 stores
  (ruling 3); the 24 fields of each α = 0.1 history's `dissipation` line equal the stored columns at `:.6g`.
- **α = 0 reproduces 2026.7.0**: 548 922 values, every one bitwise equal (§2.4).

### 2.1 The cluster and the builds

- **The cluster** ([`11-cluster.txt`](inelastic-reflection/out/11-cluster.txt)): `ray_cluster.sh start
  "inelastic-reflection 11"`, exit 0, `started in 7s; gcs_server pid 52326`, at 02:58:18 GMT (load before,
  19.35 / 17.14 / 11.97). `status`: lock HELD, purpose `inelastic-reflection 11`, 19 processes, owner pid
  alive. The `gcs_server` (52326) and the `raylet` (52360) run with `RAY_AUTH_MODE=token`; `ray status`:
  `0.0/10.0 CPU`, no failures. `grep -c` of the four token strings in `start.txt`: 0 each.
- **Each build** went through the wrapper, detached through `sh`, with a recorder (`beat_times.py`) beside
  it, its registration waited on in the foreground (`--timeout 120`, 0.0 s each), checked once
  ([`11-checks.txt`](inelastic-reflection/out/11-checks.txt): the pid file equals `status.json`'s pid,
  state `running`, lines 1–3 the wrapper's and line 4 `>> packages:`), and its end waited on by a
  background `wait_for_line.py` (`--timeout 3000`).

| Run id | Slug | Store | pid | State | Exit | Launched → ended (GMT) | Wall | Load at launch | `results_store_id` | Fingerprint at finish |
|---|---|---|---|---|---|---|---|---|---|---|
| `inelastic-reflection-11-reference-a-20261101T025902` | `reference-a` | `reference` | 52705 | `done` | 0 | 02:59:01 → 03:03:47 | 4 min 46 s | 11.82 | `null` | `afd2f329…1264`, taken 03:03:23; sidecar not written (none existed) |
| `inelastic-reflection-11-resumed-a-20261101T030431` | `resumed-a` | `resumed` | 53257 | `killed` | 143 | 03:04:29 → 03:08:46 | 4 min 17 s | 7.65 | `null` | `c3f1c762…94e0`, taken 03:08:22; sidecar not written (none existed) |
| `inelastic-reflection-11-resumed-a2-20261101T030928` | `resumed-a2` | `resumed` | 53954 | `done` | 0 | 03:09:27 → 03:10:54 | 1 min 27 s | 8.85 | `191efe9e778e4235b6460df5731a0cab` | `afd2f329…1264`, taken 03:10:30; sidecar `written` |

- **The stage times** (each log's `ALL WORK ITEMS COMPLETE`): `reference-a` α = 0: scalar 2m 33.1s,
  adiabatic 0.974s, BBN 18.5s; α = 0.1: 43.2s, 0.865s, 15.5s. `resumed-a` α = 0: 2m 34.9s, 0.96s, 18.4s;
  α = 0.1 scalar interrupted. `resumed-a2` α = 0: no scalar line (nothing to compute), adiabatic 0.113s,
  BBN 0.0158s; α = 0.1: 31s, 1.47s, 16.7s ([`11-stage-counts.txt`](inelastic-reflection/out/11-stage-counts.txt)).
- **The manifests** ([`11-<slug>.manifest.json`](inelastic-reflection/out/)): `script` `main.py`,
  `script_sha256` `7ec58822…2226`; `git_head` `6d1a77c0…`; **`git_dirty` `true`** for all three, since the
  measurement scripts of §1.1 existed, untracked, under `.documents/inelastic-reflection/measure/`, as the
  note's order puts them before the cluster; no tracked file differed (log 11 D2). `scope` `config
  exponential.yaml; β 0.5, 1.5, 3, 5.5; M 0.001 Mp; n 1; φ* 5 Mp; tuning tolerance 1e-05; α 0, 0.1; 16
  shards; job inelastic-reflection-11-<slug>`.
- No log has a `Traceback`, an `INTEGRITY WARNING` or a prune report. Every log prints
  `-- AdiabaticHistory: 0 models skipped …`; `resumed-a2`'s α = 0 BBN stage prints `1 computations skipped
  because of a stored failure` (β = 0.5's).
- **The stores**: `store create` after `reference-a`, `store_id` `7b179dfa36b94700b8c15ea48aa39c02`; after
  `resumed-a`, `191efe9e778e4235b6460df5731a0cab` ([`11-store.txt`](inelastic-reflection/out/11-store.txt),
  the sidecars [`11-reference-store.sidecar.json`](inelastic-reflection/out/11-reference-store.sidecar.json),
  [`11-resumed-store.sidecar.json`](inelastic-reflection/out/11-resumed-store.sidecar.json)).

### 2.2 The interruption (ruling 4)

- **The delay** ([`11-delay.txt`](inelastic-reflection/out/11-delay.txt)). `reference-a`'s recorder saw
  `>> RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1` at 03:02:23.291 UTC. The reference store's
  α = 0.1 `ScalarHistory.timestamp`s (UTC) are β = 0.5 at +14.881 s, β = 1.5 at +15.269 s, β = 3 at
  +24.626 s, β = 5.5 at +41.867 s. **t₁ = 14.881 s, t₄ = 41.867 s**, t₄ − t₁ = 26.986 s, so the window was not
  short; (t₁ + t₄)/2 = 28.374 s, **delay 28 s**. Expected: β = 0.5, 1.5 and 3 stored, β = 5.5 not.
- **The signal** ([`11-resumed-a.interrupt.log`](inelastic-reflection/out/11-resumed-a.interrupt.log)):
  `saw "RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1"; waiting 28 s` at 03:07:53, `sent SIGTERM to
  pid 53257` at 03:08:21. The wrapper's handler line, `** SIGTERM at 2026-11-01 03:08:21 +0000: recorded
  killed, exit code 143`, came in the same second; the final record (heartbeat 03:08:45) 24 s after the
  signal; `main.py` ended with `SystemExit(1)`; `** launcher saw exit status 143` at 03:08:46.764.
- **What was stored at the signal** (the reader on the resumed store before `store create`,
  [`11-histories-resumed-after-a.txt`](inelastic-reflection/out/11-histories-resumed-after-a.txt)): the four
  α = 0 histories, and **α = 0.1 β = 0.5, 1.5 and 3, each `validated = 1`**, stored at +14.9, +15.3 and
  +24.5 s after the banner; **β = 5.5 not stored**. `ScalarHistory` 7, `AdiabaticHistory` 4, `BBNData` 4
  (α = 0's). The expected outcome: **the interruption is shown.**
- **Ray after it** ([`11-ray-state.txt`](inelastic-reflection/out/11-ray-state.txt)): jobs `01000000` and
  `02000000` `SUCCEEDED` (Ray's record of a `SIGTERM`ed driver, as before), workers 44 recorded, 0 alive;
  actors 34, 0 `ALIVE`. `ray_cluster.sh status`: the same 19 processes as after `start` (the pid lists'
  `diff` empty).
- **The resume** (`resumed-a2`, `--prune-unvalidated`): no prune report (no row was unvalidated). At α = 0
  it computed nothing; at α = 0.1 its scalar stage looked up, computed, stored and validated **one**
  history (β = 5.5), and its adiabatic and BBN stages computed all four.

### 2.3 The histories

From the reference store ([`11-histories-reference.txt`](inelastic-reflection/out/11-histories-reference.txt));
the resumed store's rows are the same in every value but `serial`-free bookkeeping (§2.5). M = 10⁻³ M_P,
φ* = 5. `compute_time` is the final integration's; T_J in MeV.

| α | β | integrations | plain / secant / guard | residual_H | residual_φ | `compute_time` (s) | steps | park N | park T_J | release N | release T_J | inj. | ΔN_eff today | ΔN_eff 1 MeV | Yp | D/H |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.5 | 4 | 1/2/0 | −6.582e-06 | +6.279e-06 | 0.73 | 24 309 | — | — | — | — | 0 | 0.0 | 0.0 | BBN failure | |
| 0 | 1.5 | 4 | 1/2/0 | +1.272e-07 | +1.790e-06 | 5.93 | 194 746 | — | — | — | — | 0 | 0.0 | 0.0 | 0.2375649458 | 2.222747690 |
| 0 | 3 | 4 | 1/2/0 | −1.440e-06 | +9.425e-06 | 9.68 | 293 177 | — | — | — | — | 0 | 0.0 | 0.0 | 0.2343979348 | 2.145602813 |
| 0 | 5.5 | 5 | 1/3/0 | −9.983e-10 | +5.114e-08 | 22.47 | 605 244 | — | — | — | — | 0 | 0.0 | 0.0 | 0.2307605351 | 2.052524898 |
| 0.1 | 0.5 | 4 | 1/2/0 | −1.661e-06 | +3.935e-06 | 1.40 | 25 311 | — | — | — | — | 13 (resolved) | **8.9525** | 0.0 | BBN failure | |
| 0.1 | 1.5 | 5 | 1/3/0 | −3.523e-08 | −2.124e-08 | 2.37 | 39 251 | 36.3435 | 17.7 eV | 47.5136 | 2.37×10⁻¹⁰ | 31 | 0.062168 | 0.056851 | 0.2385699182 | 2.294564350 |
| 0.1 | 3 | 4 | 1/2/0 | −1.699e-06 | +9.702e-06 | 4.99 | 75 132 | 29.4083 | 23.0 | — | — | 59 | 0.031567 | 0.030592 | 0.2349475983 | 2.150627273 |
| 0.1 | 5.5 | 5 | 1/3/0 | −3.292e-10 | +3.089e-08 | 4.50 | 67 886 | 37.8084 | 777.8 | — | — | 59 | 0.0058968 | 0.0057146 | 0.2309233158 | 2.055765592 |

- Every history converged (failure 0, validated 1, both residuals inside 10⁻⁵).
- **What dissipation does at M = 10⁻³** (prompt §0.3 left it to the rehearsal): β = 1.5 **parks at 17.7 eV
  and is released at N = 47.51, T_J = 2.37×10⁻¹⁰ MeV**, at today's temperature; β = 3 and 5.5 park and are
  not released; **β = 0.5 neither parks nor is released**: its 13 drains are resolved, all after 1 MeV
  (N ≈ 41), and leave `injected_fraction` 1.202, ρ_χ/ρ_R today 1.202 and ΔN_eff today 8.95 (§6).
- **The BBN failure of β = 0.5** is at both α, with the reason JN 09 found at α = 0: `PRyMordial:
  ComputationFailureError: T_in_MeV=0.00019824 MeV is smaller than T_BBN_spline_min=0.0002 MeV`.
- The α = 0 values are JN 09's to the digit (its verification §1.3): 4 / 4 / 4 / 5 integrations, the
  same residuals and `z_samples`.

### 2.4 α = 0 against 2026.7.0 (ruling 2; gate Measure)

`compare_jn09.py` on the two stores' shards, read-only
([`11-compare-jn09.txt`](inelastic-reflection/out/11-compare-jn09.txt)), **exit 0, `RESULT: every
compared value bitwise equal`, `DIFFERENCES: none`**.
- **Matched:** each of the four α = 0 histories once in each store, by (M, β, φ*) through its model's row;
  the stored M equal by `float.hex` (`0x1.01d9940d10e2cp+81` eV). Samples by ln(1 + z) through each store's
  own redshift table: every sample matched, none on one side only, no duplicate (4 620, 5 161, 5 973 and
  7 328; BBN 0, 1 497, 1 481, 1 479).
- **Left out, by name:** `serial` and every `*_serial` column; `version`; `label`; `timestamp`;
  `compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`, `NP_compute_time`, `BBN_compute_time`.
- **Compared:** `ScalarHistory`'s 31 shared columns (the tuning's outputs, `first_bounce_*`, the
  fixed-temperature values, `RHS_evaluations`, `compute_steps`, `failure`, `failure_reason`, `z_samples`,
  `validated`, …) and `extra_data` key by key over the keys both hold (`cap_fraction`, `cap_floor`,
  `cap_global_max_step`, `jacobian_factor_max`, `accepted_steps`; none on one side only); all 18 of
  `ScalarHistoryValue`'s value columns; `AdiabaticHistory`'s 6 and `AdiabaticHistoryValue`'s 5;
  `BBNData`'s 10 and `BBNDataValue`'s 4.
- **The count:** `ScalarHistory` 124, `extra_data` 20, `ScalarHistoryValue` 415 476, `AdiabaticHistory` 24,
  `AdiabaticHistoryValue` 115 410, `BBNData` 40, `BBNDataValue` 17 828: **548 922 values, all equal.**
- **The new columns, listed, not compared**: `alpha_serial` and the 24 dissipation columns; at α = 0 each
  history holds `injections=0`, both candidate counts 0, `injected_fraction`, the three ρ_χ/ρ_R and both
  ΔN_eff `0.0`, and NULL for 𝓡, parking, release, the margins and the shares (R24). The new
  `ScalarHistoryValue.rho_chi_over_rho_rad` is 0.0 at every sample.
- The script, run on JN 09's store against itself before the rehearsal, gave the same count and no
  difference.

### 2.5 The stores compared, and the tool (ruling 3)

- **`compare_stores`** ([`11-compare-stores.txt`](inelastic-reflection/out/11-compare-stores.txt)), exit 0,
  **`RESULT: equal`**, `problems: none`. Every table matched with 0 rows on either side only:
  `AdiabaticHistory` 8, `AdiabaticHistoryValue` 46 162, `BBNData` 8, `BBNDataValue` 8 909,
  `ExponentialModel` 4, `ScalarHistory` 8, `ScalarHistoryValue` 46 162, `alpha_value` 2, `beta_value` 4,
  `redshift` 75 001, and the rest. The bookkeeping columns that differ: every table's `timestamp`, the
  labels, and the compute times; no `extra_data`.
- **`store fingerprint`** on each closed store: both **`afd2f329d79b3b3e65fd7af53dd1ef33b80f60f74e49eb1d77698da0ea6f1264`**,
  `problems: none`, format 6, the sixteen classes' counts and digests identical (`ScalarHistory 8
  697f71924c04`, `AdiabaticHistory 8 bf476e04d8c6`, `BBNData 8 05fe7a129591`, `alpha_value 2 2d20742d6220`, …).
  The resumed store's "matches the fingerprint taken 2026-11-01T03:10:30+00:00 by run
  inelastic-reflection-11-resumed-a2-…, at 6d1a77c0… (dirty)"; the reference's sidecar records none, having
  been created after its only run.
- **The eight tool runs** (ruling 3), after the stop, serially, one Bash call each, 03:14–03:19 at a
  one-minute load of 5.3–9.6 at their starts ([`11-tool-*.txt`](inelastic-reflection/out/)), and `compare_tool.py` on both
  stores ([`11-compare-tool.txt`](inelastic-reflection/out/11-compare-tool.txt)), exit 0: **`SUMMARY: inputs
  all as the tool; converged all; bitwise all 8 x 15 x 2 equal; dissipation lines (measured, :.6g) all
  equal`**. The tool's BBN line prints each stored Yp and D/H to its ten digits, and β = 0.5's failure with
  the stored reason, at both α.
- **The silences** ([`11-silences.txt`](inelastic-reflection/out/11-silences.txt)): the longest heartbeat
  silence was **153 s** (`resumed-a`, the α = 0 scalar stage; `reference-a` 151 s, `resumed-a2` 31 s),
  against the 900 s window. Since `tuning-followups` 02 the tune lines are quiet, so the scalar stage beats
  only on a tune line after 120 s or on a stored history.

### 2.6 The readers (gates Driver and R)

Each unregistered, in the foreground, on the reference store, with `--config exponential.yaml --database
… --shards 16` and, for the plots, `--ray-address 127.0.0.1:6380` (ruling 5). `shasum -a 256` of the store's
17 files and its sidecar before and after each: **equal every time** (`diff` exit 0;
[`11-shasums.txt`](inelastic-reflection/out/11-shasums.txt)).

- **`main.py --inventory`**, exit 0 ([`11-inventory.log`](inelastic-reflection/out/11-inventory.log)):
  under "Redshifts, tolerances and parameter values", `@@ alpha_value (replicated): 2 records` with
  `values = [ 0, 0.1 ]`; `alpha=0` or `alpha=0.1` on each of the eight `ScalarHistory` lines and on each
  `AdiabaticHistory` and `BBNData` line's history; `ScalarHistory (sharded): 8 records: 8 validated`.
- **`plot_by_beta.py --phi-init-Mp 5 --alpha-values 0 0.1`**, exit 0, **23 s**
  ([`11-plot-by-beta.log`](inelastic-reflection/out/11-plot-by-beta.log)). Under
  `var/inelastic-reflection-11/rehearsal/plot-by-beta/QCD_Cosmology/`: `alpha=0/` and `alpha=0.1/`, each
  with `csv/M=2.4353e+24eV_n=1/data.csv`, `plots/M_convergence.pdf` and `plots/M=2.4353e+24eV_n=1/` holding
  `abundance_shifts`, `BBN`, `fixed_T`, `max_Q`, `T_deliver` and `timings` (PDF and PNG); at the label's
  level `histories.csv` and `plots/M=2.4353e+24eV_n=1/Delta_N_eff.pdf`. It printed one BBN model dropped per
  α (β = 0.5, the reason above).
- **`histories.csv`** (SHA-256 `9e2105ae9d6963eb464677aa6329f6d3b875117326af051e37827299011c558b`, as
  written; [`11-histories.csv`](inelastic-reflection/out/11-histories.csv)): **eight rows, sorted by (α, M,
  n, β)**, with `alpha`, `Delta_N_eff_today` and `Delta_N_eff_1MeV`; **all 16 ΔN_eff values equal the
  stored columns by `float.hex`**. The file as written has CRLF line endings; the committed copy is stored with LF
  (`core.autocrlf input`), so the digest above is the written file's.
- **`plot_ScalarHistory.py --phi-init-Mp 5 --alpha-values 0 0.1`**, exit 0, **18 s**
  ([`11-plot-history.log`](inelastic-reflection/out/11-plot-history.log)): every history's figures under
  `alpha=<α>/plots/beta=<β>/M=2.4353e+24eV_n=1/` and its `fields.csv` under `alpha=<α>/csv/…`;
  **`dark_radiation.pdf` for each of the four α = 0.1 histories and none at α = 0**; `fields.csv` has
  `rho_chi_over_rho_rad` and `Delta_gstar` (its columns 25 and 26).
- **The unstored α**, `plot_by_beta.py --phi-init-Mp 5 --alpha-values 0.3 --output …/plot-unstored`:
  **exit 2**, with `!! plot: the store holds no alpha_value for --alpha-values 0.3; main.py was not run with
  it, so no history in this store can match. Give the plot the options the build used`
  ([`11-plot-unstored.log`](inelastic-reflection/out/11-plot-unstored.log)); **`plot-unstored` does not exist
  after the run.**
- **The figures, looked at** (PNG; none committed). `…/plot-by-beta/QCD_Cosmology/plots/M=2.4353e+24eV_n=1/
  Delta_N_eff.png`: two panels, ΔN_eff(T_CMB) above and ΔN_eff(1 MeV) below against β, one curve, α = 0.1,
  log scales; the upper panel falls from 8.95 at β = 0.5 to 0.0059 at β = 5.5; the lower has three points,
  β = 0.5's zero not drawn on the log axis. `…/plot-history/QCD_Cosmology/alpha=0.1/plots/beta=1.5/
  M=2.4353e+24eV_n=1/dark_radiation.pdf`: Δg*(N) above and ρ_χ/ρ_R below, log scales, from the first impact
  near N = 19, with the bounce-era spikes, a plateau through the BBN window falling across e± annihilation,
  and the parking (N = 36.34) and release (N = 47.51) marked.
- **The cluster's stop**, after every driver's pid answered `no such process`, every recorder had written
  its exit line and `interrupt_when.py` its `sent` line: Ray state then, six jobs (three builds, three
  plots) `SUCCEEDED`, workers 124 recorded, 0 alive, actors 99, 0 `ALIVE`. `ray_cluster.sh stop`, exit 0:
  `grace period of 15s over; SIGKILL to: 52326 52327`, `no process names /tmp/ray-cpbh; ports 6380 6381
  8267 52367 are free`, `released the rehearsal lock`, `stopped in 19s`. Then `status`, exit 0: `lock: free`,
  no Ray process, the four ports free; `raypoolkit.rehearsal_lock status`, exit 0, `lock: free`. `ray stop`
  was never run.

### 2.7 The suites, the token, the registry, and what was written

- **The suites**, serially, never with the cluster up, ComputeTargets in its two halves
  ([`11-suites.txt`](inelastic-reflection/out/11-suites.txt); `unittest`'s time, the one-minute load before the
  root): CosmologyModels 26 OK (64.4 s; 2.71 / after 64.0 s; 16.13), ComputeTargets `test_[a-f]*` 122 OK
  (131.1 s; 2.23 / 129.9 s; 12.33), `test_[g-z]*` 375 OK (447.0 s; 12.98 / 446.2 s; 15.76), ObjectFactories
  129 OK (78.2 s; 14.00 / 77.9 s; 10.92), tools 123 OK (130.2 s; 13.03 / 129.8 s; 8.33), RunDrivers 68 OK (22.4 s;
  33.91 / 22.4 s; 8.04). Prompt 10's counts exactly, before and after.
- **The token** ([`11-token.txt`](inelastic-reflection/out/11-token.txt)): `grep -c` of the four strings in
  `start.txt`, the three build logs and the four readers' logs, 0 each; every driver attached
  (`Connected to Ray cluster` in each); the token file's mode and mtime as at the Gate; `grep -r -F -l -f
  ~/.ray/auth_token` over every committed file, no output, exit 1.
- **The registry** after: 14 runs, all finished; this prompt's three `done`, `killed`, `done`. The lock free.
- **Written:** `var/datastores/` (the two stores, 17 files and a sidecar each), `var/runs/` (three run
  directories), `var/inelastic-reflection-11/` (the launch logs, pid files, recorders, signaller's log, the
  readers' outputs and figures, the tool and grid outputs, the helpers), the lock under `~/.raypoolkit/`, taken
  and released, and the cluster's temp dir. Nothing under `var/` was deleted; JN 09's store was only read.

---

## 3. The BBN spline across injections (P14, P46)

From each point's `ratio` lines, ρ_NP/ρ_R,J per window [lo, hi) keV, against natural mode at M = 10⁻⁷, the
same β ([`11-grid-tables.md`](inelastic-reflection/out/11-grid-tables.md) §2.9):
- **Below 10 keV every dissipation history is parked**, except β = 1.2 at α = 0.03, which parks at 0.57 eV.
  Its ratio is flat there: rms_step 1×10⁻¹⁴ at M = 4×10⁻³¹ and 5×10⁻¹²–1×10⁻¹⁰ at M = 10⁻¹², against
  1×10⁻⁵–1×10⁻² for natural mode. β = 1.2 at α = 0.03, still injecting, has rms_step 1.02–1.04× natural's in
  the three windows below 10 keV.
- **In the 10–100 keV window, α > 0 raises rms_step where the history injects or parks there**: β = 1.2,
  α = 0.03 (injecting through it) **6.4×** natural's (0.0106 against 0.00165); β = 1.2, α = 0.1 (parks at
  11.6 keV) **8.4×** (0.0139); β = 1.2, α = 0.3 (parks at 44.2 keV) **2.9×** (0.0048); β = 2, α = 0.03 (parks
  at 25.4 keV) **1.26×** (0.0201 against 0.0160). Everywhere else it is below natural's (0.26× to 3×10⁻⁵×),
  the history having parked above 100 keV. The same at both M.
- **So P14's effect is there**, and it is of the size of natural mode's own resolved-bounce steps
  (`[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`, `jordan-normalization` board) and up to 8×
  larger. Whether PRyMordial's spline rings on these steps, and what it does to Yp and D/H, is not measured
  here. Opened as `[11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]` (board §3); not
  fixed (P14).

## 4. The Standard Model exchange (U7)

Quoted as data from the scoping (`prompts/inelastic-reflection/source/scoping_2026-10-27.md` §7, "The
Standard Model exchange"), not re-derived here. Its verdict is "**Not worth implementing.**" Both
exchange operators are gravitational in strength, so Γ/H ~ β² × (couplings) × T/M_P, which the scoping
puts at "at most about 10⁻¹¹ for β ≤ 25 and T ≤ 20 TeV", of order one only for β ≳ (M_P/T)^{1/2}, that is
10⁷–10⁹; hard momenta do not rescue it, and the O₆ operator is smaller by a further β²T²/M_P². The user
ruled it **out of scope** (U7, README §6.1, ruled 2026-10-27; P16): no prompt models it, R11's quanta "[do] not feel the Standard
Model kicking function", and α is the drained fraction alone.

## 5. The commands, verbatim

From the repository root. `<T>` is `var/inelastic-reflection-11/tools/`, `<L>` `var/inelastic-reflection-11/rehearsal`.
JN 09's helpers were copied there: `beat_times.py`, `wait_for_line.py`, `ray_state.py`, `silences.py`
unchanged (`cmp` 0; [`11-*.py.txt`](inelastic-reflection/out/)); `suite.sh` and `check.sh` adapted to
`var/inelastic-reflection-11` and `11-` names, `suite.sh` also recording `uptime` after each root, `check.sh`
reading `<L>` ([`11-suite.sh.diff`](inelastic-reflection/out/11-suite.sh.diff),
[`11-check.sh.diff`](inelastic-reflection/out/11-check.sh.diff)); `compare_tool.py` adapted to α
([`11-compare_tool.py.diff`](inelastic-reflection/out/11-compare_tool.py.diff)). The stored-values reader
is committed as `measure/histories.py` ([`11-histories.py.diff`](inelastic-reflection/out/11-histories.py.diff)
against JN 09's). The committed logs have ANSI colour codes removed (`sed -E 's/\x1b\[[0-9;]*m//g'`); the
`.json` files are copied unchanged.

```bash
# the Gate (11-gate.txt)
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > <L>/gate-status.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > <L>/gate-lock-status.txt 2>&1; echo $?
./venv/bin/python -m runregistrykit list
./venv/bin/python -m runregistrykit store show var/datastores/jordan-normalization-09-reference.db

# the suites, before and after, each root in its own Bash call (the g-z half in the background)
sh <T>/suite.sh before CosmologyModels CosmologyModels/tests
sh <T>/suite.sh before ComputeTargets-a-f ComputeTargets/tests 'test_[a-f]*.py'
sh <T>/suite.sh before ComputeTargets-g-z ComputeTargets/tests 'test_[g-z]*.py'
sh <T>/suite.sh before ObjectFactories ObjectFactories/tests
sh <T>/suite.sh before tools tools/tests
sh <T>/suite.sh before RunDrivers RunDrivers/tests

# the scripts' checks
PYTHONPATH=. ./venv/bin/python -u tools/history_and_bbn.py 2 4e-31 --alpha 0.1
PYTHONPATH=. ./venv/bin/python -u .documents/inelastic-reflection/measure/eps_point.py 1e-6 -- 2 4e-31 --alpha 0.1
PYTHONPATH=. ./venv/bin/python -u .documents/inelastic-reflection/measure/run_grid.py --only dis-M4e-31-b2-a0.1 dis-M1e-12-b2-a0.1   # twice

# the cluster: each alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "inelastic-reflection 11" > <L>/start.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > <L>/status-up.txt 2>&1; echo $?
RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380 > <L>/ray-status.txt 2>&1
RAY_AUTH_MODE=token ./venv/bin/python <T>/ray_state.py

# R1: the load, the build, its recorder, the registration wait, the one check, the end wait
echo "reference-a launch: $(date '+%F %T %z') $(uptime)" | tee -a var/inelastic-reflection-11/11-load.txt
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/inelastic-reflection-11/rehearsal && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign inelastic-reflection --prompt 11 --register reference-a --purpose "inelastic-reflection 11 R1: the reference build (beta 0.5 1.5 3 5.5 at M 1e-3 Mp, phi* 5, alpha 0 and 0.1)" -- --config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16 --job-name inelastic-reflection-11-reference-a --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5 --alpha-values 0 0.1 > $L/reference-a.log 2>&1 & echo $! > $L/reference-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference-a.log' > /dev/null 2>&1 &
export L=$PWD/var/inelastic-reflection-11/rehearsal && nohup ./venv/bin/python -u $PWD/var/inelastic-reflection-11/tools/beat_times.py $L/reference-a.log $L/reference-a.beats > $L/reference-a.beats.err 2>&1 &
./venv/bin/python <T>/wait_for_line.py <L>/reference-a.log "** registered_run: registered as" --timeout 120
sh <T>/check.sh reference-a
./venv/bin/python <T>/wait_for_line.py <L>/reference-a.log "** launcher saw exit status" --timeout 3000   # in the background
./venv/bin/python -m runregistrykit store create var/datastores/inelastic-reflection-11-reference.db --purpose "inelastic-reflection 11 R1: the reference build of jordan-normalization 09's run a at alpha 0 and 0.1, kept for the user"
./venv/bin/python -m runregistrykit store show var/datastores/inelastic-reflection-11-reference.db
./venv/bin/python .documents/inelastic-reflection/measure/histories.py var/datastores/inelastic-reflection-11-reference > <L>/histories-reference.txt

# R2: as R1, slug resumed-a into …-resumed.db (purpose "inelastic-reflection 11 R2: the build into the resumed store, interrupted with SIGTERM in the alpha = 0.1 scalar stage"), and beside it
export L=$PWD/var/inelastic-reflection-11/rehearsal && nohup ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1" --after 28 > $L/resumed-a.interrupt.log 2>&1 &
RAY_AUTH_MODE=token ./venv/bin/python <T>/ray_state.py
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > <L>/status-after-resumed-a.txt 2>&1; echo $?
./venv/bin/python .documents/inelastic-reflection/measure/histories.py var/datastores/inelastic-reflection-11-resumed > <L>/histories-resumed-after-a.txt
./venv/bin/python -m runregistrykit store create var/datastores/inelastic-reflection-11-resumed.db --purpose "inelastic-reflection 11 R2: the reference build's grid built by a resume after SIGTERM in the alpha = 0.1 scalar stage (runs a, a2), kept for the user"
# then slug resumed-a2, the same arguments into …-resumed.db with --prune-unvalidated (purpose "inelastic-reflection 11 R2: the same build again into the resumed store, the resume after SIGTERM")

# R3: each in the foreground, shasum -a 256 var/datastores/inelastic-reflection-11-reference* before and after each
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16 --inventory > $L/inventory.log 2>&1
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16 --phi-init-Mp 5 --alpha-values 0 0.1 --output $L/plot-by-beta --ray-address $ADDRESS > $L/plot-by-beta.log 2>&1
PYTHONPATH=. ./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16 --phi-init-Mp 5 --alpha-values 0 0.1 --output $L/plot-history --ray-address $ADDRESS > $L/plot-history.log 2>&1
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16 --phi-init-Mp 5 --alpha-values 0.3 --output $L/plot-unstored --ray-address $ADDRESS > $L/plot-unstored.log 2>&1
RAY_AUTH_MODE=token ./venv/bin/python <T>/ray_state.py
kill -0 52705; kill -0 53257; kill -0 53954

# the stop: each alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop > <L>/stop.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > <L>/status-after.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > <L>/lock-status-after.txt 2>&1; echo $?

# the eight tool runs, each alone in its Bash call, output to var/inelastic-reflection-11/tool/<beta>-a<alpha>.txt with uptime
PYTHONPATH=. ./venv/bin/python -u tools/history_and_bbn.py <beta> 1e-3 --phi-init-Mp 5                 # beta 0.5 1.5 3 5.5
PYTHONPATH=. ./venv/bin/python -u tools/history_and_bbn.py <beta> 1e-3 --phi-init-Mp 5 --alpha 0.1     # beta 0.5 1.5 3 5.5

# the comparisons
PYTHONPATH=. ./venv/bin/python <T>/compare_tool.py <L>/histories-reference.txt <L>/histories-resumed.txt var/inelastic-reflection-11/tool
./venv/bin/python .documents/inelastic-reflection/measure/compare_jn09.py var/datastores/inelastic-reflection-11-reference var/datastores/jordan-normalization-09-reference
PYTHONPATH=. ./venv/bin/python tools/compare_stores.py var/datastores/inelastic-reflection-11-reference.db var/datastores/inelastic-reflection-11-resumed.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/inelastic-reflection-11-reference.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/inelastic-reflection-11-resumed.db
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py <L>/reference-a.log <L>/resumed-a.log <L>/resumed-a2.log
./venv/bin/python <T>/silences.py <L>/reference-a.beats <L>/resumed-a.beats <L>/resumed-a2.beats

# the SM baseline, then the grid
PYTHONPATH=. ./venv/bin/python -u tools/bbn_from_store.py --sm-baseline --small-network
nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u .documents/inelastic-reflection/measure/run_grid.py > var/inelastic-reflection-11/grid/run_grid.out 2>&1' > /dev/null 2>&1 &
./venv/bin/python <T>/wait_for_line.py var/inelastic-reflection-11/grid/run_grid.out "** grid done" --timeout 3600   # in the background
# the eight epsilon points again, after the fix of §1.1
nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u .documents/inelastic-reflection/measure/run_grid.py --only eps0.0001-M4e-31-b1.2-a0.1 eps0.0001-M4e-31-b2-a0.1 eps0.0001-M4e-31-b3-a0.1 eps0.0001-M4e-31-b5-a0.1 eps1e-08-M4e-31-b1.2-a0.1 eps1e-08-M4e-31-b2-a0.1 eps1e-08-M4e-31-b3-a0.1 eps1e-08-M4e-31-b5-a0.1 > var/inelastic-reflection-11/grid/run_grid-eps-rerun.out 2>&1' > /dev/null 2>&1 &
./venv/bin/python <T>/wait_for_line.py var/inelastic-reflection-11/grid/run_grid-eps-rerun.out "** grid done" --timeout 3600   # in the background
./venv/bin/python .documents/inelastic-reflection/measure/summarise_grid.py

# the suites again, with "after"; then
./venv/bin/python -m runregistrykit list
grep -r -F -l -f ~/.ray/auth_token .documents/inelastic-reflection/ .documents/inelastic-reflection-verification.md prompts/inelastic-reflection/logs/11-measure.md prompts/inelastic-reflection/IMPLEMENTATION_STATE.md .documents/OPEN_ISSUES.md
```

## 6. Observed, not acted on

- **β = 0.5 at M = 10⁻³ M_P, α = 0.1 ends with ΔN_eff today = 8.95.** Its 13 drains are resolved (𝓡 =
  10^−94.5 at the first impact), all late (N ≈ 41, after 1 MeV), and leave ρ_χ/ρ_R = 1.20 today; it never
  parks. Its natural-mode twin's BBN fails on the spline's floor, as in JN 09. Dissipation mode at large M is
  "unofficial" (R17), and nothing here says the value is wrong; it is far outside the grid's range of
  0.003–0.56. Recorded for the user, not opened.
- **The ε patch's first version patched a class** (§1.1): `import ComputeTargets.ScalarHistory as SH`
  binds the class that `ComputeTargets/__init__.py` exports under the module's name. Anything that patches
  a module constant of `ComputeTargets.ScalarHistory` must take the module from `sys.modules`. The tests'
  patches (prompt 06's guard test) are not affected by this prompt; not checked here.
- **Ray's log deduplication drops the dissipation warnings of parallel histories.** In `reference-a` the
  α = 0.1 histories β = 1.5 and 3 printed their three warnings (nearly elastic, birth and reflection
  margins: the tool prints them, `11-tool-1.5-a0.1.txt`, `11-tool-3-a0.1.txt`), but the build's log holds
  them only as β = 5.5's lines with `[repeated 3x across cluster]`. `[09-ray-deduplicates-the-tune-lines-of-
  parallel-histories]` was closed by `tuning-followups` 02 as superseded by quiet tune lines; the warnings
  (R6, P23, R13), printed whatever the quiet rule says, are now what is lost. Opened as
  `[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]` (board §3).
- **`stage_counts.py` reads a stage with nothing to compute as `INCOMPLETE`**: `resumed-a2`'s α = 0 scalar
  stage prints its banner and no `ALL WORK ITEMS COMPLETE` line when every history is stored. The tool
  predates a pipeline per α; not acted on.
- **`silences.py` keys stages on their banner**, so with two α its stage times are the first pipeline's
  only. A helper's limit; not acted on.
- **`Delta_N_eff.pdf`'s lower panel omits a zero** on its log axis (β = 0.5 at 1 MeV) without a mark, and
  shows a single tick label. Cosmetic; not acted on.
- **`plot_by_beta.py` and `plot_ScalarHistory.py` print `!! WARNING: number of shards read from database
  (=16) does not match specified number of shards (=10)`** although given `--shards 16`, as JN 09 §1.13
  recorded.
- **The manifests are `git_dirty`** (§2.1), as the note's order writes the scripts before the cluster.
- **The machine's load** reached 33.9 from macOS's own daemons during the suites; the suites' walls rose
  (ComputeTargets `test_[g-z]` 447 s) and nothing failed.
