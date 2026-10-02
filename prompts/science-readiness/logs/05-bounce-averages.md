# Log 05 — Bounce averages on the dense output

**Prompt:** prompts/science-readiness/05-bounce-averages.md
**Commit:** the commit that adds this file ("Withdraw the bounce averages: BBN's window is not aliased"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `a522005` (prompt 04's tree plus the orchestrator's ruling commit).
`VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION` (`"bf24c3d+ri02+sr01"`) are unchanged.

## What shipped

**No production code, no schema change, no test.** The bounce averages were implemented in full,
measured, and then discarded by the user's ruling of 2026-10-02 (Deviation 1): the premise that
the sub-3-keV ratio noise is aliasing was measured false. This commit carries this log, the
board, the `integrator-remediation` board's Narrowed line and `.documents/OPEN_ISSUES.md` only.

What was built, for a later reader (none of it is in the tree):

- `ComputeTargets/ScalarModel.py`: `SampleValues` gained `H_Jordan_sq_cell_mean` and
  `phi_Einstein_cell_mean` (last). A pure function
  `bounce_cell_means(ts, interpolants, sample_N, evaluate) -> CellMeans(edges, H_Jordan_sq,
  phi_Einstein, evaluations)` built P7's cells (midpoints between samples, the first and last
  edges at `ts[0]`, `ts[-1]`, clipped), walked steps and cells together once in increasing `N`,
  and on each piece of an accepted step inside a cell applied three-point Gauss–Legendre on that
  step's own interpolant (vectorised over the three nodes), calling `evaluate(N, y) -> (H_J²,
  φ_E)`; a non-finite node value or mean raised `ComputationFailureError`.
  `cell_mean_evaluator(policy, hubble)` was the production `evaluate` (ODEPolicy then
  HubblePolicy). `sample_history(result, sample_N, cosmology, policy, coupling)` held the point
  sampling loop verbatim plus the cell means. `ScalarModelValue` gained the two fields.
- `Datastore/SQL/ObjectFactories/ScalarModel.py`: two non-null `Float(64)` columns on
  `ScalarModelValue`, `H_Jordan_sq_cell_mean_Mp2` and `phi_Einstein_cell_mean_Mp`, written and
  read on both build paths.
- `ComputeTargets/BBNData.py`: `H2_Jordan = value.H_Jordan_sq_cell_mean` in place of
  `value.H_Jordan * value.H_Jordan`, with the point `ρ_R,J` and `f_m`.
- `tools/history_and_bbn.py`: an `averages` line (evaluation count, sampling wall time) and the
  ratio windows printed from the cell mean with `| point: …` beside them.
- Tests: `ComputeTargets/tests/test_bounce_averages.py` (8: (a) exact cell means on a synthetic
  piecewise-polynomial `OdeSolution` to 1e-10, interpolants never evaluated outside their step,
  three evaluations per piece, non-finite → `ComputationFailureError` and a failure row; (b) the
  partition; (c) P2 window, φ cell mean within 1e-3 of the point value and within 1e-10 of a
  512-interval Simpson rule; (d) point fields bit-identical to HEAD~1's loop on the P1 window;
  (e) `compute_BBN_data` reads the cell mean) and `Datastore/tests/test_cell_means_round_trip.py`
  (2). All 10 failed on `a522005` (1 failure, 13 errors) and passed on the built tree.

## Deviations from the prompt

### 1. The averaging is withdrawn: its premise was measured false — STRUCTURALLY REQUIRED (by the user's ruling of 2026-10-02)

The prompt assumed (README §6.6) that below 3 keV at small `M` each cell spans "about 20
half-periods", so that the point samples catch the bounces at random phase and a cell mean
removes the noise. Measured, in PRyMordial's window the z grid **resolves** the bounces: most
cells hold none, and the ratio is a resolved sawtooth. The user ruled (relayed by the
orchestrator, 2026-10-02): withdraw the averaging; in PRyMordial's window the point samples are
the behaviour of H on the solution, and a PRyMordial failure on that input is a finding about
PRyMordial's limits, not a reason to change the input. Prompt 05 lands as a measurement-only
commit. The measurements behind it, all on the built tree (trajectories bit-identical to
`a522005`) unless stated:

**The breakage witness** (`tools/history_and_bbn.py β M`, full network; "before" on a clean
`git archive a522005` tree, "after" on the built tree, run back to back):

| history | window | rms step, point (before) | rms step, cell mean (after) | after / before | target |
|---|---|---|---|---|---|
| β = 1.6, M = 1e-5 | [0.3, 1) keV | 1.253e-3 | 9.906e-4 | 0.79 | ≤ 0.1 |
| β = 1.6, M = 1e-5 | [1, 3) keV | 7.573e-4 | 5.772e-4 | 0.76 | ≤ 0.1 |
| β = 2, M = 1e-3 | [0.3, 1) keV | 2.129e-3 | 1.782e-3 | 0.84 | ≤ 0.5 |
| β = 2, M = 1e-3 | [1, 3) keV | 1.586e-3 | 1.206e-3 | 0.76 | ≤ 0.5 |
| β = 2, M = 1e-5 | [0.3, 1) keV | 2.238e-3 | 1.767e-3 | 0.79 | — |
| β = 2, M = 1e-5 | [1, 3) keV | 1.652e-3 | 1.165e-3 | 0.71 | — |

This is prompt 05 §5's first stop condition, which was raised with the user.

**Why** (scratch `diag.py`, built tree, β = 1.6, M = 1e-5, which recomputes each cell's H_J² by a
4000-point Simpson rule on the `OdeSolution`; it agreed with the Gauss–Legendre cell means to
5.6e-12 and 2.3e-12):

- Only **6 of 130** cells in [0.3, 1) keV and **2 of 120** in [1, 3) keV contain a sign change of
  π; the median per cell is 0 sign changes and 0 accepted-step boundaries. At β = 2, M = 1e-3 the
  maximum is 1 per cell in both windows.
- The ratio is a **resolved sawtooth**: it jumps by about +0.005 to +0.007 at a bounce, then
  falls smoothly by about 1.2e-4 per sample. The **10 largest steps carry 95 %** ([0.3, 1) keV)
  and **99 %** ([1, 3) keV) of the rms². A cell mean can only split each jump between two
  samples, which is the ~20 % fall above.

**The β = 2, M = 1e-5 PRyMordial failure on averaged input.** With the cell means BBN failed:
`PRyMordial: PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)':
status=-1, message='Required step size is less than spacing between numbers.'; t reached
1.28875e+06 of target 1.31741e+06` (the source's failure mode at β = 1.6). Attribution on the
same history (scratch `attrib.py`: one history, then `compute_BBN_data._function` on the stand-in
with `H_Jordan_sq_cell_mean` replaced as stated; full network):

| BBN input H_J² | Yp | D/H ×10⁵ |
|---|---|---|
| point everywhere | 0.2467016048 | 2.46477019 (= `a522005`) |
| cell mean everywhere | **FAILURE** (above) | — |
| cell mean below 3 keV, point above | 0.2467016048 | 2.464770225 |
| cell mean below 10 keV, point above | 0.2467016048 | 2.464770694 |
| cell mean above 10 keV, point below | 0.2467031415 | 2.466775599 |
| point × sinh(4h)/(4h) everywhere | 0.2466980772 | 2.466060693 |
| cell mean ÷ sinh(4h)/(4h) everywhere | 0.2466837205 | 2.463106813 |

The cell mean below 3 keV moves D/H by 1.4e-8; the failure needs the combination.

β = 1.6, M = 1e-5 completed on averaged input: Yp 0.2469032243, D/H 2.464191437 (−2.35e-4 against
2.4647705).

**The β = 2, M = 0.5 shift** (target ≤ 3e-4): D/H 2.560889654 → 2.564920856 (**1.57e-3**), Yp
0.249229266 → 0.249257434 (1.13e-4). Attribution (`attrib.py 2 0.5`): cell mean below 3 keV only,
D/H 2.560889657 (1e-9); above 10 keV only, 2.56491984; point × sinh(4h)/(4h), 2.564724333; cell
mean ÷ sinh(4h)/(4h), 2.561935873. The cause is mostly the **curvature bias** of an N-average of
H_J² ∝ e^{−4N} over a cell of half-width h = ΔN/2 = 0.0046 with point ρ_R,J and f_m:
sinh(4h)/(4h) − 1 = **5.65e-5** in H², +5.9e-5 in the ratio (visible as a uniform +6e-5 in every
M = 0.5 window). PRyMordial's response to H² at this level is not smooth: scaling the point H² by
1 + ε moves D/H by −2.0e-4 (ε = 1e-6), +3.0e-4 (1e-5), +1.1e-4 (−5.65e-5), +1.95e-3 (1e-4) and
+3.06e-3 (1e-3).

**The cost.** β = 2, M = 1e-5: history wall 45.7 s against 44.6 s on `a522005`, back to back:
**1.02×**. The averaging made **504 336** policy evaluations (β = 1.6, M = 1e-5: 427 065;
β = 2, M = 1e-3: 100 239; M = 0.5: 29 580) and the sampling took 4.5 s outside the integration.
The machine carried other users' load throughout (load average 10–30 on 10 cores), so the
absolute times are inflated; the two runs were adjacent.

**`N` against time** (acceptance 5; scratch `n_vs_t.py`, β = 2, M = 1e-5, the 250 samples with
T_J in [0.3, 3) keV, ⟨H²⟩_t = ⟨H_J⟩_N / ⟨1/H_J⟩_N on the same cells and interpolants):
mean |⟨H²⟩_N − ⟨H²⟩_t| / ⟨H²⟩_N = **5.669e-5** (median 5.709e-5, max 5.856e-5).

**Trajectories and point values were bit-identical.** RHS, accepted steps and first bounce `N`
on all four histories equalled `a522005`'s (Verification); test (d) showed every point field
bit-identical to HEAD~1's sampling loop; and point input reproduced `a522005`'s abundances
exactly (`attrib.py`, "point" rows).

### 2. The other choices of the built implementation — moot

Listed for the record; none is in the tree. `sample_history` factored the sampling loop out of
`compute_scalar_model` (IMPLEMENTATION CHOICE); an unstored `cell_mean_evaluations` payload key
for the driver (IMPLEMENTATION CHOICE); the column names with unit suffixes `_Mp2`, `_Mp`
(IMPLEMENTATION CHOICE); the `compute_BBN_data` docstring edited with the `density_NP` line
(IMPLEMENTATION CHOICE); `test_bbn_callbacks (g)`'s stand-in given the cell-mean field
(STRUCTURALLY REQUIRED); CosmologyModels could not "rise" under acceptance 6, its tests being
outside the allowed files (STRUCTURALLY REQUIRED; moot, the ruling removed "rise"); test (c)'s
parked P2 window took 0.02 s rather than "about two seconds" and held no bounce, so a Simpson
check was added to make it discriminating (IMPLEMENTATION CHOICE).

## Verification performed

**Suites** (`PYTHONPATH=. ./venv/bin/python -m unittest discover -s <pkg>/tests -t .`):

- before, on a clean `git archive a522005` tree: CosmologyModels 18, ComputeTargets 86, Datastore
  26, all OK;
- on the built tree: 18 OK; 94 (one error, `test_bbn_callbacks (g)`, its stand-in lacking the
  field; it passed alone after the fix); 28 OK;
- after the ruling, on the restored tree (this commit): CosmologyModels 18 (80 s), ComputeTargets
  86 (95 s), Datastore 26 (3 s), all OK.

**The driver on both trees** (`./venv/bin/python tools/history_and_bbn.py β M`, full network,
from the repository root; "before" from a clean `git archive a522005` extraction with the same
venv; run alternately, one at a time, loaded as above). The history, bounce and BBN lines:

| history | RHS / accepted steps (both) | first bounce N (both) | wall before / after | Yp, D/H×10⁵ before (point) | after (cell mean) |
|---|---|---|---|---|---|
| β = 2, M = 0.5 | 40 580 / 4 469 | 20.343026853 | 1.5 / 1.8 s | 0.249229266, 2.560889654 | 0.249257434, 2.564920856 |
| β = 2, M = 1e-3 | 271 783 / 27 979 | 20.352082230 | 7.5 / 8.4 s | 0.2467606164, 2.463862263 | 0.2467037604, 2.46355006 |
| β = 2, M = 1e-5 | 1 679 987 / 162 676 | 20.352100202 | 44.6 / 45.7 s | 0.2467016048, 2.46477019 | FAILURE (Deviation 1) |
| β = 1.6, M = 1e-5 | 1 445 132 / 137 137 | 18.974433718 | 36.6 / 39.5 s | 0.2468788501, 2.4647705 | 0.2469032243, 2.464191437 |

The "before" lines equal the orchestrator's figures for `a522005` in every RHS, step, bounce,
ratio and abundance digit. The point-ratio windows printed by the built driver equalled the
"before" ratio windows to every printed digit on all four histories.

**The orchestrator's measurement** (provenance as given by the orchestrator: scratch script
`orch_bounce_density.py` in the session scratchpad, run on `a522005` plus the uncommitted built
tree, trajectories bit-identical to `a522005`; it counts sign changes of π between accepted
steps, per sample cell, and reproduces verification §4.9's 43–45 half-periods in 10 MeV–1 keV at
β = 2). Median half-periods per cell / fraction of cells with at least one:

| T_J window | β = 1.6, M = 1e-5 | β = 2, M = 1e-5 | β = 2, M = 0.5 |
|---|---|---|---|
| 10–100 MeV | 0 / 0.01 | 0 / 0.03 | 0 / 0.03 |
| 1–10 MeV | 0 / 0.01 | 0 / 0.00 | 0 / 0.00 |
| 100 keV–1 MeV | 0 / 0.21 | 0 / 0.11 | 0 / 0.10 |
| 10–100 keV | 0 / 0.06 | 0 / 0.04 | 0 / 0.02 |
| 3–10 keV | 0 / 0.01 | 0 / 0.01 | 0 / 0.01 |
| 1–3 keV | 0 / 0.02 | 0 / 0.01 | 0 / 0.01 |
| 0.3–1 keV | 0 / 0.05 | 0 / 0.03 | 0 / 0.02 |
| 100–300 eV | 0 / 0.17 | 0 / 0.12 | 0 / 0.00 |
| 10–100 eV | 1 / 0.75 | 1 / 0.64 | 0 / 0.00 |
| 1–10 eV | 7 / 1.00 | 5 / 1.00 | 0 / 0.00 |
| 0.1–1 eV | 16 / 1.00 | 17 / 1.00 | 0 / 0.00 |

The median |⟨H²⟩_cell/H²_point − 1| is 5.6–5.9e-5 in the windows 1–10 MeV, 10–100 keV,
3–10 keV, 1–3 keV, 0.3–1 keV and 100–300 eV, on all three histories. Elsewhere: 100 keV–1 MeV,
9.3e-5 (β = 1.6) and 1.2e-4 (β = 2, both M); 10–100 MeV, 8.2e-5 (β = 1.6) and 1.1e-4 (β = 2);
10–100 eV, 8.2e-5 (both M = 1e-5 histories). Below 10 eV, at M = 1e-5, it is 6e-4 to 4e-3, where
the bounces are aliased. "About 20 half-periods per cell" (README §6.6) came from §4.9's median
over 1 keV–0.1 eV, which its cold end dominates.

My own count (`diag.py`) agrees: 6/130 and 2/120 cells with a sign change in [0.3, 1) and
[1, 3) keV at β = 1.6, M = 1e-5, i.e. 0.05 and 0.02.

**`black`**: no file under `black`'s remit is changed by this commit (the built files were
`black --check` clean before they were discarded).

## Observations not acted on

- **The cubic spline through the ratio's resolved bounce jumps may overshoot or ring.** Each jump
  (+0.005 to +0.007 at β = 1.6, M = 1e-5) falls between two samples 0.0092 e-folds apart; the
  spline's excursion between samples was not measured. Opened as
  `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]` (board §3).
- **`ScalarModelValue_factory.build` compares the stored φ against π.**
  `Datastore/SQL/ObjectFactories/ScalarModel.py:1008` on `a522005`,
  `fabs(row_data.phi_Einstein_Mp - pi_Einstein_Mp)`, under a message about π. Opened as
  `[05-the-value-factory-compares-stored-phi-against-pi]` (board §3).
- The +5.65e-5 curvature bias of any N-average of H² taken against point ρ_R,J is recorded in
  Deviation 1; with the averaging withdrawn it has no consumer and no issue is opened.

## State handed to the next prompt

- **No change to the tree from prompt 05**: no new fields, columns or functions. BBN reads the
  point `H_Jordan` as on `a522005`; `ScalarModelValue` and the `ScalarModelValue` table are
  unchanged; prompt 06's before/after compares point-input BBN, and its reference tree is
  `a522005`'s code.
- **The driver's output** (`./venv/bin/python tools/history_and_bbn.py β M`, full network, on
  `a522005`'s code, which this commit leaves unchanged; loaded machine):
  ```
  history beta=2 M=0.5: RHS=40580 accepted_steps=4469 reflections=0 samples=5392 wall=1.5 s
  bounce beta=2 M=0.5: N=20.343026853 T_J=746.634744 MeV phi=4.573705e-03 reflected=False
  ratio beta=2 M=0.5 [0.3,1) keV: n=130 min=0.05184 median=0.05748 max=0.06183 rms_step=0.0002526
  ratio beta=2 M=0.5 [1,3) keV: n=120 min=0.04373 median=0.04666 max=0.05377 rms_step=0.0001986
  ratio beta=2 M=0.5 [3,10) keV: n=130 min=0.05003 median=0.05301 max=0.05466 rms_step=3.784e-05
  ratio beta=2 M=0.5 [10,100) keV: n=256 min=-0.0284 median=0.05571 max=0.11 rms_step=0.006658
  bbn beta=2 M=0.5: Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487
  history beta=2 M=0.001: RHS=271783 accepted_steps=27979 reflections=0 samples=5435 wall=7.5 s
  bounce beta=2 M=0.001: N=20.352082230 T_J=746.686275 MeV phi=9.150507e-06 reflected=False
  ratio beta=2 M=0.001 [0.3,1) keV: n=131 min=-0.008412 median=0.001347 max=0.008622 rms_step=0.002129
  ratio beta=2 M=0.001 [1,3) keV: n=120 min=-0.008535 median=-0.001016 max=0.008724 rms_step=0.001586
  ratio beta=2 M=0.001 [3,10) keV: n=130 min=0.003037 median=0.005897 max=0.007478 rms_step=3.634e-05
  ratio beta=2 M=0.001 [10,100) keV: n=257 min=-0.06806 median=0.008126 max=0.08414 rms_step=0.01604
  bbn beta=2 M=0.001: Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365
  history beta=2 M=1e-05: RHS=1679987 accepted_steps=162676 reflections=0 samples=5437 wall=44.6 s
  bounce beta=2 M=1e-05: N=20.352100202 T_J=746.686377 MeV phi=9.150513e-08 reflected=False
  ratio beta=2 M=1e-05 [0.3,1) keV: n=130 min=-0.008945 median=0.001831 max=0.009007 rms_step=0.002238
  ratio beta=2 M=1e-05 [1,3) keV: n=120 min=-0.009047 median=-0.001186 max=0.008933 rms_step=0.001652
  ratio beta=2 M=1e-05 [3,10) keV: n=131 min=0.003273 median=0.006159 max=0.007748 rms_step=3.637e-05
  ratio beta=2 M=1e-05 [10,100) keV: n=257 min=-0.07367 median=0.008392 max=0.07816 rms_step=0.01604
  bbn beta=2 M=1e-05: Yp=0.2467016048 DoH=2.46477019 He3oH=1.042619141 Li7oH=5.407992384
  history beta=1.6 M=1e-05: RHS=1445132 accepted_steps=137137 reflections=0 samples=5219 wall=36.6 s
  bounce beta=1.6 M=1e-05: N=18.974433718 T_J=420.758153 MeV phi=9.316270e-08 reflected=False
  ratio beta=1.6 M=1e-05 [0.3,1) keV: n=130 min=-0.00409 median=-0.0004114 max=0.004121 rms_step=0.001253
  ratio beta=1.6 M=1e-05 [1,3) keV: n=120 min=-0.004084 median=-0.0005007 max=0.00414 rms_step=0.0007573
  ratio beta=1.6 M=1e-05 [3,10) keV: n=131 min=0.0003462 median=0.002184 max=0.003195 rms_step=2.316e-05
  ratio beta=1.6 M=1e-05 [10,100) keV: n=257 min=-0.02773 median=0.00377 max=0.02713 rms_step=0.007134
  bbn beta=1.6 M=1e-05: Yp=0.2468788501 DoH=2.4647705 He3oH=1.042121506 Li7oH=5.419865323
  ```
  (each `bbn` line also prints `network=full PRyM_time=… wall=… PRyM_version=bf24c3d+ri02+sr01`;
  omitted.) **Point input completes BBN on all four histories.**
- The averaged-input figures, for comparison only, are in Deviation 1.
- **Suite counts after this prompt** (restored tree, the three commands of README §5 rule 6):
  CosmologyModels 18, ComputeTargets 86, Datastore 26, all OK — unchanged from `a522005`.
