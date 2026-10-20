# Campaign — jordan-normalization

**Written:** 2026-10-20 on branch `jordan-normalization`, cut from `main` at `ac187c8`, by Claude
Opus 5.5. Planned at the user's request of the same day, from the brief of 2026-10-06 and an
evaluation that the user and the planner worked through together on 2026-10-20. The user's rulings
of that conversation are R1–R14 (§6.0). **Ruled by the user on 2026-10-20: U1–U3 and P1–P20 as proposed; U4 and U5
await the user's answer to a question each** (§6.1). No prompt is written yet (§2.1).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- the brief, [`source/brief_jordan_normalization.md`](source/brief_jordan_normalization.md),
  written 2026-10-06 at `9b3db51` and committed at `c21cdfb`, with its probe
  [`source/phiref_iter.py`](source/phiref_iter.py). Its §4 rulings U1–U5 are carried here as R1–R5;
  everything else in it is a recommendation that this plan checks;
- the planner's probes of 2026-10-20 at `ac187c8`,
  [`source/planning-probes-2026-10-20/`](source/planning-probes-2026-10-20/README.md), whose
  results are §0.3;
- the open issue left for this campaign's planner, `[05-surfing-scan-runs-at-a-fixed-lambda]`
  (`surfing-temperature` board §3, U9), and the other open issues in code this campaign rewrites
  (§6.1, U3);
- the handovers in `.documents/review-remediation-verification.md` §4.12–§4.15, which hold the
  science rebuild until this campaign lands and list what in the run scripts changes;
- `rrk-adoption`'s README P4 and board §3, on the heartbeat of runs whose items take longer than
  the 900 s staleness window.

## 0. Why this campaign exists

### 0.1 The problem

The model is Einstein-frame gravity with reduced Planck mass M_P, a canonical scalar φ, and matter
coupled to the Jordan metric g_J = A²(φ) g_E with A = exp(βφ/M_P). The Standard Model masses are
constants of the Jordan-frame Lagrangian, so the Planck mass that matter sees is M_J(φ) = M_P/A(φ).
A laboratory measures G_N at some field value φ_ref, so the physical requirement is
M_P = A(φ_ref) M_lab. ChamPBH identifies M_P with M_lab (`Units/Planck_units.py`) and has A(0) = 1
(`ExponentialCoupling`). Together these silently choose φ_ref = 0, which at the simulated M is far
from where the field is today. Every Jordan-frame input (the SM masses through g_*(T), T_init,
T_CMB, H0, ρ_m0, ρ_r0) is then mislabelled by A(φ_ref) to the power of its mass dimension.

The user has ruled (R2, R6–R8) that the fix is built into the model, not applied afterwards:
A(φ) = exp(β(φ − φ_ref)/M_P), so that A(φ_ref) = 1 and M_P = M_lab exactly. φ_ref and the
normalization of the potential are found for each history by integrating it to T_J = T_CMB and
tuning both until φ_ref = φ_min(T_CMB) and H_J(T_CMB) = H0. At β = 1.5, M = 10⁻³ M_P this reverses
the sign of the BBN signal (the brief's §2.3: Yp −3.6 % instead of ≈ 0), and it sets the late-time
history used for the dark-energy figures.

While the model's interface is rebuilt, three more things change, by the user's rulings:
- the potential and the coupling become one `AbstractModel`, so that the history is tied to no
  model's parameters (R6, R7);
- `ScalarModel` is renamed `ScalarHistory`, keeping "model" for a Lagrangian (R9);
- the sampling axis stores ln(1 + z) instead of z, which overflows past 709.8 e-folds (R12).

### 0.2 The brief, checked at `ac187c8`

- **Where A enters.** The brief's four sites are where it said, all in
  `ComputeTargets/ScalarModel.py`: `HubblePolicy` (`:384`, H_J = H_E(1 + βφ′/M_P)/A),
  `fixed_T_value_at` (`:1074`), the initial Einstein-frame radiation density (`:1164`) and the
  sampled ρ_R,J (`:1280`). Everything else reads only d ln A/dφ, which φ_ref does not change: the
  ODE, `AdiabaticHistory`'s M²_eff, the surfing turning points, the reachability roots, BBN.
- **A fifth use, outside `ScalarModel`.** `pipeline_selection.super_planckian_couplings`
  (`pipeline_selection.py:197`, `:311`) evaluates `log_Omega(φ*)` to warn about a super-Planckian
  start. It only warns, but it runs before any history exists, so before φ_ref is known (P11).
- **The units (§2.4).** Confirmed. `Planck_units` sets eV = M_P/2.436e27, and its kelvin, from the
  Planck temperature, then implies 1 K = 8.61973e-5 eV against k_B = 8.617333e-5 eV/K: 2.78e-4. With
  PRyMordial's G_N = 6.70883e-45 MeV⁻² (reduced M_P = 2.4353236e27 eV), 1 K agrees with k_B to
  1.7e-7, ħc to 4e-8, ħ to 1.6e-7 and the kilogram to 2.1e-7. **The brief missed** that `Mpc_units`
  (`:43`) and `GeV_units` (`:35`) hard-code the same 2.436e27, and that three test files pin
  strings that carry it (`2.436e+24 eV`: `tools/tests/test_inventory_report.py`,
  `tools/tests/test_compare_stores.py`, `ObjectFactories/tests/test_store_fingerprint_golden.py`).
- **The brief's script** samples on a z grid to 1 + z = 10³⁵, which fails at β = 25: that history
  needs 165 Einstein e-folds. Production's grid (`exponential.yaml`, 10⁸⁵) suffices.
- **What the brief predates.** Four campaigns landed after it (`dk-and-rpk-migration`,
  `read-only-plots`, `surfing-temperature`, `rrk-adoption`). For this campaign that means:
  - the factories are `ObjectFactories/` on `datastorekit` v0.4.0, and every change to an
    `inventory_spec()` or to the registry bumps `config/fingerprint.py`'s `FINGERPRINT_FORMAT`,
    with a new `SPEC_DIGESTS` pin and a new golden file (P2);
  - long runs go through `RunDrivers/registered_run.py`, whose heartbeat is read off stdout, and
    which reads `--potential-type` and the Λ options for its `scope` (`:678`, `:699-703`);
  - the surfing tools (`tools/surfing_scan.py`, `tools/plot_surfing_*.py`) run at a fixed
    Λ = 1 meV with no φ_ref, and stop at T_stop = 1 MeV by default (`surfing_scan.py:119`).

### 0.3 Measured, 2026-10-20, read-only

All with the scripts in `source/planning-probes-2026-10-20/`, on `ac187c8`'s code with a subclass
that subtracts φ_ref in `log_Omega`, φ* as given, π* = 0, T* = 2×10⁴ GeV, tolerance 10⁻⁵ in both
residuals, at a machine load of 6–12. No store was opened.

**1. The ruled fixed point converges on the production grid.** φ_ref ← φ_min of the history's own
state today, Λ ← Λ (V_target/V(φ₀))^¼ with V_target from Eq. 7.

| β | M/M_P | φ* | integrations | tuned Λ (meV) | φ_ref/M_P | φ_min/φ₀ | tracking φ₀′/(1.5φ₀) | time |
|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | 7 | 2.125913 | 0.0349624 | 1.046 | 0.878 | 38 s |
| 25 | 1e-3 | 5 | 7 | 1.843465 | 0.0068090 | 1.000 | 0.862 | 686 s |
| 5.5 | 1e-3 | 5 | 7 | 2.021177 | 0.0167195 | 1.006 | 0.917 | 145 s |
| 3 | 1e-3 | 5 | 7 | 2.074576 | 0.0236629 | 1.016 | 0.913 | 67 s |
| 1.1 | 1e-3 | 2 | 7 | 2.145591 | 0.0415339 | 1.069 | 0.851 | 36 s |
| 0.5 | 1e-3 | 1 | 7 | 2.185554 | 0.0638212 | 1.174 | 0.755 | 20 s |
| 3 | 1e-5 | 5 | 6 | 2.227091 | 0.0026705 | 1.003 | 0.988 | 289 s |
| 1 | 1e-5 | 5 | 6 | 2.237209 | 0.0046642 | 1.019 | 0.888 | 26 s |
| 3 | 0.01 | 5 | 8 | 1.831913 | 0.0622282 | 1.025 | 0.811 | 35 s |
| 1 | 0.03 | 5 | 8 | 1.881702 | 0.2028458 | — | — | 11 s |
| 3 | 0.1 | 5 | 13 | 1.389305 | 0.1477194 | 1.023 | 0.613 | 38 s |
| 1 | 0.5 | 5 | 19 | 1.332621 | 0.6276160 | 1.162 | 0.463 | 15 s |
| 3 | 0.5 | 5 | 15 | 0.937734 | 0.2708441 | 1.015 | 0.416 | 32 s |

- The brief's point reproduces: Λ = 2.1259 meV against its 2.1272 (it used φ_ref = φ₀), and
  φ_min/φ₀ = 1.046 against its 1.0456.
- **The V_target ≤ 0 stop is an artefact of the update order** at M ≥ 0.1. With φ_ref = 0 the first
  history has A(φ₀) = 1.6–2.3, so Eq. 7's V_target is negative before φ_ref has moved. Updating
  φ_ref first (and halving Λ) cures it at every point tried. No point has a V_target ≤ 0 that
  persists.
- **The plain update converges linearly**: a factor 5–10 per iteration at M ≤ 10⁻², but only
  about 1.8 at M = 0.5, where the field responds most strongly to its normalization (R10's secant
  is for this).
- **Across the capture window** β = 2.995–3.020 at M = 10⁻³, φ* = 5, the map is smooth: Λ falls
  linearly from 2.074711 to 2.074035 meV and every point converges in 7 integrations.

**2. The dependence on the initial field.** The tuned values at fixed (β, M), π* = 0:

| β, M/M_P | surfs | Λ: φ* = 5 against φ* = 1 | Λ: φ* = 2 against φ* = 1 |
|---|---|---|---|
| 1.5, 1e-3; 3, 0.1 | yes | < 10⁻⁶ | < 10⁻⁶ |
| 3, 1e-3 | yes | φ* = 5 against φ* = 2: < 10⁻⁶ | — |
| 0.8, 1e-3 | no | +1.1e-3 | < 10⁻⁶ |
| 0.5, 1e-3 | no | +3.4e-3 | +6.5e-5 |
| 0.3, 1e-3 | no | +1.6e-2 (φ_ref +2.2 %, 2βΔφ_ref/M_P = 1.1e-3) | +5.7e-4 |
| 1, 0.01 | — | +1.8e-4 | < 10⁻⁶ |
| 1, 0.1; 1, 0.5 | — | −4e-5; −4e-4 | ≈ 10⁻⁶ |
| 1, 1e-5 | — | +4e-4 | +4e-6 |

Surfing forces every φ* onto one trajectory, and the tuned values then agree to the tolerance.
Without it, memory is erased only by the late relaxation onto the matter-era tracker, and survives
where that has not finished by T_CMB: small β, large φ* (at β = 0.3, φ* = 5, 1 + βπ₀ = 1.0025
against 1.020 at φ* = 1). The non-surfing block of the 2026.6.0 run (`C5`, β 0.5–1.1 at φ* = 1,
2, 5) lies where it is largest. These are measurements, not defects (R13 follows from them).

**3. The analytic minimum** (the user's question of 2026-10-20). With A(φ_ref) = 1 and Planck2018's
ρ_m0, V′(φ) + (β/M_P) ρ_m0 = 0 locates the minimum:
- **its location, given the right amplitude, is accurate**: at the converged Λ, 2βΔφ/M_P is
  ≤ 1e-6 at M = 10⁻⁵, ≤ 4.5e-4 at M = 10⁻³, 2.4e-3 at 10⁻², 1e-2 at 0.1, 1.5e-2–0.11 at 0.5;
- **the amplitude is not**: V(φ_ref)/ρ_Λ at the converged history is 0.98 (M = 10⁻⁵, β = 1),
  0.82 (β = 1.5, M = 10⁻³), 0.52 (β = 25), 0.19–0.29 (M ≥ 0.1), because H_J carries the field's
  kinetic energy and the factor (1 + βφ′)²;
- a **static seed** (field at rest, V = 3M_P²H0² − ρ_m0 − ρ_r0) puts φ_ref 1–2 % high at
  M = 10⁻⁵, 4–38 % at M = 10⁻³ (rising with β) and 27–41 % at M = 0.01–0.1; one history at it
  misses H0 by +3.5 % to +30 % at M = 10⁻³ (β 0.5–25), and by +68 % and +108 % at M = 0.5. In BBN (small network) it shifts Yp by −0.42 % and D/H by −1.05 %
  at β = 1.5 (the signal itself is Yp −3.75 %), and −0.77 % and −2.07 % at β = 3;
- a **tracking seed** (φ′ = 1.5φ in Eq. 7 at A = 1) puts φ_ref within 1.4–3.8 % at M = 10⁻³ (2βΔφ
  ≈ 2e-3 for β ≤ 5.5, 1.3e-2 at β = 25) and BBN within +0.06–0.11 % in Yp and +0.18–0.20 % in D/H.
  It has no solution at M ≥ 0.1;
- **as a seed** it saves 0–3 integrations (static 5–8, tracking 3–7 at M ≤ 10⁻², against the plain
  start's 6–8). The analytic route therefore does not pin M_P without the dynamics, except at
  M ≲ 10⁻⁵, and the tuning to H0 stands (R1).

**4. The redshift label.** `compute_scalar_model` builds `largest_z = exp(final_N) - 1.0`
(`ComputeTargets/ScalarModel.py:1250`), which overflows at N > 709.8, and the grid is a `logspace`
in 1 + z (`main.py:1062-1073`) with the same ceiling; then it takes `N_backward = log(1.0 + z.z)`
(`:1271`). z itself is used as the join key of the three value tables (`z_serial`), a sort key, and
one plotting axis (`extract_common.py:342`, `:407`); nothing in the physics reads it. The redshift
lookup matches by relative difference (`ObjectFactories/redshift.py`), the code of
`[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`.

**5. What the strip removes** (R11). `InversePower`, `Starobinsky`, `Recliner` and `Reflecting`
appear in 30 files outside `prompts/`, `science-runs/` and `.documents/`, among them
`config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, `config/argument_parser.py`,
`main.py`, `plot_inputs.py`, `ObjectFactories/ScalarModel.py`'s spec, four factories, four classes,
and seven test modules. `starobinsky.yaml` and `recliner.yaml` go with them. `ReclinerPotential.log_V`
multiplies ln Λ⁴ where it should add (`CosmologyConcepts/Potentials/ReclinerPotential.py:121`, since
`a38b4eb`); it is deleted with the class, so no issue is opened.

**6. What the rename touches** (R9). `ScalarModel` occurs 478 times in 62 Python files outside
`prompts/` and `science-runs/`, and in 12 documents. It names the tables `ScalarModel`,
`ScalarModel_tags` and `ScalarModelValue`, the `--drop` group `scalar-model`, the proxies, and the
`Parent` references in the specs of `AdiabaticHistory` and `BBNData`.

**7. Cost.** The 2026.6.0 science store holds 710 histories, 1.4–263 s each (median 22 s). Tuned,
each costs 5–19 integrations to T_CMB: about 30 CPU-hours for the scalar stage at the median, with
the slowest single histories (M = 10⁻⁵, large β) at 5–30 min. Under the plain update a history at
β = 25, M = 10⁻³ took 11 min at load 6–12.

### 0.4 Baselines

Measured at `ac187c8` with `CLAUDE.md`'s commands: CosmologyModels **18**, ComputeTargets **176**
(61 + 115), ObjectFactories **78**, tools **84**, RunDrivers **61**, all passing.

## 1. Scope

**In scope:**
- the strip to the exponential model (01) and the rename to `ScalarHistory` (02);
- the sampling axis in ln(1 + z), and the Jordan-frame redshift on every sample (03);
- `AbstractModel` and `ExponentialModel`, holding V and A with a model-interpreted normalization and
  φ_ref (04);
- the Planck mass from PRyMordial's G_N, in all three unit systems (05);
- the tuning layer over a primitive integrator: the secant, the seed, the residuals, the failure
  reasons and the progress lines (06);
- the store: the model's table, the history's key and its tuned outputs, the factories, the
  fingerprint and `VERSION_LABEL`, with every writer and reader moved to them (07);
- `main.py`'s options, the warnings and the tools that run histories, the surfing tools with the
  tuned normalization among them (08);
- a registered pilot build of tuned histories on the isolated cluster (09, if U2);
- the documents, `CLAUDE.md`, the handover for the science rebuild, and the close (10).

**Out of scope:**
- **The science rebuild** (R5): the user runs it from 10's handover.
- **Fitting θ\* or the deceleration parameter** (R1): q₀ is an output, tested later through the
  reconstructed equation of state of dark energy.
- **Any model other than the exponential–exponential one** (R11). A later campaign brings back
  any of the four as an `AbstractModel` class.
- **The BBN interface** (`compute_BBN_data`, the vendored `PRyM/`), which needs no change:
  with A normalized at φ_ref, Eq. 7's ratio is the one PRyMordial needs. `PRYM_VERSION` does not
  move.
- **A post-hoc φ_ref rescaling** of stored histories, anywhere (`tools/bbn_from_store.py`
  included): rejected in the brief (§1, §2.3).
- **Existing stores.** None is read or adopted; every store is regenerated (`rrk-adoption` R5).
- **Storing per-history surfing values** (the `surfing-temperature` handover's point 4): a later
  campaign's columns.
- **DatastoreKit, RayPoolKit, RunRegistryKit**: read, never edited. A defect there stops the
  prompt and is reported upstream (`CLAUDE.md`, "The packages").

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-strip-to-the-exponential-model.md` | The four other potentials, their factories, registry, sharding and spec entries, `--potential-type`'s choices, two YAML configs and their tests removed; a golden short history pinned; values bitwise unchanged; fingerprint format 2 | Sonnet |
| 02 | `02-rename-to-scalarhistory.md` | `ScalarModel` → `ScalarHistory` in code, tables, the drop group, proxies, specs and current documents; values bitwise unchanged; format 3 | Sonnet |
| 03 | `03-the-sampling-axis.md` | `redshift` stores ln(1 + z_E); the grid as exact multiples; absolute-tolerance lookup; ln(1 + z_J) on every sample; the readers' z; the change in values measured; format 4 | Opus |
| 04 | `04-the-model-interface.md` | `AbstractModel`, `ExponentialModel` (M, n, β), `realize(normalization, φ_ref)`, `renormalize`, `effective_minimum`, `seed`; the integrator and every consumer on it, built from the stored rows by an adapter; φ_ref = 0 bitwise; the shift law | Opus |
| 05 | `05-the-planck-mass.md` | One reduced Planck mass from PRyMordial's G_N for `Planck_units`, `Mpc_units` and `GeV_units`; consistency tests; the shift in values measured | Sonnet |
| 06 | `06-the-tuning-layer.md` | `integrate_history` (any T_stop) and `tune_history` (T_CMB): secant, deterministic seed, residuals, failure reasons, per-iteration lines; secant against the plain update on §0.3's grid | Opus |
| 07 | `07-the-store.md` | The `ExponentialModel` table; `ScalarHistory`'s key and tuned outputs; `T_stop` out of the key; factories, sharding, fingerprint format 5, golden file, `VERSION_LABEL` 2026.7.0; `main.py` and every reader moved to them | Opus |
| 08 | `08-the-driver-and-the-tools.md` | `main.py`'s options (Λ, `--T-stop-GeV`, `--potential-type` gone); the super-Planckian warning; the wrapper's scope; `history_and_bbn.py`, `bbn_from_store.py` and the surfing tools on tuned histories; `extract_common`'s columns | Opus |
| 09 | `09-rehearse-a-tuned-build.md` | On the isolated cluster, registered: a pilot build of tuned histories, interrupted and resumed, compared bitwise; the tuned values against the tuning layer called directly; the longest heartbeat silence | Opus |
| 10 | `10-close.md` | `architecture-summary.md`, `numerical-strategies.md`, `CLAUDE.md`; the handover for the science rebuild (run scripts, cost); the board closed; the indexes | Sonnet |

### 2.1 When each prompt file is written

01–10 are written once the user has ruled on §6.1 and §6.2. The order is a chain: each prompt
needs the one before it, except that 05 needs only 02 (it could run after 02 if a reason arose; P1
keeps it after 04 so that 04's bitwise check is on the old units). 09 needs 08; 10 needs 09.

### 2.2 The charters

The charters say what each prompt changes and how it is accepted. The prompt files will add the
file-by-file detail, the deliberate breakages and the stop conditions. Every prompt runs the five
suites and reports the counts (§0.4 for the start); a count that falls is a stop.

**01 — Strip to the exponential model** (Sonnet).
- **What it changes.**
  1. Delete `CosmologyConcepts/Potentials/{InversePower,Starobinsky,Recliner,Reflecting}Potential.py`
     and `ObjectFactories/{InversePowerPotential,Starobinsky,ReclinerPotential,ReflectingPotential}.py`,
     their ids in `model_ids.py`, their entries in `config/datastore.py` (factories, batch sizes),
     `config/sharding.py`, and `ObjectFactories/ScalarModel.py`'s spec; `starobinsky.yaml`,
     `recliner.yaml`.
  2. `--potential-type` keeps only `Exponential` (its removal is 08's, with the other options).
     `main.py`'s and `plot_inputs.py`'s dispatch on it shrinks to the one case.
  3. Tests that build the deleted classes are cut to the exponential case; none is deleted
     outright without the log saying which assertion went with it.
  4. **A golden short history.** A new test, `ComputeTargets/tests/test_golden_history.py`, pins
     the samples of two short histories (β = 1.5 and β = 5.5 at M = 10⁻³, φ* = 5, T_stop = 1 MeV,
     the production z grid; under a minute together), as a committed JSON of their floats, and checks them bitwise. Later
     prompts that change values regenerate it deliberately (P3).
  5. `FINGERPRINT_FORMAT` 2, its pin and golden file.
- **Acceptance.** The golden history is generated at `ac187c8`'s tree (before any deletion) and
  passes unchanged after it; the suites pass at the counts the log states, with every test removed
  named; `grep` finds none of the four names outside `prompts/`, `science-runs/` and
  `.documents/`; `[00-declare-reflects-at-origin-for-the-other-potentials]` closed (moot: the
  classes are gone).

**02 — Rename to `ScalarHistory`** (Sonnet).
- **What it changes.** `ScalarModel` → `ScalarHistory`, `ScalarModelValue` →
  `ScalarHistoryValue`, `ScalarModelProxy` → `ScalarHistoryProxy`, `compute_scalar_model` →
  `compute_scalar_history`, the module files (`ComputeTargets/ScalarHistory.py`,
  `ObjectFactories/ScalarHistory.py`), the tables and `ScalarHistory_tags`, the drop group
  `scalar-history`, the `Parent` references, `plot_ScalarModel.py` → `plot_ScalarHistory.py`, and
  the current documents (`architecture-summary.md`, `numerical-strategies.md`, `CLAUDE.md`).
  Campaign records and dated documents keep the old names, as with the rename of 2026-10-13
  (`CLAUDE.md`, "Repository mechanics"). Each driver's argument of `--drop` accepts only the new
  name. `FINGERPRINT_FORMAT` 3.
- **Acceptance.** The golden history passes unchanged; no occurrence of the old names outside the
  records; `git log --follow` keeps each moved file's history (moves by `git mv`); suites at 01's
  counts.

**03 — The sampling axis** (Opus).
- **What it changes.**
  1. `redshift` holds `log_one_plus_z`, ln(1 + z_E), the Einstein-frame redshift counted back from
     T_J = T_CMB (R12). `redshift.z` stays as a derived property, computed with `expm1`, for the
     few readers that want it, and returns `inf` past float range rather than raising.
  2. The factory matches on the absolute difference in `log_one_plus_z`, with a tolerance far below
     the grid spacing (P8); `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` closes.
  3. **The grid** is `log_one_plus_z = k Δ`, k = 0 … K, Δ = ln 10 / S, S =
     `--samples-per-log10-z` (default 250, unchanged, so the density is today's). The upper limit's
     default moves from 10⁸⁵ to 10³⁰⁰ in 1 + z (P8).
  4. `compute_scalar_history` reads N_backward from the axis directly; no `exp` or `log` of the
     label remains.
  5. **ln(1 + z_J)** on every `ScalarHistoryValue` (R4), `log_one_plus_z_Jordan`, computed at
     sampling as ln(1 + z_E) + ln A(φ₀) − ln A(φ). It is a column, not a key: H_J < 0 during a
     bounce (`ComputeTargets/ScalarModel.py:386`), so a_J is not monotonic.
  6. The readers: `extract_common`'s z axis, `plot_ScalarHistory.py`'s frame, `bbn_from_store.py`'s
     SQL, `inventory_report.py`.
  7. While in `ScalarHistoryValue`'s factory: `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`
     and `[05-the-value-factory-compares-stored-phi-against-pi]` (one defect, opened twice) close.
  8. `FINGERPRINT_FORMAT` 4.
- **Acceptance.**
  1. A history of 800 e-folds samples without overflow (a primitive run with a cheap synthetic
     setup, or the lookup and grid alone if no physical history reaches it; the prompt file says
     which).
  2. The golden history is regenerated, and the log records the largest relative change of each
     sample field against 02's (expected at the 10⁻¹⁵ level, from the grid points' last bits).
  3. ln(1 + z_J) agrees with `cosmology.z(T_J)` (entropy conservation in the Jordan frame) to the
     integration's accuracy along a history away from the bounces, and with the identity in item 5
     exactly.
  4. The suites.

**04 — The model interface** (Opus).
- **What it changes.**
  1. `CosmologyConcepts/Models/AbstractModel.py`: the interface (R6, R7). A model is a stored
     object with its Lagrangian parameters and a `shard_key`. `realize(normalization, phi_ref)`
     returns an immutable `RealizedModel` with log V, d log V/dφ, the second derivative, log A,
     d log A/dφ and its derivative, the `log_V_floor` and `reflects_at_origin` the integrator reads.
     A(φ_ref) = 1 is enforced in the base class (ln A(φ) − ln A(φ_ref)), so no model interprets
     φ_ref. The normalization is the model's to interpret. Also on the interface:
     `renormalize(normalization, phi, V_target)`, the frozen-state step, which makes V(φ) =
     V_target; `normalization_coordinate` and its inverse, the scalar on which 06's secant acts;
     `effective_minimum(normalization, phi_ref, rho_m_E, phi_bracket)`, a generic root-find of
     V′ + (ln A)′ ρ_m,E that a model may override; and `seed(cosmology)`, P5.
  2. `ExponentialModel(M, n, β)`: V = V₀ exp((M/φ)ⁿ), A = exp(β(φ − φ_ref)/M_P); the normalization
     is V₀ (multiplicative; its coordinate is ln V₀); a closed-form `effective_minimum` for n = 1.
  3. `compute_scalar_history`, `HubblePolicy`, `ODEPolicy`, `fixed_T_value_at`, `AdiabaticHistory`,
     `BBNData`, `surfing_turning_points` and `pipeline_selection` take a `RealizedModel` in place of
     a potential and a coupling. The `bounce_region_*`, `default_max_step` and
     `hard_reflection_point` properties are not carried over:
     `[00-region-properties-on-the-potentials-become-unread]` closes.
  4. **The store does not change yet.** An adapter builds the `ExponentialModel` and its
     realization from the existing `ExponentialPotential` and `ExponentialCoupling` rows, with
     V₀ = Λ⁴ and φ_ref = 0, so `main.py` and the readers run as before. 07 removes it.
- **Acceptance.**
  1. The golden history passes **bitwise** at φ_ref = 0, V₀ = Λ⁴ (the brief's §5 first stop
     condition).
  2. The shift law: at a fixed Einstein-frame state, shifting φ_ref by δ multiplies 1 + r + f_m
     by e^{−2βδ/M_P} to rounding (the brief's R7).
  3. `effective_minimum` agrees with a brute-force root-find, and the closed form with the
     generic one.
  4. The suites; `black` clean.

**05 — The Planck mass** (Sonnet).
- **What it changes.** One constant, the reduced Planck mass in eV from PRyMordial's G_N
  (`6.70883e-39 GeV⁻²`, `PRyM/PRyM_init.py:136`), in `Units/base.py` (P9), used by
  `Planck_units`, `Mpc_units` and `GeV_units`; nothing imports `PRyM` for it. The three pinned
  `2.436e+24 eV` strings follow.
- **Acceptance.** Tests: the constant equals PRyMordial's G_N, read from the vendored file's text,
  to the last digit; in each unit system 1 K = k_B eV, 1/eV = ħc metres and ħ seconds, and the
  kilogram, each to ≤ 10⁻⁶; the golden history regenerated, with the largest relative change per
  field logged (expected ≈ 2.8 × 10⁻⁴ where an eV-labelled quantity enters).

**06 — The tuning layer** (Opus).
- **What it changes.** In `ComputeTargets/ScalarHistory.py`:
  1. `integrate_history(cosmology, realized_model, T_init, T_stop, φ*, π*, z_grid, …)`: the
     primitive (R8), today's integration with no normalization logic. Tests, diagnostics and the
     tuning layer use it; it may stop at any T_stop.
  2. `tune_history(cosmology, model, T_init, φ*, π*, z_grid, tolerance, …)`: always to T_CMB.
     Each step integrates, reads today's state, and forms the residuals r_H = ln(H_J(T_CMB)/H0)
     and r_φ = φ_ref − φ_min. Converged when |H_J/H0 − 1| and |φ_ref/φ_min − 1| are both below
     the tolerance (default 10⁻⁵, a key of the history, P7).
  3. **The step** (R10, P4): the first from the seed's frozen-state update (`renormalize` to Eq. 7's
     V_target, φ_ref ← φ_min); after that a two-variable secant (Broyden) on
     (`normalization_coordinate`, φ_ref) driven by the measured residuals. A step whose V_target
     ≤ 0 moves φ_ref only and halves V₀ (§0.3 item 1).
  4. **The seed** (P5) is deterministic in the history's own inputs (R13): the model's tracking
     seed, or, where it has no solution, the static one, or failing that the probes' plain start
     (V₀ = ρ_Λ, φ_ref = 0).
  5. **Failure reasons**, each its own clause for `summarise_failure_reasons`: "the normalization
     did not converge" (after the cap, default 30 integrations), "no normalization reaches H0"
     (V_target ≤ 0 four steps running), and every integration failure as today.
  6. **Progress.** One line per integration, printed by the task: the iteration, both residuals,
     the normalization and φ_ref (P12). Ray forwards it to the driver's stdout, which the
     wrapper's heartbeat reads.
  7. What it returns for storage (P6): the samples of the final integration, the normalization,
     V_ref = V(φ_ref), φ_ref, φ₀, φ₀′, the tracking diagnostic, both residuals, the iteration
     count, and the seed's normalization and φ_ref.
- **Acceptance.**
  1. A converged history (a cheap point, e.g. β = 3, M = 0.01, about 30 s, its docstring says so)
     has both residuals inside the tolerance and A(φ_ref) = 1 exactly.
  2. Re-running gives bitwise the same result (determinism).
  3. **Secant against the plain update** on §0.3's thirteen points: integrations to converge and
     the converged values, in the log. The secant is accepted if it needs no more integrations
     than the plain update at any point and the two agree to the tolerance; otherwise stop and ask.
  4. A forced non-convergence (cap 2) and a forced V_target failure give their reasons.
  5. The suites.

**07 — The store** (Opus).
- **What it changes.**
  1. A table `ExponentialModel` keyed (M, n, β) (R6, R14), sharded by β; `ExponentialPotential`,
     `ExponentialCoupling` and `Lambda_value` leave the registry (the adapter of 04 goes).
  2. `ScalarHistory`'s key (P7): version, cosmology, model (type and serial), φ*, π*, T_init,
     atol, rtol, the tuning tolerance, solver. `T_Jordan_stop` leaves it: a stored history always
     ends at the cosmology's T_CMB (R8).
  3. Its outputs (P6): `normalization` (the model's opaque value), `V_ref`, `phi_ref`, `phi_0`,
     `pi_0`, `tracking`, `residual_H`, `residual_phi`, `tuning_iterations`, `seed_normalization`,
     `seed_phi_ref`, with the existing columns.
  4. `AdiabaticHistory` and `BBNData` rebuild the realized model from their history's row.
  5. `main.py`'s scalar stage looks up and computes by (model, initial data); its potential and
     coupling grids become a model grid over (M, n, β). While there,
     `[02-scalar-stage-missing-filter-keeps-every-model]` and
     `[03-step-1-first-pass-lookup-filters-nothing]` close.
  6. Every reader moves to the new tables: `plot_inputs.py` (the M × Λ product becomes the stored
     models), `plot_by_beta.py`, `plot_ScalarHistory.py`, `extract_common.py`,
     `tools/compare_stores.py`, `tools/inventory_report.py`, `tools/bbn_from_store.py`. Only what
     the schema forces changes here (P17).
  7. `FINGERPRINT_FORMAT` 5, its pin and golden file; `VERSION_LABEL` 2026.7.0 (P16).
- **Acceptance.** Round trips through a temporary store: a tuned history stored and read back
  equal bitwise, and its realized model rebuilt equal to the one it was computed with; a lookup
  by (model, initial data) finds it, and one with another φ* or tolerance does not; the golden
  fingerprint; `--drop` groups refuse as before; the suites.

**08 — The driver and the tools** (Opus).
- **What it changes.**
  1. `main.py`: `--log10-Lambda-*`, `--Lambda-values-*`, `--samples-per-log10-Lambda-eV`,
     `--T-stop-GeV` and `--potential-type` removed (`configargparse` refuses them, so the old run
     scripts fail loudly); `--tuning-tolerance` and `--tuning-max-iterations` added, with the
     defaults of P4. `exponential.yaml` follows. `plot_inputs.py`'s option matching follows.
  2. The super-Planckian warning evaluates A(φ*) at the seed's φ_ref before Ray, and says so (P11).
  3. `RunDrivers/registered_run.py`'s `scope` loses the Λ fields and the potential type, and gains
     n and the tolerance.
  4. `tools/history_and_bbn.py`: a tuned history by default, with its tuning lines; a
     `--fixed-normalization V0 PHI_REF` option runs the primitive (P18).
  5. The surfing tools (U4): `tools/surfing_scan.py` tunes each β to T_CMB and analyses the final
     integration's dense output; its CSV gains the normalization and φ_ref per row, and its
     `_INPUT_MATCH` drops Λ. `[05-surfing-scan-runs-at-a-fixed-lambda]` closes.
     `plot_surfing_histories.py` follows.
  6. `extract_common`'s `histories.csv`: Λ replaced by V_ref, the normalization and φ_ref, sorted
     by (M, n, β); the tuning diagnostics as columns.
- **Acceptance.** The old run scripts' options are refused, exit 2; the wrapper's scope tests; a
  surfing scan of three β (short) reproduces its roots and capture as before and records the tuned
  values; `histories.csv` from a temporary store has the new columns; the suites.

**09 — Rehearse a tuned build** (Opus; if U2). On the isolated cluster, under the shared lock
(`ray_cluster.sh start "jordan-normalization 09"`), launched through the wrapper, detached, checked
once (`CLAUDE.md`, rule 4).
- **What it does.** A pilot build into `var/datastores/jordan-normalization-09-reference.db`:
  M = 10⁻³ with β = 0.5 (φ* = 1 and 5), 1.5, 3, 5.5; M = 0.1, β = 3; M = 10⁻⁵, β = 3; 16 shards;
  scalar, adiabatic and BBN stages. About 10 CPU-minutes of tuning at §0.3's costs. Then a second
  build into `…-resumed.db`, `SIGTERM`ed during the scalar stage and resumed. Then
  `plot_by_beta.py` on the reference.
- **Acceptance.**
  1. Every history converged: both stored residuals inside the tolerance.
  2. The stored tuned values equal those of `tune_history` called directly (`tools/history_and_bbn.py`)
     bitwise.
  3. The resumed store equals the reference by `tools/compare_stores.py` (deterministic seeds,
     R13).
  4. Measured: the longest heartbeat silence (`list`), each history's integrations, the stage
     times; the tuned values against §0.3's (expected to differ by about 2.8 × 10⁻⁴ from the units,
     05, and to agree otherwise).
  5. `.documents/jordan-normalization-verification.md` created, outputs under
     `.documents/jordan-normalization/out/`.

**10 — Close** (Sonnet).
- `architecture-summary.md` and `numerical-strategies.md`: the model interface, the two layers,
  the secant, the stored tuning outputs, the sampling axis; the brief's R6 audit of the documents
  (any statement that Planck units "scale out" M_P, or that A(0) = 1).
- `CLAUDE.md`: the "Units" and "Redshift" bullets (ln(1 + z_E) on the axis, ln(1 + z_J) on each
  sample), the renamed names, the suite counts.
- A dated addendum to `.documents/review-remediation-verification.md` (§4.16): the handover for the
  science rebuild. The run scripts' options that are gone; the blocks through the wrapper with
  `--campaign jordan-normalization --prompt 10`; the cost (§0.3 item 7, 09's measurements); the
  heartbeat's silences; the RayPoolKit wait (P19).
- The board closed; `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`. The final suites.

## 3. Stores

Only 09's, under `var/datastores/`, ignored by git, kept for the user. Every other prompt opens no
store but the tests' temporary ones (`_TempStoreCase`). No existing store is opened.

## 4. The interfaces between prompts

- **01 ships** the golden history. Every later prompt runs it; a prompt that changes values
  regenerates it in its own commit and logs the change (P3).
- **03 ships** the axis, which 06's `integrate_history` and 07's value tables use unchanged.
- **04 ships** `AbstractModel`, `ExponentialModel` and `RealizedModel`; 06 tunes through their
  interface only, and never reads V₀ or β itself. The adapter lives until 07.
- **06 ships** `tune_history` and what it returns (P6); 07 stores exactly that.
- **07 ships** the schema; 08 changes no factory. If 08 needs one changed, 08 stops and a lettered
  prompt (07b) changes it.
- **08 ships** the driver 09 runs unchanged. If 09 finds a defect, 09 stops; the fix is a lettered
  prompt.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics". From `dk-and-rpk-migration`: rule 8
(deliberate breakages, recorded as diffs), rule 10 (no test needs Ray, and none opens a store
outside a `tempfile` directory), rule 11 (a package defect is reported upstream), rule 12 (no change
in a sibling repository), rule 15 (no `ray stop`; a cluster only through `ray_cluster.sh`), rule 16
(the shared rehearsal lock: a held lock is a stop, never a wait). And:
- **Values.** 01, 02 and 04 change no computed value (the golden history bitwise). 03 and 05 change
  values only by what they measure and log. 06 and 07 introduce the tuned values.
- **Tests are modified only where a prompt changes what they test** (the strip, the rename, the
  axis, the interface, the schema), and each modification is listed in the log with its reason.
- **09's rehearsal is a one-off exception** to `CLAUDE.md`'s rule that no campaign prompt starts a
  `main.py` run with a datastore, as `rrk-adoption`'s 04 was (U2).
- **Nothing runs longer than a few PRyMordial solves**, except 06's comparison on §0.3's grid (about
  half an hour, as the planning probes) and 09's builds; both are named here.
- **No agent polls a run** (registry rule 4).
- **Stop and ask if** (the brief's §5, updated): the φ_ref = 0 path does not reproduce bitwise (04);
  a class of histories does not converge, or V_target ≤ 0 persists, in 06 or 09; A's normalization
  is found to enter somewhere not listed in §0.2, with consequences beyond relabelling; the store
  needs a design choice that R6–R14 and P6–P8 do not make.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.0 Ruled by the user

Carried from the brief's §4 (2026-10-06), and confirmed or added on 2026-10-20:
- **R1 — the target** (the brief's U1, confirmed 2026-10-20): tune so that H_J(T_CMB) = H0
  (Planck 2018) at fixed ω_m. The deceleration parameter is not a condition: "Whether we also match
  the deceleration parameter is probably what we want to test by building the reconstructed
  equation of state of dark energy, so that is not an extra condition (in this framing)."
- **R2 — the reference point** (the brief's U2): φ_ref = φ_min(T_CMB), the minimum of the
  Einstein-frame effective potential at the history's own densities today. The actual φ₀ is
  stored too.
- **R3 — the Planck mass** (the brief's U3): PRyMordial's G_N.
- **R4 — the Jordan-frame redshift** (the brief's U4): computed and stored.
- **R5 — the store** (the brief's U5): the user re-runs the science store; no prompt starts a
  production run.
- **R6 — one model object.** "I would prefer to avoid changes that would structurally tie
  `ScalarModel` to any particular combination of parameters needed to specify the potential."
  An `AbstractModel` defines the interface and absorbs both V(φ) and A(φ). "It is backed by a single
  database table for each model type (e.g. potential = exponential, coupling = exponential is a
  single type), that stores all the required Lagrangian parameters in a single row." The
  exponential model is keyed by M and β, with V₀ derived.
- **R7 — the history finds its own normalization**: (V₀, φ_ref) depend on the history's initial
  data and dynamics, so the history computes them, and the model's interface "has to accept a
  parameter representing the normalization of the potential, which it can interpret how it wishes
  (some models might implement it as an additive uplift; for the exponential potential we would
  interpret it as a multiplicative normalization)."
- **R8 — two layers**: a primitive that integrates to any T_stop with no normalization logic, and
  a layer that always integrates to T_CMB and tunes. The stored history no longer stops early.
- **R9 — the rename**: `ScalarModel` becomes `ScalarHistory`, keeping "model" for the Lagrangian.
- **R10 — the update step**: a secant.
- **R11 — exponential only**: "for now, strip everything down to just the exponential."
- **R12 — the sampling axis** stores ln(1 + z), anchored at T_CMB, in place of z.
- **R13 — no seeding from other histories**: "I'd only be happy with this if we were sure there was
  no dependence on the initial field value" (§0.3 item 2 shows that there is, without surfing).
  The initial field momentum stays zero.
- **R14 — n is part of the exponential model's key**: "It's cheap and lets us easily generalize to
  potential with `n > 1` later."

### 6.1 For the user to rule (U1–U5)

**Ruled by the user, 2026-10-20:** U1 "OK", U2 "OK", U3 "OK". U4 and U5 are open: the user
asked which 79-β scan U4 means and which units change U5 means (§5.1 of the board has the
answers given).

| | Proposed | Alternatives |
|---|---|---|
| **U1** — names | `AbstractModel`, `ExponentialModel`, `RealizedModel` under `CosmologyConcepts/Models/`; `ScalarHistory`, `ScalarHistoryValue`, `compute_scalar_history`, `integrate_history`, `tune_history`; columns `log_one_plus_z`, `log_one_plus_z_Jordan`, `normalization`, `V_ref`, `phi_ref`; the drop group `scalar-history` | Any other; the prompts take the names from this table |
| **U2** — a rehearsal | **Yes** (09): a registered pilot build of tuned histories on the isolated cluster, interrupted and resumed, as the one-off exception to `CLAUDE.md`'s rule. It is the only test of the tuned pipeline end to end before the science rebuild | No rehearsal; 08's tests and the handover only |
| **U3** — the issues taken | **Assigned at planning:** `[00-declare-reflects-at-origin-for-the-other-potentials]` (01, moot); `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` and `[05-the-value-factory-compares-stored-phi-against-pi]` (03); `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` (03); `[00-region-properties-on-the-potentials-become-unread]` (04); `[02-scalar-stage-missing-filter-keeps-every-model]` and `[03-step-1-first-pass-lookup-filters-nothing]` (07); `[05-surfing-scan-runs-at-a-fixed-lambda]` (08). Eight, each in code the campaign rewrites | Fewer; any left are recorded in the logs as "Observations not acted on" |
| **U4** — the surfing tools | **Tuned** (08 item 5): each β tuned to T_CMB, the scan's analysis on the final integration. The 79-β scan then costs about seven integrations to T_CMB per β (5–20 s each at β 1.1–5) instead of one to 1 MeV (3.2 s on average): about 1½ hours instead of 5 min; 08 measures it | The tools take (V₀, φ_ref) from a stored tuned history (`--from-store`), which keeps them cheap but ties a scan to a store; or they keep a fixed normalization by option, with today's 1 meV as the default |
| **U5** — the order of the units | **After the interface** (05 after 04), so that 04's bitwise check runs on today's units and 05's shift is measured alone | Units first (after 02), so that every later measurement is on the final units; 04's check is then against 05's tree |

### 6.2 Proposed by the planner (P1–P20)

Accepted as proposed unless the user rules otherwise. **Ruled by the user, 2026-10-20:** "P1-P20
seem ok".
- **P1 — the order** is §2's table: what changes no value (01, 02) first; then the axis (03),
  whose readers are few; then the interface on the old store (04), so that its bitwise check is
  meaningful; the units (05, U5); the tuning in the compute layer alone (06); the store, which
  moves every writer and reader at once (07); the options and tools (08); the rehearsal (09).
- **P2 — fingerprint formats.** Each prompt that changes a spec or the registry bumps
  `FINGERPRINT_FORMAT` in its own commit, as `config/fingerprint.py` requires: 2 (01), 3 (02),
  4 (03), 5 (07). Golden files are added, never overwritten.
- **P3 — the golden history** (01 item 4) is the campaign's regression. 01, 02 and 04 pass it
  bitwise; 03 and 05 regenerate it and log the largest relative change per field; 06 adds a
  tuned golden history (P4's cheap point), which 07 and 08 pass bitwise.
- **P4 — the secant.** Broyden's two-variable update on (`normalization_coordinate`, φ_ref) from
  the measured residuals, its first Jacobian estimate from the frozen-state step; the V_target ≤ 0
  guard of §0.3; tolerance 10⁻⁵ in both residuals; cap 30 integrations. 06 compares it with the
  plain update and stops if it is ever worse.
- **P5 — the seed** is the model's: for `ExponentialModel`, the tracking analytic minimum
  (φ′ = 1.5 φ_ref, A(φ_ref) = 1, Planck2018's densities), else the static one, else the plain start.
  The seed kind is stored with its values (P6).
- **P6 — stored per history**: the opaque `normalization` (exact reconstruction), `V_ref = V(φ_ref)`
  (the model-independent amplitude), `phi_ref`, `phi_0`, `pi_0`, the tracking diagnostic
  φ₀′/(1.5 φ₀), `residual_H`, `residual_phi`, `tuning_iterations`, the seed's kind, normalization and
  φ_ref. The analytic seed thereby records, for free, how far each history lags the minimum.
- **P7 — the history's key**: version, cosmology, model (type and serial), φ*, π*, T_init, atol,
  rtol, the tuning tolerance (a `tolerance` row), solver. No T_stop.
- **P8 — the axis**: `log_one_plus_z` as a `Float(64)`; the lookup by absolute difference below
  10⁻¹⁰ (the grid spacing is 9.2 × 10⁻³); the grid's upper default 300 decades of 1 + z (about 690
  e-folds; histories need 80–200), its rows cheap since each history samples only up to its own end;
  the option names kept (`--samples-per-log10-z`, `--log10-one-plus-z-high`).
- **P9 — one Planck mass.** `Units/base.py` holds `REDUCED_PLANCK_MASS_EV` from G_N, with G_N's value
  and source in a comment; a test reads `PRyM/PRyM_init.py`'s text for the same number, so the two
  cannot drift without a failure, and nothing imports `PRyM` at import time (`PRyM_init` reads files
  from the working directory).
- **P10 — the shift law and the Eq. 7 identity** are tests in 04 (the brief's R7), at stored
  samples and at fixed states.
- **P11 — the super-Planckian warning** is evaluated at the seed's φ_ref, before Ray, and its text
  says so; the warning stays a warning (`warn-dont-filter`).
- **P12 — progress lines.** One per integration from the tuning task. Ray forwards worker output to
  the driver, and `rrk-adoption` 03 found that worker output beats; 09 measures the longest silence.
  The 900 s window is not changed.
- **P13 — tests**: `ComputeTargets/tests/test_golden_history.py` (01, regenerated in 03 and 05, a
  tuned one added in 06); `CosmologyConcepts`' model tests under `ComputeTargets/tests/` (04);
  `CosmologyModels/tests/test_planck_mass.py` (05); `test_tuning*.py` (06), each tuned test under
  a minute with its cost in its docstring; the round trips in `ObjectFactories/tests/` (07). No test
  needs Ray or a persistent store.
- **P14 — models**: Sonnet for 01, 02, 05, 10; Opus for 03, 04, 06, 07, 08, 09.
- **P15 — the branch** is `jordan-normalization`, cut from `main` at `ac187c8`, merged into `main`
  at the close.
- **P16 — `VERSION_LABEL`** moves once, to 2026.7.0, in 07, the first prompt whose histories can be
  stored. No store is built between prompts, so an earlier bump would mark nothing.
- **P17 — 07 against 08.** 07 changes what the schema forces in every writer and reader, and no
  more; 08 changes behaviour (options, warnings, tools). If 07 cannot keep a reader's tests passing
  without a behaviour change, the change is made in 07 and logged `STRUCTURALLY REQUIRED`.
- **P18 — the primitive in the tools**: `tools/history_and_bbn.py --fixed-normalization V0 PHI_REF`
  runs `integrate_history` with no tuning, for diagnostics; `main.py` has no such option, so no
  stored history is untuned.
- **P19 — not assigned**: `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`
  (RayPoolKit's `ray.wait`), which is the user's to rule upstream; 10's handover restates it with
  09's measurements, since tuned histories lengthen the wait it describes.
- **P20 — the planning probes** stay in `source/planning-probes-2026-10-20/` as the record of §0.3;
  no prompt imports them.

## 7. What this campaign hands to the user

- **The science rebuild** (R5), from 10's addendum: the run scripts without the Λ, T_stop and
  potential-type options, every block through the wrapper, and the cost.
- **09's stores**, under `var/datastores/`, until the user removes them.
- **The RayPoolKit wait** (P19), with 09's measurements.
- **The decisions this campaign defers**: models other than the exponential one (R11); per-history
  surfing columns; the deceleration parameter as a test (R1).
