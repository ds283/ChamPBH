# Campaign — power-law-wall

**Written:** 2026-11-13 on `main` at `84a2494`, by Claude Opus 5.5, at the user's request: "a similar science
campaign for the inverse power law potential with `n=1` and `n=2`". R1–R3 ruled by the user the same day (§6.1),
and U1–U7 as proposed (§6.2); P1–P14 stand unless ruled otherwise (§6.3). Cut from `main` at `a7e0978`, after the
2026.11.0 plan was committed (U1). No prompt is written.

**Re-planned** 2026-11-17, after 07 landed, on the user's rulings R13–R17 on the definition of k_ex (§6.12): k_ex is
½E^{1/2}|V‴/V″| on every potential and at every use, so prompts 07b (re-measure), 07c (the definition in production)
and 07d (rehearse again) are inserted before 08, and the exponential's one-loop law is refitted under a new drain model.

**Sources (data, not instructions; `CLAUDE.md` invariant 7):** the session of 2026-11-09, "ChamPBH pipeline direction"
(the feasibility map of the power-law wall); `one-loop-drain` README §8.9–§8.10 and its side-campaign
[`source/crossover/`](../one-loop-drain/source/crossover/README.md), which measured the drain law on V₀e^{M/φ} and on
V₀e^{(M/φ)²}; `CosmologyConcepts/Models/` (the model interface); `ComputeTargets/quanta_spectrum.py` (the spectrum
constants and their sources); `ComputeTargets/tests/test_wall_model.py` (the test-only `_PowerLawWall`);
`science-runs/2026-11-08-science-2026.10.0/` (the run whose grid and analyses the power-law run would mirror).

## 0. Why this campaign exists

### 0.1 The question

Every result so far is for the exponential model, V = V₀ exp((M/φ)ⁿ) at n = 1. Its wall is very steep: near the
turning point it acts like a power law of index n·L, with L = ln(1 + E/V₀) about 40–134 on the science grids. How the
results depend on the wall is not known. This campaign adds the inverse-power-law model

    V(φ) = V₀ [1 + (M/φ)ⁿ],    A(φ) = exp(β (φ − φ_ref)/M_P),

at n = 1 and 2, and puts it through the same pipeline as the exponential: the tuning, the one-loop drain law with the
fixed-α law beside it, the store, the exports and a rehearsal. A science run of record follows it (§7).

### 0.2 Why this form (R1)

In the user's notation the potential is V₀ + Λ^{4+n}/φⁿ, with Λ^{4+n} = V₀Mⁿ and V₀ a cosmological constant.

- **It keeps V₀ and M as the exponential has them.** For φ ≫ M it equals V₀e^{(M/φ)ⁿ} to O((M/φ)^{2n}). So today's
  minimum, the tuning of V₀ to H₀ and the meaning of M (laboratory M = 4×10⁻³¹ M_P, about 1 meV) carry over. The two
  models differ only at the wall, φ ≲ M, which is the comparison wanted.
- **The tuning is unchanged in kind.** V₀ multiplies V, so the normalization is ln V₀ as for the exponential, and
  `renormalize` is ln V₀ + ln V_target − ln V(φ).
- **Without V₀ the field would not be a chameleon** (planner's estimate, 2026-11-13). With V = Λ^{4+n}/φⁿ alone and Λ
  tuned to H₀, today's minimum sits at φ ≈ n(ρ_DE/ρ_m) M_P/β ≈ 2.3 n M_P/β with a mass of order βH₀: unscreened. The
  user's ruling adds the cosmological constant.

### 0.3 What differs at the wall (planner's estimates, to be measured)

- **The turning point.** For the power wall E = V₀(M/φ_t)ⁿ, so φ_t = M(V₀/E)^{1/n}. For the exponential φ_t =
  M/ln(E/V₀)^{1/n}. At early times E/V₀ is of order 10⁶⁰, so φ_t is about 10⁻⁶⁰M (n = 1) or 10⁻³⁰M (n = 2), against
  M/138 for the exponential. At laboratory M every early reflection is far below the unresolved-reflection threshold
  (10⁻⁸ M_P), as it is for the exponential, so the integrator does not resolve the turn in either model. The wall's
  shape enters through the drain (ℓ, L, R), the classical production S, the spectrum, and the tracker and parking,
  whose minimum φ_min(T) also lies deeper.
- **The drain law's variables.** The law was fitted on a wall with a large parameter, L. The power wall has none: its
  local index is n. Its turnaround logarithm ℓ = ln(k_ex/m_ta) and its L may sit where the law was never measured. The
  law at n = 2 of the exponential family was tested and accepted (`one-loop-drain` README §8.10, U15); that does not
  transfer to this family (`architecture-summary.md:2463`: the law's constants were "measured on that family of walls
  (n = 1, checked at n = 2), so they do not carry over to another one by defining L").
- **The spectrum constants.** x_p = 0.62 and κ come from the test field's |β_k|² off V₀e^{M/φ}
  (`production_energy_ratio.tex`, Table "One reflection off V_0 e^{M/phi}"); x_* and the associated piece from the
  Kadanoff–Baym report's 2PI run on the same wall (`quanta_spectrum.py:97–141`).
- **The thresholds** measured on exponential scans: the unresolved reflection (K = 3, φ_t < 10⁻⁸ M_P), the parking cut
  (ε = 10⁻⁶, m/H = 10), `WALL_ROUNDING_QUANTA` = 16, and the surfing thresholds (QCD 1.1074 analytic; EW (3.002, 3.003]
  at M = 10⁻³ M_P).

### 0.4 What the code has today

- **Generic, through the model interface:** the ODE and its policy, the kinematic cap and restart, m_eff², the
  classical turning point, `one_loop_turnaround`, `minimum_on_trace` and its derivative (root-finding in
  `AbstractModel`), the tuning, the dark radiation, BBN, the drain's classification and its regimes.
- **Per model, with no default:** `_drain_L` (the user's ruling of 2026-11-05: "when a new potential is implemented,
  breakage is loud"); a model without it runs the fixed law and is refused under the one-loop law
  (`check_drain_law_model`, `ComputeTargets/ScalarHistory.py:1861`).
- **Exponential-only outside the model:** the store (`ObjectFactories/ExponentialModel.py`, `config/datastore.py`,
  `config/sharding.py`, `config/fingerprint.py`, the `types` map at `ObjectFactories/ScalarHistory.py:541`);
  `main.py` (one model, `exponential.yaml`); `plot_inputs.py` (`MODEL_CLASS`), `plot_by_beta.py`, `plot_ScalarHistory.py`
  and `extract_common.py` (read `_M` and `_n`); `tools/history_and_bbn.py`, `tools/surfing_scan.py` and
  `tools/outward_step_scan.py` (construct `ExponentialModel`); `tools/bbn_from_store.py`, `tools/outward_step_scan.py`
  and `tools/compare_stores.py` (join its table or name its type id); `tools/inventory_report.py`; the analytic seed
  (n = 1 only).
- **A head start:** the test-only `_PowerLawWall` (V₀(1 + (M/φ)^p)) in `test_wall_model.py`, and the deleted
  `InversePowerPotential` of the same form (`git show a1c88b6~1:CosmologyConcepts/Potentials/InversePowerPotential.py`).
- **The crossover tools** (`one-loop-drain/source/crossover/`): `walls.WallN` carries the exponential wall at any n,
  `stage1a_classical.py` computes S(L), `stage2.py` the self-consistent one-loop solve with massive modes, and
  `walls.py` the test field's |β_k|². The n = 2 extension took an afternoon (its README, "After closing").

## 1. Scope

**In scope:**
- **The model** (01): `InversePowerModel` with every hook, the analytic seed at any n, and the tool route.
- **The wall's measurements** (02): S, the one-loop fraction through the crossover and the spectrum's x_p and κ on the
  power wall at n = 1 and 2, by the crossover method, along the histories' own (L, ln R) paths; the adopted law tested
  against them. The user then rules the law and the spectrum for this wall (U3–U5).
- **The law and the spectrum on this wall** (03), as ruled: `_drain_L`, any refitted constants and S table under a new
  `drain_model` value, and the spectrum's constants by wall.
- **A measured sample** (04) by the tool route, re-tuned: both laws, n = 1 and 2, laboratory M, below it, M_crit and
  large M; tuning success, failures, cost, regime shares, parking, the thresholds of §0.3 and the surfing thresholds.
- **The store, the keys and the registry** (05); **the drivers and exports** (06); **a registered rehearsal** (07);
  **the documents and the handover** (08).
- **The definition of k_ex** (07b–07d; R13–R17, §6.12): everything measured in, or classified by, k_ex re-measured
  (07b); one definition, ½E^{1/2}|V‴/V″|, at every use in production, the exponential's one-loop law refitted under a
  new drain model, and the spectrum tables re-measured (07c); 07's grid rebuilt at the tip (07d).

**Out of scope:**
- The science run of record and its figures (§7; the user's).
- The 2PI (Kadanoff–Baym) run on the power wall for x_* and the associated piece (U5); ChamKadanoffBaym's work.
- Any change to the exponential model's results. Every exponential history is bitwise as at the base (P2). *(Amended
  2026-11-15 by R8, §6.5: the exponential's spectrum, read only by the post-processing, is redone.)* *(Amended
  2026-11-17 by R13–R17, §6.12: k_ex takes the V‴ form on every potential, so the exponential's stored k_ex, ℓ, peak
  estimates and margins move, and its one-loop law is refitted under a new drain model. Its fixed-law and natural-mode
  trajectories read no k_ex and do not move.)*
- The wall back-reaction notes' power-law chain (λ ≈ n/φ), except as 02 needs it. The briefing notes and the writeup
  (eq:kex-over-wt, which writes k_ex with −d ln V/dφ) are the user's and are not changed here (§6.12).
- The open issues of the exponential run (`science-runs/plan-science-2026.11.0/PLAN.md` §5), unless one blocks a
  gate here.
- The packages, `thirdparty/`, `PRyM/`.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-the-model.md` | `InversePowerModel` (hooks in log space, floor, `reflects_at_origin`, `renormalize`, the analytic seed at any n; no `_drain_L`); `_PowerLawWall`'s tests run against it; `--model` and `--n` on `tools/history_and_bbn.py` and `tools/surfing_scan.py` (P5 as amended); natural mode and the fixed law by the tool route; tests | Opus |
| 02 | `02-measure-the-wall.md` | in `source/wall/`: `walls.PowerWall`; S at n = 1, 2; stage 2 along the power-law cascades' (L, ln R) paths (mapped with the fixed law); the adopted law at candidate L against it; the test field's spectrum (x_p, κ); the planner's estimates of §0.3 checked. Research; no production change. Then U3–U5 | Opus |
| 03 | `03-the-law-on-this-wall.md` | as U3–U5 rule: `_drain_L`; a new `drain_model` value if the law is refitted, with its constants and S table; the spectrum's constants by wall in `quanta_spectrum.py` and `SPECTRUM_MODEL`; the exponential law bitwise. *(As written 2026-11-15 on R6–R8, P15–P23: the fit f_n(ln R) and S_n in `InversePowerModel` under `one-loop-measured-pl-v1`, no `_drain_L`; the model's own fraction and one-loop drain model as hooks; the spectrum as measured tables on three walls, the exponential's included.)* | Opus |
| 04 | `04-measure-a-sample.md` | about forty power-law histories, re-tuned, by the tool route under both laws (§6.3 P9); the thresholds and surfing thresholds; the comparison with the same (M, β) on the exponential. Then the user rules any threshold that does not hold | Opus |
| 05 | `05-the-store.md` | model type id 2, its table and factory keyed (M, n, β), the registry, sharding, the `types` maps, `FINGERPRINT_FORMAT` 8 and `VERSION_LABEL` (U6), `compare_stores`, `inventory_report`, `bbn_from_store`, `outward_step_scan`; tests in a temporary store. *(As written 2026-11-16 on P33–P44: also the key's drain model checked at `ScalarHistory`'s construction, and every raw join of a model table filtered by `model_type`.)* | Opus |
| 06 | `06-drivers-and-exports.md` | `main.py`'s model option and its config file; `plot_inputs`, `plot_by_beta`, `plot_ScalarHistory`, `extract_common`, `pipeline_selection` through the model interface; the model in `histories.csv` and in the export paths (P11); the exponential exports byte-identical. *(Added 2026-11-15, P17, P19: the one-loop drain model taken from the model, where they name `DRAIN_MODEL_ONE_LOOP`; the quanta tools' spectrum wall taken from the export's model column.)* *(As written 2026-11-16 on R12 and P45–P58: `--models`, a list, for `main.py` and the plot scripts; each potential's one-loop rows under its own drain model; every export under `<output>/<label>/<model>/` with a `model` column, for every potential; `spectrum_wall` on the model and the quanta tools by row; the registered run's scope; the gate's evidence by two trees on a harness store; Opus, P58.)* | Sonnet |
| 07 | `07-rehearse.md` | registered builds on the isolated cluster: the exponential at base and tip, bitwise; the power law at n = 1, 2 under both laws, interrupted and resumed, by value and against 04 *(As written 2026-11-16 on P59–P72: U7's grid; five stores, the exponential at `a7e0978` and at the tip, the power law straight and resumed, and both potentials in one store; one cluster per tree; the readers and the quanta tools on every store.)* | Opus |
| 07b | `07b-remeasure-k-ex.md` | *(Inserted 2026-11-17 on R13–R17, §6.12.)* Research in `source/kex/`, no production change: the definition and its check against Erickcek et al.; every stage-2 point of crossover and of 02 re-chained, its regime re-classified; new stage-2 points where the exponential's regime moved; the candidate laws for `one-loop-exponential-v2` against them, at n = 1 and 2; the power wall's law shown unchanged; the three spectrum tables and x_* in the new x; the mapped paths re-chained; the inventory of every use of k_ex for 07c. Then U13 | Opus |
| 07b2 | `07b2-measure-the-min-form.md` | *(Inserted 2026-11-18 at the user's word, §6.13.)* Research in `source/kex/`, no production change: the form f = 1 − exp(−min(y_rise, y_fall)), the rise in ln R and L as v1's gap, the fall A/(ℓ − ℓ₀) in the new ℓ with no L term, and its variants, at n = 1 and 2; on three stated selections, like for like beside v1 and 07b's (i)–(iv); parameter errors and leave-one-set-out; new stage-2 points on the history curves and a held-out set predicted before they are fitted; the crossing, the cap and the edges; the paths; the reference implementation. Then U13 | Opus |
| 07c | `07c-k-ex-in-production.md` | *(Inserted 2026-11-17; as written 2026-11-18 on P99–P110, §6.14.)* As 07b's measurements and U11–U13 rule: the third-derivative hook on every model; k_ex = ½E^{1/2}\|V‴/V″\| in `classical_turning_point` and `one_loop_turnaround`; the exponential's law under `one-loop-exponential-v2`, `one-loop-measured-v1` retired; the spectrum tables and x_* re-measured; every other use of the inventory; the label; tests re-pinned with old and new values | Opus |
| 07d | `07d-rehearse-again.md` | *(Inserted 2026-11-17; as written 2026-11-18 on P111–P124, §6.15.)* 07's grid rebuilt at the tip in fresh stores, registered, on the isolated cluster; every history whose law reads no ℓ equal to 07's in its integration and BBN, with only the k_ex columns moving; the exponential's one-loop histories under v2 against 07's under v1, measured; the readers on every store | Opus |
| 08 | `08-close.md` | `.documents/power-law-wall-verification.md`, `architecture-summary.md`, `numerical-strategies.md`, `CLAUDE.md`, the handover for the science run (its grid's inputs from 04), the issues and the indexes | Opus |

Written one at a time, each after the previous one lands and is reviewed. 03 waits for U3–U5, and is dropped if they
rule the law and spectrum carried over unchanged with `_drain_L` alone, which then moves into 04's first commit by
ruling. 06 needs only 05's schema; 08 needs 07's stores. *(Added 2026-11-17:)* 07c waits for 07b's measurements and the
user's U11–U13; 08 needs 07d's stores in place of 07's. *(Added 2026-11-18:)* 07c waits also for 07b2 (§6.13).

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Model** | each hook of `InversePowerModel` equals its closed form (and finite differences of the one below it) across 10⁻⁹⁰ M ≤ φ ≤ 10 M_P at n = 1, 2, with no overflow; the turning point, the minimum and the seed against closed forms; under the one-loop law the model is refused at entry; every exponential golden bitwise; the suites at their counts or above | 01 |
| **Wall** | S, the one-loop fraction and the spectrum measured at n = 1, 2 along the power-law paths, each validated as crossover's stage 2 was (Born limit, classical limit, energy); the adopted law's residuals at each candidate L stated; nothing ruled by the agent | 02 |
| **Law** | the power wall's law as ruled equals its reference implementation at synthetic inputs and reproduces 02's maps candidate by candidate; the exponential law bitwise; the spectrum as R8 rules on every wall *(amended 2026-11-15 by R8)* | 03 |
| **Sample** | every sampled history measured under both laws, re-tuned, with its failures stated; every threshold of §0.3 reported against its use on this wall | 04 |
| **Store** | power-law histories round-trip in a temporary store; the exponential store's fingerprint golden unchanged apart from the format | 05 |
| **Export** | the exponential exports byte-identical to the base's on the same store; the power-law exports carry the model and read back *(amended 2026-11-16 by R12, §6.10: every potential's exports are under `<output>/<label>/<model>/` with a `model` column, so the exponential's equal the base's once that directory and column are accounted for, P54)* | 06 |
| **R** | exponential builds bitwise base to tip; power-law builds resumed by value; inventories "problems: none" *(made exact 2026-11-16 by P64–P68, §6.11: also the power-law builds against 04's tool route, both potentials in one store equal to each alone, and the readers on every store)* | 07 |
| **Remeasure** *(added 2026-11-17)* | the definition reproduces Erickcek et al.'s Eqs. (66) and (71); the old chain reproduces every recorded ℓ; every stage-2 point re-chained and every new point validated as crossover's stage 2 was; the candidate laws' residuals stated at n = 1 and 2 beside v1's; the three tables and x_* measured in the new x; the inventory complete; nothing ruled by the agent | 07b |
| **Form** *(added 2026-11-18)* | the min(rise, fall) form and its variants fitted at n = 1 and 2 on each stated selection, with their parameters, errors and residuals beside v1's and 07b's candidates on the identical point sets; 07b's constants reproduced by the fitting code; the new points validated as crossover's stage 2 was and predicted before being fitted; the paths re-mapped; the reference implementation equal to the fit; nothing ruled by the agent | 07b2 |
| **Consistent** *(added 2026-11-17)* | one k_ex at every use of 07b's inventory; each model's V‴ hook equal to its closed form and to the centred difference of V″; the law as ruled equal to its reference implementation; the spectrum kernels within 10⁻⁷ of quadrature on the new tables; every history whose law reads no ℓ bitwise in its integration by the tool route; the suites at their counts or above | 07c |
| **R2** *(added 2026-11-17)* | 07's grid rebuilt at the tip: the power law (both laws) and the exponential's fixed-law and natural-mode histories equal to 07's in every value but the k_ex columns, which are listed; the exponential's one-loop histories measured against 07's; resumed by value; inventories "problems: none"; the readers on every store | 07d |
| **Close** | documents, issues, indexes and the handover written; the suites at their counts | 08 |

## 3. Stores

- 01–04 open none but temporary SQLite stores in `tempfile` directories; 02 and 04 write text under the campaign's
  `source/` and `.documents/power-law-wall/`.
- 05's tests use the stand-in harness (`_TempStoreCase`), never Ray.
- 07 writes its own stores under `var/datastores/` (`power-law-wall-07-*`) through `main.py`, the registry wrapper and
  the isolated cluster. It reads no science store.
- The 2026.11.0 stores are never opened for writing. Whether this tree can read them is U6.

## 4. The interfaces

- **01 → 02, 04.** The model and the tool route; 02 maps the cascades with the fixed law, which needs no `_drain_L`.
- **02 → the user → 03.** The measurements and the residuals; the user rules the law (U3), S (U4) and the spectrum (U5).
- **03 → 04.** The one-loop law on this wall: f_n(ln R) and R·S_n under `one-loop-measured-pl-v1`, through the model's
  hooks (P15–P16), and the spectrum per wall (R8).
- **03 → 05, 06** *(added 2026-11-15)*. The model names its one-loop drain model (P16). 05 keys and checks a stored
  history's `drain_model` against it, and its store tools (`bbn_from_store`, …) select by it; 06 does the same in
  `main.py`, `plot_inputs.py`, the plot scripts and `config/argument_parser.py`, which name `DRAIN_MODEL_ONE_LOOP` today
  (P17), and has the quanta tools take the spectrum's wall from the export's model column (P19).
- **04 → the user → 05.** The sample; the user rules any threshold that does not hold on this wall.
- **05 → 06 → 07.** The schema, the drivers, the rehearsal stores.
- **07b → the user → 07c → 07d** *(added 2026-11-17)*. 07b's re-measurements and inventory; the user rules the
  exponential law's form (U13), the power wall's drain-model name (U11) and the label (U12); 07c puts one k_ex into
  production with them; 07d rebuilds 07's grid on it, and 08 reads 07d's stores.
- **08 → the science run.** The handover: what a power-law run of record needs, the grid's inputs from 04, the
  standard analyses it repeats and what each needs (convention 8).

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–8 and "Repository mechanics", and:
- **The exponential does not move.** Every exponential history, export and golden record is bitwise as at the base. A
  prompt that cannot keep that stops. *(Amended 2026-11-15 by R8, §6.5: the exponential's spectrum, read only by the
  post-processing, is redone as the power wall's is, so its relativistic-quanta outputs change; nothing stored or
  exported moves.)* *(Amended 2026-11-16 by R12, §6.10: the potentials are treated symmetrically in the exports, so
  the exponential's exports move under `exponential/` and gain a `model` column; every exponential history, stored row
  and golden stays bitwise.)* *(Amended 2026-11-17 by R13–R17, §6.12, from 07c on: k_ex has one definition on every
  potential, so the exponential's k_ex, ℓ, peak estimates and margins move, and so does every one-loop exponential
  history, under its refitted law. What still does not move is stated per prompt: every history whose law reads no ℓ
  (the fixed law, natural mode, and the power wall under both laws) keeps its integration and BBN bitwise.)*
- **The integration stays decoupled from the potential.** Anything whose meaning depends on the wall's form goes in
  the model class, with no generic default; `ComputeTargets/ScalarHistory.py` gains no power-law expression (the user's
  ruling of 2026-11-05).
- **Measurements, not bounds.** 02 and 04 record what they measure. A threshold enters only where the user rules one.
- **Gates hold in any units** (`UnitsLike`); none singles out a column because M_P = 1.
- **The main checkout is not touched while a science run is building from it.** The campaign works in its own
  worktree. 07 takes the rehearsal lock and so waits for any run on this machine to end; a cluster is started from
  the tree that builds (`electroweak-eos` board §3, the worktree's data files).
- **Long runs are detached and recorded** (`CLAUDE.md`, "Long-running jobs").

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 Ruled by the user, 2026-11-13, in their words

- **R1, the form.** "Power law: yes, the intention to is add a cosmological constant. We can write it in the form you
  propose, to maintain the distinction between `V_0` and `M` from the exponential potential." The form proposed was
  V = V₀[1 + (M/φ)ⁿ], V₀ tuned as now, M a parameter (§0.2).
- **R2, n.** "the inverse power law potential with `n=1` and `n=2`, i.e. `V = Lambda^5/phi` and `V = Lambda^6/phi^2`".
  The model takes any integer n ≥ 1; the science grid has n = 1 and 2.
- **R3, the drain.** "Drain: we want this the same as the exponential potential run – the one-loop drain law, with a
  comparison to the fixed `alpha` drain law." So the one-loop law must be defined on this wall, which is 02's and 03's
  work.

### 6.2 For the user to rule (U1–U7)

**Ruled by the user, 2026-11-13:** "U1–U7 as proposed; commit the rebuild plan on main". So:
- **U1:** the campaign is `power-law-wall`, on its branch cut from `main` at `a7e0978` (the 2026.11.0 plan), worked
  in its own worktree; 01–06 may run while the rebuild builds, and 07 waits for it to end.
- **U2:** the 2026.11.0 rebuild goes ahead now, before this campaign's store changes.
- **U3–U5:** the law, S and the spectrum on this wall are ruled after 02, on the options proposed; x_* and the
  associated piece are carried over from the exponential's 2PI run and flagged as unmeasured on this wall.
- **U6:** `FINGERPRINT_FORMAT` 8 and `VERSION_LABEL` 2026.12.0; the tip does not read the 2026.11.0 stores, and the
  models are compared through their exports.
- **U7:** 04's sample is the 40 histories proposed.

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U1** — name, branch, timing | `power-law-wall`, on a branch of that name cut from `main` after the 2026.11.0 plan is committed, worked in its own worktree; prompts 01–06 may run while the 2026.11.0 rebuild builds, 07 after it ends | wait for the rebuild to end before starting |
| **U2** — the order with the rebuild | **rebuild now, as planned.** The exponential's results do not change in this campaign (§5), so a rebuild after it would compute the same histories; the rebuild's figures are wanted first | hold the rebuild until 05 lands, so that one tree reads both models' stores (U6) |
| **U3** — the one-loop law on this wall | **ruled after 02**, from its residuals: (a) the adopted law with an L the model defines, if 02 finds one within the law's accuracy on the exponential (0.07 one-loop, 0.06 gap); else (b) the law refitted on this wall's stage-2 points, as a new `drain_model` value (`one-loop-measured-pl-v1`, say) | (c) the adopted law applied unmeasured, with L = ln(1 + E/V₀), stated as an assumption |
| **U4** — S on this wall | **measured in 02** (stage 1a on the power wall) and tabulated per wall family in 03 | carry n = 1's S(L) over, stated as an assumption |
| **U5** — the spectrum | **x_p and κ measured in 02** from the test field on the power wall, used by wall in 03; **x_* and the associated piece carried over** from the exponential's 2PI run, flagged in every output as unmeasured on this wall | ask for a Kadanoff–Baym run on the power wall (ChamKadanoffBaym) before the science run |
| **U6** — the label and the format | `FINGERPRINT_FORMAT` 8 (a new table) and `VERSION_LABEL` 2026.12.0. **Consequence:** `datastorekit` refuses a store that lacks a declared table (the retire of `science-2026.8.0` on 2026-11-13 failed with this error), so the tip will not read the 2026.11.0 stores; that run is read by its own tree, and the two models are compared through their exports | ask DatastoreKit for a read of a store missing a table the code declares, and move the pin (dk-and-rpk-migration rule 11); or U2's alternative |
| **U7** — 04's sample | **40 histories**, at each of n = 1 and 2: at 4×10⁻³¹ M_P, β 1.2, 2, 2.9 under A = 0.05, 1, 10 and α = 0.01, 0.1 (15); at 10⁻³³ and 10⁻¹⁵ M_P, β = 2 at A = 1 (2); at 10⁻³ M_P, β 1.2, 2, 2.9 in natural mode (3). Each beside the exponential's counterpart from the 2026.11.0 export. The surfing edges by `tools/surfing_scan.py --model`, outside the forty. Detached, a few hours | a smaller sample at laboratory M only |

### 6.3 Proposed by the planner (P1–P14)

Accepted as proposed unless the user rules otherwise.
- **P1 — the class.** `CosmologyConcepts/Models/InversePowerModel.py`, type id `INVERSE_POWER_MODEL = 2`, the coupling
  as the exponential's. ln V = ln V₀ + log1p of (M/φ)ⁿ computed through its logarithm, so that no power overflows at
  φ ≪ M; its derivatives through u/(1 + u) as `_PowerLawWall` writes them. `_log_V_floor` = ln V₀;
  `reflects_at_origin` True. Its `name` and shard key as the exponential's.
- **P2 — the exponential bitwise.** No exponential hook, constant or code path changes. 01 and 03 run the goldens; 05–07
  the fingerprint and export goldens. *(Amended 2026-11-15 by R8, §6.5: except the spectrum's constants and shape in
  `quanta_spectrum.py`.)*
- **P3 — the seed.** At any n, the static and tracking minimum of V₀[1 + (M/φ)ⁿ] + ρ_m0 A in the exponential seed's
  loop, with V′ = −n(V − V₀)/φ in place of n = 1's −V M/φ²; tested at n = 1, 2 against a root-find.
- **P4 — `_drain_L` not in 01.** 01 leaves it unimplemented, so the one-loop law refuses the model loudly until 03 (or
  04's first commit, U3 (a)) defines it.
- **P5 — the tool route.** `tools/history_and_bbn.py` and `tools/surfing_scan.py` gain
  `--model {exponential, inverse-power}` and `--n` (defaults exponential and 1, which leave every current command's
  output unchanged). *(Amended 2026-11-13, writing 01: crossover's `cascade_map.py` is a record of a closed campaign
  and still passes the removed `--gap-prescription`, so it is not changed; 02 maps the paths with its own copy.)*
- **P6 — 02 is research in `source/wall/`.** It copies `walls.py`, `stage1a_classical.py`, `stage2.py` and
  `stage2_validate.py` there, adds `PowerWall(L, n)` in the units walls.py uses, and validates it as crossover's
  stage 2 was. It changes no production code. Its brief is written from crossover's `stage2-brief.md`.
- **P7 — where the paths are mapped.** 02 maps each sampled history's candidates (L, ln R, ℓ) with the fixed law by the
  tool route, so the stage-2 points sit on the paths that histories reach, as crossover's scan did.
- **P8 — the drain-model key.** A refitted law is a new value of `alpha_value.drain_model`, never a change to
  `one-loop-measured-v1`. Which value a model may take is checked at entry, as `_drain_L` is.
- **P9 — 04's laws.** One-loop at A = 0.05, 1, 10 and fixed at α = 0.01, 0.1, as the exponential run's figures use;
  natural mode only at large M (the user's ruling of 2026-11-07 for the exponential, carried over).
- **P10 — the thresholds are reported, not changed.** 04 reports each threshold of §0.3 on this wall (how many
  histories reach it, how far each sits from it). A change is the user's ruling, made in a prompt of its own.
- **P11 — the export layout.** `histories.csv` gains a `model` column, and the per-(M, n) folders are named by the
  model (`<model>/M=…eV_n=…`) for the power law only, so that the exponential's paths and bytes are unchanged and the
  standard analyses read both. 06 checks this against the four 2026.11.0 analyses' readers.
  *(Amended 2026-11-16 by R12, §6.10: every potential's exports are under `<output>/<label>/<model>/`, and every
  `histories.csv` has the column; no potential keeps a layout of its own.)*
- **P12 — the config file.** `inverse_power.yaml` beside `exponential.yaml`, naming the model; `main.py` and the plot
  scripts take the model from it, and the stored model is matched as the stored α are (`PlotOptionNotStored`).
- **P13 — the format.** 05 bumps `FINGERPRINT_FORMAT` to 8 and the fingerprint covers the new table; the golden
  fingerprint is regenerated with a statement of what moved (the format and the new class's empty entry only).
- **P14 — the counts.** Every prompt records the five suites' counts before and after; none falls.

### 6.4 Ruled by the user after 01, 2026-11-14, in their words

On prompt 01's two measurements (log 01 §3.6 and §7; board, "Orchestrator review of prompt 01"):
- **R4, φ ≤ 0.** "Keep φ ≤ 0 as a failure." The hooks of `InversePowerModel` raise `ComputationFailureError` at
  φ ≤ 0, as 01 committed them, and no continuation to φ < 0 is adopted. The even continuation V(|φ|) was measured in
  scratch and let two histories accept a step at φ < 0.
- **R5, natural mode at n = 1, large M.** "The `n=1` failures in natural mode – if that's what the model does, I am not
  sure what we can do about it. This is natural mode which is less important for us. I suggest we simply note it and
  move on." The three histories at 10⁻³ M_P (β 1.2, 2, 2.9) fail at the integrator's guard G2. This is recorded as a
  measurement, with no issue opened and no fix planned. The planner reads it as also covering 04: its natural-mode rows at large M (U7) record the same
  failure where it recurs, and do not stop on it.

### 6.5 Ruled by the user after 02, 2026-11-15, in their words

On prompt 02's measurements (verification §2.10; board, "Orchestrator review of prompt 02"):
- **R6 (U4), S on this wall.** Asked whether to take S₁ and S₂ as constants, the user ruled: "yes, just use the
  constants". On the power wall the law's classical branch is f = R·S_n, with S₁ = 1.02620×10⁻² and
  S₂ = 5.21615×10⁻³ (stage 1a, `.documents/power-law-wall/wall-02/stage1a_S_n.txt`), constant in x; this replaces the
  exponential's S(L) table on this wall only. The exponential's classical branch is unchanged.

- **R7 (U3), the law on this wall.** Shown the candidate laws against stage 2's fraction on the three curves, and a
  fit of that fraction in ln R alone, per n, the user ruled: "yes, I'd like to use the `f_n(ln R)` fit". The measured
  fraction depends on ln R and n alone (validate V6), so on the power wall the drain fraction is a function f_n(ln R)
  for each n, above the classical branch R·S_n (R6); it does not read ℓ or an L. The orchestrator's scratch fit, shown
  to the user and not committed, was two branches in ln R switched where they cross (at the peak, ln R ≈ 23.1 for
  n = 1 and 16.7 for n = 2): f = 1 − exp(−G (ln R − R₀)^P) below, f = 1 − exp(−A/(ln R − c)) above. Over the 62 | 60
  non-classical points where the field came back out its largest miss was 0.054 | 0.056 (rms 0.019 | 0.022), within
  the exponential law's own accuracy (0.074 one-loop, 0.062 gap); over the mapped histories' points, 0.004 | 0.003.
  03 fits and records the law; it is a new `alpha_value.drain_model` value (P8), defined for n = 1 and 2 only, and
  refused at entry for any other n.

- **R8 (U5), the spectrum.** Asked whether the test field's spectrum should be described in three regions, the user
  ruled: "Yes, this is the design I want. The three-region fit you describe sounds correct, and also the universal
  `q^2` power law at low `q` (neglecting the `q^2 ln q` term)." Earlier, conditional on that design: "(a) yes, it
  makes sense to do the same for the exponential". So, for each wall (the exponential, and the power wall at n = 1
  and n = 2), the direct piece of `quanta_spectrum.py`'s spectrum replaces the shape x² up to x_p and
  e^{−κ(x − x_p)} above with:
  - **the peak region**, a table of the test field's measured |β_k|², interpolated, running from k/ω_t = 10⁻³ (in
    the orchestrator's scratch probe, the solver's precision floor is near 10⁻⁴) out to an upper edge far enough
    above the peak that the tail carries well under 1% of the energy x³|β|² (x ≈ 5 or beyond; 03 measures the
    share);
  - **below the table**, k², matched in value at the table's lower edge, on every wall. The probe found the local
    power tending to 2 on all three walls (2.000 on the exponential at k/ω_t = 10⁻³; 1.989 at n = 2, with a
    shortfall linear in k; 1.66 at n = 1, with a shortfall (2 − p) ln(1/k) ≈ 2.3, as k² ln²(1/k) would give); the
    logarithm at n = 1 is neglected, by the user's ruling. Prompt 02's slopes below the peak (1.34, 1.76) were fitted
    on k/k_ex ∈ [0.02, 0.1], short of the asymptotic region;
  - **above the table**, an exponential matched in value and slope at the table's upper edge.

  The associated piece (x_*) is carried over from the exponential's 2PI run and flagged as unmeasured on the power
  wall, as U1–U7 ruled. The exponential's spectrum is redone in the same way, so `SPECTRUM_MODEL` (`U4-U17-v1`)
  changes. **This amends §5's rule "The exponential does not move", P2 and the gate Law, for the spectrum alone:** the
  spectrum is read only by the post-processing (`tools/quanta_measures.py`, `tools/plot_quanta_history.py`,
  `tools/plot_quanta_grid.py`, `tools/quanta_figures.py` and their tests), so every exponential history, store,
  export and golden record stays bitwise, while the exponential's relativistic-quanta outputs change. Under
  convention 8, a rebuild after this campaign repeats the standard analysis "Relativistic quanta" (`science-runs/
  README.md`) for the exponential as well as the power law; its inputs (`histories.csv`, `fields.csv`, `bounces.csv`)
  do not change, so its row there is not amended.

U3, U4 and U5 are all ruled; 03 is written from R6–R8.

### 6.6 Proposed by the planner with prompt 03, 2026-11-15

**P15–P23** stand unless the user rules otherwise. U8 and U9 were the user's to rule before 03 is dispatched.

**Ruled by the user, 2026-11-15, in their words:**
- **R9 (U8), the spectrum's scale near the floor.** "U8 (a) as proposed. What happens late in the cascade is not of so
  much concern. These diagnostics are only expected to be approximate." One table per wall in x = k/k_ex, at the
  scale-free reference; 03 measures the error in x over E/V₀, reported and not acted on.
- **R10 (U9), the associated piece on the new shape.** "U9 accept as proposed." 1/x up to the table's x_p, then the
  direct piece's own fall; C₁ = 49 x_* s_dir(x_*); x_* = 0.0058 in x on every wall, flagged as unmeasured on the power
  wall.

The table below is the proposal as it was put.
- **P15 — the law's home.** The power wall's constants (f_n's and S_n) and its fraction are in `InversePowerModel`, each
  constant with its source. `ScalarHistory.py` gains no power-law expression (§5); `drain_fraction`, its constants and the
  S table stay the exponential family's law (the "L law"), unchanged.
- **P16 — the hooks.** `AbstractModel` gains, with no generic default, the model's own fraction and the model's one-loop
  drain model. A model defines exactly one of `_drain_L` and its own fraction. `ExponentialModel` keeps `_drain_L` and
  names `one-loop-measured-v1`; `InversePowerModel` defines its own fraction and names `one-loop-measured-pl-v1` at
  n = 1 and 2, and its one-loop law is refused at entry at any other n (R7). This replaces P4's "03 defines `_drain_L`":
  the power wall has no L.
- **P17 — the drain models.** `one-loop-measured-pl-v1` joins `DRAIN_MODELS` after the two there, with the one-loop law.
  `model_of_law` is written out and still returns `one-loop-measured-v1`. The call sites that name
  `DRAIN_MODEL_ONE_LOOP` (`main.py`, `plot_inputs.py`, the plot scripts, `config/argument_parser.py`, `bbn_from_store`)
  are 05's and 06's to make model-aware (§4).
- **P18 — the fit.** f_n(ln R) as the user was shown it (orchestrator note 03 §0): y = −ln(1 − f), a rising branch
  G (ln R − R₀)^P and a falling branch A/(ln R − c), switched where they cross; R·S_n at ln R ≤ 0; 0 unassessed. Fitted
  by 03 in `source/wall/fit_fn.py` on 02's points, whose reference implementation shares production's literals.
- **P19 — the spectrum by wall.** `quanta_spectrum.spectrum(wall, assoc_scale)` with the wall required (no default): the
  exponential (its table serving the exponential family at every n, as today) and the power wall at n = 1, 2. The tables
  are literals in the module. `SPECTRUM_MODEL` changes and every output names the wall's spectrum. The quanta tools pass
  the exponential's wall until 06 takes it from the export's model column.
- **P20 — the kernels.** Moments and kernels by quadrature on the new shape, every asymptote re-derived, each kernel
  within 10⁻⁷ relative of its quadrature reference, as one-loop-drain 05 required.
- **P21 — the exponential's reference.** Its table is taken at L = 90 (`KAPPA_FIT_L` today), on `WallN(90, 1)` with
  k_ex exact; its shape over L = 40–134 is measured and reported.
- **P22 — the tests that move.** Only assertions of the three quanta test modules are re-pinned, each listed with its old
  and new value; no test that pins an exponential history, golden or fingerprint is edited.
- **P23 — the tool route.** 03 runs the one-loop law at laboratory M, β = 2, A = 1 on n = 1, n = 2 and the exponential;
  the exponential's output is identical to the base's.

| | Proposed | Alternatives |
|---|---|---|
| **U8** — the spectrum's scale on the power wall near the floor | **(a) one table per wall in x = k/k_ex, at the scale-free reference (E/V₀ → ∞)**, as R8 reads literally and as the exponential does today at one L. On this wall the bounce in k/ω_t is the same at every E/V₀; only k_ex/ω_t = (s_t/2)√(n/(n + 1)), s_t = E/(V₀ + E), moves with the floor, so the table's x is off by the factor s_t: x_p is 0.466 at s_t = 1, 0.533 at E/V₀ = 6.9 and 0.966 at 0.93 (n = 1; 02's `spectrum.txt`). The first, energetic bounces have s_t = 1; the floor matters in the late cascade only. 03 measures the error in x over E/V₀ and 04 the share of K from generations with E/V₀ < 100 | **(b) the table in k/ω_t**, and each classical or gap generation's scale converted by its own k_ex/ω_t, from a model hook evaluated at the generation's ln E and the history's normalization (one-loop generations keep the turnaround's k_ex, as U16 ruled). Exact for the classical spectrum on this wall; it puts a model hook into the post-processing, which then needs the model (06's model column) for the power wall, and changes the exponential's generations too, where k_ex/ω_t moves with L (0.4945 at L = 90, ½ as L → ∞) |
| **U9** — the associated piece on the new shape | **1/x up to the table's x_p, then the direct piece's own fall**, s_assoc = (C₁/x)·s_dir(x)/s_dir(x_p), which is U17's form exactly when s_dir is U4's; **C₁ = 49 x_* s_dir(x_*)**, so that s(x_*) = 50 s_dir(x_*) at c_a = 1 as U4 put it (49 x_*³ when s_dir = x² there); **x_* = 0.0058 in x on every wall**, the module's constant, flagged as unmeasured on the power wall | above x_p, the tail exponential's κ_hi from x_p (U17's form with κ replaced); C₁ = 49 x_*³ kept as a number; x_* carried in k/ω_t (0.0029 ω_t, the 2PI run's probe), which gives x_* = 0.0082 (n = 1) and 0.0071 (n = 2) on the power wall |

### 6.7 Proposed by the planner with prompt 04, 2026-11-15

After 03 the user ruled on D12 (log 03 §2.2), in their words: "No need to argue about D12. Leave it as it is." The six
tests 03 re-pinned outside the quanta modules stand.

**P24–P32** stand unless the user rules otherwise. None needs a ruling before 04 is dispatched.

**Ruled by the user, 2026-11-15, in their words:** "P24–P32 as proposed; don't yet dispatch prompt 04, though: I'll do that
in a fresh session."

The list below is the proposal as it was put.
- **P24 — the runner.** 04 runs each history through `tools/history_and_bbn.py`'s own `main`, in-process, with
  `integrate_scalar_history` wrapped to keep the final `IntegrationResult` (as 02's `path_map.py` does), in
  `source/sample/sample_run.py`. It prints the tool's lines unchanged, then a per-candidate table. Its tool lines are
  checked against a plain run of the tool at three histories.
- **P25 — the counterparts.** The exponential's twenty rows are read from the 2026.11.0 main export's `histories.csv`
  (a copy, never the main checkout) and committed as an extract with the source's SHA-256. The tool route is checked
  against three of them, so that the comparison is between walls and not between routes.
- **P26 — the overlaps.** Fourteen of the forty were run by 01 or 03. Each is compared with its committed output,
  timings masked, and a difference is a stop.
- **P27 — R9's share.** The share of K from candidates with E/V₀ < 100 and < 10³, from each injection's assessed ln E less
  the tuned normalization, with the table's error 1/s_t at the smallest x reached. Planner's expectation, from 02's
  maps: zero, since every mapped candidate has ln(E/V₀) ≥ 33 and the parking cut ends the cascade there.
- **P28 — the thresholds.** Each of §0.3's is read off the forty as the prompt lists it (§1.4). Reported, not changed
  (P10).
- **P29 — the surfing edges.** By `tools/surfing_scan.py --model inverse-power` at M = 10⁻³ M_P, where electroweak-eos
  measured the exponential's: a coarse grid at 0.05, then 0.001 grids at the onset of QCD capture, and from the last
  QCD capture to the first EW capture, per n. The exponential's reference is electroweak-eos 02's scans on the same w
  table, checked at three rows, plus a fine grid at its QCD onset, which was never measured. Rows that fail at G2 (R5)
  are kept and the edges reported around them. The edges at laboratory M are not scanned.
- **P30 — M_crit on this wall.** Estimated from 02's path formula and the sample's own ln x and ln(V₀/M⁴), with no history
  added: the M at which ln R reaches 0 at the first impact and at the smallest ln R of a cascade.
- **P31 — no production change.** 04 writes only under `prompts/power-law-wall/` and `.documents/`, and the board.
- **P32 — the counts.** The suites run in full at the base. Since the commit changes nothing they import, the counts after
  are taken by discovery (P14).

### 6.8 Ruled by the user after 04, 2026-11-16, in their words

On prompt 04's measurements (verification §4.9; board, "Orchestrator review of prompt 04"):
- **R11, the surfing thresholds at n = 1.** Asked whether R5's "note it and move on" also covers the n = 1 surfing scan
  at 10⁻³ M_P (74 of 79 coarse β failing, 73 of them at G2; the QCD onset bracketed only in (1.10, 1.30]; no EW edge),
  the user ruled: "I'm not going to chase this now. Note it and move on." That is option (a) of verification §4.9: the
  scan is recorded as measured, no further scan is run, no issue is opened and no fix is planned. The science run's
  surfing figure and any standard analysis reading the edges at large M have no n = 1 EW edge, and an onset known only
  to (1.10, 1.30]. At n = 2 the exponential's edges hold. Every other threshold of §0.3 holds on this wall as measured,
  so none is changed before 05.

### 6.9 Proposed by the planner with prompt 05, 2026-11-16

**P33–P44** stand unless the user rules otherwise. None needs a ruling before 05 is dispatched; there is no U question.
They rest on the planner's scratch probe at `1233229` (orchestrator note 05 §0).

- **P33 — the factory.** `ObjectFactories/InversePowerModel.py`, table `InversePowerModel`, keyed (M, n, β) with the
  exponential's columns and spec, building `InversePowerModel`. A module of its own; `ObjectFactories/ExponentialModel.py`
  is not changed.
- **P34 — the registry.** Registered immediately after `ExponentialModel`, batch size 50, sharded by `shard_key` (β), in
  no drop group, as the exponential. The probe found that, so placed, an exponential-only store's fingerprint differs
  from format 7's only by the format, the new class's empty entry and the top digest.
- **P35 — the polymorphic key.** `ScalarHistory`'s spec maps type id 2 to `InversePowerModel`; `compare_stores` moves
  id 2 from `UNSTORED_TYPE_VALUES` into `POLYMORPHIC_REFERENCES` (01's log §7, the user's ruling of 2026-11-14);
  `inventory_report` lists the class under "Models".
- **P36 — the key's drain model at entry** (§4: "05 keys and checks a stored history's `drain_model` against it").
  `ScalarHistory`'s constructor refuses an α whose drain model is neither `fixed` nor the model's
  `one_loop_drain_model`, through the model interface. Today only the compute-path guard refuses such a key, after the
  integration. The guard stays.
- **P37 — joins filter the model type.** Every raw SQL join of a model table in production code filters `model_type`.
  The probe found the first `InversePowerModel` row at serial 1, as the first `ExponentialModel` row, so a join on the
  serial alone pairs a power-law history with an exponential row. Records under `.documents/` and `science-runs/` are
  not changed.
- **P38 — `bbn_from_store`.** `--model` and `--n` as `tools/history_and_bbn.py`'s (defaults exponential, 1); its
  queries join that model's table and filter `model_type` and `n_value`; under the one-loop law the drain model is the
  model's own. Every existing command's output is unchanged. (The exponential's science stores hold n = 1 only;
  `--n-values` defaults to [1].)
- **P39 — `outward_step_scan`.** Stays exponential-only, as a reflection-restart diagnostic that re-runs rows as
  `ExponentialModel` at n = 1: its store read filters `model_type = EXPONENTIAL_MODEL` and counts other models' rows with
  those it already skips. No `--model` option.
- **P40 — the golden.** `build_store` is unchanged (the exponential rows it holds today); the new golden file
  `store_fingerprint_power-law-wall-05.json` differs from format 7's only by the format, the empty `InversePowerModel`
  class and the top digest, shown by a diff. The power-law rows are exercised by the round-trip tests, not the golden.
- **P41 — the label.** `VERSION_LABEL` 2026.12.0 and `FINGERPRINT_FORMAT` 8 (U6). The paragraph states that this tree
  cannot read a 2026.11.0 store: the probe found DatastoreKit's `StoreSchemaMismatch` from the inventory and the
  fingerprint, and a `RuntimeError` from the pool on reopening, for a store lacking the new table.
- **P42 — the tests.** Through `_TempStoreCase`: the factory; the two models apart at the same (M, n, β) and the same
  serial; one real tuned power-law round trip under each law at laboratory M, β = 2 (fixed α = 0.1, one-loop A = 1);
  the entry check; the inventory, `compare_stores`, `inventory_report`, `outward_step_scan` and `bbn_from_store` on a
  mixed store; the configuration, the format guard and the golden.
- **P43 — the exponential bitwise.** The goldens, `test_tuned_history_round_trip`, `test_drain_store` and
  `test_dissipation_store` pass with no change but the label and format literals; the exponential's tool route at
  laboratory M, β = 2, A = 1 equals 04's file with timings masked.
- **P44 — the counts.** The five suites in full at the base and after (P14); `CLAUDE.md`'s counts updated, nothing else
  in it.

### 6.10 Proposed by the planner with prompt 06, 2026-11-16; re-planned on R12 the same day

**R12, ruled by the user 2026-11-16, in their words** (on U10, the export layout, as first proposed below): "We don't
want to start privileging one kind of potential over another. We want to treat all potentials symmetrically. Having a
dedicated root is the wrong choice unless all potentials get it. So `<output>/<label>/inverse-power` is OK, but then we
would also have to have `<output>/<label>/exponential`." Asked whether the concern was that one science store can hold
both potentials, they added "(and indeed likely eventually will – we'll run both together)", and ruled: "Yes, re-plan
06 on the symmetric design". So:
- **Every potential is named the same way.** No potential has a layout, column or default path of its own. An export of
  any potential is `<output>/<label>/<model>/…`, where `<output>` is `--output` and `<label>` the cosmology's label
  (`QCD_Cosmology`). Every `histories.csv` has a `model` column.
- **One run can build, and one plot run can export, several potentials.** `--models` takes a list, as `--n-values` does.
- **This amends P11, the gate Export and README §5 for the exports alone.** The exponential's exports move one directory
  down (`exponential/`) and gain a column. Every exponential history and stored row, and every golden, stays bitwise. The
  gate Export becomes: each exponential file equals the base's once the directory and the column are accounted for (P54).
- U10 is closed by R12; neither (a) nor (b) is adopted.

**P45–P58**, as re-planned on R12, stand unless the user rules otherwise. None needs a ruling before 06 is dispatched.
They rest on the planner's survey and scratch probe at `3ce578d` (orchestrator note 06 §0).

- **P45 — the option.** `--models {exponential, inverse-power} …`, `nargs="+"`, in `config/argument_parser.py`, so that
  `main.py` and both plot scripts have it; YAML key `models`; distinct values in the order given. **The default is
  `[exponential]`**, so that every existing command and config file keeps its meaning; that default is the one place a
  potential is named without being asked for, and the help says so. `--potential-type` stays removed. The values are
  the tools' `--model` values (`exponential`, `inverse-power`), named once, with their classes, in a new module
  `CosmologyConcepts/Models/model_options.py`. `tools/history_and_bbn.py`, `tools/surfing_scan.py` and
  `tools/bbn_from_store.py` keep their own single `--model`, unchanged.
- **P46 — the config files (P12).** `exponential.yaml` gains `models: [exponential]`, and `inverse_power.yaml` is
  `exponential.yaml`'s keys with `models: [inverse-power]`, so that each file names its potential.
- **P47 — the one-loop rows (P17).** Each potential's `--A-values` rows are looked up under its own
  `one_loop_drain_model`, read through the model interface from a model built at each n of `--n-values` (at M = M_P and
  β = 1, which the hook does not read; log 05 D6). The fixed rows are shared by every potential. With `--A-values`
  given, a potential whose n's name more than one drain model, or none (`inverse-power` at n ∉ {1, 2}), is refused at
  parse, exit 2, before any Ray, naming `--A-values`, `--models` and `--n-values`. `drain_strength_A` checks the one-loop
  law's rule on A (`check_drain_law` under `DRAIN_LAW_ONE_LOOP`), which accepts and refuses the same values as today.
- **P48 — `main.py`.** For each potential of `--models` in the order given: its (M, n, β) rows in its table, built as
  today, and one pipeline per alpha row of its own (the fixed rows, then its A rows). A run of one potential builds its
  rows, keys and pipelines in today's order, so an exponential run's store is bitwise as at the base (07's gate R). The
  grid's line is printed once per potential; for the exponential it is today's line.
- **P49 — `plot_inputs.py`.** The models of the grid are looked up per potential, in its table, by one generic function;
  `exponential_models` stays as the exponential's call of it, so its tests do not move. A potential of `--models` with
  no row at any stored (M, β) is `PlotOptionNotStored` naming `--models`. The A rows are looked up per potential under
  its own drain model.
- **P50 — the export root.** Every product of a plot run goes under `<output>/<label>/<model>/`, for every potential,
  through one helper of `extract_common.py`; below it nothing changes (`alpha=…/`, `<drain_model>/A=…/`, `plots/`,
  `csv/`, `M=…eV_n=…`, `beta=…/M=…eV_n=…`, the file names). Each potential is exported in a pass of its own, since
  `plot_by_beta.py` groups by (M serial, n), which two potentials at the same M would share.
- **P51 — the `model` column.** Every `histories.csv` has `model`, directly after `n`, holding the option's value. It is
  a `histories.csv` of one potential, at that potential's root. A reader that needs the potential (the quanta tools)
  refuses a file without the column, naming it, rather than assuming one.
- **P52 — the spectrum's wall is the model's.** `AbstractModel` gains `spectrum_wall`, with no default, declared as
  `one_loop_drain_model` is (the base raises `NotImplementedError`; not an `abstractmethod`): `ExponentialModel` names
  `quanta_spectrum`'s exponential wall at every n (its table serves the exponential family, P19), `InversePowerModel`
  the power wall at n = 1 and 2, raising at any other n. No value of the integration reads it.
- **P53 — the quanta tools (P19).** `tools/quanta_measures.py`, `tools/plot_quanta_history.py`,
  `tools/plot_quanta_grid.py` and `tools/quanta_figures.py` take each row's wall from its `model` and `n` through P52's
  hook, and build one spectrum per wall met. `SPECTRUM_WALL` goes. A row whose potential has no wall at its n is refused
  and counted with the refusals. `EXPORT_ROOT` is a potential's root, `<output>/<label>/<model>/`.
- **P54 — the gate Export (amended by R12)** and its evidence, as one-loop-drain 04 made it
  (`.documents/one-loop-drain/export-04/`): a mixed store built through 05's test harness (no Ray), exported by the base
  tree and by the tip. **The exponential:** every file of the base's `<label>/<rest>` equals the tip's
  `<label>/exponential/<rest>` byte for byte, and no other file is there, except `histories.csv`, which equals the base's
  on every base column, row for row and byte for byte, with `model` after `n` holding `exponential`; the quanta tools'
  outputs on the two exponential roots are equal, timing fields aside. **The power law:** its root, its column, its rows
  equal to the stored records, and `quanta_measures` measuring each with its own wall. The scripts and their outputs in
  `.documents/power-law-wall/export-06/`, recorded in `.documents/power-law-wall-verification.md` §5.
- **P55 — the 2026.11.0 analyses' readers** (P11). `fig6`, `fig6b`, `m-trend` and `changes` of
  `science-runs/2026-11-13-science-2026.11.0/`, read at `main` by `git show` (not on this branch; the main checkout is
  not touched). For each, the log records what it reads and whether an export of 06 gives it. None is run or changed.
  Under convention 8 the standard analyses' inputs keep their names and columns; only the export path gains the
  potential's directory, which `science-runs/README.md` does not name, so its rows are not amended.
- **P56 — no store change.** No table, column or key moves: `FINGERPRINT_FORMAT` stays 8 and `VERSION_LABEL` 2026.12.0.
- **P57 — the registered run's scope.** `RunDrivers/registered_run.py`'s `describe_scope` names the potentials
  (`models <values>`) for every run, after the config file's part. Every manifest written from 06 on changes; the
  manifests on disk are records and are not touched.
- **P58 — the tests, the model and the counts.** Through the stand-in cluster and temporary stores, never Ray: the
  option and the YAML files, P47's rows and refusals, P48's order, P49's lookups and refusal, the root and the column
  for both potentials, P52's hook, the quanta tools on a root of each potential and on a file without the column, P57's
  scope. Existing tests change only where they pin a string, path or column this prompt changes, each listed with its
  old and new value. 06 is written for **Opus**, not Sonnet as §2's table proposed: it spans the parser, three drivers,
  the exports, four quanta tools and a two-tree comparison, and the exponential's histories must not move in any of
  them. The five suites in full at the base and after (P14); `CLAUDE.md`'s counts updated, nothing else in it.

**U10, as first proposed** (closed by R12; recorded because R12 answers it):

| | Proposed | Alternatives |
|---|---|---|
| **U10** — the export layout (amends P11) | (a) a root of its own for the power law, `<output>/<label>/<model>/`, with a `model` column in its `histories.csv` only, and the exponential's exports untouched | (b) P11 word for word: `histories.csv` gains `model` for every model; the power law's per-(M, n) folders are `<model>/M=…eV_n=…`; `histories.csv` stays at `<output>/<label>/`, so two potentials' plot runs into one `--output` overwrite each other's |

### 6.11 Proposed by the planner with prompt 07, 2026-11-16

**P59–P72** stand unless the user rules otherwise. None needs a ruling before 07 is dispatched; there is no U question.
They rest on the planner's survey at `c7ba5ca` (orchestrator note 07 §0).

- **P59 — the base.** The campaign's base, `a7e0978` (the merge base with `main`: `VERSION_LABEL` 2026.11.0,
  `FINGERPRINT_FORMAT` 7), checked out in a scratch worktree with a `venv` link only, and removed before the commit. The
  registry places runs and stores in the main checkout's `var/` from any worktree (P61), so no `var` link is needed.
- **P60 — one cluster per tree.** Each tree's builds and plot runs go through a cluster started from that tree's root,
  `electroweak-eos` 02's practice for its open issue `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`:
  the base's first, stopped, then this worktree's. A code probe before each tree's first build prints, from the driver,
  a task and an actor, the module files, the label and format, the working directory, `PYTHONPATH`, and the SHA-256 of
  the two data files read by relative path. The data files are the same at both commits, so any mismatch is a stop.
- **P61 — where things go.** The registry resolves its roots to the main checkout's `var/runs` and `var/datastores`
  from this worktree (the planner's probe), so the stores are `<main>/var/datastores/power-law-wall-07-<store>.db` and
  the run records `<main>/var/runs/power-law-wall-07-*`, where `runregistrykit list` finds them (`CLAUDE.md` rule 1).
  Launch logs, exports and comparisons go under this worktree's `var/power-law-wall/07/`. No tracked file of the main
  checkout is read or written.
- **P62 — the grid is U7's sample.** Each store is built by three registered runs, at φ* = 5 M_P with the config file's
  tolerances and 16 shards:
  - `a`: M = 4 × 10⁻³¹ M_P, β 1.2, 2.0, 2.9, α 0.01, 0.1 and A 0.05, 1, 10;
  - `b`: M = 10⁻³ M_P, β 1.2, 2.0, 2.9, α = 0 (natural mode);
  - `c`: M = 10⁻³³ and 10⁻¹⁵ M_P, β = 2.0, A = 1, `--no-alpha-values`.

  That is 20 histories per potential and n, exactly 04's forty for the power law and their twenty exponential
  counterparts. Five stores:

  | store | tree | potentials | n | histories |
  |---|---|---|---|---|
  | `base` | `a7e0978` | exponential (`exponential.yaml`) | 1 | 20 |
  | `exp` | tip | exponential (`exponential.yaml`) | 1 | 20 |
  | `pl` | tip | inverse-power (`inverse_power.yaml`) | 1, 2 | 40 |
  | `pl-resumed` | tip | inverse-power, run `a` interrupted and resumed | 1, 2 | 40 |
  | `mixed` | tip | both, `--models exponential inverse-power` | 1 | 40 |

  04 measured 3–97 s per history by the tool route, so each run is minutes.
- **P63 — the interruption.** `pl-resumed`'s run `a` is sent `SIGTERM` by `interrupt_when.py` during the A = 0.05
  pipeline, whose histories' walls spread most (16–32 s in 04), with the window, the delay and the miss rule of
  `one-loop-drain` 06, read from `pl`'s run `a`. Then `--prune-unvalidated`, runs `b` and `c`.
- **P64 — R (i), the exponential bitwise.** Every history of `base` matched once in `exp` by its key, and every column
  both schemas hold compared by `float.hex`, as `one-loop-drain` 06's `compare_base.py` did. A difference is a stop:
  between `a7e0978` and the tip no exponential code path changed (P2). Beside it, the two sidecars' fingerprints class by
  class: equal in count and digest for every class both name, and `InversePowerModel` empty in `exp`'s (P40).
- **P65 — R (ii), the power law resumed.** `tools/compare_stores.py` on `pl` and `pl-resumed`: `RESULT: equal` and
  `problems: none`; the two fingerprints equal at format 8.
- **P66 — R (iii), against 04.** Each of `pl`'s and `pl-resumed`'s forty histories against 04's output for it
  (`.documents/power-law-wall/sample-04/runs/<name>.txt`): its outcome and failure reason; the tuning's values, which
  the tool prints in full, bit for bit; every other value the store holds at the digits the line prints; every candidate
  row by `candidate_index`. `exp`'s three histories that 04 ran by the plain tool (`tool-route/exp-*.txt`) the same
  way. 04's twenty exponential counterparts from the 2026.11.0 export are compared with `exp`'s export as a measurement
  only, since that run's options were its own.
- **P67 — R (iv), two potentials in one store** (R12: "we'll run both together"). `mixed`'s exponential histories
  equal `exp`'s, and its power-law histories `pl`'s at n = 1, bitwise as P64. Its plot run with both potentials
  writes two roots: the exponential root's CSV files equal `exp`'s byte for byte, and the power law's equal `pl`'s for
  every n = 1 file, with `histories.csv` equal to `pl`'s n = 1 rows.
- **P68 — R (v), the readers.** Unregistered, each store's SHA-256 unchanged by every one: the inventory on every store
  (`problems: none`); both plot scripts with `--no-plots` on every store, the base's by the base tree, and the base's
  exports against `exp`'s by 06's rule (`exponential/` and `model` accounted for); one plot run with figures, on `pl`; the
  refusals of a potential or an A that the store does not hold; `tools/quanta_measures.py` on every root of the tip;
  `tools/plot_quanta_grid.py` on `pl`'s root, whose refusal of two n at one cell (board §3) is recorded, not a stop;
  `tools/bbn_from_store.py` re-solving one stored history per potential and n.
- **P69 — the measurement.** As `one-loop-drain` 06's §3: the probe, the runs, the interruption, the heartbeat silences,
  the cost per store and per history (the power law's beside the exponential's), the stores' sizes, the readers, a look at
  the power law's first figures under Ray. In `.documents/power-law-wall-verification.md` §6.
- **P70 — the suites.** In full at the base. No code changes, so the counts after are taken by discovery, as 04's were
  (P32).
- **P71 — the scripts.** Under `.documents/power-law-wall/rehearse-07/`, adapted from `one-loop-drain` 06's
  `rehearse-06/` and `electroweak-eos` 02's `effect/`, each adaptation shown by a diff in `out/`. They read a store only
  by `sqlite3` `mode=ro`. `out/` holds small text only.
- **P72 — no code change; one commit.** No code, test, golden or configuration changes. A run not ended 60 minutes after
  its launch is a stop. Opus, as §2 plans.

### 6.12 The definition of k_ex: ruled by the user, 2026-11-16 and 17, and re-planned 2026-11-17

**What was found.** On 2026-11-16 the user passed on a ruling from the agent writing up the work: k_ex is the inverse
duration of the mass pulse V″(φ̄(t)) at the turnaround, k_ex = ½E^{1/2}|V‴/V″| (Erickcek et al., arXiv:1310.5149,
Eq. (64)), whereas `one_loop_turnaround` takes λ_ta = −d ln V/dφ, and `classical_turning_point` λ_t likewise. The planner
checked the paper: Eq. (64) is k_ex = ½[V_D′V‴/(2V″)]^{1/2} at φ̄_ta, where V_D(φ̄_ta) = φ̇²/2; with the dressing
V_D ∝ (V″)², as the one-loop turnaround takes it, V_D′ = 2E V‴/V″ there and Eq. (64) becomes ½E^{1/2}|V‴/V″|. It
reproduces their Eq. (71) on the power wall and Eq. (66) to leading order in (φ̄_ta/M)ⁿ on the exponential. New over old
is exactly (n + 2)/n · (1 + u)/u on the power wall, u = (M/φ)ⁿ, and 1 + O(1/u) on the exponential (at n = 1, 1.044 at
u = 90, 1.099 at 40, 1.38 at 10).

**Ruled by the user, in their words:**
- **R13, the scope.** "The V''' form applies to every potential type."
- **R14, the exponential's law.** "Drain law: yes, we need to change the drain law." Then: "[Drain] law for the exponential
  potential: agree, needs a refit. We get a new drain model. We are going to need to distinguish the exponential drain
  laws from the inverse power law ones. Presumably `one-loop-exponential-v2`?" The planner reads the name as the user's,
  and the power wall's as U11.
- **R15, one k_ex.** "there is only one `k_ex`. If we change it, we change all occurrences of it." And: "Code change:
  yes, we want to be sure that everywhere we use `k_ex`, we are using a consistent definition for it." So the classical
  turning point's k_ex and ℓ_t, the one-loop turnaround's k_ex and ℓ, the spectrum tables' x = k/k_ex and every constant
  measured in that x take the new form.
- **R16, the classical turning point.** "this isn't a judgement call. As far as I can see, the correct answer is that the
  potential difference should absorb all the KE. I don't actually see where V' comes into that at all." φ_t is the root
  of V(φ_t) − V_floor = E and reads no V′; it, ω_t and ln R do not change. V′ entered only through the k_ex attached to
  φ_t, which R15 changes.
- **R17, re-measure.** "Remeasuring the table: yes, we'd better do this; a number of things will need re-measuring. Best
  to do the job right, the first time."
- **The route.** "prompt 07 has landed so we have an opportunity implement this now, if that seems prudent", and "The
  prompts can go into `power-law-wall`."

**What the planner's probe found** (orchestrator note 07b §0; scratch, not committed). Crossover's 107 stage-2 rows on
V₀e^{M/φ}, re-chained in their own (L, ln R):
- the old form reproduces every recorded ℓ to the printed digits and crossover's fit to the last digit (b₀ = 0.9691,
  c = 0.4099; the gap's 0.0198, 1.5601, 1.5472, 0.841), so the re-chaining is sound;
- with the V‴ form, ℓ rises by 0.035 at L = 132, 0.24 at L = 52 and 1.0–2.3 at L ≲ 10, where the late cascade at
  laboratory M sits;
- **20 rows change regime**: the chain now has a solution where it had none, so points measured as gap become one-loop,
  at ℓ = 0.14–2.4, with f = 0.16–0.84;
- **v1's form does not carry over**: refitted on the crossover's on-curve rows, the one-loop branch's largest miss is
  0.19 (rms 0.079) against v1's 0.074, and the rows below ℓ = 1 have f = 0.16–0.78 where v1's plateau says 0.9; the gap,
  on its 21 remaining rows, fits better than before (0.047).

So the exponential's law needs its form re-measured, not only its constants: which rule separates the regimes, and what
f does at small ℓ, are measurements for 07b and a ruling for the user (U13). The power wall's law reads neither ℓ nor the
difference between one-loop and gap (R7), so it does not change. On the power wall the new k_ex/ω_t is
(n + 2)/(2√(n(n + 1))) at every E/V₀, so R9's error of the table near the floor disappears.

**P73–P90** stand unless the user rules otherwise. U11–U13 are the user's; none is needed before 07b is dispatched,
U11 and U12 before 07c is written, U13 after 07b.

- **P73 — where.** Three prompts inserted before 08: 07b (research), 07c (production), 07d (rehearse again). 08 keeps
  its number and is written after 07d.
- **P74 — 07b is research** in `prompts/power-law-wall/source/kex/`, as 02 was in `source/wall/`: copies of the scripts
  it needs from crossover and from `source/wall/`, each with a header naming its source and commit, and its own chain
  with both forms. It changes no production code, no crossover file and no file of `source/wall/`.
- **P75 — the definition.** k_ex = ½E^{1/2}|V‴/V″| at the point the estimate is taken: φ_t for the classical turning
  point, φ_ta for the one-loop turnaround. V″ = V[(ln V)″ + (ln V)′²] as now; V‴ = V[(ln V)‴ + 3(ln V)′(ln V)″ +
  (ln V)′³], through a new hook `_d3_logV_dphi3` on every model, with no generic default (the user's ruling of
  2026-11-05: a new potential's breakage is loud). The named tuples' λ fields become the pulse rate |V‴/V″|, under a
  new name. 07c.
- **P76 — the check of the form.** 07b derives Erickcek et al.'s Eqs. (66) and (71) from P75 and compares them with the
  chain's k_ex at stated points, so that the factor ½ and the point of evaluation are confirmed, not assumed.
- **P77 — every stage-2 point re-chained.** Crossover's three scans (`stage2_scan`, `stage2_mscan`, `stage2_n2`) and 02's
  power-wall scans, each row in its own (L, ln R, n): ℓ, m_ta and φ_ta in both forms, the regime in both, and the rows
  that move, tabled. No recorded fraction is re-used without its row's new regime.
- **P78 — new stage-2 points.** Stage 2 sets its mode grid from the chain's k_ex, so (i) three recorded points are re-run
  with the grid set from the new k_ex, and must give the recorded fraction within stage 2's stated precision (the
  measurement does not depend on the label); and (ii) the exponential's moved region is measured more densely: on the
  laboratory curve and crossover's other mapped curves, from the gap's entry down to R = 1, enough points to fit at
  n = 1, and a few at n = 2 (U15). Validated as crossover's stage 2 was.
- **P79 — the candidate laws for `one-loop-exponential-v2`.** Against every exponential point in its new regime, at n = 1
  and 2: (i) v1's form refitted (A held at 2.69, b₀ and c fitted); (ii) with A free; (iii) the regimes split at an ℓ
  threshold, with the gap's form below it; (iv) any form the agent proposes, stated with its reason; the plateau and the
  cap reported as measured. For each, the largest residual, the rms and the mean over one-loop and gap points
  separately, beside v1's 0.074 and 0.062. Measured, not ruled (U13).
- **P80 — the power wall's law unchanged.** On 02's rows: the regime moves, and f_n(ln R) giving each row the same
  value, since it reads neither ℓ nor the one-loop/gap difference. The law, S_n and `one-loop-measured-pl-v1`'s constants
  do not move (its name is U11).
- **P81 — the spectrum tables re-measured**, by `source/wall/spectrum_table.py`'s method, at the three references, in
  x = k/k_ex with the new k_ex: the exponential at `WallN(90, 1)` (planner: k_ex/ω_t = 0.516395, from 0.494535), the
  power wall at n = 1 and 2 (1.060660 and 0.816497, from 0.353553 and 0.408248). X_LO = 10⁻³ ω_t/k_ex, X_HI by
  orchestrator note 03 ruling 5's rule; x_p, the slope, κ_hi, the tail's share, the spline check; the exponential's shape
  over L = 40–134 (P21); on the power wall, the table's error near the floor (R9), which the planner expects to vanish.
- **P82 — x_*.** It was measured as a momentum, q = 0.0029 ω_t, in the 2PI run on the exponential; 0.0058 is that
  over k_ex = ω_t/2. In the new x on the exponential's reference it is 0.0029/0.516395 = 0.005616 (planner), carried to
  every wall in x as R10 rules, and flagged as before. 07b prints it; 07c sets it.
- **P83 — the paths.** Crossover's cascade maps, 02's maps (1148 power-wall candidates) and 04's sample candidates,
  re-chained: ℓ in both forms, the regime in both, and f under v1 and each candidate of P79 on the exponential. A
  measurement of where the change matters; ΔN_eff is 07d's to measure in histories.
- **P84 — the inventory.** Every place in the tree that computes or reads k_ex, ℓ, ℓ_t, λ_t, λ_ta or the chain's
  existence, and every constant measured in x = k/k_ex or fitted against ℓ: file, line, what it is, and what 07c does to
  it. Production, `tools/`, tests and goldens; records (closed campaigns' `source/`, `.documents/`, `science-runs/`) are
  listed as records and not changed. Note 07b §0 gives the planner's list as a start.
- **P85 — the drain models.** The exponential's refitted law is `one-loop-exponential-v2` (R14), a new value of
  `alpha_value.drain_model` (P8), named by `ExponentialModel.one_loop_drain_model`. `one-loop-measured-v1` is retired:
  its law reads ℓ in the old form, which no longer exists at the tip, so the tip refuses it at entry with a message
  naming R13. It stays a known name, so that a store or option naming it is refused, not misread.
- **P86 — what 07c re-pins.** Every test or golden that pins a k_ex, ℓ, λ, regime count, law constant, spectrum constant
  or drain-model name, each listed with its old and new value. A test of a history whose law reads no ℓ moves only in
  those columns; an integration value that moves there is a stop.
- **P87 — the gate R2.** 07d rebuilds 07's five-store grid at the tip (the base's store is not rebuilt: 07 holds it).
  Every history whose law reads no ℓ equals 07's in every stored value but the k_ex columns (k_ex, ℓ, ℓ_t, the pulse
  rate, the peak, log q, the regime label, the birth margin and the margins' records), which are listed; the exponential's
  one-loop histories under v2 are compared with 07's under v1 as a measurement (ΔN_eff, Y_p, D/H, the drained share).
- **P88 — the thresholds.** `MARGIN_THRESHOLD` (ln 10, the birth margin ln(k_ex/m) among them) is not changed. 07d
  reports how the failing shares move. A change is the user's.
- **P89 — the standard analyses** (convention 8). The exponential's one-loop law changes what Figure 6 reads, and k_ex,
  the tables and x_* change what Relativistic quanta reads, for both potentials. A rebuild after this campaign repeats
  both for the exponential as well as the power law. Their inputs keep their files and columns unless 07c renames one,
  in which case 07c amends `science-runs/README.md`'s row.
- **P90 — the counts.** As P14.

**Ruled by the user, 2026-11-17, in their words:** "U11 and U12 as proposed; dispatch 07b in a fresh session". So:
- **U11 (a):** 07c renames `one-loop-measured-pl-v1` to `one-loop-inverse-power-v1`; the power wall's law and constants
  do not change.
- **U12 (a):** 07c sets `VERSION_LABEL` 2026.13.0; `FINGERPRINT_FORMAT` stays 8 unless 07c changes a table or column.
- U13 is ruled after 07b.

The table below is the proposal as it was put.

| | Proposed | Alternatives |
|---|---|---|
| **U11** — the power wall's drain-model name | **(a) rename `one-loop-measured-pl-v1` to `one-loop-inverse-power-v1`** in 07c, so that every one-loop drain model names its potential, as R14 names the exponential's and as R12 treats the potentials. Its law does not change, so it stays v1. No store of record holds the old name: only 07's rehearsal stores | (b) keep `one-loop-measured-pl-v1` |
| **U12** — the label | **(a) `VERSION_LABEL` 2026.13.0**, since stored values change meaning: 07's rehearsal stores were made under 2026.12.0 with the old k_ex, and the label is the guard against reusing a store whose values mean something else (`config/version.py`). `FINGERPRINT_FORMAT` stays 8 unless 07c changes a table or column | (b) keep 2026.12.0, which no store of record uses, and have the user retire 07's stores |
| **U13** — the exponential's law | **ruled after 07b**, on P79's residuals, as U3 was after 02 | — |


### 6.13 The min(rise, fall) form: asked for by the user, 2026-11-18

**What was found** (the orchestrator, 2026-11-18, in scratch at `4f9b1bd`; its scripts, figures and residuals were committed
later at the user's request, in [`source/kex/probe-07b/`](source/kex/probe-07b/README.md)). After 07b the user asked why
ℓ moved on the exponential, where −d ln V/dφ and V‴/V″ had been expected to agree, and whether 07b's fits had the wrong
form. Read from 07b's records:
- **Why ℓ moved.** The wall is V₀e^u with u = (M/φ)ⁿ, not V ∝ e^{λφ}. |V‴/V″| = λ[n²u² + 3n(n+1)u + (n+1)(n+2)]/[nu(nu +
  n + 1)] = λ(1 + 2(n+1)/(nu) + …), equal to λ only as u → ∞ (Eq. (66) is that term). At the turnaround u is 1–50, so the
  new k_ex is 1.06–3.85 times the old (07b's `definition.txt`, E66), and ℓ moves by about its logarithm.
- **In the new ℓ, f collapses.** Beyond its peak (ℓ_new ≳ 2.3), f = 1 − exp(−A/(ℓ − ℓ₀)) fits with no L term: at n = 1,
  A 2.60, ℓ₀ 1.08, max 0.046 (28 points; 0.010 on the planner's selection, 15); at n = 2, A 2.59, ℓ₀ 1.12, max 0.012
  (21). In the old ℓ the same branch needs −C ln L (C ≈ 0.45): without it, max 0.096 and 0.130. The peak sits at
  ℓ_new ≈ 2.2. Below it f rises, and is not a function of ℓ alone at n = 1 (y = aℓ^b: max 0.54).
- **So candidates (i) and (ii) carry v1's layout,** a plateau below ℓ = 1 and the fall above it, which was built where
  the peak sat in the old ℓ.
- **The form min(rise, fall):** y_rise = G(ln R − R₀)^P(1 + Q ln L), v1's gap form; y_fall = A/(ℓ − ℓ₀), infinite where
  there is no chain or ℓ ≤ ℓ₀; f = 1 − exp(−min). Fitted on 07b's one-loop and gap points (A free, no L term): n = 1 on
  the planner's selection, G 0.0180, R₀ 1.610, P 1.546, Q 1.008, A 2.42, ℓ₀ 1.243, max 0.071 one-loop / 0.046 gap; n = 2,
  A 2.54, ℓ₀ 1.181, max 0.041 / 0.056. On every n = 1 point, max 0.494, from two grid corners.
- **Like for like** (the same points for every law; back out, ln R > 0.3, at n = 2 ln(V₀/M⁴) ≤ 10), largest miss:
  n = 1 every point, v1 0.629, (i) 0.332, (iii) 0.612, (iv) 0.417; n = 1 planner's selection, v1 0.091, (i) 0.332,
  (iii) 0.071, (iv) 0.107; n = 2, v1 0.125, (i) 0.355, (iii) 0.056. 07b's tables set v1 on its own selection (0.074)
  beside the candidates on theirs, which hold the corners.
- **The corners** (n = 1, `grid`, L = 1.3 and 2 at ln R = 25: f 0.33 and 0.61) have ln(V₀/M⁴) = 23.8 and 20.3. They
  are crossover's off-curve corner (its README, U15: "It needs M below laboratory M, which no history of the sample
  has"), the reason for the n = 2 cut ln(V₀/M⁴) ≤ 10; every history has ln(V₀/M⁴) ≤ 3.3. No cut was applied at n = 1.

**The user's words, 2026-11-18:** "Measure the min(rise, fall) form properly before 07c". So 07b2 is inserted before
07c. U13 is ruled after it; U11 and U12 stand.

**P91–P98** stand unless the user rules otherwise.
- **P91 — where.** 07b2, research in `source/kex/`, as 07b: new files only; 07b's scripts and outputs are records of
  07b and are imported, never changed. Outputs in `.documents/power-law-wall/kex-07b2/`, the verification document §8.
- **P92 — the form and its variants.** (v) as above, six parameters per n; (v-L), the fall A/(ℓ − ℓ₀ − C ln L), C free,
  which says whether the L term is needed; (v-A), A held at 2.69; (v-J), A and ℓ₀ shared by n = 1 and 2, each n with its
  own rise. One further variant may be proposed, with its reason. Classical points (ln R ≤ 0) stay R·S(L) and are not
  fitted.
- **P93 — the selections, like for like.** Each fit on three selections: S1, 07b's fit_law selection; S2, the planner's
  (07b note §0.3); S3, the histories' domain, ln(V₀/M⁴) ≤ 10 at both n (U15's cut, applied at n = 1 too), with every
  point it drops listed. Every law (v1 by the old chain, 07b's (i)–(iv) at their committed constants, each (v) variant)
  is scored on the identical point set, by regime and by active branch.
- **P94 — the errors.** Standard errors from the fit's covariance; leave-one-set-out refits (each set of points left out
  in turn), with the spread of each parameter and the held-out residuals.
- **P95 — new stage-2 points, predicted first.** (v)'s S3 fit at each n is committed to a file before any new point
  runs; then, by 07b's stage-2 machinery and its rule for the machine: on the laboratory curve (ln(V₀/M⁴) ≈ 3.3) and one
  low-M mapped curve at n = 1, the region from ℓ_new = 1 to the predicted crossing + 1.5, at least six points per curve;
  on the n = 2 history curve, at least eight points from ℓ_new 0.3 to 4; and at least ten held-out points inside S3 away
  from every measured row, chosen before they run. Each predicted from the committed fit, then the fits re-run with them.
  At most 40 points. Validated as 07b's were.
- **P96 — the crossing, the cap, the edges.** Where rise meets fall on each curve, the predicted peak beside the
  measured; whether any cap is needed; f at ln R → 0⁺ beside R·S(L) at ln R = 0; the change of f at points where the
  chain's 100-iteration limit (07b's issue) would call a one-loop point gap.
- **P97 — the paths.** f under each (v) variant on every candidate of 07b's paths, and the first-order drained share per
  history beside v1's and 07b's candidates.
- **P98 — the reference implementation.** One function of (ℓ or no chain, L, ln R, n) and the constants of each fitted
  variant to full precision, in a JSON file, for 07c's gate Consistent. Not production code.

**07b2 landed** (`2f638e0`, reviewed `213b68c`; verification §8). After it, the orchestrator checked in scratch whether
the rising branch can also be shared between n = 1 and 2, on 07b2's S3 points with its new ones (146): every constant
per n, sum of squares 0.06456 (12 constants); A and ℓ₀ shared, 0.06466 (10); every constant shared, 0.09906 (6), largest
miss 0.075 | 0.090, mean +0.009 | −0.014. Sharing the fall: F = 0.11, p = 0.9. Sharing the rise as well: F = 18,
p ≈ 6×10⁻¹². Not committed.

**U13 ruled by the user, 2026-11-18, in their words:**
- The law: "I think it's clearly the one for which 07b2 produced those figures. They seem to show an excellent fit,
  substantially better than we had before." So `one-loop-exponential-v2` is 07b2's form, f = 1 − exp(−min(y_rise,
  y_fall)) for ln R > 0.
- The constants: "I don't see an adequate justification for having separate constants per `n`. I would share them
  between `n=1` and `n=2`, and use the values obtained from the joint fit." Then, after the check above: "Joint fit as it
  stands now: falling branch shared, rising branch fitted per-`n`, as described above." So the constants are 07b2's
  (v-J) on S3 with the new points (`minform.json`, `with_new`, `n12/S3/v-J`): A = 2.514 ± 0.052 and ℓ₀ = 1.186 ± 0.047
  shared; the rise at n = 1 G 0.0202, R₀ 1.729, P 1.495, Q 1.035, at n = 2 G 0.0141, R₀ 1.204, P 1.731, Q 0.908 (full
  precision in the file).
- A = 2.69: "I didn't see any reason to have `A = 2.69` – why are you suggesting this? It doesn't look anywhere near the
  fit." (v-A) is not used; A is the fitted value.
- The fit region: "yes, do the sensible cut `ln(V₀/M⁴) ≤ 10`". S3 at both n, as U15 had it at n = 2.

**What the law covers.** For ln R > 0 the one form replaces both of v1's branches: at a one-loop candidate (the chain
has a solution, ℓ > 0) f is the smaller of the rise and the fall; at a gap candidate (no chain, or ℓ ≤ 0) the fall is
infinite and f is the rise alone, which is v1's gap form with refitted constants per n; f = 0 where ln R ≤ R₀ or
1 + Q ln L ≤ 0, as v1's gap. v1's plateau (0.9 below ℓ = 1) and cap (0.95) go. The classical region (ln R ≤ 0) keeps
R·S(L) unchanged: it reads no k_ex (R16), and U15's L at n = 2 stands. The stored regime label (classical, one-loop, gap)
is kept; under v2 it no longer selects a formula for ln R > 0, only whether the fall exists.

### 6.14 Proposed with prompt 07c, 2026-11-18

Written by the orchestrator at the user's word ("We are ready to write 07c, so please do so"), on R13–R17, U11–U13 and
07b's inventory (`.documents/power-law-wall/kex-07b/inventory.md`, 278 rows, and its "Beyond the planner's list"). P75,
P82, P85–P89 stand and are made exact here. P99–P110 stand unless the user rules otherwise; U14 is the user's, before
07c is dispatched.

- **P99 — each potential owns its one-loop law.** `one-loop-exponential-v2` is `ExponentialModel._drain_fraction`, with
  its constants in `ExponentialModel.py`, as the power wall's is `InversePowerModel._drain_fraction`; the S table and
  S(L), the exponential's classical branch, move there with it. `ScalarHistory.py` keeps no potential's constants, and
  under the one-loop law `drain_alpha_effective` always calls the model's own fraction. `_drain_L` stays on the
  exponential, as the law's input inside the model and as the wall variable recorded per candidate (the bounce column
  `L`), so the rule "a model defines exactly one of `_drain_L` and `_drain_fraction`" becomes "`_drain_fraction` is
  required under the one-loop law; `_drain_L` is optional and recorded where defined". v1's module-level
  `drain_fraction` and `DRAIN_*` constants go. Reason: the user's ruling of 2026-11-05 (potential-specific quantities in
  the model class, no default) and R12 (no potential privileged); v2's rise is per n, which a module-level law could only
  hold as a table keyed by n.
- **P100 — the form, as U13 rules.** For ln R > 0, f = 1 − exp(−min(y_rise, y_fall)): y_rise = G_n (ln R − R₀,n)^{P_n}
  (1 + Q_n ln L), with f = 0 exactly where ln R ≤ R₀,n or 1 + Q_n ln L ≤ 0 (as v1's gap); y_fall = A_f/(ℓ − ℓ₀) at a
  one-loop candidate with ℓ > ℓ₀, and +∞ otherwise (so a gap candidate takes the rise). Classical: exp(ln R) S(L), as
  now. Unassessed: 0. No plateau and no cap. The constants are 07b2's joint fit (`minform.json`, `with_new`,
  `n12/S3/v-J`), at full precision: A_f 2.5143466540073995, ℓ₀ 1.1859055953178366; n = 1 G 0.020236734836939167,
  R₀ 1.7292636698176154, P 1.4945632951339265, Q 1.0354234565531648; n = 2 G 0.014077566206585982, R₀ 1.2044868612303559,
  P 1.7311233766914242, Q 0.9084029819604703. The fall's coefficient is not named `A` in code: `--A-values` already names
  the law's strength A. The implementation equals `minform.fraction` (07b2's reference) bit for bit at synthetic inputs
  and at the 146 fitted points; the docstring states the fitted domain, ln(V₀/M⁴) ≤ 10.
- **P101 — the drain-model names.** `"one-loop-exponential-v2"` (R14) and `"one-loop-inverse-power-v1"` (U11), the
  constants in `drain_model.py` renamed to say which potential each is. `"one-loop-measured-v1"` and
  `"one-loop-measured-pl-v1"` stay known names, refused wherever a drain model is read (`alpha_value`'s check, the
  argument parser, `plot_inputs`, the store tools), with a message naming R13 or U11. `DRAIN_MODELS` keeps its order
  (fixed, exponential, power), so `extract_common`'s sort does not move. `model_of_law` follows the exponential's new name
  or goes, if nothing in production calls it.
- **P102 — one k_ex** (P75 made exact). `_d3_logV_dphi3` on every model and every test stand-in, closed forms, no
  default (the base raises naming the class), with a `RealizedModel` delegate. V‴/V = (ln V)‴ + 3(ln V)′(ln V)″ +
  (ln V)′³, in log space as V″ is now. `classical_turning_point` at φ_t and `one_loop_turnaround` at φ_ta take
  ln k_ex = ln|V‴/V″| + ½ ln E − ln 2. The tuples' λ fields become `pulse_rate_t` and `pulse_rate_ta`, holding |V‴/V″|.
  The refusal of a wall that does not repel stays on −d ln V/dφ ≤ 0; a pulse rate that is zero or not finite raises
  `ComputationFailureError` naming the hook. Checked against 07b's `production_v3_classical` and
  `production_v3_turnaround`.
- **P103 — the fixed point's cap.** `ONE_LOOP_MAX_ITERATIONS` 100 → 400. Under v2 the cap changes no f (07b2 §8.5: where
  it binds, ℓ < ℓ₀), but it decides the stored regime label and ℓ; 07b's points need up to 195. If every point of 07b and
  07b2 converges within 400, 07c closes `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`.
- **P104 — the spectrum** (P81, P82 made exact). The three tables and `_TABLES` as 07b wrote them
  (`.documents/power-law-wall/kex-07b/spectrum_tables.txt`); `X_STAR` 0.005615853347600149 on every wall (R10);
  `SPECTRUM_MODELS` → `R8-exponential-v2`, `R8-power-n1-v2`, `R8-power-n2-v2`. `G_Y_MIN` and `OMEGA_U_MAX` re-derived
  on the criteria their docstrings state, against the new x_lo (power-n1's 9.43e-4 is below `G_Y_MIN` = 1e-3); the
  kernels within 10⁻⁷ of quadrature on the new tables (the gate Consistent).
- **P105 — the label and the format.** `VERSION_LABEL` 2026.13.0 (U12), with a dated comment in `config/version.py`.
  `FINGERPRINT_FORMAT` stays 8: no stored table or column is renamed or added. A change that would need one is a stop.
- **P106 — the tests** (P86 made exact). Every pin of the inventory's §3 re-pinned, each listed in the log with its old
  and new value; tests of v1's law become tests of v2's; new tests of the V‴ hooks (closed form, and the centred
  difference of V″), of the law against `minform.fraction`, of the retired names refused, and of U14. A test that
  compares with a verbatim copy of the old method (`classical_turning_point_before_fix`) compares only what k_ex does
  not define, and says so. The goldens are fixed-law or natural mode and pin no k_ex column: they stay bitwise.
- **P107 — the tool route** (the gate Consistent). Before any change, at the base, five histories by
  `tools/history_and_bbn.py`: the exponential in natural mode; the exponential under the fixed law at α > 0, reflecting;
  the power wall at n = 1 and n = 2 under the one-loop law; the exponential at n = 1 under the one-loop law at laboratory
  M. After: the first four equal to the base in every sample and in BBN, bit for bit, with only the k_ex columns moving,
  each listed; the fifth measured (ΔN_eff, Y_p, D/H, the drained share). Their parameters are taken from 04's sample or
  07's grid, whose costs are recorded.
- **P108 — the documents.** `CLAUDE.md`'s two mentions of `one-loop-measured-v1` (the `alpha_value` pair, the plot
  scripts' `--A-values` check) take the new names: a name, not a rule. `architecture-summary.md` and
  `numerical-strategies.md` are 08's (additive). `science-runs/README.md` changes only if a file or column is renamed
  (none is, P105). The verification document gets §9.
- **P109 — the domain is stated, not checked.** No runtime check of ln(V₀/M⁴): every history has ln(V₀/M⁴) ≤ 3.3
  (crossover). 07d reports whether any candidate of its grid exceeds 10.
- **P110 — 07c changes nothing 07b's inventory does not name**, apart from P99's move of the S table and P103. A use of
  k_ex the inventory missed is changed and listed in the log as found.

**For the user to rule before 07c is dispatched:**

| | Proposed | Alternative |
|---|---|---|
| **U14** — the exponential at n other than 1 and 2 | **(a) no one-loop law**: `one_loop_drain_model` raises at n ∉ {1, 2}, so the parser and the drivers refuse `--A-values` there, as on the power wall (R7). v2's rise was measured at n = 1 and 2 only; U15 (b) let v1 run at every n with n = 1's constants. No run of record uses n ≥ 3 | (b) n ≥ 3 takes n = 2's rise, unmeasured |

**U14 ruled by the user, 2026-11-18, in their words:** "U14: yes, for now, refuse `n >= 3`. I am only expecting to do
production runs for `n=1` and `n=2`." So (a): the exponential has a one-loop law at n = 1 and 2 only.

### 6.15 Proposed with prompt 07d, 2026-11-18

Written by the orchestrator at the user's word ("Please write 07d (and an orchestration note, if needed) now"), after 07c
landed (`c8461ab`, reviewed `2e3c551`), on P87–P89 and P109, 07's prompt, note and log, 07c's log §7, and a survey of
the tip and of 07's records (orchestrator note 07d §0). P87 stands and is made exact here. P111–P124 stand unless the
user rules otherwise. None needs a ruling before 07d is dispatched.

- **P111 — the stores.** Four fresh stores at the tip, `power-law-wall-07d-exp`, `-pl`, `-pl-resumed` and `-mixed`, built
  by 07's commands (prompt 07 §1.3) with only `--database` and `--job-name` changed. 07's `base` is not rebuilt, and
  07's four tip stores, built at `945bf5d`, are the comparison: no line of code differs between `945bf5d` and 07c's base
  `c4258ef`. 07's stores are read only through `sqlite3` `mode=ro`, never by the tip's drivers or tools, except the one
  refusal of P117. They are not retired here. Reason: P87; a 2026.12.0 store is never extended at 2026.13.0 (U12).
- **P112 — one cluster**, from this worktree, at 2026.13.0 and format 8, with 07's probe (prompt 07 §1.2) run before the
  first build. Reason: one tree builds; the probe still shows the data files read in every process, as before.
- **P113 — the interruption**, as P63: `pl-resumed`'s run `a`, `SIGTERM` at the banner `RUNNING PIPELINE FOR MODEL
  QCD_Cosmology, A = 0.05 (one-loop-inverse-power-v1)`, the window read from 07d's own `pl-a`; one retry into
  `-pl-resumed2` on a miss; a second miss is a stop.
- **P114 — the k_ex columns**, by name. A history whose law reads no ℓ (the fixed law, natural mode, and the power
  wall under both laws) is matched to 07's by its key, with `alpha_value.drain_model` `one-loop-measured-pl-v1` in 07's
  store matched to `one-loop-inverse-power-v1` in 07d's (U11). Every column both stores hold is compared by `float.hex`,
  as 07's `compare_histories.py` does, with its exclusions (serials, `version`, `label`, `timestamp`, the timings) and its
  failure-reason mask (07's ruling (a)). These columns may move, and each that does is listed with the number of values
  that moved and the largest change:
  - `ScalarHistoryBounce`: `regime`, `ell`, `log_k_ex_Mp`, `log_m_Mp`, `log_q_Mp`, `birth_margin`, `reflection_margin`,
    `reflection_margin_N`, `tracker_margin`, `tracker_margin_N`;
  - `ScalarHistory`: `worst_<kind>_margin`, `worst_<kind>_margin_N` and `<kind>_failing_share` for birth, reflection and
    tracker, `unassessed_share`, and of the drain record `candidates_<regime>`, `drained_share_<regime>`, `ell_min`,
    `unresolvable_assessments`, `all_worst_<kind>_margin`, `all_worst_<kind>_margin_N` and `all_<kind>_failing_share`;
  - `extra_data`'s `margins`, key by key.

  Any other value that differs is a stop: in particular every sample, every BBN value, `alpha_eff`, `delta_K_Mp4`,
  `share`, `parking_share`, the parking and release N and the counts of reflections and integrations. Under the power
  wall's law `drained_share_<regime>` may move only between regimes: their sum, and each candidate's `delta_K_Mp4`, are
  compared bitwise. A candidate that 07 assessed and 07d does not (`regime` `unassessed`), as below the third hook's range
  (`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`), changes the power wall's drain and is a stop.
- **P115 — the exponential's one-loop histories, measured.** Matched by key with `one-loop-measured-v1` (07) to
  `one-loop-exponential-v2` (07d): per history, its outcome, ΔN_eff today and at 1 MeV, Y_p, D/H, the drained share per
  regime and the parking share, the candidates per regime, `ell_min`, the parking and release N, the reflections, the
  integrations and the tuning's iterations, 07 beside 07d. A history that fails in one only is reported with its
  reason. Measured, not judged (P87).
- **P116 — the tip's own identities**, as 07's §2.2 and §2.4, within 07d: `pl-resumed` equal to `pl` by
  `compare_stores.py` (`RESULT: equal`, `problems: none`), by `store fingerprint` and by its exports; `mixed` equal to
  `exp` (exponential) and to `pl` (n = 1) in every shared column, and in its exports. Any difference is a stop.
- **P117 — the readers**, as 07's R1–R7 on 07d's four stores, from this worktree:
  - R1 the inventory, `problems: none`; R2 the exports without figures; R7 `histories.py`;
  - R3 the figures **on `exp`**, not `pl`: the exponential's one-loop law is what moved, and 07 drew the power law's;
  - R4 07's three refusals, the banner's model name updated, and a fourth: `main.py --config exponential.yaml
    --n-values 3 --A-values 1` refused at parse (U14), before Ray, against a path that does not exist;
  - R5 the quanta tools on every root, each row's `spectrum_model` `R8-<wall>-v2`, and `plot_quanta_grid` on `pl` (the
    open issue `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`, a measurement);
  - R6 the BBN re-solve on `exp` (fixed α 0.1, and the one-loop law at A = 1) and on `pl` (A = 1, n = 1 and 2), at
    4 × 10⁻³¹ M_P and β = 2, each equal to the stored values to its ten digits;
  - **R8, U12's guard**, last of all, after every comparison: the tip's `plot_by_beta.py --no-plots` on 07's `exp` store,
    with its build's options. It must exit without writing an export and leave the store's `shasum` unchanged. One that
    exports 07's rows as current is a stop. What it prints is recorded.

  Every reader's store `shasum` before and after; a change is a stop.
- **P118 — the exports against 07's.** Each of 07d's export roots against 07's (`var/power-law-wall/07/`), file by file,
  with a one-loop directory named by 07's drain model mapped to 07d's. Where the law reads no ℓ, only the columns P114
  lets move may differ (in `histories.csv`, the failing shares and `unassessed_share`; the `drain_model` column renamed
  under U11); anything else is a stop. The exponential's one-loop files and every quanta file (new tables and x_*) are
  measured: which columns differ, and by how much.
- **P119 — the margins** (P88): per history, the three failing shares, `unassessed_share` and the worst margins, 07
  beside 07d, and the count of histories in which each failing share rose, fell or held. `MARGIN_THRESHOLD` is not
  changed.
- **P120 — the domain and the third hook** (P109). For every exponential one-loop candidate, ln(V₀/M⁴) from its stored row
  (V₀ = E/(e^L − 1), with M from the history's key); the largest and how many exceed 10. For every potential and n, the
  least φ_t over the grid's candidates, from the stored E with the model's closed form (the power wall φ_t =
  M(V₀/E)^{1/n}), and |(ln V)‴| there against the largest float, in decades.
- **P121 — the costs**, per store and per history, beside 07's, with the loads at launch.
- **P122 — the scripts** in `.documents/power-law-wall/rehearse-07d/`: 07's (`rehearse-07/`) copied and adapted, each
  adaptation shown by a diff in `out/`; `compare_histories.py` gains a mode for P114 (the drain-model map and the k_ex
  columns reported, not failed) and `compare_v2.py` is new for P115. `rehearse-07/` is a record of 07 and is not edited.
- **P123 — files and the label.** No code, test or config changes; the counts after are by discovery and equal those
  before (07c's: 26, 805, 175, 312, 79). The verification document gets §10. `science-runs/README.md` is not edited (no
  file or column is renamed).
- **P124 — what 08 reads.** 07d's four stores, its exports and its costs, in place of 07's (P87); 07's stores are left in
  place, and retiring them (`runregistrykit store retire`, a person's act) is the user's.

## 7. What this campaign hands to the user

- A tree that builds, stores, exports and plots the power-law model under both drain laws at any n, beside an
  unchanged exponential.
- The wall's measurements (02) and the sample (04), from which the user plans the science run of record: its grid, its
  probe, and which standard analyses it repeats. That run is planned in `science-runs/` after 08, as 2026.10.0 was, and
  launched by the user.
