# Campaign — power-law-wall

**Written:** 2026-11-13 on `main` at `84a2494`, by Claude Opus 5.5, at the user's request: "a similar science
campaign for the inverse power law potential with `n=1` and `n=2`". R1–R3 ruled by the user the same day (§6.1),
and U1–U7 as proposed (§6.2); P1–P14 stand unless ruled otherwise (§6.3). Cut from `main` at `a7e0978`, after the
2026.11.0 plan was committed (U1). No prompt is written.

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

**Out of scope:**
- The science run of record and its figures (§7; the user's).
- The 2PI (Kadanoff–Baym) run on the power wall for x_* and the associated piece (U5); ChamKadanoffBaym's work.
- Any change to the exponential model's results. Every exponential history is bitwise as at the base (P2).
- The wall back-reaction notes' power-law chain (λ ≈ n/φ), except as 02 needs it.
- The open issues of the exponential run (`science-runs/plan-science-2026.11.0/PLAN.md` §5), unless one blocks a
  gate here.
- The packages, `thirdparty/`, `PRyM/`.

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-the-model.md` | `InversePowerModel` (hooks in log space, floor, `reflects_at_origin`, `renormalize`, the analytic seed at any n; no `_drain_L`); `_PowerLawWall`'s tests run against it; `--model` and `--n` on `tools/history_and_bbn.py` and `tools/surfing_scan.py` (P5 as amended); natural mode and the fixed law by the tool route; tests | Opus |
| 02 | `02-measure-the-wall.md` | in `source/wall/`: `walls.PowerWall`; S at n = 1, 2; stage 2 along the power-law cascades' (L, ln R) paths (mapped with the fixed law); the adopted law at candidate L against it; the test field's spectrum (x_p, κ); the planner's estimates of §0.3 checked. Research; no production change. Then U3–U5 | Opus |
| 03 | `03-the-law-on-this-wall.md` | as U3–U5 rule: `_drain_L`; a new `drain_model` value if the law is refitted, with its constants and S table; the spectrum's constants by wall in `quanta_spectrum.py` and `SPECTRUM_MODEL`; the exponential law bitwise | Opus |
| 04 | `04-measure-a-sample.md` | about forty power-law histories, re-tuned, by the tool route under both laws (§6.3 P9); the thresholds and surfing thresholds; the comparison with the same (M, β) on the exponential. Then the user rules any threshold that does not hold | Opus |
| 05 | `05-the-store.md` | model type id 2, its table and factory keyed (M, n, β), the registry, sharding, the `types` maps, `FINGERPRINT_FORMAT` 8 and `VERSION_LABEL` (U6), `compare_stores`, `inventory_report`, `bbn_from_store`, `outward_step_scan`; tests in a temporary store | Opus |
| 06 | `06-drivers-and-exports.md` | `main.py`'s model option and its config file; `plot_inputs`, `plot_by_beta`, `plot_ScalarHistory`, `extract_common`, `pipeline_selection` through the model interface; the model in `histories.csv` and in the export paths (P11); the exponential exports byte-identical | Sonnet |
| 07 | `07-rehearse.md` | registered builds on the isolated cluster: the exponential at base and tip, bitwise; the power law at n = 1, 2 under both laws, interrupted and resumed, by value and against 04 | Opus |
| 08 | `08-close.md` | `.documents/power-law-wall-verification.md`, `architecture-summary.md`, `numerical-strategies.md`, `CLAUDE.md`, the handover for the science run (its grid's inputs from 04), the issues and the indexes | Opus |

Written one at a time, each after the previous one lands and is reviewed. 03 waits for U3–U5, and is dropped if they
rule the law and spectrum carried over unchanged with `_drain_L` alone, which then moves into 04's first commit by
ruling. 06 needs only 05's schema; 08 needs 07's stores.

### 2.1 The gates

| Gate | Holds when | Prompt |
|---|---|---|
| **Model** | each hook of `InversePowerModel` equals its closed form (and finite differences of the one below it) across 10⁻⁹⁰ M ≤ φ ≤ 10 M_P at n = 1, 2, with no overflow; the turning point, the minimum and the seed against closed forms; under the one-loop law the model is refused at entry; every exponential golden bitwise; the suites at their counts or above | 01 |
| **Wall** | S, the one-loop fraction and the spectrum measured at n = 1, 2 along the power-law paths, each validated as crossover's stage 2 was (Born limit, classical limit, energy); the adopted law's residuals at each candidate L stated; nothing ruled by the agent | 02 |
| **Law** | the power wall's law as ruled equals its reference implementation at synthetic inputs and reproduces 02's maps candidate by candidate; the exponential law and spectrum bitwise | 03 |
| **Sample** | every sampled history measured under both laws, re-tuned, with its failures stated; every threshold of §0.3 reported against its use on this wall | 04 |
| **Store** | power-law histories round-trip in a temporary store; the exponential store's fingerprint golden unchanged apart from the format | 05 |
| **Export** | the exponential exports byte-identical to the base's on the same store; the power-law exports carry the model and read back | 06 |
| **R** | exponential builds bitwise base to tip; power-law builds resumed by value; inventories "problems: none" | 07 |
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
- **03 → 04.** The one-loop law on this wall.
- **04 → the user → 05.** The sample; the user rules any threshold that does not hold on this wall.
- **05 → 06 → 07.** The schema, the drivers, the rehearsal stores.
- **08 → the science run.** The handover: what a power-law run of record needs, the grid's inputs from 04, the
  standard analyses it repeats and what each needs (convention 8).

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–8 and "Repository mechanics", and:
- **The exponential does not move.** Every exponential history, export and golden record is bitwise as at the base. A
  prompt that cannot keep that stops.
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
  the fingerprint and export goldens.
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

## 7. What this campaign hands to the user

- A tree that builds, stores, exports and plots the power-law model under both drain laws at any n, beside an
  unchanged exponential.
- The wall's measurements (02) and the sample (04), from which the user plans the science run of record: its grid, its
  probe, and which standard analyses it repeats. That run is planned in `science-runs/` after 08, as 2026.10.0 was, and
  launched by the user.
