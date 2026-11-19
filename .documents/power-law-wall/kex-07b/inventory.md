# 07b inventory: every use of k_ex, ℓ and the chain (for 07c)

Every place in the tree that computes or reads k_ex, ℓ, ℓ_t, λ_t, λ_ta or the chain's existence (the one-loop/gap
regime), and every constant measured in x = k/k_ex or fitted against ℓ (power-law-wall README §6.12, P84). Compiled on
2026-11-17 at the worktree's HEAD (`e4ae58b`) by `git grep -n` over the tracked tree, `venv/` excluded, each hit read
in place; untracked files are this prompt's own (§7). Each row gives `file:line` (or a range), what is there, and what
07c does to it:

- **changes**: production logic, a constant or a statement of the form that must take k_ex = ½E^{1/2}|V‴/V″|;
- **re-pins**: a test or golden whose pinned value or closed form moves (the value given where it can be seen);
- **reads**: reads the stored or computed value and needs nothing (a test of it that pins a moved value is said);
- **name only**: names a drain model or spectrum model that U11 (`one-loop-measured-pl-v1` →
  `one-loop-inverse-power-v1`), R14/P85 (`one-loop-measured-v1` retired, `one-loop-exponential-v2` new) or a
  re-measured table (`SPECTRUM_MODELS`) renames;
- **record**: not changed.

"New" values quoted for the spectrum tables are 07b's measurements (`.documents/power-law-wall/kex-07b/
spectrum_tables.txt`). Under the fixed law the integration reads no ℓ, but `history_margins` still calls
`peak_estimate`, so a fixed-law history at α > 0 on a wall moves in its peak, birth margin and estimate kind too.

## 1 Production

### 1.1 `CosmologyConcepts/Models/AbstractModel.py`

| file:line | what | 07c |
|---|---|---|
| `AbstractModel.py:33-36` | module docstring: the hooks a model implements (`_log_V`, `_d_logV_dphi`, `_d2_logV_dphi2`) | changes (add `_d3_logV_dphi3`) |
| `AbstractModel.py:42-45` | docstring: `classical_turning_point`, `one_loop_turnaround` are generic defaults built on the hooks | record |
| `AbstractModel.py:66-74` | `ClassicalTurningPoint` comment and fields `phi_t, log_omega_t_squared, log_R, lambda_t, log_k_ex, ell_t`; comment states λ_t = −d ln V/dφ, k_ex = λ_t E^{1/2}/2 | changes (λ field renamed to the pulse rate \|V‴/V″\|, P75; comment) |
| `AbstractModel.py:76-90` | `OneLoopTurnaround` comment and fields `phi_ta, log_m_ta, lambda_ta, log_k_ex, ell, iterations, outside_classical`; comment states k_ex = λ_ta E^{1/2}/2 | changes (as above) |
| `AbstractModel.py:113-116` | `ONE_LOOP_ELL_START` 32, `ONE_LOOP_ELL_RTOL` 1e-12, `ONE_LOOP_MAX_ITERATIONS` 100: the fixed point's start, stop, cap | record (iteration unchanged); but near the chain's end the new form's fixed point needs up to 195 iterations at 07b's new points (`stage2_kex.py`; issue `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]`): a ruling for 07c |
| `AbstractModel.py:118` | `_LOG_2`, the ½ of k_ex | record (the ½ stays) |
| `AbstractModel.py:399-403` | `RealizedModel.d_logV_dphi`, `d2_logV_dphi2` delegates | changes (add a `d3_logV_dphi3` delegate) |
| `AbstractModel.py:421-430` | `RealizedModel.drain_fraction(regime, ell, log_E, log_R)` | reads |
| `AbstractModel.py:443-445` | `RealizedModel.one_loop_drain_model` | reads |
| `AbstractModel.py:458-466` | `RealizedModel.classical_turning_point`, `one_loop_turnaround` delegates | reads |
| `AbstractModel.py:505-511` | abstract hooks `_d_logV_dphi`, `_d2_logV_dphi2` | changes (add `_d3_logV_dphi3`, no generic default, P75) |
| `AbstractModel.py:563-582` | `_drain_fraction` base: docstring names ell (one_loop only) as an input | record |
| `AbstractModel.py:592-606` | `one_loop_drain_model` base (raises) | record |
| `AbstractModel.py:748-752` | `classical_turning_point` docstring: λ_t = −d ln V/dφ, k_ex = λ_t E^{1/2}/2 (eq:kex-over-wt), ℓ_t ≈ ln ½ on the exponential | changes |
| `AbstractModel.py:764` | docstring: raises for "lambda_t <= 0 at phi_t" | changes |
| `AbstractModel.py:788-802` | the computation: `lambda_t = -d1`, refusal if ≤ 0, `log_k_ex = log(lambda_t) + 0.5 log_E - _LOG_2` | changes (k_ex from V‴/V″ at φ_t; the repulsiveness refusal re-stated) |
| `AbstractModel.py:803-810` | `ClassicalTurningPoint(...)`: `lambda_t=`, `log_k_ex=`, `ell_t = log_k_ex - ½ ln ω_t²` | changes (field name; ℓ_t expression unchanged, value moves) |
| `AbstractModel.py:818-843` | `one_loop_turnaround` docstring: k_ex = λ_ta E^{1/2}/2, λ_ta = −d ln V/dφ (eq:kex-over-wt); chain with no solution raises | changes |
| `AbstractModel.py:850-857` | `log_V2` (ln V″ from the hooks), the root's target | record |
| `AbstractModel.py:873-879` | `lambda_ta = -_d_logV_dphi(φ_ta)`, refusal if ≤ 0, `log_k_ex` | changes |
| `AbstractModel.py:880-886` | `ell_new = log_k_ex - log_m_ta`; ℓ ≤ 0 raises "no one-loop turnaround": the chain's existence | changes in outcome (07b: 20 crossover rows gain a solution); expression unchanged |
| `AbstractModel.py:887-906` | convergence test, `OneLoopTurnaround(lambda_ta=…, log_k_ex=…, ell=…)`, non-convergence raise | changes (field name) |
| `AbstractModel.py:942`, `:991-992` | `minimum_on_trace`, `d_minimum_d_log_trace` read `_d_logV_dphi` for the minimum, not k_ex | record |

### 1.2 `CosmologyConcepts/Models/ExponentialModel.py`, `InversePowerModel.py`, `drain_model.py`, others

| file:line | what | 07c |
|---|---|---|
| `ExponentialModel.py:34` | imports `DRAIN_MODEL_ONE_LOOP` | name only |
| `ExponentialModel.py:83-93` | `_drain_L`, the law's L = ln(1 + E/V₀) (reads no k_ex) | record (whether v2 keeps L is U13) |
| `ExponentialModel.py:95-100` | `one_loop_drain_model` → `DRAIN_MODEL_ONE_LOOP`; comment: "the L law … which the measured law was fitted on" | name only (→ `one-loop-exponential-v2`); comment changes |
| `ExponentialModel.py:123-153` | hooks `_d_logV_dphi`, `_d2_logV_dphi2` | changes (add `_d3_logV_dphi3`, closed form) |
| `InversePowerModel.py:39-47` | docstring: law stored under `"one-loop-measured-pl-v1"`; reads neither ell nor L | name only (`:43`) |
| `InversePowerModel.py:59-64` | imports `DRAIN_MODEL_ONE_LOOP_PL`, regimes | name only |
| `InversePowerModel.py:79-102` | `DRAIN_PL_*` constants and their sources (fitted in ln R, not ℓ) | record (P80; unchanged) |
| `InversePowerModel.py:168-174` | `one_loop_drain_model` → `DRAIN_MODEL_ONE_LOOP_PL` | name only (→ `one-loop-inverse-power-v1`) |
| `InversePowerModel.py:188-216` | `_drain_fraction`: reads the regime only as classical (ln R ≤ 0) or unassessed; one_loop and gap give the same f; reads no ell | record |
| `InversePowerModel.py:253-268` | hooks `_d_logV_dphi`, `_d2_logV_dphi2` via s = u/(1+u) | changes (add `_d3_logV_dphi3`, overflow-safe as these) |
| `drain_model.py:30-35` | docstring: `DRAIN_MODEL_ONE_LOOP` names the measured law, `DRAIN_MODEL_ONE_LOOP_PL` the power wall's | changes (v2, retired v1, new PL name) |
| `drain_model.py:49-62` | comment ("THE VERSION MOVES …") and `DRAIN_MODEL_ONE_LOOP = "one-loop-measured-v1"`, `DRAIN_MODEL_ONE_LOOP_PL = "one-loop-measured-pl-v1"`, `DRAIN_MODELS` | changes (v2 value; PL rename; v1 kept as a known, refused name, P85) |
| `drain_model.py:64-69` | `_LAW_OF_MODEL` | changes (v2 and the renamed PL; v1's entry refused) |
| `drain_model.py:71-78` | `REGIME_*`, `DRAIN_REGIMES`; comment "a one-loop solution of ell <= 0 called gap (P24)" | record (unless U13 changes the separation) |
| `drain_model.py:81-92` | `law_of` | changes (the refusal of v1 naming R13 lands here or in `check_drain_model`) |
| `drain_model.py:95-119` | `_MODEL_OF_LAW` and `model_of_law`: the one-loop law's current model is `DRAIN_MODEL_ONE_LOOP` | name only (follows the constant) |
| `drain_model.py:164-171` | `check_drain_model` | changes if the v1 refusal goes here |
| `CosmologyConcepts/alpha_value.py:17-45` | `alpha_value` checks (value, drain_model) by `check_drain_model` | reads (refuses v1 through `drain_model.py`) |
| `CosmologyConcepts/Models/model_options.py:102-124` | `one_loop_drain_model(_over)`: each potential's own model | reads |

### 1.3 `ComputeTargets/ScalarHistory.py`

| file:line | what | 07c |
|---|---|---|
| `ScalarHistory.py:44-65` | imports `DRAIN_MODEL_ONE_LOOP`, regimes, `ClassicalTurningPoint`, `OneLoopTurnaround` | reads |
| `ScalarHistory.py:166-172` | `MARGIN_THRESHOLD` = ln 10 (the birth margin ln(k_ex/m) among the margins) | record (P88) |
| `ScalarHistory.py:175-228` | the law's comment: f by regime, one_loop ℓ ≥ 1 / ℓ < 1, gap, classical; constants' sources (fit_law.txt) | changes (v2 form, U13) |
| `ScalarHistory.py:229` | `DRAIN_ALPHA_CAP` 0.99 (not fitted against ℓ) | record |
| `ScalarHistory.py:230-239` | `DRAIN_ONE_LOOP_A/B0/C`, `DRAIN_ELL_PLATEAU`, `DRAIN_F_CAP`, `DRAIN_F_PLATEAU`, `DRAIN_GAP_G/R0/P/Q`: fitted against ℓ and the regime | changes (refit, U13) |
| `ScalarHistory.py:241-295` | `DRAIN_S_TABLE_L/S`, S(L) of the classical branch (an energy ratio, no ℓ, no x) | record (whether v2 keeps it is U13) |
| `ScalarHistory.py:297-301` | comment: `DRAIN_MODEL_ONE_LOOP ("one-loop-measured-v1")`, version moves with the law | name only |
| `ScalarHistory.py:303-307` | comment on the regimes (gap includes ℓ ≤ 0) | record |
| `ScalarHistory.py:620-635` | `Injection.classical_turning_point` comment: "phi_t, ln omega_t^2, ln R, lambda_t, ln k_ex and ell_t" | changes (field name) |
| `ScalarHistory.py:793-797` | `IntegrationResult.peak_estimates` | reads |
| `ScalarHistory.py:830-846` | `DrainCandidate`; comment `ell: ln(k_ex/m_ta) of the one-loop turnaround` | reads (value moves) |
| `ScalarHistory.py:849-856` | `DrainRecord.model` comment: `DRAIN_MODEL_ONE_LOOP` for the exponential, `_PL` for the power wall | name only |
| `ScalarHistory.py:1500-1532` | `assess_infall` → `classical_turning_point` | reads |
| `ScalarHistory.py:1535-1550` | `unresolved_turnaround`: reads φ_t and ln R only | record |
| `ScalarHistory.py:1875-1908` | `check_drain_law_model`: reads the model's one-loop drain model | reads |
| `ScalarHistory.py:1911-1930` | `check_key_drain_model`: a key's model is `fixed` or the model's own | reads (v1 refused upstream, P85) |
| `ScalarHistory.py:1933-1960` | `drain_regime`: one_loop iff ln R > 0 and the chain solves with ℓ > 0; else gap | reads (classification moves); changes only if U13 rules another separation (P79 iii) |
| `ScalarHistory.py:1963-1977` | `drain_law_inputs`: (L, ln R) | record |
| `ScalarHistory.py:1980-1990` | `drain_S_of_L` | record |
| `ScalarHistory.py:1993-2031` | `drain_fraction`: v1's f by regime, reads ℓ | changes (v2, U13) |
| `ScalarHistory.py:2034-2065` | `drain_alpha_effective`: passes ℓ to the L law or to the model's own fraction | reads |
| `ScalarHistory.py:2280-2307` | the loop's drain model: the model's own under the one-loop law | reads |
| `ScalarHistory.py:2545-2563` | the loop: `drain_regime`, `drain_law_inputs`, `drain_alpha_effective` once per candidate | reads |
| `ScalarHistory.py:2611-2625` | `DrainCandidate(regime=, ell=, …)`, `peak_estimates.append` | reads |
| `ScalarHistory.py:4009-4010`, `:4076-4077` | `log_R_first_impact` (ln R, not k_ex) | record |
| `ScalarHistory.py:4036-4061` | `DissipationValues.worst_birth_margin(_N)`, `birth_failing_share` | reads (values move) |
| `ScalarHistory.py:4118-4128` | `ESTIMATE_*`; `PeakEstimate(estimate, log_k_ex, log_m, log_R, gap_reason)` | reads (values and estimate kinds move) |
| `ScalarHistory.py:4130-4148` | `MarginCandidate.log_k_ex` | reads |
| `ScalarHistory.py:4155-4192` | `Generation`: `log_q = ln k_ex + N_turn`, `birth_margin = ln k_ex − ln m` ("ell_t or ell, bitwise") | reads (values move) |
| `ScalarHistory.py:4194-4200` | `MarginFailure.log_q` | reads |
| `ScalarHistory.py:4265-4282` | `peak_estimate` docstring: "the chain has no solution, the gap of 1 < R < 10^2.9" | changes (docstring: the gap's extent moves) |
| `ScalarHistory.py:4285-4333` | `_assessed_peak`: classical (`ct.log_k_ex`, ln ω_t), one-loop (`turnaround.log_k_ex`, ln m_ta) or classical_gap | reads (values move) |
| `ScalarHistory.py:4405-4412`, `:4459-4500` | `relativistic_margins`: `log_q`, `birth = log_k_ex − log_m`, failures | reads |
| `ScalarHistory.py:4610-4648` | `margin_inputs`: reuses the loop's estimates, else calls `peak_estimate` (the fixed law) | reads (fixed-law values move here) |
| `ScalarHistory.py:4822-4857` | `BounceRow`: `regime`, `ell`, `log_k_ex`, `log_q`, `birth_margin` | reads |
| `ScalarHistory.py:4916-5005` | `bounce_rows`: fills `log_k_ex`, `ell`, `log_q`, `birth_margin` | reads |
| `ScalarHistory.py:5105-5127` | `DrainHistoryValues`: `drained_share_<regime>`, `candidates_<regime>`, `ell_min`, `all_worst_birth_margin` | reads |
| `ScalarHistory.py:5135-5166` | `all_generation_margins` (`MARGIN_THRESHOLD`) | reads |
| `ScalarHistory.py:5186-5208` | `drain_history_values`: regime counts and shares, `ell_min` | reads |
| `ScalarHistory.py:6280-6300` | `extra_data["margins"]`: first failures (with `log_q`), generation counts per estimate | reads |

### 1.4 `ComputeTargets/quanta_spectrum.py`

| file:line | what | 07c |
|---|---|---|
| `quanta_spectrum.py:15-21` | docstring: a re-measurement changes this module and the wall's `SPECTRUM_MODELS` string | record |
| `quanta_spectrum.py:23-27` | Q_g = k_ex,g a(N_g), x = q/Q_g | record |
| `quanta_spectrum.py:44-61` | shape docstring: x_lo at k/ω_t = 1e-3, x_* = 0.0058 on every wall | changes (x_*) |
| `quanta_spectrum.py:63-69` | "x = k/k_ex, k_ex the production's (lambda_t sqrt(E)/2 at phi_t …)"; 0.494535 ω_t; power wall k_ex = (s_t/2)√(n/(n+1)) ω_t and R9's near-floor shift by s_t | changes (07b: 0.516395; power wall (n+2)/(2√(n(n+1))) ω_t at every E/V₀, so the s_t paragraph goes) |
| `quanta_spectrum.py:76-111` | kernels docstring: "y < G_Y_MIN < x_lo on every wall" (`:90`), "κ_hi ≥ 2.15", "7.4e-12 on the power wall at n = 1" (`:97-98`), "OMEGA_U_MAX = 9 puts x_lo e^u ≥ 16.4 on every wall" (`:105-106`) | changes (new x_lo of power-n1, 9.43e-4, is below G_Y_MIN = 1e-3; x_lo e^9 falls to 7.64; κ_hi now 4.34/6.55/5.47) |
| `quanta_spectrum.py:116-118` | `PEAK_SPECTRUM` docstring, s = δ(x − 1) | record |
| `quanta_spectrum.py:141-150` | `SPECTRUM_MODELS` comment and strings `R8-exponential-v1`, `R8-power-n1-v1`, `R8-power-n2-v1` | name only (new strings, re-measured tables) |
| `quanta_spectrum.py:168-177` | `X_STAR` = 0.0058 = 0.0029 ω_t over k_ex = ω_t/2; its comment | changes (P82: 0.005616) |
| `quanta_spectrum.py:191-195` | `G_Y_MIN`, `G_Y_MAX`, `OMEGA_U_MAX`: grid ends chosen against x_lo and κ_hi | changes (bounds re-derived on the new x_lo; values may stay) |
| `quanta_spectrum.py:210-222` | tables' header: references, k_ex/ω_t 0.4945…, 0.3535…, 0.4082…; X_LO, X_HI rule; spline check 2.8e-6 | changes |
| `quanta_spectrum.py:224-226` | `WallTable` fields incl. `k_ex_over_omega_t`, `x_lo`, `x_hi` | record (names stay) |
| `quanta_spectrum.py:228-382` | `_LN_BETA2_EXPONENTIAL` (153 nodes) | changes (07b: 151 nodes, x_hi 3.5) |
| `quanta_spectrum.py:384-541` | `_LN_BETA2_POWER_N1` (156 nodes) | changes (07b: 163 nodes, x_hi 3.0) |
| `quanta_spectrum.py:543-700` | `_LN_BETA2_POWER_N2` (156 nodes) | changes (07b: 158 nodes, x_hi 3.0) |
| `quanta_spectrum.py:702-727` | `_TABLES`: `k_ex_over_omega_t`, `x_lo`, `x_hi` per wall | changes (07b: 0.5163952511757223 / 0.0019365011543448797 / 3.5; 1.0606601717798214 / 0.0009428090415820634 / 3.0000000000000004; 0.816496580927726 / 0.001224744871391589 / 3.0000000000000004) |
| `quanta_spectrum.py:735-806` | `_Direct`: x_p, κ_hi, C₁ = 49 x_* s_dir(x_*) derived from the table | reads (derived; 07b x_p 0.619424, 0.155250, 0.253372) |
| `quanta_spectrum.py:832-834` | `x_peak` | reads |
| `quanta_spectrum.py:905-963` | `QuantaSpectrum`: model string, x_lo, x_hi, x_p, I, J, `log_s_peak` | reads |
| `quanta_spectrum.py:1161-1189` | `PeakSpectrum`, δ(x − 1) (Derive gate comparison only) | record |

### 1.5 `ObjectFactories/`, `config/`, drivers, `extract_common.py`, `RunDrivers/`

| file:line | what | 07c |
|---|---|---|
| `ObjectFactories/ScalarHistory.py:165-192` | `BOUNCE_COLUMNS`: `regime`, `ell`, `log_k_ex`→`log_k_ex_Mp`, `log_q`→`log_q_Mp`, `birth_margin` | reads (values move; no column changes, FINGERPRINT_FORMAT 8) |
| `ObjectFactories/ScalarHistory.py:475-477` | columns `worst_birth_margin`, `worst_birth_margin_N`, `birth_failing_share` | reads |
| `ObjectFactories/ScalarHistory.py:485-511` | columns of `DrainHistoryValues` (`ell_min`, `candidates_<regime>`, `drained_share_<regime>`, `all_worst_birth_margin…`) | reads |
| `ObjectFactories/ScalarHistory.py:1419-1438` | `ScalarHistoryBounce` columns `regime`, `ell`, `log_k_ex_Mp`, `log_q_Mp`, `birth_margin` | reads |
| `ObjectFactories/alpha_value.py:21-25`, `:54-71` | the key (value, drain_model), checked by `check_drain_model` | reads |
| `config/argument_parser.py:129-138` | `--A-values` looked up under each potential's own model | reads |
| `config/argument_parser.py:363-366` | `--A-values` help: "one-loop-measured-v1 for exponential, one-loop-measured-pl-v1 for inverse-power" | name only |
| `config/version.py:76-79` | dated comment (2026.10.0) naming `"one-loop-measured-v1"` | record |
| `config/version.py:100-109` | dated comment (2026.12.0) naming `"one-loop-measured-pl-v1"` | record (07c appends a 2026.13.0 paragraph) |
| `config/version.py:110` | `VERSION_LABEL = "2026.12.0"` | changes (U12: 2026.13.0) |
| `main.py:1015-1037`, `:1181-1183`, `:1224-1226` | A rows looked up under each potential's own model | reads |
| `plot_inputs.py:108-211` | `A_drain_models`, each potential's own | reads |
| `plot_by_beta.py:1145-1230`, `plot_ScalarHistory.py:1621-1692` | the same, and `alpha_path` per row's model | reads |
| `extract_common.py:144-157` | `alpha_path`: `<drain_model>/A=<A>` (directory name follows the model) | reads |
| `extract_common.py:637` | `DRAIN_CSV_COLUMNS` = `DrainHistoryValues` fields (`ell_min`, regime shares and counts, birth margins) | reads |
| `extract_common.py:1475-1505` | `BOUNCES_CSV_COLUMNS` from `BOUNCE_COLUMNS` (`ell`, `log_k_ex_Mp`, `log_q_Mp`, `birth_margin`) | reads |
| `RunDrivers/` | no hit | — |

## 2 tools/

| file:line | what | 07c |
|---|---|---|
| `tools/quanta_measures.py:29-30` | docstring: one spectrum per wall, `SPECTRUM_MODELS` | reads |
| `tools/quanta_measures.py:42-43` | generations: ln Q_g = `log_q_Mp` | reads |
| `tools/quanta_measures.py:71-73` | phase slip and k_ex/H from `log_k_ex_Mp` | reads |
| `tools/quanta_measures.py:126-172` | output columns `spectrum_model`, `k_ex_over_H`, `k_ex_over_H_min` | reads |
| `tools/quanta_measures.py:265-284` | `spectrum_models_text`, `qs.spectrum_model(wall_of(row))` | reads (strings follow the module) |
| `tools/quanta_measures.py:309-311` | reads `log_k_ex_Mp`, `log_q_Mp` | reads |
| `tools/quanta_measures.py:468-545`, `:662`, `:688`, `:716`, `:801` | A_g, y, u, weights from `log_q`; k_ex/H; the model columns | reads (outputs move with the stored values and the tables) |
| `tools/plot_quanta_history.py:30`, `:40-47` | docstring: `log_q_Mp` points by regime; `k_ex_over_H` panel | reads |
| `tools/plot_quanta_history.py:133-135`, `:214-216`, `:261` | generations by `log_q_Mp`, coloured by regime | reads |
| `tools/plot_quanta_history.py:383-406` | the k_ex/H panel from `log_k_ex_Mp` | reads |
| `tools/plot_quanta_history.py:565-584` | `spectrum_model_of`, `read_checked` | reads |
| `tools/plot_quanta_grid.py:44`, `:108-109` | panels (d) `ell_min` and (e) `all_worst_birth_margin` | reads |
| `tools/plot_quanta_grid.py:411`, `:464` | one-loop rows by `drain_model != fixed`; `spectrum_model_of` | reads |
| `tools/quanta_figures.py:21-30`, `:58-66` | regime colours and labels | reads |
| `tools/quanta_figures.py:123-127`, `:186-190` | `spectrum_model_of`; refusal of a row whose `spectrum_model` is not the wall's | reads |
| `tools/history_and_bbn.py:92-100` | docstring: "the model is the record's DrainRecord.model, `one-loop-measured-v1`"; `ell_min` | name only |
| `tools/history_and_bbn.py:389-412` | `drain_values`: counts and shares by regime, `ell_min` | reads |
| `tools/bbn_from_store.py:66-67` | docstring: `one-loop-measured-v1` for the exponential, `one-loop-measured-pl-v1` for the power law | name only |
| `tools/inventory_report.py:302-310`, `tools/outward_step_scan.py:126-928` | `DRAIN_MODEL_FIXED` only | record |

## 3 Tests

### 3.1 `ComputeTargets/tests/`

| file:line | what | 07c |
|---|---|---|
| `test_drain_law.py:67`, `:794`, `:822` | imports and calls `classical_turning_point_before_fix` (raising behaviour only) | reads |
| `test_drain_law.py:128-148` | synthetic `ClassicalTurningPoint(lambda_t=…)`, `OneLoopTurnaround(lambda_ta=…)` | re-pins (field names) |
| `test_drain_law.py:217-235` | constants and names: `DRAIN_MODEL_ONE_LOOP == "one-loop-measured-v1"` (`:232`) | name only |
| `test_drain_law.py:385` | sentinel `DrainRecord(…, "one-loop-measured-v1", …)` | name only |
| `test_drain_law.py:433-503` | regimes on mocked turnarounds (ℓ ≤ 0 is gap) | reads (unless U13 changes the separation) |
| `test_drain_law.py:504-518` | alpha_eff by v1's form f = 1 − exp(−2.69/(ℓ + 0.9691 − 0.41 ln L)) | re-pins (v2 form) |
| `test_drain_law.py:520-531` | the cap at v1's plateau (f = 0.9 at ℓ < 1, A = 1.1) | re-pins (v2) |
| `test_drain_law.py:612-708` | 2197 at A = 1: `PINNED_ALPHA_EFF` (`:627-633`, 8.2277e-02 … 1.6185e-01), `PINNED_PARKING_N` 28.876391476457353 (`:634`), five one_loop candidates (`:701`), model name (`:653`) | re-pins (v2 and new ℓ) |
| `test_drain_law.py:717-754` | reuse of estimates; regime ↔ estimate correspondence | reads |
| `test_inverse_power_model.py:227-246` | centred differences of the hooks | changes (add the third hook) |
| `test_inverse_power_model.py:252` | `HOOKS` raising at φ ≤ 0 | changes (add the third hook) |
| `test_inverse_power_model.py:297-319` | `_the_chain`: closed form λ_ta = n s/φ_ta, k_ex = λ_ta E^{1/2}/2 | re-pins (new closed form) |
| `test_inverse_power_model.py:322-342` | docstring: λ_t = n s_t/φ_t and its tolerance | changes |
| `test_inverse_power_model.py:345-387` | classical turning point against closed forms: `lam` (`:362`), `log_k` (`:364`), `c.lambda_t` (`:378`), `ell_t` (`:384`) | re-pins |
| `test_inverse_power_model.py:389-405` | one-loop turnaround against `_the_chain` (`lambda_ta`, `log_k_ex`, `ell`) | re-pins |
| `test_measured_drain_law.py:20-31` | docstring: v1's law by regime | changes |
| `test_measured_drain_law.py:119-139` | `fraction_adopted`, v1's reference implementation | re-pins (v2 reference) |
| `test_measured_drain_law.py:145-155` | synthetic `ClassicalTurningPoint(lambda_t=…)` | re-pins (field name) |
| `test_measured_drain_law.py:237-289` | constants pinned (`:240-250`) and `DRAIN_MODEL_ONE_LOOP == "one-loop-measured-v1"` (`:252`) | re-pins / name only |
| `test_measured_drain_law.py:291-346` | the S table | reads (unless U13 drops it) |
| `test_measured_drain_law.py:348-457` | v1's law at synthetic inputs (ℓ = 1, cap, d ≤ 0, gap's r0 1.5601, …) | re-pins |
| `test_measured_drain_law.py:459-533` | bitwise against `fraction_adopted` on the grid | re-pins |
| `test_measured_drain_law.py:535-592` | the gap below L₀ = e^{−1/q} | re-pins (if v2 keeps the gap form) |
| `test_measured_drain_law.py:595-688` | `TestLAndU15`: L and ln R only | reads |
| `test_measured_drain_law.py:737-835` | the model hook `_drain_L` | reads |
| `test_unresolvable_turning_points.py:242-290` | `classical_turning_point_before_fix`, a verbatim copy of the old method, computing λ_t and k_ex in the old form (`:264-285`) | re-pins (copy computes old k_ex) |
| `test_unresolvable_turning_points.py:405-460` | (3) the method equals the copy bitwise, whole tuple, where the predicate is False | re-pins (λ, k_ex, ℓ_t differ; compare φ_t, ω_t, ln R, or update the copy) |
| `test_unresolved_turnarounds.py:157-166` | synthetic `ClassicalTurningPoint(lambda_t=1.0, …)` | re-pins (field name) |
| `test_wall_model.py:107-176` | `_PowerLawWall` stand-in model with its hooks | changes (third hook) |
| `test_wall_model.py:178-208` | `_BumpWall` stand-in | changes (third hook, if the search reaches k_ex) |
| `test_wall_model.py:224-262` | (1) closed form on the exponential: λ_t = y²/M, k_ex = (y²/M)E^{1/2}/2 (`:228-229`, `:253`), to 1e-12 | re-pins |
| `test_wall_model.py:273-298` | `_the_notes_chain`: k_ex = y²E^{1/2}/2M (eq:kex-Ts) | re-pins |
| `test_wall_model.py:301-334` | (2) worked example: chain agreement; the notes' printed y = 39.1, Λ = 53.8, k_ex = 5.6e13 GeV ± 3% (`:330-334`) | re-pins (the notes' values are the old form's) |
| `test_wall_model.py:343-349` | comment: ℓ = 40, 25 (n = 1), 36, 13 (n = 2); the chain has none at M = 1e-16 M_P, R = 10^-2.9 | changes (comment; chain existence) |
| `test_wall_model.py:382-414` | (3) identities: `c.lambda_t == -d_logV_dphi` (`:408`), `log_k` (`:409-414`) | re-pins |
| `test_wall_model.py:421-423` | comment: "d ln V''/dx needs (ln V)''', which no hook gives" | changes (the hook exists) |
| `test_wall_model.py:452-458` | `o.lambda_ta == -d1`, k_ex = λ_ta E^{1/2}/2 | re-pins |
| `test_wall_model.py:680-692` | (5) "a one-loop call without a solution raises" at M = 1e-16 M_P, R = 10^2.49 (ℓ falls to −0.19) | re-pins (the chain may now solve there) |
| `test_wall_model.py:717-735` | (6) `RealizedModel` delegates | changes (the new delegate) |
| `test_model_interface.py:78` | `POTENTIAL_METHODS = ("log_V", "d_logV_dphi", "d2_logV_dphi2")` | changes (the new delegate) |
| `test_model_interface.py:107-150` | `_MinimalModel` stand-in with the two hooks | changes (third hook if abstract) |
| `test_margins.py:118-130`, `:150-300` | constructed candidates, the core against a hand computation | reads |
| `test_margins.py:398` | `MARGIN_THRESHOLD == log(10.0)` | record (P88) |
| `test_margins.py:446-510` | (3) M = 1e-16 M_P, β = 2, fixed law α = 0.1: classical_gap for 0 < log10 R ≤ 2.92, one-loop at ≥ 3.00; 3 gap / 1 one-loop; shares 0.5877 / 0.2812; birth margin = ℓ / ℓ_t bitwise | re-pins (chain existence moves) |
| `test_margins.py:514-540` | (4) M = 0.5 M_P: every generation classical, birth margin = ℓ_t and negative ("k_ex/ω_t → 1/2") | re-pins if the sign moves (07b: k_ex/ω_t = 0.516 at L = 90; larger at small L) |
| `test_margins.py:727` | warning line formats `worst_birth_margin` | reads |
| `test_power_wall_law.py:20-36` | docstring naming both models | name only |
| `test_power_wall_law.py:138-161` | `DRAIN_MODEL_ONE_LOOP_PL == "one-loop-measured-pl-v1"`, `DRAIN_MODELS`, `model_of_law → "one-loop-measured-v1"` (`:144`), labels | name only |
| `test_power_wall_law.py:183-197` | each model's one-loop drain model | name only |
| `test_power_wall_law.py:295-301` | `DRAIN_PL_*` against `fit_fn.py`'s | record (P80) |
| `test_power_wall_law.py:399` | n = 2 history's record names `one-loop-measured-pl-v1` | name only |
| `test_drain_store_records.py:83-95`, `:244-264`, `:479-483` | constructed `DrainCandidate`/`PeakEstimate` (ℓ = 8, 9, 7.5) | reads |
| `test_drain_store_records.py:106-154`, `:529`, `:658`, `:668` | `DRAIN_MODELS` tuple, `law_of`, `alpha_value` rows under `"one-loop-measured-v1"` | name only |
| `test_drain_store_records.py:687-767` | 2197 under the one-loop law: estimates ↔ regimes, tool equality | reads |
| `test_quanta_spectrum.py:93-112` | `SPECTRUM_MODELS` structure; `!= "U4-U17-v1"` | reads |
| `test_quanta_spectrum.py:120-134` | removed names (X_PEAK, KAPPA, …); `X_STAR == 0.0058` (`:133`) | re-pins (0.005616) |
| `test_quanta_spectrum.py:140-155` | k_ex/ω_t 0.4945…, 0.3535…, 0.4082… (`:142-144`); node counts 153/156 (`:151`); x_hi (4.0, 6.5, 5.5) (`:153-155`) | re-pins (0.5163…, 1.0606…, 0.8164…; 151/163/158; 3.5, 3.0, 3.0) |
| `test_quanta_spectrum.py:211-221` | x_p expected 0.646803, 0.465750, 0.506743 | re-pins (0.619424, 0.155250, 0.253372) |
| `test_quanta_spectrum.py:236-250` | C₁ = 49 x_* s_dir(x_*); on the exponential C₁ ≈ 49 x_*³ to 3 places | re-pins if x_* moves the check |
| `test_quanta_spectrum.py:366-385` | the G series at `G_Y_MIN`, whose derivation assumes y < x_lo | re-pins (power-n1's new x_lo < G_Y_MIN) |
| `test_quanta_spectrum.py:408-432` | Ω asymptote at `OMEGA_U_MAX`; remainder above x_lo e^U below 1e-14 | re-pins (x_lo e^9 = 7.64 on power-n1) |
| `test_a_values_option.py` `:25,47,63,305,375-392,424-450` | `ONE_LOOP = "one-loop-measured-v1"`, paths `one-loop-measured-v1/A=…`, labels, `model_of_law` | name only |
| `test_a_values_plot_inputs.py` `:45,77,118,155-161,196-197,222` | rows and messages under `DRAIN_MODEL_ONE_LOOP` / the literal | name only |
| `test_bbn_from_store_model.py` `:26-27,53-54,73-74,175,181,229,241,256` | rows under both names, incl. "inverse-power under v1" (`:27`) | name only |
| `test_models_export.py` `:47-59,128-129` | CSV bytes and paths naming both models | name only |
| `test_models_main.py` `:46-47,239,274` | `V1`, `PL` and the printed grid lines | name only |
| `test_models_option.py` `:26,59-60` | `PL`, `V1` | name only |
| `test_models_plot_inputs.py` `:52-53,299` | `V1`, `PL` | name only |
| `test_wall_rounding.py:226-280` | `classical_turning_point` at band inputs; asserts φ_t only | reads |

### 3.2 `ObjectFactories/tests/`, `tools/tests/`, `RunDrivers/tests/`, `CosmologyModels/tests/`

| file:line | what | 07c |
|---|---|---|
| `ObjectFactories/tests/test_drain_store.py:26-30` | docstring: L-M8e-18 at A = 2 has "three one-loop candidates and one in the gap" | changes (docstring) |
| `ObjectFactories/tests/test_drain_store.py:81` | `ONE_LOOP = "one-loop-measured-v1"` | name only |
| `ObjectFactories/tests/test_drain_store.py:287` | P-2197: every candidate one-loop | reads (regime-dependent; expected to hold, ℓ rises) |
| `ObjectFactories/tests/test_drain_store.py:334-352` | `test_a_gap_candidate` (L-M8e-18): a gap bounce row exists | re-pins (the gap candidate may now solve) |
| `ObjectFactories/tests/test_drain_store.py:484` | unknown model `one-loop-measured-v0` refused | reads |
| `ObjectFactories/tests/test_export_04.py` `:290-313` | labels `A = … (one-loop-measured-v1)` | name only |
| `ObjectFactories/tests/test_export_04.py:201`, `:340-380` | constructed `ell`, `log_k_ex`, `log_q`, `birth_margin` | reads |
| `ObjectFactories/tests/test_inverse_power_store.py` `:34,40-41,83,465` | `PL` name; `InversePowerModel` under v1 refused (`:40`) | name only (v1 then refused for every model) |
| `ObjectFactories/tests/test_inverse_power_store.py:480` | power wall n = 1, A = 1: every candidate one-loop | reads (regime-dependent) |
| `ObjectFactories/tests/test_dissipation_store.py:115`, `:120`, `:348` | margins keys; `worst_birth_margin` not None | reads |
| `ObjectFactories/tests/test_store_fingerprint_golden.py:180-181` | literal `worst_birth_margin` in the golden store's row | record |
| `tools/tests/test_quanta_measures.py` `:62,68` | `ONE_LOOP = DRAIN_MODEL_ONE_LOOP` | name only |
| `tools/tests/test_quanta_measures.py:120-203`, `:225-266`, `:371-417`, `:649-672`, `:730`, `:824-866` | expectations formed from `qs` (x_peak, shape, I) and the rows' `log_q`, `log_k_ex` | reads (follow the module) |
| `tools/tests/test_quanta_figures.py` `:47,53,343,347,355` | `ONE_LOOP`, paths `one-loop-measured-v1/A=1/…` | name only |
| `tools/tests/test_quanta_figures.py:57`, `:60-71`, `:102-103`, `:162-163`, `:298-329` | constructed generations (ln q, ln k_ex), `ell_min`, spectrum-model check | reads |
| `tools/tests/test_plot_quanta_history.py` `:236-237,256,280` | `one-loop-measured-v1 …` title and path; `"R8-exponential-v1"` (`:237`) | name only |
| `tools/tests/test_plot_quanta_history.py:211-212` | six assessed candidates have k_ex/H (constructed) | reads |
| `tools/tests/test_plot_quanta_grid.py` `:237,239,251,289-355,369,397,440` | `one-loop-measured-v1` paths; `"R8-exponential-v1"` (`:237`) | name only |
| `tools/tests/test_plot_quanta_grid.py:85-88`, `:204`, `:375-380` | panels `ell_min`, `all_worst_birth_margin` (constructed) | reads |
| `tools/tests/test_quanta_models.py` `:20-21,56,132,222,230,237,245,253` | `PL = "one-loop-measured-pl-v1"`; `R8-power-n1-v1`, `R8-power-n2-v1` | name only |
| `tools/tests/test_quanta_models.py:85-89`, `:152-160` | constructed `ell_min`, birth margin; `log_q_Mp` | reads |
| `tools/tests/quanta_exports.py:97-129` | constructed rows (`log_q`, `log_k_ex`, `birth_margin = 5.0`) | reads |
| `tools/tests/test_history_and_bbn_drain_law.py` `:28-31,154,186,218,223` | `one-loop-measured-v1` in records and the printed line; `ell_min` 9.5 (constructed) | name only |
| `tools/tests/test_history_directory.py:59` | path `QCD_Cosmology/one-loop-measured-v1/A=1` | name only |
| `tools/tests/test_inventory_alpha.py:167` | `ONE_LOOP = "one-loop-measured-v1"` | name only |
| `tools/tests/test_no_alpha_values.py` `:55,249,258,267` | `DRAIN_MODEL_ONE_LOOP`, `A_drain_models=["one-loop-measured-v1"]` | name only |
| `tools/tests/test_one_loop_tools.py` `:51,123,134,151,162,173,177,180,187,190,218` | `DRAIN_MODEL_ONE_LOOP` in queries and texts | name only |
| `RunDrivers/tests/` | no hit | — |
| `CosmologyModels/tests/` | no hit (`KAPPA_*` in `test_temperature_law.py` is the temperature law's) | — |

## 4 Goldens and test data

No golden holds a k_ex column (`ell`, `ell_t`, `ell_min`, `log_k_ex_Mp`, `log_q_Mp`, `birth_margin`, λ) or an ℓ-dependent
value, and none is under a law that reads ℓ. The `ee01` files are the live ones (the generators' `GOLDEN_PATH`); the others
are records, never rewritten.

| file:line | what | 07c |
|---|---|---|
| `ComputeTargets/tests/data/golden_history_01.json`, `_03`, `_05`, `_rr03` | natural mode (α = 0), M = 1e-3 M_P, β = 1.5, 5.5: samples' SHA, counts, excerpts; no k_ex field | record |
| `ComputeTargets/tests/data/golden_history_ee01.json` | the same, live | record (bitwise) |
| `ComputeTargets/tests/data/golden_history_06_tuned.json`, `_rr03_tuned` | natural mode, tuned, M = 1e-2 M_P, β = 3, no reflection | record |
| `ComputeTargets/tests/data/golden_history_ee01_tuned.json` | the same, live | record (bitwise) |
| `ComputeTargets/tests/data/golden_history_rr03_reflecting.json` | fixed law α = 0.05, M = 1e-12 M_P, β = 1.5, tuned, 32 reflections; pins samples, counts, tuning, not the margins | record |
| `ComputeTargets/tests/data/golden_history_ee01_reflecting.json` | the same, live (35 reflections); the integration reads no ℓ; `history_margins` computes peaks under the new k_ex but the golden pins none | record (bitwise) |
| `ObjectFactories/tests/data/store_fingerprint_*.json` (8 files) | schema digests of the golden store (format 8 live: `power-law-wall-05`); no k_ex value; the golden store's `alpha_value` rows are fixed-law | record (unless 07c changes a table or column) |

## 5 Docstrings and comments stating the old form

Each of these is also a row above; this section collects the statements of λ = −d ln V/dφ, k_ex = λE^{1/2}/2,
eq:kex-over-wt, k_ex = ω_t/2, or the gap's old extent.

| file:line | what | 07c |
|---|---|---|
| `CosmologyConcepts/Models/AbstractModel.py:69-70`, `:77` | the tuples' comments (§1.1) | changes |
| `CosmologyConcepts/Models/AbstractModel.py:750-752`, `:764` | `classical_turning_point` docstring (eq:kex-over-wt, ℓ_t ≈ ln ½) | changes |
| `CosmologyConcepts/Models/AbstractModel.py:798`, `:876` | error messages "-d ln V/dphi = … <= 0" | changes |
| `CosmologyConcepts/Models/AbstractModel.py:821-825` | `one_loop_turnaround` docstring | changes |
| `ComputeTargets/ScalarHistory.py:633` | `Injection` comment, "lambda_t, ln k_ex and ell_t" | changes |
| `ComputeTargets/ScalarHistory.py:834` | `DrainCandidate` comment, ℓ = ln(k_ex/m_ta) | record (still true) |
| `ComputeTargets/ScalarHistory.py:4273` | "the gap of 1 < R < 10^2.9" | changes |
| `ComputeTargets/quanta_spectrum.py:63-69` | "lambda_t sqrt(E)/2 at phi_t", 0.494535, s_t | changes |
| `ComputeTargets/quanta_spectrum.py:170-171` | "With k_ex = omega_t/2, x_* = 0.0058" | changes |
| `ComputeTargets/quanta_spectrum.py:216-218` | k_ex/ω_t of the references | changes |
| `ComputeTargets/tests/test_inverse_power_model.py:301`, `:326` | k_ex = λ_ta E^{1/2}/2, λ_t = n s_t/φ_t | changes |
| `ComputeTargets/tests/test_wall_model.py:27`, `:228-229`, `:278`, `:308`, `:452` | the closed forms and the notes' k_ex | changes |
| `ComputeTargets/tests/test_wall_model.py:345`, `:683-684` | the chain's non-existence at M = 1e-16 M_P (R = 10^-2.9, 10^2.92) | changes |
| `ComputeTargets/tests/test_margins.py:450-452`, `:517` | the gap at log10 R ≤ 2.92; k_ex/ω_t → ½ | changes |
| `ComputeTargets/tests/test_unresolvable_turning_points.py:274`, `:278` | the copy's message and k_ex (a verbatim copy of the old method) | record or changes, with §3.1's choice |

## 6 CLAUDE.md and the documents of record

| file:line | what | 07c |
|---|---|---|
| `CLAUDE.md:139-141` | `alpha_value`'s pair: `"one-loop-measured-v1"`, the one-loop law's model and version | changes (the name; whoever edits CLAUDE.md) |
| `CLAUDE.md:146-148` | the plot scripts' `--A-values` check names `one-loop-measured-v1` | changes (the name) |
| `.documents/architecture-summary.md` (21 lines; e.g. `:1180`, `:1486-1512`, `:1567`, `:1868-1876`, `:1920-1923`) | `ClassicalTurningPoint`/`OneLoopTurnaround` with λ, k_ex = λ_t E^{1/2}/2; `one-loop-measured-v1` | record (additive, convention 6) |
| `.documents/numerical-strategies.md` (25 lines; e.g. `:324-392`, `:422-428`, `:1685-1689`, `:1946-2004`) | the laws by ℓ, k_ex = ω_t/2, birth margin = ℓ_t or ℓ; §6.12 under `one-loop-measured-v1` | record (additive) |
| `science-runs/README.md:37-38` | standard analyses Figure 6 and Relativistic quanta: inputs `histories.csv`, `fields.csv`, `bounces.csv`, `quanta_measures.py` | record (P89: amended only if 07c renames a file or column) |
| `prompts/INDEX.md:33` | the live campaign's row (re-plan on one k_ex) | record |
| `.documents/OPEN_ISSUES.md` | no row on k_ex, ℓ or the gap | record |

## 7 Records (directory-level)

Files tracked by git that mention k_ex, ℓ, λ_t/λ_ta, ℓ_t, `log_k_ex` or `one-loop-measured` (`git grep -l`). Not changed.

| directory | files | 07c |
|---|---|---|
| `prompts/one-loop-drain/source/` (`crossover/` 55, `probe/` 11, three notes) | 69 | record (v1's reference `measured_law_map.py`) |
| `prompts/one-loop-drain/` (prompts 13, `logs/` 11, `orchestrator/` 7) | 31 | record |
| `prompts/inelastic-reflection/` (`source/` 9, prompts 7, `logs/` 6, `orchestrator/` 2) | 24 | record |
| `prompts/electroweak-eos/` | 1 | record |
| `prompts/power-law-wall/source/` (`wall/` 11, `sample/` 1) | 12 | record (`wall/fit_fn.py`, the PL law's reference) |
| `prompts/power-law-wall/` (prompts 10, `logs/` 6, `orchestrator/` 6) | 22 | record (the live campaign's own; 07c updates its row) |
| `.documents/one-loop-drain/` (`rehearse-06` 35, `figures-07` 10, `derive-05` 5, seven more) | 59 | record |
| `.documents/power-law-wall/` (`rehearse-07` 82, `wall-02` 28, `sample-04` 27, `export-06` 9, `law-03` 4) | 150 | record |
| `.documents/electroweak-eos/` (`effect` 13, `close-03` 1) | 14 | record |
| `.documents/*-verification.md` (electroweak-eos, one-loop-drain, power-law-wall, review-remediation) | 4 | record (additive) |
| `science-runs/2026-11-08-science-2026.10.0/` (`logs` 23, `m-trend` 23, `probe-out` 19, `relativistic` 10, `fig6` 3, three more) | 81 | record |
| `science-runs/plan-science-2026.11.0/` | 1 | record |
| `prompts/power-law-wall/source/kex/` (21 files, untracked) | — | this prompt's research |
| `.documents/power-law-wall/kex-07b/` (16 outputs and this file, untracked) | — | this prompt's research |

## Beyond the planner's list

Found here and not in note 07b §0's list:

1. **The kernels' grid ends against the new x_lo** (`quanta_spectrum.py:76-111`, `:191-195`): power-n1's new
   x_lo = 9.43e-4 is below `G_Y_MIN` = 1e-3, so the docstring's "y < G_Y_MIN < x_lo on every wall" (`:90`) fails, and
   `OMEGA_U_MAX` = 9 gives x_lo e^9 = 7.64, not "≥ 16.4" (`:105-106`). `test_quanta_spectrum.py:366-385` and
   `:408-432` test these bounds. κ_hi ≥ 2.15 still holds (4.34, 6.55, 5.47).
2. **R9's near-floor note** (`quanta_spectrum.py:65-69`): with k_ex/ω_t = (n+2)/(2√(n(n+1))) at every E/V₀ the s_t
   shift goes; the paragraph changes.
3. **`classical_turning_point_before_fix`** (`test_unresolvable_turning_points.py:242-290`) is a verbatim copy of the old
   method, k_ex included, and test (3) (`:405-460`) compares whole tuples bitwise; `test_drain_law.py` imports it
   (`:67`, `:794`, `:822`).
4. **Chain-existence pins**: `test_wall_model.py:680-692` (no solution at M = 1e-16 M_P, R = 10^2.49) and its comment
   `:343-349`; `test_margins.py:446-510` (gap ≤ 10^2.92, counts 3/1, shares 0.5877/0.2812, under the *fixed* law);
   `ObjectFactories/tests/test_drain_store.py:334-352` and `:26-30` (L-M8e-18's gap candidate). Each may flip.
5. **The notes' worked example** (`test_wall_model.py:301-334`): k_ex = 5.6e13 GeV ± 3% from wall_backreaction.tex,
   computed in the old form.
6. **The third hook in test stand-ins and delegate tests**: `test_wall_model.py` `_PowerLawWall` (`:107-176`),
   `_BumpWall` (`:178-208`), delegates (`:717-735`), the comment "(ln V)''', which no hook gives" (`:421-423`);
   `test_model_interface.py:78` (`POTENTIAL_METHODS`) and `:107-150` (`_MinimalModel`); `test_inverse_power_model.py:227-246`
   and `:252` (`HOOKS`); `RealizedModel` delegates (`AbstractModel.py:399-403`).
7. **Fixed-law histories move in their k_ex columns**: `margin_inputs` (`ScalarHistory.py:4610-4617`) calls
   `peak_estimate` under the fixed law, so peaks, birth margins, `log_q` and the estimate kind move at α > 0 although
   the integration does not.
8. **`peak_estimate`'s docstring** "the gap of 1 < R < 10^2.9" (`ScalarHistory.py:4273`).
9. **`model_of_law`** (`drain_model.py:95-119`) returns the exponential's model for the one-loop law, pinned to
   `"one-loop-measured-v1"` in `test_a_values_option.py:442-450` and `test_power_wall_law.py:144`.
10. **The S table** (`ScalarHistory.py:241-295`, `DRAIN_S_TABLE_*`; tested at `test_measured_drain_law.py:291-346`) is
    the v1 law's classical branch: not fitted against ℓ, but part of the versioned law; whether v2 keeps it is U13.
11. **Export columns** carrying moved values: `extract_common.py:637` (`DRAIN_CSV_COLUMNS`) and `:1475`
    (`BOUNCES_CSV_COLUMNS`).
12. **Other tools**: `tools/history_and_bbn.py:92-100` (docstring names v1) and `:389-412` (`ell_min`, regimes);
    `tools/bbn_from_store.py:66-67` (both names); `tools/plot_quanta_grid.py:44`, `:108-109` (`ell_min`, birth-margin
    panels); `tools/quanta_figures.py:123-127`, `:186-190` (spectrum-model check).
13. **Spectrum-model strings pinned in tools tests**: `test_quanta_models.py` (`:21`, `:132`, `:222`, `:230`, `:237`,
    `:245`, `:253`), `test_plot_quanta_history.py:237`, `test_plot_quanta_grid.py:237`.
14. **v1 retired everywhere**: `test_inverse_power_store.py:40` and `test_bbn_from_store_model.py:27` (v1 refused or
    stored on the power wall) become the general case; `test_models_export.py:47-59` pins CSV bytes with the v1 name.
15. **`CLAUDE.md:139-141`, `:146-148`** name `one-loop-measured-v1`.
16. **`config/version.py:76-79`, `:100-109`**: dated comments naming both models (records; 07c appends).
17. **`InversePowerModel._drain_fraction`** (`:188-216`) does read the regime, but only as classical (ln R ≤ 0) or
    unassessed, which do not move; one-loop and gap give the same f, so the planner's "reads neither ℓ nor the regime"
    holds in effect.
18. **Regime-dependent structural checks**: `test_drain_store.py:287` (P-2197 all one-loop) and
    `test_inverse_power_store.py:480` (power wall all one-loop), expected to hold since ℓ rises.
19. **`ONE_LOOP_ELL_RTOL`** (`AbstractModel.py:115`, used at `test_wall_model.py:445`) beside `ONE_LOOP_ELL_START`.
20. **Documents of record** `.documents/architecture-summary.md` (21 lines) and `.documents/numerical-strategies.md`
    (25 lines) state λ and k_ex = λ_t E^{1/2}/2 and v1's law.

Counts: §1 Production 118 rows (1.1 AbstractModel 23, 1.2 models and drain_model 19, 1.3 ScalarHistory 40, 1.4
quanta_spectrum 19, 1.5 store, config and drivers 17, one of them a "no hit"); §2 tools/ 19; §3 Tests 98 (3.1 69, 3.2 29,
two of them "no hit"); §4 Goldens 7; §5 Docstrings 15 (each also a row of §1 or §3); §6 Documents of record 7; §7 Records
14. Total 278 rows.
