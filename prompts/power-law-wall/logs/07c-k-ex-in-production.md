# Log 07c — one k_ex in production, and the exponential's law v2

**Subject:** k_ex = ½E^{1/2}|V‴/V″| at every use (the third hook `_d3_logV_dphi3` on every model and stand-in, the
classical turning point and the one-loop turnaround, the tuples' pulse-rate fields, the fixed point's cap 400); the
exponential's one-loop law `one-loop-exponential-v2` in `ExponentialModel` (U13, U14, P99, P100); the drain-model names
(U11; the retired names refused, `model_of_law` removed); the spectrum's tables, x_* and grid ends in the new x; the label
2026.13.0; the tests re-pinned (R13–R17, U11–U14, P99–P110).
**Commit:** `c8461ab`, on branch `power-law-wall`, on top of `c4258ef`
**Date:** 2026-11-18, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Consistent holds (§4).
- **One k_ex:** the production chain equals 07b's `production_v3_*` within 2.6e-11 at its 40 candidates; φ_t, ln ω_t²
  and ln R bitwise (R16); the third hook equals its closed form (worst 0.0026 of its tolerance) and the centred difference
  of (ln V)″ (5.9e-10) on both models.
- **The cap:** all 333 stage-2 points of 07b and 07b2 with a chain converge in production within 400, the largest 195;
  `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]` closed.
- **v2:** bitwise equal to `minform.fraction` at the 146 fitted points and 28 synthetic inputs covering every branch.
- **The tool route:** histories 1–4 bitwise in every sample, tuning, ledger and BBN value, only the k_ex columns moving;
  history 5 (the exponential's one-loop law at laboratory M) ΔN_eff 0.04462 → 0.04163 at T_CMB.
- **Issues:** one closed (07b's), one opened (`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`).
- **Suites:** at the base 26, 783 (226 + 557), 175, 312, 79; after 26, 805 (226 + 579), 175, 312, 79. Only the two worktree failures failed.

## 1. What shipped

- **`CosmologyConcepts/Models/AbstractModel.py`:** the hook `_d3_logV_dphi3` (the base raises `NotImplementedError`
  naming the class; not abstract, so a stand-in without it constructs) and `RealizedModel.d3_logV_dphi3`; `_pulse_rate`
  (|V‴/V″| from the hooks in log space; zero or not finite raises naming `_d3_logV_dphi3`); `classical_turning_point` and
  `one_loop_turnaround` take ln k_ex = ln|V‴/V″| + ½ ln E − ln 2, the refusal of a non-repelling wall kept on
  −d ln V/dφ ≤ 0; `ClassicalTurningPoint.pulse_rate_t`, `OneLoopTurnaround.pulse_rate_ta` (were `lambda_t`, `lambda_ta`);
  `ONE_LOOP_MAX_ITERATIONS` 400; the docstrings and comments of the old form; `_drain_L`'s and `_drain_fraction`'s
  docstrings on P99's rule.
- **`CosmologyConcepts/Models/ExponentialModel.py`:** the law v2 (`one_loop_fraction`, the constants `DRAIN_EXP_FALL_*`
  and `DRAIN_EXP_RISE_*` from `minform.json`), the S table and `drain_S_of_L` (moved, unchanged), `_drain_fraction`,
  `one_loop_drain_model` → `one-loop-exponential-v2` at n = 1, 2 and a `ValueError` naming U14 elsewhere,
  `_d3_logV_dphi3`; the module docstring.
- **`CosmologyConcepts/Models/InversePowerModel.py`:** `_d3_logV_dphi3` (overflow-safe in φ as the second, raising on
  overflow), the renamed drain model.
- **`CosmologyConcepts/drain_model.py`:** `DRAIN_MODEL_ONE_LOOP_EXPONENTIAL`, `DRAIN_MODEL_ONE_LOOP_INVERSE_POWER`,
  `DRAIN_MODELS` in the order fixed, exponential, power; `RETIRED_DRAIN_MODELS` refused by `law_of` with the ruling and the
  successor; `model_of_law` and `_MODEL_OF_LAW` removed; the docstring and comments.
- **`ComputeTargets/ScalarHistory.py`:** v1's long comment, constants, S table, `drain_S_of_L` and `drain_fraction` gone
  (a short comment points at each model); `check_drain_law_model` on P99; `drain_alpha_effective(regime, ell, log_R, A,
  w_gate, *, model, log_E)` always the model's own fraction; the loop's call; the comments naming λ_t, the drain models
  and "the gap of 1 < R < 10^2.9".
- **`ComputeTargets/quanta_spectrum.py`:** 07b's three tables and `_TABLES` entries, `X_STAR`, `SPECTRUM_MODELS`
  `R8-<wall>-v2`, `G_Y_MIN` 5e-4, the docstrings on x, k_ex/ω_t, R9 and the grid ends.
- **`config/version.py`** (2026.13.0 and its dated comment), **`config/argument_parser.py`** (the `--A-values` help),
  **`CosmologyConcepts/alpha_value.py`**, **`ObjectFactories/alpha_value.py`**, **`tools/history_and_bbn.py`**,
  **`tools/bbn_from_store.py`** (the names in comments and docstrings).
- **Tests:** `ComputeTargets/tests/test_k_ex.py` (new, 12 tests); `test_measured_drain_law.py` rewritten for v2;
  re-pins and renames in 30 more test files (§2 D11, the list).
- **`CLAUDE.md`:** the two mentions of `one-loop-measured-v1` take the new names; nothing else.
- **`.documents/power-law-wall-verification.md` §9** (appended), **the board**, **`.documents/OPEN_ISSUES.md`**.

## 2. Deviations from the prompt

**The prompt's §5 rulings**, each as ruled:
- **Ruling 1 — the worktree** (*STRUCTURALLY REQUIRED*): everything from the worktree's root with `./venv/bin/python` and
  `PYTHONPATH=.`; nothing in the main checkout read or written; `venv` not staged; `/tmp/ray-cpbh` never named.
- **Ruling 2 — records stay records** (*STRUCTURALLY REQUIRED*): `source/kex/` (`precision.py`, `kexchain.py`, `fits.py`,
  `rechain.py`, `minform.py`) and `.documents/power-law-wall/` read and imported with `python -B`, never written; no
  `__pycache__` appeared there (checked). The tests read `minform.json` (data) and copy `minform.fraction` verbatim; they
  import nothing from `source/`.
- **Ruling 3 — the base first** (*STRUCTURALLY REQUIRED*): the five suites and the five histories ran at `c4258ef` before
  the first edit (§3, verification §9.7); the base's φ_t, ln ω_t², ln R and turnarounds at 07b's 40 candidates were saved
  then too (`candidates_ct.py`).
- **Ruling 4 — the two worktree failures** (*IMPLEMENTATION CHOICE, ruled*): `test_registered_run …
  test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` and `test_registry_config …
  test_roots_and_distributions` failed at the base and after, and nothing else.
- **Ruling 5 — findings outside the inventory** (*IMPLEMENTATION CHOICE, ruled*): one issue opened (§6); the uses of k_ex
  the inventory missed are D9.
- **Ruling 6 — the histories one at a time** (*IMPLEMENTATION CHOICE, ruled*), each with its load (verification §9.7).

**This prompt's deviations:**
- **D1 — the base histories beside the base suites** (*IMPLEMENTATION CHOICE*). The five base histories (about 2 min)
  ran while the ComputeTargets halves ran, one at a time among themselves, and the tip's likewise beside the tip suites,
  so their timings compare. The first base run's dump could not serialize the samples (a `SampleValues` of numpy floats);
  the wrapper was fixed and all five re-run at the base before any edit; their stdout equals the first run's but for the
  timings.
- **D2 — the tool route through a wrapper** (*IMPLEMENTATION CHOICE*). `tools/history_and_bbn.py` prints summaries, not
  every sample, so a scratch wrapper (`dump_history.py`) calls its `main` unchanged and dumps the payload, the BBN result
  and what the store derives (`dissipation_values`, `drain_history_values`, `build_extra_data`), every float as its repr,
  and `compare_dumps.py` compares base and tip leaf by leaf. The tool itself is unchanged but for its docstring.
- **D3 — the pulse rate in one helper** (*IMPLEMENTATION CHOICE*): `AbstractModel._pulse_rate(context, normalization, p,
  where)`, used by both turning points, with V‴/V = (ln V)‴ + (ln V)′(3(ln V)″ + (ln V)′²), the prompt's expression
  factored; `abs(q3/q2)` as 07b's `hook_rate`.
- **D4 — `_d3_logV_dphi3` not abstract** (*IMPLEMENTATION CHOICE*): the prompt says the base raises naming the class; an
  abstractmethod would refuse construction of every stand-in without it. Every stand-in that implements the other hooks
  implements it (`_PowerLawWall`, `_BumpWall` in closed form, `_MinimalModel`), and `test_k_ex` checks a model without it
  is refused at the turning points.
- **D5 — the exponential's third hook raises on overflow** (*IMPLEMENTATION CHOICE*): the power, the division and a
  non-finite value raise `ComputationFailureError` naming the hook, as `InversePowerModel`'s second hook does; the
  exponential's first two hooks are unchanged.
- **D6 — `one_loop_fraction` a module function** (*IMPLEMENTATION CHOICE*): the law of (n, regime, ell, L, ln R), so that
  it can be compared bit for bit with `minform.fraction` at given L; `_drain_fraction` reads L from `_drain_L` and calls
  it. The constants are `DRAIN_EXP_*`, after the power wall's `DRAIN_PL_*`; the fall's coefficient is
  `DRAIN_EXP_FALL_COEFFICIENT`, not A.
- **D7 — `drain_alpha_effective` loses L** (*IMPLEMENTATION CHOICE*, P99): the fraction is the model's, which reads L
  itself, so the argument went, and `model` and `log_E` are keyword-only and required; an old six-argument call is a
  `TypeError`. L stays recorded in `DrainCandidate.L`.
- **D8 — `model_of_law` removed** (*IMPLEMENTATION CHOICE*, P101's "or goes"): nothing in production called it, and it
  named the exponential's model as "the" one-loop model; its two tests became tests that it is gone and that `law_of`
  covers every model, and `tools/bbn_from_store.py`'s docstring no longer names it.
- **D9 — uses and pins the inventory missed** (*STRUCTURALLY REQUIRED*, found and changed): the label's pins
  `ObjectFactories/tests/test_dissipation_store.py:644` and `test_inverse_power_store.py:670` (P105 moves the label);
  `test_models_option.py`'s exponential at n = 3 under `--A-values` (U14); `test_drain_law.py`'s and
  `test_power_wall_law.py`'s calls of `drain_alpha_effective` without a model (P99); `test_power_wall_law.py`'s `_Neither`
  and `_Both` (the rule "exactly one of" changed, P99); `test_a_values_option.py:305`'s check that no driver names a model
  (a substring of a retired name); `test_quanta_spectrum.py`'s J and M₁/M₃ (`test_the_shape_s_measurements`) and the
  associated piece's share (moved on power-n1 to 1.57e-2); `test_inverse_power_model.py`'s third hook in (b) and (c)
  (inventory "Beyond" item 6 named the file's hooks); `tools/tests/test_quanta_measures.py`'s stored-history phase slip, whose tolerance was taken of a slip that
  now cancels to 1 part in 473 (inventory: "reads (follow the module)").
- **D10 — test (d)'s points beyond float range** (*STRUCTURALLY REQUIRED*): at n = 1, ln E = −60, M = 4e-31 and 1e-16
  M_P, φ_t is 1.3e-126 and 1.3e-112 M_P, where (ln V)‴ overflows; `test_inverse_power_model` (d) now asserts the raise
  there and compares every other point as before; the issue of §6.
- **D11 — the re-pin list** (*STRUCTURALLY REQUIRED*, prompt §1.7): each pin of the inventory's §3 and "Beyond" items 3, 4, 5, 9, 13, 14, 18, and those of D9, as
  `test:line (at the tip) — old → new (why)`:
  - `test_quanta_spectrum.py:135` — `X_STAR == 0.0058` → `0.005615853347600149`, and equal to 0.0029/0.5163952511757223
    (P104, R10);
  - `test_quanta_spectrum.py:147-149` — k_ex/ω_t 0.4945353550468402, 0.3535533905932738, 0.408248290463863 →
    0.5163952511757223, 1.0606601717798214, 0.816496580927726, and (n + 2)/(2√(n(n+1))) on the power wall (P104);
  - `test_quanta_spectrum.py:160-166` — nodes 153/156/156 → 151/163/158; x_hi (4.0, 6.5, 5.5) → (3.5, 3.0, 3.0) (P104);
  - `test_quanta_spectrum.py:225` — x_p 0.646803, 0.465750, 0.506743 → 0.619424, 0.155250, 0.253372 (07b's tables);
  - `test_quanta_spectrum.py:311-327` — the associated piece's share of I on the power wall below 2e-3 → power-n2 below
    2e-3, power-n1 in (1e-2, 2e-2) (measured 1.57e-2 in the new x; D9);
  - `test_quanta_spectrum.py:333-336` — J, M₁/M₃ (0.835827, 0.828542), (0.591750, 0.464252), (0.689202, 0.608930) →
    (0.872770, 0.903405), (1.775255, 4.178280), (1.378406, 2.435723) (07b's `spectrum.txt`; D9);
  - `test_quanta_spectrum.py` `test_G_small_y`, `test_Omega_large_u_and_the_symmetry` — unchanged; they pass at the new
    `G_Y_MIN` = 5e-4 and `OMEGA_U_MAX` = 9 (§9.4);
  - `test_drain_law.py:137`, `:147` — `lambda_t=`, `lambda_ta=` → `pulse_rate_t=`, `pulse_rate_ta=` (the fields' names);
  - `test_drain_law.py:235` — `DRAIN_MODEL_ONE_LOOP == "one-loop-measured-v1"` →
    `DRAIN_MODEL_ONE_LOOP_EXPONENTIAL == "one-loop-exponential-v2"` (P101);
  - `test_drain_law.py:388` — the sentinel record's model → `one-loop-exponential-v2` (name);
  - `test_drain_law.py:519-596` — alpha_eff by v1's 1 − exp(−2.69/(ℓ + 0.9691 − 0.41 ln L)) at a one-loop turnaround,
    v1's plateau cap (f = 0.9, A = 1.1), w_gate 0, unassessed, unknown regime, each through `drain_alpha_effective`
    without a model → the same items through 2197's realized `ExponentialModel` and v2's fall
    1 − exp(−A_f/(ℓ − ℓ₀)), the cap at A = 100 and at the gap (P99, U13);
  - `test_drain_law.py:676-690` — 2197 at A = 1: five one-loop candidates, alpha_eff 8.2277e-02 … 1.6185e-01, parking
    N 28.876391476457353 → eleven one-loop candidates, alpha_eff 7.7483e-02 … 2.4727e-01, parking N 33.59383919340034
    (one k_ex and v2), `:762` `("one_loop",) * 5` → `* 11`; `:735-745` the record's alpha_eff recomputed through the
    model (P99);
  - `test_measured_drain_law.py` — the whole module: v1's reference `fraction_adopted` and its constants → v2's,
    `minform.fraction` verbatim with `minform.json`'s constants; the S table's tests on `ExponentialModel`'s moved table;
    v1's branch tests → v2's branches; `TestTheGapBelowLZero` → `TestTheRiseBelowLZero` at each n; `TestTheModelHook` on
    `_drain_fraction` required (`_NoDrainFraction`); U14 (P99, P100, U13, U14);
  - `test_inverse_power_model.py:315-325` — `_the_chain`'s k_ex = n s/φ_ta · E^{1/2}/2 → (n + 2)/φ_ta · E^{1/2}/2 (R13);
    `:390`, `:404-406` — λ_t = n s_t/φ_t → (n + 2)/φ_t, `c.lambda_t` → `c.pulse_rate_t`; `:432` `o.lambda_ta` →
    `o.pulse_rate_ta`; `:370-376`, `:421-427` — the two points beyond float range assert the raise (D10); `:246-252` the
    third hook's centred difference added; `:262`, `:286` the third hook added to (c);
  - `test_wall_model.py:293-297` — (1) k_ex = (y²/M)E^{1/2}/2 → y(y² + 6y + 6)/(M(y + 2)) · E^{1/2}/2 (the
    exponential's |V‴/V″| at n = 1);
  - `test_wall_model.py:338-341` — the notes' chain's k_ex y²E^{1/2}/2M → y(y² + 6y + 6)E^{1/2}/(2M(y + 2)); `:387` the
    printed k_ex 5.6e13 GeV ± 3% → 6.2e13 GeV ± 3%, and the old form's 5.6e13 at the same y kept as a check; y = 39.1 and
    Λ = 53.8 unchanged (they hold);
  - `test_wall_model.py:398` — the comment's ℓ = 40, 25, 36, 13 → 41.3, 25.9, 37.0, 13.8;
  - `test_wall_model.py:461-466`, `:504-512` — `c.lambda_t == −(ln V)′`, `o.lambda_ta == −d1` and their k_ex →
    the pulse rate from the hooks (`_pulse_rate`, `:108`) and its k_ex; `:476-478` — d ln V″/dx by a centred difference with
    h = 1e-3 → φ |V‴/V″| from the hook;
  - `test_wall_model.py:733-738` — (5) still raises at M = 1e-16 M_P; the docstring's "ℓ falls to −0.19 … stops at
    10^-16.11, R = 10^2.92" → "−0.039 … 10^-16.09, R = 10^2.85";
  - `test_wall_model.py:179-189`, `:234-248` — `_PowerLawWall`'s and `_BumpWall`'s third hooks (closed forms); `:790-801` the
    delegate's test;
  - `test_model_interface.py:78` — `POTENTIAL_METHODS` gains `d3_logV_dphi3`; `_MinimalModel` its third hook;
  - `test_unresolvable_turning_points.py:246-325` — the copy keeps its body, building `ClassicalTurningPointBeforeFix`
    (the old fields) in place of `AM.ClassicalTurningPoint`; `outcome` compares φ_t, ln ω_t², ln R only (the fields k_ex
    does not define), said in the module docstring;
  - `test_unresolved_turnarounds.py:163` — `lambda_t=1.0` → `pulse_rate_t=1.0`;
  - `test_margins.py` (3) — unchanged values (the gap generations stay gap), docstring sentence added; (4) docstring
    "k_ex/ω_t → 1/2" → about 0.52 at L = 90;
  - `test_power_wall_law.py` — `:21`, `:26-28` docstring; `_Neither` restores `_drain_fraction` too; `:139-150`
    `model_of_law(one_loop) == "one-loop-measured-v1"` → `model_of_law` gone; `:230` the exponential
    `drain_fraction_implemented` False → True; `test_c_neither`'s message → "does not implement"; `test_c_both` refused →
    accepted (P99); `test_d_through_drain_alpha_effective` without L and with the model required, the exponential's
    "L law" comparison → its own fraction, and a call without a model a `TypeError`; (e)'s call without L; the retired names
    test added; every `one-loop-measured-pl-v1` → `one-loop-inverse-power-v1`;
  - `test_a_values_option.py:28-31` docstring; `:301-309` "no driver names `one-loop-measured`" → none names the power
    wall's model or a retired one; `:447-460` `model_of_law` inverts `law_of` → `model_of_law` gone, `law_of` on every
    model (D8); names;
  - `test_models_option.py:198-204` — the exponential at n = 1, 2, 3 under one drain model → at n = 1, 2, and a
    `ValueError` naming n = 3 and U14 at 3 (U14); `:225-232` the refusal names the first potential of `--models`;
    `:273-288` U14's parse refusal added; `:293` the exponential's n = 1 2 3 `--A-values` command → n = 1 2;
  - `tools/tests/test_quanta_measures.py:889-901` — the stored history's `phase_slip_min` against the probe's loop to
    1e-12 *of the slip* → to 1e-12 of the larger of the two phases it is the difference of (`probe_slip_scale`, `:210`):
    with the one k_ex this history's smallest slip cancels to 1 part in 473, and the tool and the probe, which form each
    phase in another order, then differ by 3.4e-12 of the slip, 7.2e-15 of the phase (inventory: "reads (follow the
    module)"; the stored k_ex moved; D9);
  - `ObjectFactories/tests/test_dissipation_store.py:49-52`, `:644` and `test_inverse_power_store.py:47-48`, `:670` —
    `VERSION_LABEL == "2026.12.0"` → `"2026.13.0"` (P105; D9);
  - name only (`one-loop-measured-v1` → `one-loop-exponential-v2`, `one-loop-measured-pl-v1` → `one-loop-inverse-power-v1`,
    `DRAIN_MODEL_ONE_LOOP(_PL)` → `DRAIN_MODEL_ONE_LOOP_EXPONENTIAL` / `_INVERSE_POWER`, `R8-<wall>-v1` → `-v2`):
    `test_a_values_option.py`, `test_a_values_plot_inputs.py` (`:45`, `:80`, `:123`, `:160`, `:166`, `:201`, `:227`),
    `test_bbn_from_store_model.py` (`:26`, `:53`, `:73`, `:175`, `:182`, `:230`, `:242`, `:257`),
    `test_drain_store_records.py` (`:106`–`:152`, `:529`, `:658`, `:668`), `test_models_export.py` (`:50`, `:58`, `:128`),
    `test_models_main.py` (`:46`, `:239`, `:274`), `test_models_option.py` (`:26`, `:60`), `test_models_plot_inputs.py`
    (`:52`, `:299`), `ObjectFactories/tests/test_drain_store.py:81`, `test_export_04.py` (`:290`, `:299`, `:313`),
    `test_inverse_power_store.py` (`:34`, `:40`, `:84`, `:466`), `tools/tests/test_history_and_bbn_drain_law.py`
    (`:31`, `:154`, `:186`, `:218`, `:223`), `test_history_directory.py:59`, `test_inventory_alpha.py:167`,
    `test_no_alpha_values.py` (`:55`, `:252`, `:261`, `:273`), `test_one_loop_tools.py` (`:51`–`:221`, eleven lines),
    `test_plot_quanta_grid.py` (`:237`–`:440`, ten lines), `test_plot_quanta_history.py` (`:236`, `:256`, `:280`),
    `test_quanta_figures.py` (`:47`, `:56`, `:346`, `:350`, `:358`), `test_quanta_measures.py` (`:62`, `:71`),
    `test_quanta_models.py` (`:20`, `:56`, `:132`, `:222`, `:230`, `:237`, `:245`, `:253`);
  - held, as the inventory expected they might not: `test_drain_store.py:287` (P-2197 all one-loop),
    `test_inverse_power_store.py:480` (the power wall all one-loop), `test_drain_store.py:334-352` (L-M8e-18 keeps a gap
    candidate: one-loop, one-loop, one-loop, gap), `test_margins.py` (3)'s counts and shares, the goldens (inventory §4).
- **D12 — tests kept equal in number or more** (*IMPLEMENTATION CHOICE*): `test_measured_drain_law` 29 → 36 tests;
  `test_k_ex` 12 new; `test_power_wall_law` +1 (the retired names); `test_wall_model` +1 (the third hook's delegate);
  `test_models_option` +1 (U14).
- **D13 — `x_hi` 3.0** (*STRUCTURALLY REQUIRED*, prompt §1.5): the file's `WallTable` literals, not its headers' 3.0000000000000004 (§6).

## 3. Verification performed

| suite | at the base `c4258ef` (time, load at start) | after (time, load at start) |
|---|---|---|
| CosmologyModels | 26 OK (69 s; 65.5) | 26 OK (67 s; 5.3) |
| ComputeTargets `test_[a-f]` | 226 OK (154 s; 41.9) | 226 OK (152 s; 5.3) |
| ComputeTargets `test_[g-z]` | 557 OK (561 s; 37.1) | 579 OK (556 s; 12.0) |
| ObjectFactories | 175 OK (160 s; 38.3) | 175 OK (158 s; 4.6) |
| RunDrivers | 79, 1 failure (ruling 4) (30 s; 28.9) | 79, 1 failure (ruling 4) (26 s; 4.6) |
| tools | 312, 1 failure (ruling 4) (318 s; 52.1) | 312, 1 failure (ruling 4) (320 s; 5.5) |

A first run after the edits (11:49–12:10, loads 5.5–35.4) gave the same counts with one more failure,
`test_quanta_measures … test_K_and_the_generations_and_one_value_of_each` (the phase slip's tolerance, D9); after
its re-pin the five suites were run again in full, the table's "after" column (12:12–12:34). The +22 in
`test_[g-z]` are D12's. No count fell. `black` on every Python file changed (13 reformatted after the edits). The checks of §1.2–§1.8 are in
verification §9.2–§9.7, each with its numbers and tolerance; the scratch scripts that made them (`check_chain.py`,
`candidates_ct.py`, `iter_cap.py`, `v2_check.py`, `bounds*.py`, `kernels.py`, `breakage_kex.py`, `dump_history.py`,
`compare_dumps.py`) and their outputs are in the session's scratch directory, `agent-07c/`, with the base histories'
outputs under `base/h1` … `base/h5` and the commands in `COMMANDS.txt`.

**The `git grep`** of `lambda_t\|lambda_ta\|one-loop-measured\|DRAIN_ONE_LOOP_\|R8-.*-v1\|0\.0058\b` outside the records
finds: the retired names' definitions and refusals (`drain_model.py`), the dated comments (`config/version.py`,
`AbstractModel.py`'s tuple comments and `one_loop_turnaround`'s docstring, `InversePowerModel.py`'s and `quanta_spectrum.py`'s
docstrings and comments, `test_inverse_power_model.py`'s and `test_wall_model.py`'s docstrings), the verbatim copy of the
old method and its old tuple (`test_unresolvable_turning_points.py`), the tests that v1's names are gone
(`test_measured_drain_law.py`) and the unknown name `one-loop-measured-v0` of two refusal tests
(`test_drain_store_records.py:122`, `test_drain_store.py:484`).

## 4. The gate Consistent, item by item

- **One k_ex at every use of 07b's inventory:** every "changes" row of §1–§5 changed, every "re-pins" row re-pinned or
  shown to hold (D11), every "name only" row renamed; the `git grep` above; the production chain against 07b's
  `production_v3_*` within 2.6e-11 at 40 candidates (verification §9.2).
- **Each model's V‴ hook equal to its closed form and to the centred difference of V″:** 202 points each, worst 0.0026 of
  the tolerance and 5.9e-10 (§9.2).
- **The law as ruled equal to its reference implementation:** bitwise at 146 fitted points and 28 synthetic inputs (§9.3).
- **The spectrum kernels within 1e-7 of quadrature on the new tables:** 270 evaluations, worst 2.2e-10 (§9.4).
- **Every history whose law reads no ℓ bitwise in its integration by the tool route:** histories 1–4, every sample,
  tuning value and BBN value bitwise; only the k_ex columns moved (§9.7).
- **The suites at their counts or above:** §3.

## 5. The deliberate-breakage record

- **§1.2:** (i) the pulse rate as |V‴/V′|: k_ex against production_v3 off by up to 2.4e+84 relative, `test_k_ex` (b) 8
  failures in 5 tests; (ii) the factor ½ dropped: off by a factor 2 (1.00 relative), (b) 2 failures. Restored: 1.93e-11,
  none.
- **§1.3:** n = 1's rise used at n = 2: 32 of 146 fitted points and 6 of 28 synthetic inputs differ (largest 0.079); ℓ₀
  off by 1e-6: 61 and 2 (largest 2.2e-7). Restored: bitwise.
- **§1.8:** history 1's sample 2000 with φ_E moved by one ulp is reported as one differing sample.

## 6. Observations not acted on

- **The third hook's float range** — issue `[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]` (board §3):
  (ln V)‴ ~ n(n + 1)(n + 2)/φ³ overflows below φ ≈ 1e-103 in the units' field (Planck units), where (ln V)″ reached
  1e-154; the turning points then raise naming the hook. By estimate no history of record is near it (on the power wall
  at n = 1 a first impact from T* = 2e4 GeV reaches it for M ≲ 1e-39 M_P; 07's grid stops at 1e-33 M_P, about 1e17 below
  overflow in |(ln V)‴|). Forming the rate overflow-safely needs scaled hooks, which P102's interface does not have.
- **`spectrum_tables.txt`'s `x_hi`:** its `WallTable` literals give the power walls `x_hi=3.0` where its headers print the
  measured 3.0000000000000004; the module takes the literal (§1.5), so x_p differs from the header's in the 16th digit
  (0.15524989529273064 against …67), and the node grid by one ulp at its end.
- **The associated piece on power-n1:** in the new x it carries 1.57e-2 of I at c_a = 1 (1e-3 or less before; 1.6e-3 on
  power-n2, 6e-5 on the exponential), since x_* = 0.0056 lies nearer power-n1's peak, x_p = 0.155. Relativistic quanta's
  outputs on the power wall move with it (P89); measured, not judged.
- **The exponential's first two hooks** compute `pow(M/φ, n)` outside their `try`, so an overflow there escapes as a raw
  `OverflowError`, and their divisions return ±inf silently; the new third hook raises `ComputationFailureError` for both.
  Not changed (outside the inventory; ruling 5); no history reaches them.
- **The 2197 history under v2** (A = 1): eleven one-loop candidates and parking at N = 33.594, where v1 had five at 28.876;
  `test_drain_law` (4) re-pinned from this tree, not against a reference map (none exists for v2's histories).
- **Rows the inventory expected might flip held:** `test_margins` (3)'s three gap generations (0 < log₁₀ R ≤ 2.92),
  `test_drain_store`'s L-M8e-18 gap candidate, P-2197 and the power wall all one-loop; the worked example's chain at
  M = 1e-16 M_P still has no solution (the chain's stop moved from R = 10^2.92 to 10^2.85 at that E).
- **History 5's integration** (the exponential at laboratory M, β 2, A 1) moves by construction: 46 candidates (45),
  ΔN_eff 0.04163 (0.04462) at T_CMB, Y_p 0.247200 (0.246909), D/H 2.47039 (2.46958), ℓ_min 19.964 (19.331).

## 7. State handed to the next prompt

**For 07d:**
- **The commit:** `c8461ab`, on top of `c4258ef`. `VERSION_LABEL` 2026.13.0, `FINGERPRINT_FORMAT` 8 (no table or column
  changed). 07's stores (2026.12.0) are not extended: 07d builds fresh stores. A 2026.12.0 store's one-loop `alpha_value`
  rows name `one-loop-measured-v1` or `one-loop-measured-pl-v1`, which this tree refuses wherever it reads a drain model;
  a comparison of 07's stores with 07d's reads 07's with `compare_stores` from its own tree or by value.
- **The tool route's table** (verification §9.7): histories 1–4 bitwise in every sample, tuning and BBN value; history 5
  measured. Commands in the scratch `agent-07c/COMMANDS.txt`; the base outputs under `agent-07c/base/h1` … `h5`.
- **The columns that move, and by how much, where the law reads no ℓ** (at laboratory M, β = 2):
  - the power wall at n = 1: every turning point's ℓ_t by exactly ln 3 = 1.0986 (pulse rate × 3), ℓ, the birth margin
    and the reflection margin by up to +1.103, ln k_ex, ln q and the tracker margin by up to +1.094, the one-loop peak's
    ln m by up to 0.013; the worst margins by +1.10; `ell_min` 21.411 → 22.514;
  - at n = 2: ℓ_t by ln 2 = 0.6931, ℓ and the birth margin by up to +0.697, ln k_ex and ln q by +0.691, ln m by 0.008;
  - the exponential under the fixed law (α 0.1): ℓ_t by up to +0.068, the generations' ln k_ex, ln q, birth, reflection
    and tracker margins by up to +0.21, ln m by 0.0028, the worst margins by +0.18;
  - natural mode: nothing.
  - the bounce rows' `regime` did not move in these histories (all one-loop on the power wall); the drain model's string
    moves (U11) in every power-wall one-loop record.
- **What 07d's rebuild must expect:**
  - the exponential's one-loop histories move in their integration and BBN (v2; history 5: ΔN_eff −6.7% at T_CMB);
  - `--A-values` with the exponential at n ≥ 3 is refused at parse (U14); 07's grid has the exponential at n = 1 only;
  - the quanta tools' `spectrum_model` column reads `R8-<wall>-v2`, and their outputs move with the tables, x_* and the
    stored k_ex columns; on power-n1 the associated piece carries 1.6% of I;
  - the one-loop fixed point may take up to about 200 iterations near the chain's end (cap 400);
  - P109: report whether any candidate has ln(V₀/M⁴) > 10; and the least φ_t of the grid against the third hook's float
    range (issue of §6; 07's least power-wall M, 1e-33 M_P, gives φ_t ≈ 1e-97 M_P, inside it).
