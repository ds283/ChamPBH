# Log 07c2 — the pulse rate in closed form

**Subject:** |V‴/V″| as each model's hook `_pulse_rate_hook` in closed form ((n + 2)/φ on the power wall;
[(n + 1)(n + 2) + 3n(n + 1)u + n²u²]/([(n + 1) + nu]φ) on the exponential), read by `AbstractModel._pulse_rate` in place of
07c's composition of the three log hooks, which overflowed below φ ≈ 3 × 10⁻¹⁰³ M_P; the stop of 07d reproduced; the range
07's grid's assessments reach measured; the tool route; `VERSION_LABEL` 2026.14.0 (P125–P134, README §6.16).
**Commit:** this commit, on branch `power-law-wall`, on top of `7a202b7`
**Date:** 2026-11-19, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Range holds (§3).
- **The stop reproduced at the base:** `classical_turning_point` on `pl-a`'s inputs raised with `pl-a.log`'s message
  character for character; histories 4 and 5 failed in the tuning's first integration with the same message.
- **The hook:** equal to the composition of each model's hooks within 2.85 eps (tolerance 32 eps) at 4071 points where
  that is finite; the power wall's (n + 2)/φ bit for bit down to 1e-300 M_P (the composition non-finite at 6328 of the
  6344 points below 1e-102 M_P); the stop's inputs return, φ_t, ln ω_t² and ln R bit for bit the base path's.
- **The range:** the new end (V″/V leaving float range) is 1.05e-154 M_P at n = 1, 1.83e-154 at n = 2; the least φ_t of
  07's grid is 1.07e-109 M_P (n = 1, β = 2.9, M = 4e-31 M_P, near N = 5.04, not at the start): **least margin 45.0
  decades**. `[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]` closed.
- **The tool route:** histories 1, 2 and 6 bitwise but `pulse_rate_t` (one ulp, in 30, 18 and 9 injections); no other k_ex
  column moved. History 3 bitwise but `pulse_rate_t`. Histories 4 and 5 build, equal to 07's stored ΔN_eff, Y_p, D/H,
  reflections and bounce rows.
- **Suites** (CosmologyModels, ComputeTargets, ObjectFactories, RunDrivers, tools): at the base 26, 805 (226 + 579), 175,
  79, 312; after 26, 809 (226 + 583), 175, 79, 312. Only the two worktree failures failed.

## 1. What shipped

- **`CosmologyConcepts/Models/AbstractModel.py`:** the hook `_pulse_rate_hook(normalization, phi)` (the base raises
  `NotImplementedError` naming the class; not abstract, so a stand-in without it constructs); `_pulse_rate` keeps its
  signature and check and calls it, naming `_pulse_rate_hook` in its refusal; the module docstring, `_d3_logV_dphi3`'s
  docstring (no production caller now) and `_pulse_rate`'s docstring say how the rate is formed.
- **`CosmologyConcepts/Models/InversePowerModel.py`:** `_pulse_rate_hook` = (n + 2)/φ, its checks through `_log_u`, an
  overflow raising `ComputationFailureError` naming `pulse_rate_hook`.
- **`CosmologyConcepts/Models/ExponentialModel.py`:** `_pulse_rate_hook` in P126's closed form from u = pow(M/φ, n), an
  overflow of the power or of the value raising naming the hook (as its third hook does).
- **`config/version.py`:** `VERSION_LABEL = "2026.14.0"` with a dated comment; `FINGERPRINT_FORMAT` 8.
- **Tests:** `ComputeTargets/tests/test_k_ex.py` (new class (d), four tests; (a), (b) re-pinned), `test_inverse_power_model.py`
  ((d)'s raises become returns), `test_wall_model.py` (the stand-ins' hooks, (3) re-pinned), `test_model_interface.py`
  (`_MinimalModel`'s hook), `ObjectFactories/tests/test_dissipation_store.py` and `test_inverse_power_store.py` (the label).
- **`.documents/power-law-wall-verification.md` §10** (appended), **the board**, **`.documents/OPEN_ISSUES.md`**.

## 2. Deviations from the prompt

**The prompt's §5 rulings**, each as ruled:
- **Ruling 1 — the worktree** (*STRUCTURALLY REQUIRED*): everything from the worktree's root with `./venv/bin/python` and
  `PYTHONPATH=.`; the main checkout read only by §1.5's `sqlite3` `file:…?mode=ro` queries of 07's `pl` and `mixed`
  shards; `venv` not staged; `/tmp/ray-cpbh` never named. No Ray, cluster, lock, registry write or store write.
- **Ruling 2 — records stay records** (*STRUCTURALLY REQUIRED*): `var/power-law-wall/07d/runs/pl-a.log` read only; nothing
  under `prompts/power-law-wall/source/`, `.documents/power-law-wall/` or `var/power-law-wall/` written or run.
- **Ruling 3 — the base first** (*STRUCTURALLY REQUIRED*, P129): the five suites, the stop and the six histories at
  `7a202b7` before the first edit of a tracked file. The edits were drafted on copies in the scratch directory while the
  base suites ran, and copied into the tree after the last suite ended (03:21:22).
- **Ruling 4 — the two worktree failures** (*IMPLEMENTATION CHOICE, ruled*): `test_registered_run …
  test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` and `test_registry_config …
  test_roots_and_distributions` failed at the base and after, and nothing else.
- **Ruling 5 — another range limit** (*IMPLEMENTATION CHOICE, ruled*): none found on 07's grid (§6).
- **Ruling 6 — the histories one at a time** (*IMPLEMENTATION CHOICE, ruled*), each with its load (verification §10.6).

**This prompt's deviations:**
- **D1 — the first assessment from stored samples, not from `phi_0` and `pi_0`** (*STRUCTURALLY REQUIRED*). §1.5 asks for
  the first assessment's φ_t "computed from its stored normalization, phi_0 and pi_0". Those columns are the field and its
  velocity at the end of the history (`TUNING_OUTPUT_KEYS`; `phi_0` is "the field at the end"), and the initial π is 0 in
  every history (`pi_value` serial 1), so the first assessment is at accepted step 1, which no stored value gives. In their
  place the φ_t of an assessment at every stored sample with π < 0 (φ, π, H_E of `ScalarHistoryValue`, about 0.009 in N
  apart early on), with the power wall's closed form; the first such sample stands for the first assessment, and the
  least over samples for the least assessment. Checked against `pl-a`'s message (the samples either side of N = 3.984
  bracket ln E = −110.29 and φ_t = 2.31e-103) and against the probe's least φ_t at histories 2 and 4–6 (equal to four
  digits). README §6.16 and P129 say "from its stored values", which this meets.
- **D2 — the least φ_t is not at the start** (*observation, as §1.5 asks*): README §6.16 and the prompt's §0 put the stop at
  "the first steps"; the probe puts it at accepted step 47, N = 3.984, after a kick raised ρ_kin from e^−137.5 to e^−110.3
  while φ stayed at 5 M_P; the least φ_t of each history is near N = 5.05 (step 75), 10–18 decades below the first
  assessment's.
- **D3 — the tool route through a wrapper, with the probe in it** (*IMPLEMENTATION CHOICE*). 07c's scratch wrapper
  `dump_history.py` (read as data from its session's scratch directory, copied and adapted) calls the tool's `main`
  unchanged and dumps the payload and BBN result, every float as its repr; added to it, §1.5's probe, which wraps
  `AbstractModel.classical_turning_point` and `one_loop_turnaround`, returns what they return, and records each call with
  the integrator frame's `accepted_steps` and N. The same wrapper ran at base and tip, so the commands are the same.
  07c's `compare_dumps.py` compares base and tip leaf by leaf.
- **D4 — the base histories beside the base suites** (*IMPLEMENTATION CHOICE*), as 07c's D1: the six histories ran one at a
  time while the suites' first ComputeTargets half ran, at base and at tip alike, so their timings compare.
- **D5 — a negative rate refused** (*IMPLEMENTATION CHOICE*). The hook returns |V‴/V″|, so `_pulse_rate` takes no `abs`;
  its check `rate > 0 and isfinite(rate)` is unchanged, so a negative rate (a hook's error) raises where 07c's `abs` would
  have folded it, and the message says "zero, negative or not finite (from _pulse_rate_hook)". `test_k_ex` (b) tests it.
- **D6 — the exponential's hook in P126's literal form** (*IMPLEMENTATION CHOICE*): n²u² overflows only for u ≳ 1e154, far
  beyond any turning point on this wall (u = L there); the overflow raises naming the hook. An overflow-free rearrangement,
  nu + 2(n + 1) − n(n + 1)/(nu + n + 1), was not used, to keep the hook what the README states.
- **D7 — the stand-ins** (*IMPLEMENTATION CHOICE*): `_PowerLawWall` returns (p + 2)/φ, its closed form; `_BumpWall`
  overrides it with the composition of its own hooks (otherwise it would inherit (p + 2)/φ, which the bump makes wrong);
  `_MinimalModel` (constant V) as the composition, which is 0/0 and raises `ZeroDivisionError`, never reached, since the
  model does not reflect and the turning points refuse it first. Each says which.
- **D8 — no `RealizedModel` delegate for the new hook** (*IMPLEMENTATION CHOICE*): §1.2 asks for none, and nothing outside
  the model reads it.

**Re-pins**, as `test:line — old → new (why)`:
- `ComputeTargets/tests/test_inverse_power_model.py:363–372` (classical) — at n = 1, ln E = −60, M = 4e-31 and 1e-16 M_P,
  `assertRaises(ComputationFailureError)` naming `d3_logV_dphi3` → the turning point returned and compared as every other
  point; `:369` new `pulse_rate_t == (n + 2)/phi_t` bit for bit at every point (P128: the raise becomes a return).
- `ComputeTargets/tests/test_inverse_power_model.py:410–415` (one-loop) — the same raise → returned and compared with the
  chain; `:415` new `pulse_rate_ta == (n + 2)/phi_ta` bit for bit. `_third_in_range` removed (no user left).
- `ComputeTargets/tests/test_k_ex.py:200` — a model without `_d3_logV_dphi3` refused at `classical_turning_point`
  (`NotImplementedError`) → it returns a turning point (the turning points no longer read the third hook; the refusal moved
  to `_pulse_rate_hook`, (d)).
- `ComputeTargets/tests/test_k_ex.py:264` — the power-wall points beyond `third_in_range` left out of (b) → included (they
  return now); `third_in_range` removed. (b)'s tolerance unchanged, its reason restated for the hook.
- `ComputeTargets/tests/test_k_ex.py:344` — R16's patch of `_d3_logV_dphi3` to −1e60 → of `_pulse_rate_hook` to 1e60 (the
  turning points read the latter).
- `ComputeTargets/tests/test_k_ex.py:356–368` — the third hook patched to make the composition 0, −inf, nan, the message
  naming `_d3_logV_dphi3` → `_pulse_rate_hook` returning 0, −1, inf, nan, the message naming `_pulse_rate_hook` (the rate is
  the hook; D5).
- `ComputeTargets/tests/test_wall_model.py:486`, `:535` — `pulse_rate_t`/`_ta` equal to the composition of the hooks bit
  for bit → equal to the model's `_pulse_rate_hook` bit for bit (the method's expression now), with `:488`, `:537` new: equal
  to the composition within 32 eps. The helper `_pulse_rate` is now the hook; `_composed_pulse_rate` is the old helper.
- `ObjectFactories/tests/test_dissipation_store.py:646`, `ObjectFactories/tests/test_inverse_power_store.py:670` —
  `VERSION_LABEL` "2026.13.0" → "2026.14.0" (P130), with their docstrings' label histories.
- No golden moved; no k_ex column pin moved (none needed re-pinning: no k_ex column changed in any test).

New tests: `test_k_ex` (d) `TestThePulseRateHook` — `test_d_against_the_composition_and_the_closed_form`,
`test_d_finite_down_to_1e_300_on_the_power_wall`, `test_d_the_stop_returns`, `test_d_a_model_without_the_hook_is_refused`.

## 3. Verification performed

**The suites**, serially from the worktree root, one-minute loads at each start:

| suite | base `7a202b7` | after |
|---|---|---|
| CosmologyModels | 26, 67 s (5.37) | 26, 66 s (3.08) |
| ComputeTargets `test_[a-f]` | 226, 152 s (4.46) | 226, 149 s (3.24) |
| ComputeTargets `test_[g-z]` | 579, 541 s (4.50) | 583, 534 s (3.90) |
| ObjectFactories | 175, 156 s (4.08) | 175, 154 s (2.69) |
| RunDrivers | 79, 25 s (3.37) | 79, 25 s (3.25) |
| tools | 312, 314 s (3.30) | 312, 311 s (4.00) |

The +4 are `test_k_ex` (d). In both, the two worktree failures (ruling 4) and no other. No count fell.

**The gate Range, item by item:**
- *the pulse rate a model hook in closed form on both walls:* `_pulse_rate_hook` on `InversePowerModel` and
  `ExponentialModel`; `AbstractModel`'s raises naming the class (`test_k_ex` (d));
- *equal to the composition of each model's hooks where that is finite:* 4071 points, worst 2.85 eps (tolerance 32 eps);
- *finite down to where V″ leaves float range:* bit for bit (n + 2)/φ down to 1e-300 M_P, below the end 1.05e-154 M_P;
- *the stop's inputs return a turning point:* φ_t, ln ω_t², ln R bit for bit the base path's; the one-loop turnaround too;
- *the tool route's histories whose law reads no ℓ bitwise but the k_ex columns, and the stop's histories built:*
  histories 1, 2, 6 (§10.6), only `pulse_rate_t` moving, at one ulp; histories 4 and 5 built and equal to 07's stored
  values;
- *the least margin of 07's grid's assessments measured:* 45.0 decades (§10.5);
- *the suites at their counts or above:* the table.

**Other checks:**
- The `git grep` of `_d3_logV_dphi3`/`d3_logV_dphi3` outside `prompts/`, `.documents/`, `science-runs/`: the hook's
  definitions (`AbstractModel.py:527`, `ExponentialModel.py:334`, `InversePowerModel.py:271`), its delegate
  (`AbstractModel.py:416–417`), comments and docstrings (`AbstractModel.py:35, 529, 541, 556`, the models' comments and
  messages, `config/version.py:113`), and tests (including the stand-ins' compositions); no production caller.
- `black` on the ten Python files changed (one reformatted, `test_k_ex.py`, before the suites ran).
- `runregistrykit list` at the start: 83 runs, all finished; nothing launched.

## 4. The deliberate-breakage record

- The power wall's hook as (n + 1)/φ: 9 failures in 60 tests (`test_k_ex` (b) ×2, (d) ×3; `test_wall_model` (3) ×2;
  `test_inverse_power_model` (d) ×2).
- The exponential's hook with the n²u² term dropped: 7 failures and 1 error (`test_k_ex` (b) ×2, (c), (d);
  `test_wall_model` (1), (2), (3) ×2).
- Restored: 60 tests, none failing.
- §1.6: history 1's tip sample 2000 with φ_E moved by one ulp is reported by the comparator as one differing sample.

## 5. What the tool route found (verification §10.6)

- Histories 1, 2, 6: 67 088, 70 328, 69 048 leaves; bitwise but `injections[*].classical_turning_point.pulse_rate_t` in 30,
  18 and 9 injections (≤ 4.1e-16 relative). `log_k_ex`, `ell_t`, ℓ, `log_q`, the margins, `ell_min` and the peaks did not
  move: a one-ulp change in the rate is below half an ulp of ln k_ex.
- History 3: bitwise but `pulse_rate_t` (31 of 46); ΔN_eff 0.04163249276504718, Y_p 0.24719964618966397, D/H
  2.470389133689699, ℓ_min 19.964334270236222 at base and tip.
- Histories 4 and 5: ΔN_eff 0.03843504737508671 and 0.024529199130091498 at T_CMB, Y_p, D/H, 42 and 62 reflections, 0 and
  123 bounce rows, parking N, normalization and φ_ref equal to 07's `pl` (serials 23, 28); the birth margin and ℓ_min
  move by +1.10 (ln 3, 07c's change on this wall).

## 6. Observations not acted on

- **Where the least φ_t falls** (D2): at the peak of ρ_kin near N ≈ 5.05 (T* = 2e4 GeV), E/V₀ ≈ e^181 at β = 2.9, not at
  the first impact, which the issue's estimate took, nor at the first assessment. On the power wall at n = 1 φ_t ∝ M at fixed
  E/V₀, so by scaling the margin of 45.0 decades at M = 4e-31 M_P would reach zero near M ≈ 4e-76 M_P at β = 2.9: an
  extrapolation, recorded for a science grid's planning, not an issue.
- **The third hook's own range** (ruling 5): `_d3_logV_dphi3` still overflows below 2.2e-103 M_P at n = 1, and raises naming
  itself; no production code reads it since this prompt, so no assessment meets it. No issue opened.
- **The prompt's premise on `phi_0`/`pi_0`** (D1): they are end-of-history values; P133's restart of 07d, if it reports
  "the first assessment" from stored values, meets the same.
- **`prompts/INDEX.md`** names 07c2 as "not dispatched"; it is not among §4's files and is left for the orchestrator.

## 7. State handed to the next prompt

**For 07d:**
- **The commit:** this commit, on top of `7a202b7`. `VERSION_LABEL` 2026.14.0, `FINGERPRINT_FORMAT` 8 (no table or column
  changed). U12's guard refuses the stopped attempt's 2026.13.0 stores; 07d builds `power-law-wall-07d-r2-*` (P133).
- **The suites after:** 26, 809 (226 + 583), 175, 79, 312 (CosmologyModels, ComputeTargets, ObjectFactories, RunDrivers,
  tools), with the two worktree failures only.
- **The tool route's table** (verification §10.6): histories 1, 2, 6 bitwise but `pulse_rate_t`; history 3 bitwise but
  `pulse_rate_t`; histories 4 and 5 build and equal 07's stored values. Commands in the scratch `agent-07c2/COMMANDS.txt`
  and `histories.txt`; base and tip outputs under `agent-07c2/base/h1…h6` and `agent-07c2/tip/h1…h6`.
- **The columns that move, and by how much:** against 2026.13.0, only `pulse_rate_t`/`pulse_rate_ta`, by one ulp where
  07c's composition was finite; in these six histories nothing else moved. Against 07 (2026.12.0) the k_ex columns move as
  07c's §7 lists (on the power wall at n = 1 by ln 3 = 1.0986 in ℓ_t, up to +1.10 in ℓ, the birth and reflection margins,
  ℓ_min). The stop's five histories now build: their integration and BBN equal 07's.
- **§1.5's least margin:** 45.0 decades over 07's grid (least φ_t 1.07e-109 M_P at n = 1, β = 2.9, M = 4e-31 M_P, near
  N = 5.04; the end 1.05e-154 M_P at n = 1, 1.83e-154 at n = 2); P120's third hook, reported against it, has no production
  caller.
- **What 07d's rebuild must expect:** the five `pl-a` histories (α 0.01, 0.1; A 0.05, 1, 10 at n = 1, β = 2.9,
  M = 4e-31 M_P) succeed, as do their counterparts in `pl-resumed` and `mixed`; the exponential's one-loop histories may
  differ from 07c's at round-off only through `pulse_rate_t` (history 3 did not move otherwise); the least φ_t of a history
  falls near N ≈ 5.05, not at its start.
