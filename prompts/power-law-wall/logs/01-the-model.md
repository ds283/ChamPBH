# Log 01 — the model

**Subject:** `InversePowerModel`, V = V₀[1 + (M/φ)ⁿ], with the seed at any n and the tool route's `--model` and `--n`
**Commit:** this commit, on branch `power-law-wall`, on top of `75ac6fb`
**Date:** 2026-11-14, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE, after one stop.** The gate Model holds (§4).
- **The stop.** Adding `INVERSE_POWER_MODEL = 2` to `model_ids.py` (§1.1) failed
  `tools.tests.test_compare_stores.TestDeclaredMaps.test_13_…`. That test requires every model id to be mapped or
  declared unstored in `tools/compare_stores.py`. The user ruled, 2026-11-14: "(a), add the unstored entry to
  compare_stores" (§2, D1).
- **The class** and its 21 tests; `InversePowerModel` at n = 1 and 2 added to `test_wall_model.py`'s generic checks.
- **The tools** take `--model` and `--n`. At the defaults every line is unchanged.
- **The runs.** Eighteen runs: the twelve power-law runs and six exponential counterparts.
  - At laboratory M under the fixed law, every run succeeds in one integration from the tracking seed, at both n.
  - At 10⁻³ M_P in natural mode, n = 2 succeeds and n = 1 fails at the integrator's guard G2 on all three β.
- **§1.6.** With the committed class, no accepted state has φ ≤ 0. The even continuation (scratch, not committed)
  lets two histories accept a step at φ < 0.
- **Issues:** none opened, none closed.
- **Suites:** 26, 684 → 705 (219 + 465 → 219 + 486), 163, 292 → 302, 76. Only the two worktree failures of ruling 7
  failed, before and after.

## 1. What shipped

- **`CosmologyConcepts/Models/InversePowerModel.py`** (new): the class (§1.1 of the prompt), the seed at any n (§1.2).
- **`CosmologyConcepts/Models/model_ids.py`:** `INVERSE_POWER_MODEL = 2`. **`CosmologyConcepts/Models/__init__.py`:**
  the class and the id exported, after `ExponentialModel` (the comment says why the order matters).
- **`tools/compare_stores.py`:** one entry, `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"] = {2: …}`, naming
  prompt 05 as where the id gets its table and moves into `POLYMORPHIC_REFERENCES` (D1). Nothing else in the file.
- **`ComputeTargets/tests/test_inverse_power_model.py`** (new, 21 tests): (a) names; (b) hooks against closed forms
  and centred differences; (c) φ ≤ 0; (d) the wall searches against closed forms; (e) the seed; (f) `renormalize`;
  (g) the drain law; (h) two short integrations; (i) pickling.
- **`ComputeTargets/tests/test_wall_model.py`:** `_inverse_power` and two `_NO_CLOSED_FORM` entries (n = 1, 2, at
  M = 1e-30 and 1e-10 M_P), so test (3)'s three defining-equation tests run on the production class. The count is
  unchanged: the entries are points inside existing tests. `_PowerLawWall` stays.
- **`tools/history_and_bbn.py`:**
  - `--model`, `--n`, and the helpers `build_model`, `model_label` and `model_task_suffix`;
  - the label and task label gain the model only away from the defaults;
  - under `one_loop`, `check_drain_law_model` runs before any integration, and a refusal exits 2;
  - the docstring is updated.
- **`tools/surfing_scan.py`:**
  - `--model` and `--n`; `make_setup(model, n)`, which `run_history` reads;
  - the console names the model away from the defaults;
  - the meta.json options record it away from the defaults;
  - a resume is refused across models (D3);
  - the docstring is updated.
- **`tools/tests/test_model_option.py`** (new, 10 tests).
- **`.documents/power-law-wall/model-01/`:** `runs.sh`, `summary.py`, and `out/` (18 run outputs and `summary.md`, text
  only). **`.documents/power-law-wall-verification.md`** (new), §1.
- **The board**, `CLAUDE.md`'s counts, and this log.
- **Not committed:** the §1.6 even-extension probe and its outputs. Like the suites' full outputs, the breakage
  copies and the stop-time copies of the work, they stay in the agent's scratch directory
  (`…/scratchpad/agent-01/probe16/`).

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note §0) and the user's ruling of the stop

1. **The worktree and the main checkout.** All work, suites and runs were done in the worktree with the `venv` symlink.
   Nothing in the main checkout was touched, `venv` was never staged, and no command named `/tmp/ray-cpbh`.
   *STRUCTURALLY REQUIRED* (README §5, U1).
2. **φ ≤ 0** is a `ComputationFailureError` naming the model, the hook and φ. The even continuation was measured only
   in scratch, and is not committed. *IMPLEMENTATION CHOICE*, ruled by the note.
3. **The runs' failures are results.** The three n = 1 natural-mode failures at 10⁻³ M_P are G2 refusals, stored as
   the history's reason (`ComputationFailureError` from the integrator). They are recorded and not retried. No
   uncaught exception came from anywhere. *IMPLEMENTATION CHOICE*, ruled by the note.
4. **The exponential beside each run** is the same command without `--model` and `--n`, on this tree.
   *IMPLEMENTATION CHOICE*, ruled by the note.
5. **`CLAUDE.md`'s counts** are updated with the two new modules' tests. *As the prompt says.*
6. **No file but the prompt's §4 list**, with the one addition D1 below. *STRUCTURALLY REQUIRED.*
7. **The two worktree failures** (`test_registered_run …test_the_script_is_main_py…` and
   `test_registry_config …test_roots_and_distributions`) failed on the base and after, and nothing else did.
   *IMPLEMENTATION CHOICE*, ruled by the note.

- **D1 — `tools/compare_stores.py` in the commit.** `INVERSE_POWER_MODEL = 2` in `model_ids.py` failed
  `test_compare_stores.TestDeclaredMaps.test_13_bookkeeping_and_polymorphic_maps_name_declared_columns` (subTest
  `model_type`: "Items in the second set but not the first: 2"). That test passed on the base.
  - The test requires every id that `model_ids` defines to be mapped in `POLYMORPHIC_REFERENCES` or declared in
    `UNSTORED_TYPE_VALUES`.
  - I stopped. The user ruled on 2026-11-14: "(a), add the unstored entry to compare_stores".
  - The entry is `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"] = {2: "INVERSE_POWER_MODEL: InversePowerModel
    has no table until power-law-wall prompt 05, which gives it one and moves this id into POLYMORPHIC_REFERENCES"}`.
  - Nothing else in the file changed, and the test is unchanged.
  - `test_compare_stores` passes (21 tests, 3.5 s). No stored row carries `model_type` 2, so no comparison of an
    exponential store changes.
  - *STRUCTURALLY REQUIRED*, ruled by the user.

### 2.2 Others

- **D2 — `--n` applies to both models.** `history_and_bbn` built `ExponentialModel` at n = 1; with `--model exponential
  --n 2` it now builds the exponential at n = 2. The label names the model and n whenever either differs from the
  defaults, so `model=exponential n=2` too. *IMPLEMENTATION CHOICE.*
- **D3 — `surfing_scan` refuses to resume across models.** The CSV gains no column (§1.4), so a CSV resumed by
  another model would mix rows that cannot be told apart.
  - The model and n are recorded in the meta.json options, away from the defaults only, so a default run's entry is
    unchanged.
  - A resume is refused (exit 2) when the last entry's model and n differ from the run's. Absent keys and a missing
    meta.json count as the defaults.
  - *IMPLEMENTATION CHOICE.*
- **D4 — the model and n reach `run_history` through `make_setup`**, not as new arguments. `test_surfing_scan`'s stub
  of `run_history`, and `plot_surfing_histories`, call it with the old signature. A setup without the attributes is the
  exponential at n = 1. *IMPLEMENTATION CHOICE.*
- **D5 — the tools' tests are a new module**, `tools/tests/test_model_option.py`, beside the existing ones, which pass
  unedited. *IMPLEMENTATION CHOICE.*
- **D6 — the one-loop refusal in `history_and_bbn` exits 2**, printing `!! history_and_bbn: <check_drain_law_model's
  message>` on stderr. It is placed after the model is built and before the cosmology. The prompt says "non-zero"; 2
  is the tool's refusal status. *IMPLEMENTATION CHOICE.*
- **D7 — the seed's fixed point is in logs.** ln φ ← [ln C(φ) − ln(1 + u)]/(n + 1), so that Mⁿ cannot underflow at
  laboratory M. The returned φ_ref is the iterate whose V was computed, as the exponential's loop returns it, so V(φ_ref)
  at the returned normalization is that V. *IMPLEMENTATION CHOICE.*
- **D8 — (ln V)″ divides by φ twice.** φ² underflows below φ ≈ 1e-154; an infinite (ln V)″ raises.
  *IMPLEMENTATION CHOICE.*
- **D9 — the §1.5 table has 18 rows, not 24.** Each exponential counterpart is the same command without `--model` and
  `--n`, so it serves both n: three at laboratory M with α = 0.1, and three at 10⁻³ M_P natural. *IMPLEMENTATION CHOICE.*
- **D10 — the runs were made twice.** The first set (18:47–18:51, one-minute load 30 falling to 6) printed the φ ≤ 0
  message with a `np.float64(...)` repr. I simplified the message to "phi is not positive" (φ is shown in M_P after
  it) and remade all 18 on the final code (18:52–18:55, load 4.7–8.3).
  - Every outcome and every number in the summary table is the same in both sets, wall times apart.
  - Only the second set is committed.
  - *IMPLEMENTATION CHOICE.*
- **D11 — the tools baseline ran with my edits set aside.** My new and edited files existed by the time the tools
  suite was due. I copied them to scratch and restored the base versions with `git checkout --` (no stash). The suite
  ran on the clean base, and I copied the files back.
  - The `[g-z]` baseline started (11:20) before I wrote any file. Its discovery imported the base modules, but its
    late subprocess import tests (`test_wall_model` (7)) imported the edited package. Both passed.
  - *IMPLEMENTATION CHOICE.*

No UNINTENDED DRIFT.

## 3. Verification performed

### 3.1 The suites

From the worktree root, serially; the `[g-z]` half and `tools/tests` in the background. Load is the one-minute
average at the start and end.

| Suite | Base `75ac6fb` (ran / result / time / load) | After (ran / result / time / load) |
|---|---|---|
| CosmologyModels | 26 / OK / 67 s / 3.70 → 5.30 | 26 / OK / 67 s / 5.12 → 5.16 |
| ObjectFactories | 163 / OK / 134 s / 5.12 → 5.27 | 163 / OK / 132 s / 5.16 → 6.20 |
| RunDrivers | 76 / 1 failure (ruling 7) / 25 s / 5.09 → 3.88 | 76 / 1 failure (ruling 7) / 26 s / 6.20 → 5.69 |
| ComputeTargets `[a-f]` | 219 / OK / 147 s / 3.88 → 2.94 | 219 / OK / 152 s / 5.63 → 9.48 |
| ComputeTargets `[g-z]` | 465 / OK / 525 s / 2.94 → 3.83 | 486 / OK / 566 s / 7.94 → 6.13 |
| tools | 292 / 1 failure (ruling 7) / 284 s / 3.45 → 15.31 | 302 / 1 failure (ruling 7) / 298 s / 6.14 → 5.82 |

No count fell. The exponential goldens are in these suites and pass unchanged: `test_golden_history`,
`test_golden_reflecting_history` and `test_tuning` (e) in `[g-z]`, and `test_store_fingerprint_golden` and
`test_fingerprint_format_guard` in ObjectFactories. So do the tools' pinned lines (`test_history_and_bbn`,
`test_history_and_bbn_alpha`, `test_history_and_bbn_drain_law`, `test_surfing_scan`, `test_plot_surfing_*`; 71
tests, 118 s, also run on their own before the suites).

### 3.2 The hooks' tolerances and the closed-form checks

`.documents/power-law-wall-verification.md` §1.2 has the table, and each test's docstring gives the reason for each
tolerance. Worst measured:
- ln V against a 60-digit decimal reference: 0.90 eps·(|ln V₀| + |ln V|), against 4;
- ln(−V′) and ln V″ against their closed forms: 0.056 and 0.052 of the bound (8 eps × the logs' sizes);
- centred differences: 2.5e-9, against 1e-7;
- φ_t: exact to the last bit at all 18 points;
- the one-loop turnaround against the closed-form chain: 1.3e-13, against 1e-8;
- the minimum and its derivative: within their root-tolerance bounds.

### 3.3 The seed

At n = 1 and 2:
- tracking at (β 1.5, 10⁻³ M_P) and (β 2, 4×10⁻³¹ M_P);
- static at (β 1.5, 0.5 M_P), where tracking has no solution, as on the exponential;
- plain at β = −1.5 and 0.

Against an independent brentq of §1.2's equation: φ_ref agrees to 2.1e-14 relative at worst (0 to 2.1e-14 over ten
points, laboratory β 1.2 and 2.9 included). The normalization agrees exactly at every point (verification §1.3).

### 3.4 The tools

Under defaults, given or not, every pinned line passes unedited, and the explicit defaults reproduce
`PINNED_ALPHA_ZERO` byte for byte. Under `--model inverse-power --n 2` every `history_and_bbn` line reads
`<kind> beta=2 M=4e-31 model=inverse-power n=2: …` (or `… n=2 [window) keV: …` on a ratio line). `surfing_scan`'s
first line ends ` model=inverse-power n=2`, and each beta's line starts `beta=1.2 model=inverse-power n=2 status=…`.

### 3.5 The runs (§1.5)

`.documents/power-law-wall-verification.md` §1.5 has the table; `out/summary.md` is its source. In brief, at
laboratory M with α = 0.1, power law against exponential:

| β | ln R first impact: n = 1 / n = 2 / exp | ΔN_eff today (all three) | Y_p: n = 1 / n = 2 / exp |
|---|---|---|---|
| 1.2 | 511.074 / 308.986 / 138.168 | 0.0656475 | 0.2495309517 / 0.2495292897 / 0.2495309517 |
| 2.0 | 530.575 / 320.687 / 142.366 | 0.0524407 | 0.2470913297 / 0.2470878 / 0.2470913297 |
| 2.9 | 545.639 / 329.726 / 145.602 | 0.038435 | 0.2473962618 / 0.2473923377 / 0.2473962485 |

Every one of these runs takes one integration from the tracking seed. The reflections and injections (9, 49, 42) and
the first bounce are the exponential's.

At 10⁻³ M_P in natural mode:
- n = 2 succeeds in four integrations, with 4, 28 and 59 reflections; the exponential has none.
  - Y_p is 0.2534 / 0.2422 / 0.2413 against the exponential's 0.2481 / 0.2363 / 0.2347.
- n = 1 fails at G2 at all three β: N = 35.99, 37.76 and 40.62, with φ_E = 8.8e-13, 4.7e-14 and 5.8e-15 M_P.
  - W/(π²/2) is 1.61, 1.54 and 1.17, so the field had been stepped past its classical turning point,
    φ_E(W/(π²/2))^{1/n}, by those factors.

### 3.6 §1.6

- **Committed class.** No accepted state reached φ ≤ 0. Trial states at φ < 0 were rejected steps:
  - one at n = 1, β = 2 (N = 34.8166, φ = −1.90e-5 M_P);
  - four at n = 2, β = 2 (N = 31.324–31.346).
  The other four histories reach none. Where each history fails is §3.5's table (G2, not φ ≤ 0).
- **The even extension (scratch).** The same two histories accept a step at φ < 0 and fail as "phi <= 0 in an
  accepted state":
  - n = 1, β = 2: N = 34.82854076, φ_E = −1.1955e-4 M_P;
  - n = 2, β = 2: N = 31.36954244, φ_E = −6.9508e-4 M_P, where the committed class succeeds.
  The other four are unchanged. Verification §1.6 has the details.

## 4. The gate Model, item by item

- **Each hook equals its closed form, and the centred difference of the hook below it, from 1e-90 M to 10 M_P at
  n = 1, 2, with no overflow:** holds (§3.2; 31 φ × 2 M × 2 n, every value finite).
- **The turning point, the minimum and the seed against closed forms:** hold (§3.2, §3.3; the one-loop turnaround
  against the closed-form chain, and in `test_wall_model` (3)).
- **Under the one-loop law the model is refused at entry:** holds (`check_drain_law_model` on the model and the
  realized model; the tool exits 2 before any integration).
- **Every exponential golden bitwise:** holds (§3.1).
- **The suites at their counts or above:** hold (§3.1).

## 5. The deliberate-breakage record

Each break was made in the worktree, the named tests run, and the file restored by copy (verified with `cmp`).

| Break | What failed |
|---|---|
| B1: the sign of (ln V)′ flipped | 6 of `test_inverse_power_model`: (b) closed forms and differences, (d) all three, (h) fixed law. Also `test_wall_model` (3)'s three tests (9 in all) |
| B2: the seed's equation without the 1 + u factor | (e) at the four points with M = 10⁻³ and 0.5 M_P (n = 1, 2). At laboratory M the factor is 1 to float precision, so those points pass |
| B3: the φ ≤ 0 guard and its `ValueError` wrap removed | (c): every φ ∈ {0, −0, −1e-30, −0.1} M_P, at every hook and n, ends in a bare `ValueError` |
| B4: the tools' label always naming the model | `test_history_and_bbn_alpha` (10) (the pinned stdout), and `test_model_option` (1), (3) defaults, (6) and (7) |
| B5: the tool's one-loop check removed | `test_model_option` (4) |

## 6. Observations not acted on

- **n = 1 natural mode at 10⁻³ M_P fails at G2** on all three β. In each case the field has been stepped inside its
  turning point (W/(π²/2) = 1.17–1.61) at N ≈ 36–41, at φ ~ 1e-12 to 1e-14 M_P. Not acted on: it is a measurement
  for the user's ruling (§1.6), not this prompt's to fix, and no integrator change is allowed here.
- **n = 2 natural mode at 10⁻³ M_P reflects 4–59 times** where the exponential at the same (M, β) reflects none.
  Y_p differs by +0.005 to +0.007. A measurement for 02 and 04.
- **The φ ≤ 0 message prints M in eV** ("M=2.4353e+24 eV" at 10⁻³ M_P), as the exponential's messages do. Left as the
  exponential has it.
- **`surfing_scan`'s CSV does not name the model.** A CSV moved away from its meta.json cannot be told apart. As the
  prompt asked (no column); D3 guards a resume only.

## 7. State handed to the next prompt

- **The model.** `InversePowerModel(store_id, M, n, beta, units)` in `CosmologyConcepts/Models/`, type id 2, exported.
  - It has no `_drain_L`: the one-loop law refuses it until 03 (or 04's first commit, if U3 (a)).
  - It has no table, factory or `types` map entry; those are 05's.
- **For 05: move the id.** `tools/compare_stores.py` declares `INVERSE_POWER_MODEL` (2) under
  `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]` (D1, the user's ruling "(a), add the unstored entry to
  compare_stores"). 05, when it gives the class its table, must delete that entry and add `2: "InversePowerModel"` to
  `POLYMORPHIC_REFERENCES["ScalarHistory"]["model_serial"]`, or `test_compare_stores` (13) fails.
- **The tool route.** `tools/history_and_bbn.py BETA M --model inverse-power --n N [--alpha A]` runs a tuned
  power-law history under natural mode or the fixed law. `tools/surfing_scan.py --model inverse-power --n N` likewise.
  `--drain-law one_loop` is refused for this model.
- **For 02: ln R at the first impact** (fixed law, α = 0.1, laboratory M = 4×10⁻³¹ M_P, tuned):

  | β | n = 1 | n = 2 | exponential |
  |---|---|---|---|
  | 1.2 | 511.074 | 308.986 | 138.168 |
  | 2.0 | 530.575 | 320.687 | 142.366 |
  | 2.9 | 545.639 | 329.726 | 145.602 |

  The power-law histories at laboratory M under the fixed law match the exponential's in reflections, injections,
  first bounce and ΔN_eff to the printed digits. Y_p differs by at most 1.3e-8 at n = 1 and 3.9e-6 at n = 2.
- **§1.6, for the user's ruling.**
  - Natural mode at 10⁻³ M_P: n = 2 succeeds (4, 28, 59 reflections); n = 1 fails at G2 at N = 35.99 / 37.76 /
    40.62 (β 1.2 / 2.0 / 2.9), with φ_E = 8.82e-13 / 4.74e-14 / 5.75e-15 M_P and classical turning points
    1.42e-12 / 7.27e-14 / 6.72e-15 M_P.
  - The tuning's warning gives φ_t at the first floor reflection: 4.2e-48 / 8.4e-50 / 4.1e-51 M_P at n = 1, and
    6.6e-26 / 9.3e-27 / 2.1e-27 M_P at n = 2.
  - With the committed class, trial states at φ < 0 are rejected steps (1 at n = 1 β = 2, 4 at n = 2 β = 2), and no
    accepted state has φ ≤ 0.
  - With the even continuation (scratch, not committed), the same two histories accept a step at φ < 0 and fail,
    n = 2 β = 2 included, which succeeds with the committed class.
- **The seed** converges in one integration at laboratory M for both n. At 10⁻³ M_P the tuned natural-mode histories
  took four integrations, as the exponential's did.
- **No issue opened or closed**; `.documents/OPEN_ISSUES.md` is unchanged (32 open).
