# Log 03 — PRyMordial's passenger equation, and why a BBN solve failed

**Prompt:** prompts/review-remediation/03-prymordial-passenger-and-failure-reasons.md
**Commit:** the commit that adds this file — "Patch PRyMordial's inert T_NP equation and record BBN failures"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/03-prymordial-passenger-and-failure-reasons.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-29
**Result:** COMPLETE WITH DEVIATIONS

R2 is closed. The patch makes the oscillating case finish in 8–9 s where it was killed at 120 s,
and it changes no physical output: ρ_NP ≡ 0 with the NP machinery switched on reproduces the
no-NP run exactly (difference 0), and the constant 0.08 family moves by 7.5e-11 in Yp and 2.8e-7
in D/H. Every failure path of `compute_BBN_data` now returns a reason, the reason is stored on the
row, and `plot_by_beta.py` lists the models it drops.

**The first dispatch stopped at a §6 condition and committed nothing.** Test (c) as written
could not pass, on the unpatched tree or the patched one. Its reference, README §2 (f) row 2
(Yp 0.25409, D/H 2.6715), was not reproducible to 1e-5 by the recipe in P0, and PRyMordial's
output moves by more than 1e-5 under perturbations of ρ_NP at the 1e-9 level (§ Verification).
The user decided, 2026-09-29, verbatim: "Test (c): use option C" and "build(): yes, pick the
newest row". Both decisions are below under Deviations, and on the board.

## What shipped

Tree at dispatch: `47c50ae`. Resumed on the same tree after the user's decisions.

- **P1 — the passenger.** `PRyM/PRyM_main.py:139–150`, `dTNPdt`. The body now returns `0.0` after
  this marker comment, exactly:
  ```
  # ChamPBH review-remediation prompt 03: T_NP is inert (never read); the original
  # -3H(rho+p)/drho_dT is singular wherever drho_NP/dT = 0 and stalls LSODA.
  ```
  The original eight-line body follows, commented out, so that an upgrade of the vendored copy
  shows the diff. The signature and the function's place in `dTtotdt` are unchanged, so the
  solution vector keeps three components, `Tgamma_Tnu_TNP.txt` keeps its layout and `TNPofT` still
  exists. T_NP now stays at `Tstart_NP` throughout.
  - **T_NP is read nowhere; confirmed on the tree.**
    - `Hubble(Tg, Tnue, Tnumu, T_NP=0.0)` (`:61`) never uses its fourth argument.
    - `TNPofT` (`:280`) is used only at `:359–361`, where it is passed back into `Hubble`.
    - `TNP_vec[-1]` goes to `N_eff(…, T_NP)` at `:1310`, which does not read it either.
    - `PRyM_jl_sys.py:13–16` only forwards it.
    - `PRyM_thermo.delta_rho_NP` returns `0.` (`:157`), and `rho_NP`/`p_NP`/`drho_NP_dT` are
      evaluated at `Tg` everywhere.
  - No change to `Hubble`, `dTgdt`, `dTnudt`, the tolerances or `n_sampling`. `NP_e_flag` is not
    used anywhere.
- **P2 — the version string.** `ComputeTargets/BBNData.py:38`, a new module constant
  `PRYM_VERSION = "bf24c3d+cham03"`, with a comment naming the patch. `compute_BBN_data`'s return
  (`:353`) now uses it instead of the literal `"bf24c3d"`.
- **P3 — failure reasons.**
  - `ComputeTargets/BBNData.py:41`, new `_failure_payload(reason: str) -> dict`. It returns
    `{"failure": True, "failure_reason": str(reason)[:DEFAULT_STRING_LENGTH]}` (256).
  - The pre-check (`:94–101`) keeps its print and returns
    `_failure_payload("pre-check: T_Jordan_stop=<T> is more than 0.1*T_BBN_spline_min=<T>")`, both
    temperatures through `energy_formatter`.
  - The `except (OverflowError, ValueError, ComputationFailureError)` at `:329–330` (was `:311–312`)
    returns `_failure_payload(f"PRyMordial: {type(e).__name__}: {e}")`. The caught tuple is
    unchanged. No new failure path was added.
  - `BBNData`: new attribute `_failure_reason`, set from the payload in `__init__`, from the
    compute result in `store()` (`data.get("failure_reason")` on failure, `None` on success). New
    property `failure_reason -> Optional[str]` (`:431`). It raises only while the object is not
    queryable, like `BBN_compute_time`, and never because `failure` is true.
  - `Datastore/SQL/ObjectFactories/BBNData.py`:
    - A new nullable column, `sqla.Column("failure_reason", sqla.String(DEFAULT_STRING_LENGTH),
      nullable=True)` (`:96–99`).
    - `build()` selects it (`:132`) and puts it into the `BBNData` payload (`:262`).
    - `store()` writes `obj._failure_reason if obj._failure else None` (`:286`).
    - **The user's decision.** When `build()` is asked for `failure=True` it orders by `timestamp`
      desc, then `serial` desc, and takes the first row (`:166–172`). It therefore returns the newest
      failed row instead of raising `MultipleResultsFound`. `failure=False` and `failure=None` behave
      exactly as before.
  - `ShardedPool`, the serial broker and `RayWorkPool` are not touched; `object_get` passes the
    `failure` key through untouched.
- **P4 — dropped models listed.** `plot_by_beta.py:525`, new nested function
  `report_dropped_bbn_models(model_label, potential, model_proxies, bbn_results)`. It is called
  once per potential from `build_plot_work` (`:710`), after the BBN query and before
  `build_beta_plot.remote`.
  - Failed rows were filtered at the query (`"failure": False`), so they never reached
    `build_beta_plot`'s `not d.failure` filters. For each model whose success lookup comes back
    unavailable, the function queries with `"failure": True` to read the newest failed row's
    `failure_reason`. With no row at all it reports "no BBNData row in the store".
  - It also lists successful rows that `build_beta_plot`'s "> 0" filters drop from a panel.
  - It prints one header line with the count, (M, Λ) in eV, then one line per dropped model:
    β, M, Λ, reason.
  - Nothing that is plotted or written to CSV changes: `available_bbn` and the arguments to
    `build_beta_plot` are exactly as before.
- **Tests.**
  - New package `ComputeTargets/tests/`: `__init__.py` (empty), `prym_fixtures.py`,
    `test_prym_passenger.py`. Four tests.
  - `prym_fixtures.py` is also the P0 script, run one case at a time:
    `python -m ComputeTargets.tests.prym_fixtures {zero,constant,oscillating,reference}`.
- **`VERSION_LABEL`**: `"2026.2.0"` before and after (not touched).

## Deviations from the prompt

### Test (c)'s reference is the fixture's own unpatched value — STRUCTURALLY REQUIRED (user decision, 2026-09-29)

- **What the prompt assumed.** `run_prym(constant 0.08…)` gives Yp = 0.25409 and D/H ×10⁵ =
  2.6715 to 1e-5 relative, per README §2 (f) row 2. A miss would mean the patch is not inert.
- **What was there.** P0 measured the constant family three ways on the unpatched tree:

  | ρ_SM construction | Yp | D/H ×10⁵ | vs README (Yp / D/H) |
  |---|---|---|---|
  | raw fit `_raw_G_rho`, ρ = r · ρ_SM(T) | 0.25408672 | 2.6713933 | 1.3e-5 / 4.0e-5 |
  | spline class, ρ = r · ρ_SM(T) | 0.2540870751 | 2.671227428 | 1.6e-5 / 1.0e-4 |
  | spline class, ρ = r · (π²/30) · g · T⁴ written out | 0.2540937879 | 2.671500711 | 1.49e-5 / 2.7e-7 |

  None meets 1e-5 in Yp, and the patch changes none of them. The miss is the reference's, not the
  patch's.
- **Why.**
  - The README figures are rounded to five figures. 0.25409 carries ±2e-5 relative, which is
    coarser than the 1e-5 test.
  - PRyMordial's output responds to ρ_NP changes at the rounding level (see Verification). A 1e-9
    relative change moves Yp by 1.8e-5, and re-associating one product moves D/H by 1.0e-4.
- **What was done.** The user chose option C.
  - The fixture's g_ρ is `SaikawaShirai_EOS_spline.G_rho`, built in GeV units on first use.
  - ρ_NP is written as `ratio * (pi*pi/30) * g * T**4`, left to right. That is the construction and
    floating-point order that reproduce README row 2 (D/H to 2.7e-7), and the docstring warns
    against simplifying it.
  - Test (c) compares against that fixture's unpatched values, pinned as
    `CONSTANT_YP_UNPATCHED = 0.2540937879` and `CONSTANT_D_OVER_H_E5_UNPATCHED = 2.671500711`, at
    **1e-5**. The tolerance is unchanged.

### `build()` returns the newest failed row — IMPLEMENTATION CHOICE (confirmed by the user, 2026-09-29)

- `main.py`'s BBN lookup uses the default `failure=False`, so a model whose BBN computation fails
  deterministically is recomputed and re-stored as a new failed row on every run. A
  `failure=True` lookup would then raise `MultipleResultsFound` in `one_or_none()`.
- Alternatives considered:
  - query with `failure=None` from `plot_by_beta.py`, which also raises when a success and a
    failure coexist;
  - fetch all failed rows, which would need a new method outside `build()`'s contract.
- The user chose "pick the newest row". Newest means greatest `timestamp`, ties broken by
  `serial`. The lookup key (model serial, failure flag, tags) is unchanged.

### The whole original body is kept, commented out — IMPLEMENTATION CHOICE

- The prompt says "keep the original line". The original is eight lines, so all eight are
  commented out below `return 0.0`.

### P0 lives in the fixture module — IMPLEMENTATION CHOICE

- The P0 script is `prym_fixtures.py`'s `__main__`, one case per invocation, so the oscillating
  case can sit under an external `timeout`.
- The alternative, a separate script under `tools/`, would duplicate the fixture.

### Reason strings — IMPLEMENTATION CHOICE

- Each reason is prefixed with its path: `pre-check: …` or `PRyMordial: <ExceptionClass>: <message>`.
  A stored reason therefore says which path produced it without parsing the message.
- The pre-check's existing print (including its "more than than") is left as it was; the stored
  reason has its own wording.

### `failure_reason` raises only while unpopulated — IMPLEMENTATION CHOICE

- It follows `BBN_compute_time` and `NP_compute_time`: `RuntimeError` if not queryable, otherwise
  the value.
- The prompt's requirement, readable when `failure` is true, holds. Returning `None` silently when
  unpopulated was rejected, since it would hide a lookup that never happened.

### The listing includes non-positive abundances, and is printed on the driver — IMPLEMENTATION CHOICE

- `build_beta_plot` drops rows with Yp, D/H or ⁷Li/H ≤ 0 panel by panel. Its comment calls these
  "a PRyMordial integration failure", so they are listed too, naming the panels.
- Printing happens in `build_plot_work`, on the driver, rather than inside the Ray task
  `build_beta_plot`. It is printed once per potential, and the failed-row query needs `pool`,
  which lives on the driver.

## Verification performed

All runs were from the repository root, with `PYTHONPATH=. ./venv/bin/python`, on the tree at
`47c50ae` plus this prompt's diff. "Unpatched" means `PRyM/PRyM_main.py` as at `47c50ae`
(`git apply -R` of P1, or `git show 47c50ae:PRyM/PRyM_main.py`). Every case uses the flags
`compute_BBN_data` sets.

### P0 and the patched runs (I ran these)

Command: `python -m ComputeTargets.tests.prym_fixtures <case>`, with the final fixture. The
oscillating case ran under `timeout 120`. Wall times vary by about ±1.5 s between runs.

| case | unpatched | patched |
|---|---|---|
| ρ_NP ≡ 0, `NP_thermo_flag = True` | 7.2–9.4 s; **916 `RuntimeWarning`s**, all at `PRyM_main.py:147` (`return num / den`); N_eff 3.04439, Yp 0.2468872958, D/H 2.462251065, ³He/H 1.04205, ⁷Li/H 5.42344 | 8.2–8.9 s; **0 warnings**; identical to all digits |
| no NP (`NP_thermo_flag = False`), the reference | 7.6–9.2 s; 0 warnings; identical to the row above | 7.1–8.3 s; identical |
| constant 0.08 ρ_SM, p = ρ/3 | 8.0 s; N_eff 3.71342, **Yp 0.2540937879, D/H 2.671500711**, ³He/H 1.07198, ⁷Li/H 5.09122 | 7.3 s; Yp 0.2540937879, D/H 2.671499971 |
| oscillating ratio | **killed by `timeout 120`, exit 124** (twice, with two fixture versions) | 7.9–9.0 s; N_eff 3.70221, Yp 0.2469265751, D/H 2.787693732, ³He/H 1.10916, ⁷Li/H 4.46252 |

The README's §2 (f) SM row (0.24689, 2.4623, 1.042, 5.423) is reproduced to its figures.
N_eff for the constant family is 3.71342, against the README's 3.7129: R5 moved g_ρ below 10 keV
from 3.38 to 3.383. With 3.38 put back in a scratch fixture, N_eff is 3.71289.

### README §6.2, row by row

| Quantity | Target | Measured | |
|---|---|---|---|
| oscillating ρ_NP, `NP_thermo_flag`, wall | ≤ 60 s | **8.09 s** in the suite (7.85–8.99 s standalone); > 120 s unpatched | ✅ |
| ρ_NP ≡ 0, True vs False, Yp and D/H | ≤ 1e-6 relative | **0** and **0**; warnings 916 → 0 | ✅ |
| ρ_NP = 0.08 ρ_SM, Yp / D/H | unchanged to 1e-5 | against the unpatched pin: **7.5e-11 / 2.8e-7**. Against README's five-figure 0.25409 / 2.6715: 1.49e-5 / 1.1e-8 | ✅ by the user's option C; the literal README comparison misses in Yp, see Deviations |
| a failed `compute_BBN_data` | carries a `failure_reason`, stored, printed with (β, M, Λ) | test (d); column, `build()`, `store()`, `report_dropped_bbn_models` | ✅ |
| `PRyM_version` on new rows | names the patch | `"bf24c3d+cham03"` (`grep -n PRYM_VERSION ComputeTargets/BBNData.py`) | ✅ |

### The suite (I ran these)

- `ComputeTargets/tests`: **0 → 4**, `Ran 4 tests in 29.819s OK`. The values the tests saw, taken
  in one process in test order by a scratch script:
  - (a) oscillating: 8.09 s, all eight results finite;
  - (b) difference 0 in Yp and D/H, 0 `RuntimeWarning`s;
  - (c) 7.52e-11 and 2.77e-7 against the pins;
  - (d) the reason names "1 eV" and "0.01 eV".
- `CosmologyModels/tests`: **6 → 6**, OK (5.2 s before, 10.5 s after; the machine was busy).
- **The orchestrator's breakage check**, which I ran. With `PRyM/PRyM_main.py` replaced by
  `47c50ae`'s:
  ```
  timeout 120 env PYTHONPATH=. ./venv/bin/python -m unittest \
    ComputeTargets.tests.test_prym_passenger.TestPRyMordialPassenger.test_a_oscillating_case_completes
  ```
  It exited 124 at 120 s. The patched file was then restored.
- `black --check` is clean on every changed file. All four pre-existing files were black-clean at
  `47c50ae`.

### PRyMordial's sensitivity (I ran these; scratch scripts, not committed)

The constant family with ρ_NP multiplied by (1 + ε), raw-fit fixture:

| ε | Yp | D/H ×10⁵ |
|---|---|---|
| 0 | 0.25408672 | 2.6713933 |
| +1e-9 | 0.25409139 (+1.8e-5) | 2.6714074 (+5e-6) |
| +1e-8 | 0.25409257 | 2.6712592 |
| −1e-8 | 0.25408531 | 2.6723603 (+3.6e-4) |

The patched tree gives the same pattern; for example ε = +1e-9 gives D/H 2.6714054.
- The spline-class and raw-fit ρ_NP callbacks agree to 2.2e-10 in ρ and 1.6e-8 in dρ/dT on
  [0.3 keV, 16 MeV]. Their Yp still differs by 2.8e-5.
- The response is not monotonic in ε. It looks like step-selection noise in PRyMordial's
  integrators rather than physics.

### The datastore (I ran these; scratch scripts)

- **`build()` on in-memory SQLite.** The table was built from `sqla_BBNDataFactory.register()`'s
  columns, with three rows for one model: failed/"older failure", success, failed/"newest failure".
  - `failure=True` returned the newest failed row, `failure_reason='newest failure'`.
  - `failure=False` returned the success, with `failure_reason=None`.
  - `failure=None` still raises `MultipleResultsFound`, as before.
- **What an old store does (SQLAlchemy 2.0.46, SQLite).** `Datastore._ensure_tables` creates a
  table only if it is missing, so an existing `BBNData` table keeps its old columns.
  - Startup validation and `inventory()` do not select the new column, so they still work.
  - The first `BBNData` lookup fails with
    `sqlite3.OperationalError: no such column: BBNData.failure_reason`, and so would the first
    insert. A 2.0.46 check on an in-memory table without the column gave exactly that error.
  - Every such store is invalid anyway (README §2 (e)), so the failure is loud rather than silent,
    which is the right way round.

### What I reasoned but did not run

- `report_dropped_bbn_models` has not been run: `plot_by_beta.py` needs a Ray cluster and a
  datastore. It parses (`ast.parse`), `black` is clean, and it reuses the `RayWorkPool` +
  `pool.object_get("BBNData", …)` pattern of the query above it.
- The results of `RayWorkPool(store_results=True)` are in batch order; `main.py:600–660` relies on
  the same property with `zip`.
- It needs a user's run of `plot_by_beta.py` on a store that has failures.

## Observations not acted on

1. **`compute_BBN_data`'s `small_network` switch does nothing.** → board §3
   `[03-small-network-flag-is-never-read-by-prymordial]` (opened at the user's instruction).
   - `BBNData.py:324` (`:306` before this prompt) sets `PRyMini.small_network_flag`.
   - PRyMordial never reads that name. It reads `smallnet_flag` (`PRyM_init.py:111`, default
     `False`) at `PRyM_main.py:599, 884, 982, 989, 1161, 1167`.
   - So every production BBN solve ran the full network, and the `small_network` value stored on
     `BBNData` rows (and shown by `add_BBN_info_labels`) is a label with no effect.
   - **README §2 (f)'s figures, and this prompt's fixture with `small_network=True`, therefore
     ran the full network.** `run_prym` copies the flag faithfully, as the prompt asks.
   - With `smallnet_flag = True` set directly (scratch, `47c50ae`, raw-fit fixture, g_ρ below
     10 keV at 3.38), the constant family gives Yp 0.25408633, D/H 2.6709992, ³He/H 1.07231,
     ⁷Li/H 5.14143, in 5.9 s. The full network gives 0.25408672, 2.6713932, 1.07201, 5.0910 in
     9.1 s.
   - Not fixed: it changes physical output.
2. **PRyMordial's output responds at 1e-5–1e-4 to bit-level changes in ρ_NP** (table above). →
   board §3 `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`.
   - Any test that compares PRyMordial abundances across two constructions of the "same" ρ_NP is
     limited to that level.
   - This bears directly on prompt 04's end-to-end target (Yp, D/H to 1e-4 relative through the
     new callbacks). Its ratio spline will differ from the fixture's analytic ratio by far more
     than 1e-9.
3. **PRyMordial's solver failures are not detected, and some exceptions escape.** → board §3
   `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]`.
   - None of the eight `solve_ivp` calls in `PRyM_main.py` checks `.status` or `.success`.
   - `compute_BBN_data` catches only `(OverflowError, ValueError, ComputationFailureError)`.
   - A failed integration therefore returns whatever the truncated arrays give, which is a
     plausible source of `plot_by_beta.py`'s "negative values" filter. Any other exception
     (`ZeroDivisionError`, `RuntimeError`, a `LinAlgError`) leaves the Ray task with no failure
     row at all.
   - The prompt said not to widen anything beyond the listed paths.
4. **`main.py` recomputes and re-stores a failed BBN row on every run.** Its lookup uses the
   default `failure=False` (`main.py:617–630`). This is what makes several failed rows per model
   possible, and why `build()` now picks the newest. → board §3
   `[03-main-recomputes-failed-bbn-rows-on-every-run]`.
5. **The pre-check message says "more than than"** (`BBNData.py:97`). This is cosmetic and was
   left as it was; the stored reason has its own wording. No issue opened.

## State handed to the next prompt

- **`run_prym` signature.**
  `ComputeTargets.tests.prym_fixtures.run_prym(rho, p, drho, small_network=True, NP_thermo_flag=True)`.
  - The callbacks take T in MeV and return MeV⁴ (ρ, p) or MeV³ (dρ/dT).
  - It returns `PRyMclass(rho, p, drho).PRyMresults()`, an 8-array. The index constants are
    `RES_NEFF = 0`, `RES_YP_BBN = 4` (the Yp `compute_BBN_data` stores), `RES_D_OVER_H_E5 = 5`,
    `RES_HE3_OVER_H_E5 = 6` and `RES_LI7_OVER_H_E10 = 7`.
  - It sets `NP_thermo_flag`, `Tstart_NP`, `verbose_flag` and `small_network_flag` as
    `compute_BBN_data` does, and restores them and PRyM_thermo's four NP callbacks on exit.
  - The families are `ZERO`, `CONSTANT` and `OSCILLATING`, each a `SyntheticNP(label, rho, p,
    drho_dT)`, built with `make_family(label, ratio)`. The other helpers are `g_rho(T_MeV)` (the
    `SaikawaShirai_EOS_spline` class), `rho_SM(T_MeV)` and `ratio_constant` / `ratio_oscillating`.
- **The constant-ratio abundances** (0.08 ρ_SM, p = ρ/3, full network — see observation 1), patched
  tree, `python -m ComputeTargets.tests.prym_fixtures constant`:
  - **Yp 0.25409, D/H ×10⁵ 2.6715**, N_eff 3.7134, ³He/H ×10⁵ 1.0720, ⁷Li/H ×10¹⁰ 5.0912;
  - to ten figures, Yp 0.2540937879 and D/H 2.671499971.
- **The SM baseline** (ρ_NP ≡ 0, or no NP): Yp 0.24689, D/H 2.4623, N_eff 3.0444, ³He/H 1.042,
  ⁷Li/H 5.4234.
- **The oscillating family, patched:** Yp 0.24693, D/H 2.7877, N_eff 3.7022, in about 8–9 s.
- **Caution for prompt 04's end-to-end target (1e-4).** PRyMordial's D/H moves by 3.6e-4 when
  ρ_NP moves by 1e-8, and by 1.0e-4 when one product in ρ_NP is re-associated. Measure the spread
  before trusting a 1e-4 comparison; if it cannot be met, it is a stop.
- **`compute_BBN_data`'s failure contract.**
  - Every failure returns `_failure_payload(reason)`, that is
    `{"failure": True, "failure_reason": <str ≤ 256>}`. A new failure path (for example prompt 04's
    non-monotonic `log_T_Jordan` refusal) should return it too.
  - `PRYM_VERSION = "bf24c3d+cham03"` is the version string. A later patch to `PRyM/` should
    extend its suffix.
- **Suite counts after this prompt.** `ComputeTargets/tests` 4 (about 30 s, PRyMordial);
  `CosmologyModels/tests` 6.
