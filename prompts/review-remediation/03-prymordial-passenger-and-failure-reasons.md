# Prompt 03 — PRyMordial's passenger equation, and why a BBN solve failed

**Campaign:** [`README.md`](README.md) · **Board item:** **R2** ·
**Board:** `IMPLEMENTATION_STATE.md` — update your row and R2.
**Closes:** R2. **Recommended model:** **Opus**. The patch is three lines in a vendored file.
The judgement is in proving it changes nothing physical, and in making failures legible without
turning a boolean into a free-text dumping ground.

**Read first:**

1. [`README.md`](README.md) §0, §2 (d), (f), (g), §5, §6.2.
2. `.documents/audit-2026-09-29/README.md` §3.
3. `PRyM/PRyM_main.py:56–215` — `Hubble`, `dTnudt`, `dTgdt`, `dTNPdt`, `dTtotdt`, and the
   `solve_ivp` call with `LSODA`, `rtol=1e-6`, `atol=1e-9`. Confirm for yourself that `T_NP`
   is read nowhere: `Hubble` ignores its fourth argument, `TNPofT` (`:280`) is only ever passed
   back to `Hubble` (`:361`), and `delta_rho_NP` is `0.` (`PRyM_thermo.py:157`).
4. `PRyM/PRyM_thermo.py:148–158` and `:163–172` — the default NP callbacks, and the `NP_e_flag`
   entropy modification you must **not** adopt.
5. `ComputeTargets/BBNData.py` `:77–83` (the pre-check that returns `{"failure": True}`),
   `:190–290` (the three callbacks and their guards), `:293–312` (the flags and the swallowed
   exception), and the `BBNData` class from `:336` — how `failure` is carried.
6. `Datastore/SQL/ObjectFactories/BBNData.py` — the table (`:88–110`), `build()` (`:112–150`),
   and the store/read payloads (`:240–290`).
7. `plot_by_beta.py:130–150` — the filters that drop failed rows.
8. `CLAUDE.md` — `PRyM/` is vendored and may be patched with a marker comment.

---

## 1. P0 — measure first, change nothing

Reproduce README §2 (f) on the unpatched tree, from the root, with the flags `compute_BBN_data`
sets, and record wall-clock and abundances for:

- ρ_NP ≡ 0 (note the `RuntimeWarning`s; count them);
- ρ_NP = 0.08 ρ_SM(T), p_NP = ρ_NP/3, dρ_NP/dT by central difference of the same function;
- the oscillating ratio r(T) = 0.08 + 0.3 sin(2π ln(T/0.3 MeV)) exp(−[ln(T/0.3 MeV)/1.5]²),
  with a hard `timeout` of 120 s. Record that it does not finish. (It did not finish in 600 s on
  2026-09-29; do not spend more than 120 s confirming.)
- the same with `NP_thermo_flag = False` (no NP at all) — the reference for §3 (b).

Keep the script: it becomes the test module's fixture (§3).

## 2. The patch

**P1 — `dTNPdt` returns `0.0`.** In `PRyM/PRyM_main.py`, the function keeps its signature and its
place in `dTtotdt` (so the solution vector shape, the saved `Tgamma_Tnu_TNP.txt` layout and
`TNPofT` are all unchanged) and returns `0.0` after a comment of the form
`# ChamPBH review-remediation prompt 03: T_NP is inert (never read); the original
# -3H(rho+p)/drho_dT is singular wherever drho_NP/dT = 0 and stalls LSODA.` Keep the original
line, commented out, immediately below, so an upgrade of the vendored copy shows the diff.

**P2 — the version string.** `BBNData.py`'s `"PRyM_version": "bf24c3d"` becomes a string that
names the patch, e.g. `"bf24c3d+cham03"`, so a stored row says which PRyMordial produced it.

**P3 — failure reasons.** `compute_BBN_data` returns `{"failure": True, "failure_reason": <str>}`
from every failure path: the `T_Jordan_stop` pre-check (quote the two temperatures), the
`except` at `:311` (the exception's class and message), and any new path you add. The string is
truncated to `DEFAULT_STRING_LENGTH` (256). `BBNData` gains `_failure_reason` / `failure_reason`
(readable even when `failure` is true — the other properties raise then, this one must not), the
factory gains a nullable `failure_reason` column, `build()` selects it and the payloads carry it.
An existing store without the column is not this campaign's concern (README §2 (e): every
existing store is invalid anyway) — but say in the log what opening one would do.

**P4 — dropped models are listed.** `plot_by_beta.py`: wherever failed rows are filtered out,
print, once, one line per dropped model with its (β, M, Λ) and `failure_reason`, and the count.
Do not change what is plotted.

## 3. Tests — `ComputeTargets/tests/`, new package

`ComputeTargets/tests/__init__.py`, `prym_fixtures.py` (the three synthetic ρ_NP families and a
`run_prym(rho, p, drho, small_network=True, NP_thermo_flag=True)` helper that sets the flags
exactly as `compute_BBN_data` does and returns the results tuple), and
`test_prym_passenger.py`. **Docstring: this module runs PRyMordial; about 40 s.**

- **(a) The oscillating case completes.** `run_prym(oscillating…)` finishes in **≤ 60 s** (wall,
  asserted) and returns finite abundances.
- **(b) The patch is inert.** `run_prym(zero…, NP_thermo_flag=True)` and
  `run_prym(zero…, NP_thermo_flag=False)` agree in Yp and D/H to **1e-6 relative**, and the
  first raises **no** `RuntimeWarning` (`warnings.catch_warnings(record=True)`).
- **(c) The reference abundances are unchanged.** `run_prym(constant 0.08…)` gives
  Yp = 0.25409 and D/H ×10⁵ = 2.6715 to **1e-5 relative** (README §2 (f), row 2). If they differ
  by more, the patch is not inert and that is a §6 stop.
- **(d) A failure carries a reason.** Call `compute_BBN_data`'s body through
  `compute_BBN_data._function` (or a factored helper) with a stand-in model whose
  `T_Jordan_stop` fails the pre-check, and assert the dictionary has `failure_reason` naming both
  temperatures. If reaching the body without a `ScalarModelProxy` is impractical, factor the
  pre-check into a pure function and test that; say which in the log.

Beware `PRyM_init`'s module globals: set the flags in the helper every time, and restore them.

## 4. What this prompt does not do

- No `NP_e_flag`. No change to `Hubble`, `dTgdt`, `dTnudt`, the tolerances, or `n_sampling`.
- No change to the callbacks' representation (prompt 04), the spline domain, or the sampling.
- No schema change beyond the one nullable column.

## 5. Acceptance

1. README §6.2, every row, with measured values in the log; the P0 table beside them.
2. The orchestrator can confirm the deliberate breakage cheaply: with `PRyM_main.py` at `HEAD~1`,
   test (a) exceeds its 60 s bound (run with an external `timeout 120`).
3. `ComputeTargets/tests` **0 → 4**; `CosmologyModels/tests` unchanged. `black --check` clean on
   changed files. The marker comment is in the vendored file.
4. Board R2 done; the log's "State handed" gives prompt 04 the `run_prym` signature and the
   constant-ratio abundances to five figures.

## 6. Stop conditions — stop and ask the user

- Test (b) or (c) misses: the patch changed a physical output.
- The oscillating case still does not finish with the patch: the stall has another cause, and
  the audit's diagnosis was incomplete. Report the wall-clock and where LSODA spends it.
- Adding the column requires touching `ShardedPool` or the serial broker, or anything beyond the
  `BBNData` factory.
- You conclude the failure reason cannot be captured without changing `RayWorkPool`.

## 7. The log and the board

`logs/03-prymordial-passenger-and-failure-reasons.md`. Beyond the template: the P0 table; the
`RuntimeWarning` count before and after; what an old store without the column does; the exact
marker comment. Board: R2 done. `.documents/OPEN_ISSUES.md`: any issue you open.
