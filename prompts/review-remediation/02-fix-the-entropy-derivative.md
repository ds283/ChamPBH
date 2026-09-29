# Prompt 02 — fix the entropy derivative and the 10 keV join, tighten the guard, bump the version

**Campaign:** [`README.md`](README.md) · **Board items:** **R1** (the fix), **R5** (the fix) ·
**Board:** `IMPLEMENTATION_STATE.md` — update your row, R1 and R5.
**Closes:** R1, R5. **Recommended model:** **Opus**. The diff is a handful of lines. The work is the
re-scoring, the deliberate-breakage check, and stating the consequence for every existing store.

**Read first:**

1. [`README.md`](README.md) §0, §2 (a), (b), (e), (j), §5, §6.1, and
   `.documents/audit-2026-09-29/README.md` §11 (R5).
2. `logs/01-temperature-law-harness.md` and `CosmologyModels/tests/` — what prompt 01 built,
   the constants it left for you to flip, and the offsets it measured.
3. `CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py` `:36–75` (the grid) and
   `:114–171` (the two derivatives). Note the stale comments *"units of the output will be
   1/GeV"* on both; the output is dimensionless.
4. `SaikawaShirai_EOS_jax_autodiff.py:215–223` — the convention the fix must match.
4a. `CosmologyModels/GenericEOS/SaikawaShirai_common.py:94–122` — the low-temperature constants
    (R5) and the comment block around them.
5. `main.py:80` and `plot_by_beta.py:67` — `VERSION_LABEL = "2026.1.1"`.
6. `.documents/numerical-strategies.md` — find where it describes the temperature law and the
   g_s derivative, so you can add a dated note (additive; do not rewrite).

---

## 1. What to change

**F1 — the derivatives.** In `SaikawaShirai_EOS_spline`, make `dG_s_dlogT` and `dG_rho_dlogT`
return dg/d ln T. Two acceptable ways; pick one and say why in the log:

- divide the spline derivative by `math.log(10.0)` at the return (minimal; the log10 grid and the
  clamps stay as they are); or
- build `_log_T_grid` in ln T and convert `_LOG10_SAIKAWA_SHIRAI_T_{HI,LO}` and every `np.log10`
  in the class consistently.

Whichever you choose, **the docstrings of both methods state the convention** ("d g / d ln T,
dimensionless") and the stale unit comments go. `Xav_EOS_spline` inherits both and needs no
change; confirm it overrides neither.

**F1b — the low-temperature limits (R5).** In `SaikawaShirai_common.py`:

- **The constants.** Set `LOW_T_GSTAR = 3.383` and `LOW_T_G_S_STAR = 3.931`. These are the
  constant terms of the fit's low-T branch (2.030 + 1.353 and 2.008 + 1.923), to which the fit
  has converged at 10 keV (README §2 (j)).
- **The comment block.** Replace the block around them so it says where the values come from:
  the fit's own x → ∞ limit, and consistency with N_eff ≈ 3.046 when the neutrino T³ weight
  scales as (N_eff/3)^{3/4}. Cite audit §11. Remove the commented-out 3.36 and 3.91 and the
  `TODO: check` that the audit answers.
- **What stays.** Do not move `SAIKAWA_SHIRAI_T_LO`, the grid start at 0.8 T_LO, or any fitting
  coefficient.
- **The jax class** imports the same constants. Confirm it picks up the new values and needs no
  edit.

**F2 — the consumer stays.** `ScalarModel.py:359–360` is already written for d ln T. Do not
touch it. If you find yourself wanting to, that is a §6 stop.

**F3 — flip the guard.** In `test_temperature_law.py`, per prompt 01's comments: expected
offsets → 0 with tolerance 1e-5 (case 1); expected derivative ratio → 1 with 1e-6 (cases 3, 4);
set prompt 01's R5 residual constant (at 10 keV and T_CMB, case 2) to 0 with tolerance 1e-5;
remove the `kappa = 1` half of case 5; and re-centre the corrected half on the value you now
measure. The audit §11 probe predicts 1.00135 from 5 MeV and 0.99922 from 2×10⁴ GeV. Keep
prompt 01's tolerance and quote the measured value.
Case 2 (`kappa = 1/ln 10`) now duplicates case 1 with the shipped derivative; keep it, since it
still holds and documents what the factor was, or fold it in — say which.

**F4 — the version label.** `VERSION_LABEL = "2026.2.0"` in both `main.py` and `plot_by_beta.py`.
This is the campaign's whole answer to README §2 (e) and it is not enough on its own, so the
docstring or comment beside the constant in `main.py` says, in one sentence, that stores made
under an earlier label are invalid because the temperature law and the low-temperature g's
changed on this date.

**F5 — the documentation note.** One dated paragraph added to `.documents/numerical-strategies.md`
where the temperature law is described, saying what the convention is and that it was wrong by
ln 10 from `5962833` to this commit. Also say that the low-temperature limits are now the fit's
own (R5). Additive.

---

## 2. Show the guard bites

Before committing, with your change in place: `git stash` (or check out `HEAD` copies of the
production files into place), run `CosmologyModels/tests`, and confirm cases 1, 3, 4 and 5
**fail** with the characterised values (offset +1.422, ratio 2.303, ρ_R ratio 0.0041). Then
restore only `SaikawaShirai_EOS_spline.py`, keeping `HEAD`'s `SaikawaShirai_common.py`. Confirm
that case 2's join half fails with +1.465e-4, which shows the guard sees R5 independently of
R1. Restore everything and confirm the suite passes. Quote both runs in the log. The orchestrator will repeat this against
`HEAD~1`.

---

## 3. Re-score

Run, from the root, and quote in full:

- `venv/bin/python .documents/audit-2026-09-29/tlaw_check.py` — the `kappa=1` column must now
  equal the `kappa=1/ln10` column and the exact column to 1e-5.
- `venv/bin/python .documents/audit-2026-09-29/eos_consistency.py` — the three ratios must be
  0.99–1.01.
- The suite, with counts before and after.

Do **not** edit the audit scripts to make them agree; they measure the tree they are run on. If
`tlaw_check.py`'s `kappa=1/ln10` column now double-divides, that is expected: it applies its own
factor to a corrected derivative, and the log says so. The same goes for `low_t_join_probe.py`,
which also applies ÷ln 10 itself. On the fixed tree it is not a re-score; the tests are.

---

## 4. What this prompt does not do

- It does not change the sampling of the EOS grid (`_SAMPLES_PER_LOG10_T = 250`), the clamp
  temperatures, the fitting coefficients, or `w()`. The two R5 constants (F1b) are the only
  values in `SaikawaShirai_common.py` that change.
- It does not re-run any scalar history or touch a datastore.
- It does not key datastore lookups on the version — seeded issue
  `[00-datastore-lookups-ignore-the-version-column]`; add an **Assigned** line only if you can
  say who owns it, otherwise leave it.

---

## 5. Acceptance

1. README §6.1, every row, at its target; the measured values in the log.
2. The deliberate-breakage record (§2) in the log, both directions.
3. `VERSION_LABEL == "2026.2.0"` in both files; the note beside it.
4. Suite count unchanged or up (if you added a case); never down. `black --check` clean on
   changed files.
5. Board R1 and R5 → done, with the one-line consequence: *every store built under 2026.1.1 is
   invalid.*

---

## 6. Stop conditions — stop and ask the user

- Case 1 does not reach 1e-5 after the fix, at every T₁ including 10 keV and T_CMB.
  Something other than R1's factor and R5's constants is wrong.
- You want to move `SAIKAWA_SHIRAI_T_LO`, change the grid start, or edit a fitting coefficient
  to close the join.
- You want to edit `ScalarModel.py`, `Xav_EOS_spline.py` or the jax class.
- You discover another consumer of `dG_s_dlogT` or `dG_rho_dlogT` in production code that
  assumed the log10 convention (the audit found only the RHS and the stored sample values; if
  there is another, say where).

---

## 7. The log and the board

`logs/02-fix-the-entropy-derivative.md`. Beyond the template: the choice in F1 and the
alternative; the before/after offsets to four decimals; the exact `git` incantation used for §2.
Board: R1 and R5 done; the consequence sentence in the R1 row and in the header paragraph.
