# Prompt 05 — pin the kicking function, reconcile the EOS classes, write the paper-facing note

**Campaign:** [`README.md`](README.md) · **Board item:** **R4** (the pins) ·
**Board:** `IMPLEMENTATION_STATE.md` — update your row and R4.
**Closes:** the pinning half of R4. **Recommended model:** **Opus**. Mostly tests and a document;
the judgement is in describing what the code does without describing what the paper wishes it did.

**Read first:**

1. [`README.md`](README.md) §0, §2 (c), §5, §6.4.
2. `.documents/audit-2026-09-29/README.md` §4 — the peaks and the table's provenance gap.
3. `CosmologyModels/GenericEOS/Xav_EOS_spline.py` (the production `w`), `Xav_EOS_data.csv`
   (189 rows; first and last), `SaikawaShirai_EOS_spline.py:176–200` and
   `SaikawaShirai_EOS_jax_autodiff.py` `w()` (the 2 MeV freeze — **dead in production**),
   `GenericEOS.py:77–100` (the base formula), `SaikawaShirai_common.py:97–125` (the low-T
   constants and the comment about neutrino decoupling).
4. `CosmologyModels/tests/eos_reference.py` and `test_temperature_law.py` (prompts 01, 02).
5. `logs/04-…md` — the table–g figure prompt 04 recorded.
6. The paper's numerical section, `Paper1.tex` lines ≈ 3005–3135 in
   `/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/` (read-only; **you do not edit
   it**), so the note answers what the paper currently says.

---

## 1. Tests — `CosmologyModels/tests/test_kicking_function.py`

Through `Xav_EOS_spline.w` (never the CSV directly), Σ = 1 − 3w on ≥ 200 points per decade over
[10 keV, 30 TeV]:

1. **The three peaks** at README §6.4's values and tolerances, found by argmax in the three
   windows [10 keV, 5 MeV], [50 MeV, 1 GeV], [20 GeV, 1 TeV].
2. **The e⁺e⁻ profile**: Σ(2 MeV) = 0.0030, Σ(0.5 MeV) = 0.0345, Σ(0.2 MeV) = 0.0946,
   Σ(0.1 MeV) = 0.0680, Σ(50 keV) = 0.0029, all ± 1e-3; Σ(20 keV) < 1e-6.
3. **∫Σ d ln T over [10 keV, 3 MeV] = 0.1617 ± 2e-3.**
4. **Outside the table** `w` is exactly 1/3 (below 10 keV, above 25.1 TeV).
5. **The table is consistent with the g's** — tighten prompt 01's case 5: the corrected law from
   5 MeV to 10 keV gives ρ_R/thermodynamic = 1.005 ± 5e-3; from 100 MeV, 1.003 ± 5e-3; from
   2×10⁴ GeV, 1.003 ± 5e-3 (README §2 (c)).
6. **The base classes' freeze is not the production path**: assert `QCD_Cosmology`'s EOS is an
   `Xav_EOS_spline`, and that `SaikawaShirai_EOS_spline.w(1 MeV) == w(2 MeV)` (the freeze) while
   `Xav_EOS_spline.w(1 MeV) ≠ w(2 MeV)`. This pins the fact the paper currently gets wrong.
7. **The two derivative implementations agree** (spline vs jax, `skipUnless`) to 1e-6 on the
   60-point grid — this is prompt 02's case 4; if it already lives there, reference it rather
   than duplicate.

---

## 2. Hygiene, minimal

- Docstrings on `SaikawaShirai_EOS_spline.w` and the jax `w`: *"Not the production equation of
  state: `QCD_Cosmology` uses `Xav_EOS_spline`, which overrides this method with a tabulated
  w(T) that includes the e⁺e⁻ kick. This implementation freezes its argument at 2 MeV and has no
  e⁺e⁻ kick."* Do not delete the methods.
- A short module docstring on `Xav_EOS_spline.py` recording what the CSV establishes about
  itself: rows, range, that w = 1/3 exactly at both ends, the peaks, and that its construction
  is not in the repository (issue `[00-kicking-function-table-has-no-provenance-in-the-repository]`).
  **Do not guess how it was built.**

---

## 3. The paper-facing note — `.documents/numerical-methods-for-paper.md`

A document the authors rewrite `Paper1.tex`'s numerical section from. Sections, each with the
numbers and their provenance (test or script, commit):

1. **The temperature law** as implemented: d ln T_J/dN = −(1 + A′φ′)/(1 + ⅓ d ln g_s/d ln T_J),
   g_s from Saikawa–Shirai splined at 250 points per decade of log10 T, clamps at 10⁻⁵ and
   10¹⁶ GeV; the ln 10 error that affected every run from 2026-01-19 to prompt 02's commit, in
   one paragraph, with the consequences (audit §1).
2. **The kicking function**: from a table (not the 2 MeV freeze), w = 1/3 outside
   [10 keV, 25 TeV], the three peaks, the e⁺e⁻ profile, the table–g consistency at 0.3 %, and the
   provenance gap stated plainly.
3. **Sampling**: 250 samples per decade of Einstein-frame 1+z; what that is per decade of T_J
   in the BBN window; the Einstein/Jordan redshift distinction (README §2 (h)).
4. **The PRyMordial interface** after prompts 03–04: the definitions of ρ_NP, p_NP; the
   conservation argument (README §2 (g)); the ratio representation and its measured accuracy;
   the passenger patch and the version string; the baseline.
5. **What is not yet fixed** that the paper's text touches: the adiabaticity response term (H5),
   the initial condition bound (H8), the reflection counts — pointing at the board.

Additive to `.documents/`; does not modify the audit.

---

## 4. What this prompt does not do

- No change to the CSV, the fitting coefficients, the clamps, `w()`'s logic, or the freeze.
- No edit to `Paper1.tex`.
- No re-run of any history.

## 5. Acceptance

1. README §6.4, every row, measured, in the log.
2. `CosmologyModels/tests` up by exactly the cases added (7, or 6 if case 7 is a reference to
   prompt 02's), no other suite down.
3. The two docstrings and the module docstring in place; `black --check` clean on changed files.
4. `.documents/numerical-methods-for-paper.md` exists, every number carrying provenance.
5. Board R4: pins done; verification pending (06).

## 6. Stop conditions — stop and ask the user

- A peak is outside its tolerance: the table differs from what the audit measured on `f5896bb`,
  which would mean the CSV or the class changed.
- Case 5 misses its ± 5e-3 tolerances after prompt 02: then the table and g's disagree more than budgeted
  and the paper's Σ needs a decision from the authors.
- You find the paper's numbers (0.03 for the e⁺e⁻ peak, 0.04 for EW) come from a version of the
  table that is not the one in the repository. Report; do not reconcile.

## 7. The log and the board

`logs/05-kicking-function-and-eos-hygiene.md`. Board: R4 pins done.
