# Prompt 05 — the Planck mass

**Campaign:** [`README.md`](README.md) · **Board items:** **K1**–**K3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R3, PRyMordial's G_N; U5, "you just
mean the numerical factor that relates eV to M_P. Stick with your recommendation", so after 04;
P9). Its orchestrator note, written after 04's review, governs where it differs from this file.
**Gate:**
- 04 landed and reviewed, the gate "Model" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One constant in three places, its consistency tests, and a
measured shift.

**Read first:**

1. [`README.md`](README.md): §0.2 ("The units"), §2.2 (05's charter), §5, §6.0 R3, §6.1 U5,
   §6.2 P3, P9.
2. The brief's §2.4.
3. `Units/base.py`, `Units/Planck_units.py`, `Units/Mpc_units.py`, `Units/GeV_units.py`.
4. `PRyM/PRyM_init.py:136-137` (`GN`, `Mpl`): read only; `PRyM/` is not patched here.
5. The three tests that pin a string carrying 2.436e27: `tools/tests/test_inventory_report.py`,
   `tools/tests/test_compare_stores.py`, `ObjectFactories/tests/test_store_fingerprint_golden.py`
   (and its data file, if the string reaches the store's fingerprint).

---

## 1. What is wanted

The three unit systems set the electronvolt against the reduced Planck mass from PRyMordial's G_N,
so that the eV-based and the kelvin-, metre-, second- and kilogram-based inputs agree, as they do
not today (2.8 × 10⁻⁴).

### 1.1 The items

- **K1 — one constant.** In `Units/base.py`: `NEWTON_G_MEV_MINUS_2 = 6.70883e-45`, with a comment
  naming its source (`PRyM/PRyM_init.py`, PDG) and that it is the CODATA 2018 G the Planck length,
  time, mass and temperature below were derived from; and `REDUCED_PLANCK_MASS_EV`, computed from it
  as `1 / sqrt(8 π G_N)` in eV (≈ 2.4353236 × 10²⁷). Nothing imports `PRyM` for it: `PRyM_init`
  reads files from the working directory at import.
- **K2 — the three systems use it.** `Planck_units.eV = PlanckMass / REDUCED_PLANCK_MASS_EV`,
  `Mpc_units.eV` likewise, `GeV_units.PlanckMass = REDUCED_PLANCK_MASS_EV * eV`. Their Planck-length,
  mass, time and temperature literals stay; their comments say where each number comes from.
- **K3 — the tests**, in `CosmologyModels/tests/test_planck_mass.py` (P13):
  - `NEWTON_G_MEV_MINUS_2` equals the value in `PRyM/PRyM_init.py`, parsed from the file's text
    (`GN = 6.70883*1.e-39*1.e-6`), to the last digit;
  - in each of the three systems: 1 K equals k_B = 8.617333262 × 10⁻⁵ eV to 10⁻⁶; 1/eV equals
    ħc = 1.973269804 × 10⁻⁷ m and ħ = 6.582119569 × 10⁻¹⁶ s to 10⁻⁶; 1 kg equals
    5.609588604 × 10³⁵ eV to 10⁻⁶ (the probe at `ac187c8` measured 1.7e-7, 4e-8, 1.6e-7 and 2.1e-7
    with the new constant);
  - the three pinned strings follow the new value (an M of 10⁻³ M_P now prints as about
    `2.435e+24 eV`), changed in the tests and nowhere else.
- **The golden history** is regenerated as `golden_history_05.json` (P3).
- **The prose this commit makes false:** `CLAUDE.md`'s "Units" bullet gains one sentence: the
  electronvolt is set from PRyMordial's G_N, so ChamPBH's M_P and PRyMordial's `Mpl` are the same
  G_N (and the factor 8π between them still cancels). `.documents/architecture-summary.md` where it
  describes `Units/`.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 04 handed on; the golden history passes.
2. **The change in values.** As 03 did (a `git worktree` of 04's tree in your scratch space): per
   `SampleValues` field of the golden histories, the largest relative difference, and the sample
   count. Expected: about 2.8 × 10⁻⁴ where an eV-labelled input enters (T_init, the g*(T)
   thresholds, the Λ of the adapter), and the sample count may change by the grid's truncation.
   Record it as a measurement of an intended change. **A difference above 10⁻² in any field is a
   stop**, since nothing in the change can produce one.
3. **After:** the five suites; `black --check`; `grep -rn "2\.436e27\|2\.436e+2[47]"` outside the
   records and `science-runs/`: nothing (list what remains there by file).
4. **Deliberate breakages:** `GeV_units` left at 2.436e27: K3 fails for that system; the constant
   typed as 6.70884e-45: the PRyMordial comparison fails.

## 3. Acceptance

1. K1–K3 as §1.1 says; the golden history regenerated; `CLAUDE.md`'s sentence.
2. §2's checks in the log, with item 2's table in full.
3. **One commit**, with the log `logs/05-the-planck-mass.md`. The board: row 05, K1–K3, the gate
   "Units", `### 5.6 After prompt 05 (<date>)`.

## 4. Stop conditions: stop and ask

- A unit system cannot meet K3's 10⁻⁶ with the new constant (another of its literals is then
  inconsistent, which the brief did not find).
- §2 item 2 above 10⁻² anywhere.

## 5. What this prompt does not do

- It does not change `PRyM/`, the BBN interface, or `PRYM_VERSION`.
- **Files it changes:** `Units/base.py`, `Units/Planck_units.py`, `Units/Mpc_units.py`,
  `Units/GeV_units.py`; `CosmologyModels/tests/test_planck_mass.py` (new); the three tests of "Read
  first" item 5; the golden data; `CLAUDE.md`; `.documents/architecture-summary.md`; the board; the
  log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
