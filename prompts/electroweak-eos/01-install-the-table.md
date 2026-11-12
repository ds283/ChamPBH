# Prompt 01 — install the table

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-12, after the plan (`600d73b`) and the user's rulings (`b3d5906`). It is written from:
- README §6.1 as ruled: "U1 to U6 are fine as proposed; U7 doesn't need to be in scope here." The table is
  U2 (b), installed in place (U3), with g_ρ and g_s unchanged (U4). The rows' provenance is out of scope
  (U7);
- README §6.2 P1–P5, P10 and P11 as amended;
- the kicking-function audit's addendum (`ec6bca8`): `.documents/peakewsurf-kicking-function/report.tex`,
  the section "Addendum (12 November 2026)", its `data/` and `scripts/laine_meyer_compare.py`.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `electroweak-eos`, at or after the commit that adds this prompt; `ec6bca8` is an ancestor of
  `HEAD`;
- `git status`: no tracked change other than those the orchestrator note lists as the user's;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `CosmologyModels/GenericEOS/Xav_EOS_data.csv` is byte-identical to `git show ec6bca8:` of the same path
  (189 rows, 1e-5 to 25118.86 GeV);
- `.documents/electroweak-eos-verification.md` does not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus**. The change to production is one CSV, but its consequences run through
every characterisation test and golden history that reads w. Each moved pin must be told apart from an
accidental change and justified.

**Read first:**
1. [`README.md`](README.md): §0 whole, §1, §2.1 (the Table gate), §5, §6.1 as ruled, §6.2 P1–P5, P10, P11.
2. The audit's addendum, the section named above: what the merged table is, why the rows of 158.5, 251.2 and
   316.2 GeV are left out, and the spline's behaviour at 100 GeV and at the top row.
   `data/laine_meyer_stats.json` holds every number it quotes.
3. `CosmologyModels/GenericEOS/Xav_EOS_spline.py`: the docstring (:16–:46), `__init__` (the CSV's path, the
   spline, `_T_max`, `_T_min`), `w` and `dw_dlogT` (1/3 and 0 at and beyond the ends).
4. `config/version.py`: the history comments above `VERSION_LABEL`, for the form of a dated entry.
   `config/fingerprint.py`: what moves `FINGERPRINT_FORMAT` (a change to an `inventory_spec()`).
5. The characterisation tests that read the table: `CosmologyModels/tests/test_kicking_function.py` (its
   docstring and the pinned constants, :37–:125), `test_eos_w_derivative.py`, `test_temperature_law.py` and
   `eos_reference.py`.
6. The golden histories: `ComputeTargets/tests/golden_history.py` (its docstring: how a record is
   regenerated beside the old one, never over it), `golden_tuned_history.py`,
   `golden_reflecting_history.py`, and the tests that read their paths (`test_golden_history.py`,
   `test_golden_reflecting_history.py`, and whatever reads `GOLDEN_TUNED_PATH`).
7. `CodeComparisonTools/XavEOS_test.py`: its docstring only.

---

## 0. Where this prompt sits

The campaign moves production off a w table that is a vacuum-mass free gas above about 100 GeV and onto one
that follows Laine & Meyer through the electroweak crossover (README §0.1). This prompt installs the table and
re-pins what it moves. 02 then measures what it does to the physics, the electroweak-capture threshold and a
sample of the `2026.10.0` run's histories, and 03 writes the documents and the handover.

Nothing in this prompt changes code that computes, except the CSV the production class reads.

## 1. What is wanted

### 1.1 The inputs

- The audit's `data/notebook_w_table.csv` (the notebook's 132 rows) and
  `data/notebook_w_points_2026-11-12.csv` (the 29 points, 100–380 GeV). They are read where they are and are
  not modified (README §5).
- Today's table, copied byte for byte to `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv` before
  anything overwrites it (P1). Show the copy's SHA-256 equal to that of `git show ec6bca8:CosmologyModels/GenericEOS/Xav_EOS_data.csv`.

### 1.2 The build script (P1)

`tools/build_w_table.py`, run from the repository root, composes the table by U2 (b) and writes
`CosmologyModels/GenericEOS/Xav_EOS_data.csv`. Let T₀ be the notebook's second row, 5.011872336272725 × 10⁻⁵ GeV
(50.1 keV). The rows are:

| from | rows | expected count |
|---|---|---|
| today's table | T < T₀ (the e⁺e⁻ tail, §0.2 of the README) | 14 |
| the notebook's table | T₀ ≤ T < 100 GeV | 126 |
| the points of 2026-11-12 | 100 ≤ T ≤ 380 GeV | 29 |
| the notebook's table | T > 380 GeV (the w = 1/3 row at 386.7 GeV) | 1 |

That is 170 rows, from 10⁻⁵ to 386.7085 GeV. The script asserts, and fails loudly otherwise:
- T₀ is a row of today's table too, with the same w to 10⁻¹² (so the join is a shared row);
- the points' 100 GeV row equals the notebook's 100 GeV row bitwise;
- T is strictly increasing;
- each notebook row it leaves out (158.5, 251.2 and 316.2 GeV) lies on a cubic spline in ln T through the
  points (`make_interp_spline`, as `Xav_EOS_spline` uses) to within 10⁻⁵ in Σ. The addendum measured
  3.7 × 10⁻⁶ and 10⁻¹¹.

The output has the header `T_GeV,w`, one row per line, each number written so that it reads back to the same
float: `repr(float(x))`. Under NumPy 2 the `repr` of a `numpy.float64` is `np.float64(…)`, which pandas reads
as a string, and `Xav_EOS_spline` then fails in `__init__`. The script has a `--check` mode that builds the table in memory and compares it with the
installed CSV byte for byte, exiting 0 or 1, and writes nothing.

### 1.3 Its tests

`tools/tests/test_build_w_table.py`:
- regenerating the table gives the installed CSV byte for byte;
- the composition: each source's rows and their count as in §1.2, and the boundaries (the first notebook
  row is T₀; the last of today's rows is below it);
- each assertion of §1.2 fires on input made to break it (a perturbed shared row, a perturbed 100 GeV point,
  an out-of-order row, a left-out row moved off the spline);
- `Xav_EOS_spline` on the installed table gives Σ = 0 at and above 386.7085 GeV, and its electroweak peak
  near 54.0 GeV with Σ ≈ 0.03776, to the tolerances the test states with their reasons.

Each test's docstring states its runtime.

### 1.4 The class

`Xav_EOS_spline`'s code is not changed. Its docstring (P5) is rewritten for the new table, as this one was
for the old: the rows, range and spacing (0.05 dex up to 89.1 GeV, then 10 GeV), which source each range
comes from and why, the peaks the spline evaluates (measured), the build script and its `--check`, and the
audit's addendum. It says that the old table is in git at `ec6bca8` and in the campaign's `data/`. It says
nothing about how the notebook's rows were made: that is not recorded, and U7 put it out of scope. The
module's other comments are left alone.

`CodeComparisonTools/XavEOS_test.py`'s docstring gains a dated note: the comparison it makes is with the
table before this prompt (P5). Its code and data are not changed.

### 1.5 The labels (P2)

- `VERSION_LABEL` → `2026.11.0`, with a dated entry in `config/version.py`'s history in the form of the
  others:
  - what changed: the w table, above 50.1 keV the notebook's rows and the points of 2026-11-12, which follow
    Laine & Meyer through the electroweak crossover, with w = 1/3 from 386.7 GeV;
  - that every history whose temperature crosses the electroweak scale may change value;
  - that a `2026.10.0` store is not extended at `2026.11.0` and is read with its own tree.
- `FINGERPRINT_FORMAT` stays 7. No `inventory_spec()` changes. If `test_fingerprint_format_guard` or
  `test_store_fingerprint_golden` needs a change, stop and ask.

### 1.6 The characterisation tests (P3)

First, with the old table, run every test this prompt may re-pin and see it pass. Then install the table.
For each test that fails:
- decide that the failure is the table's, and say how you know (the quantity pinned is read through w, and
  the size and sign of the change match the table's difference where the quantity is evaluated);
- re-pin it to the value the new table gives, keeping the test's tolerance and form unless the old
  tolerance cannot hold the new value for a reason you state;
- record in the log the test, the constant, the old and new values, and the reason.

A pin that still passes is not touched. A failure that is not explained by the table is a stop. Where a
test's docstring describes the table (row counts, range, peaks), it is updated and names this campaign.

### 1.7 The golden histories (P4)

Every golden history starts at T* = 2 × 10⁴ GeV and crosses the electroweak scale, so each is expected to
move. Each is regenerated **beside** its old file, never over it, by its module's own regeneration, and the
module's path moves to the new file:

| module | old record (kept) | new record |
|---|---|---|
| `golden_history.py` (β = 1.5 and 5.5, M = 10⁻³ M_P) | `data/golden_history_rr03.json` | `data/golden_history_ee01.json` |
| `golden_tuned_history.py` (β = 3, M = 0.01 M_P, tuned) | `data/golden_history_rr03_tuned.json` | `data/golden_history_ee01_tuned.json` |
| `golden_reflecting_history.py` (β = 1.5, M = 10⁻¹² M_P, α = 0.05, tuned) | `data/golden_history_rr03_reflecting.json` | `data/golden_history_ee01_reflecting.json` |

Before the table is swapped, the three golden tests pass bitwise against the `rr03` records. After the
swap, a comparison script in `.documents/electroweak-eos/table/` compares each old record with its new one
field by field and writes its output to `out/`. The log and the verification document state, per history:
- the reflections, accepted steps and RHS evaluations, old and new;
- the first bounce (N, φ_E, ln T_J) and the fixed-temperature values, old and new;
- for the tuned records, the tuned normalization and φ_ref, old and new;
- the final sample's fields, old and new.

The modules' docstrings gain a sentence each, in the form of `golden_history.py`'s: regenerated by
`electroweak-eos` prompt 01 when the w table changed, and the `rr03` record stays the record of everything
before it. No field is added to or removed from any record.

A change in any field other than values (the record's keys, `solver_label`, the sampling axis's length) is a
stop.

### 1.8 What the table does to Σ (the Table gate)

On a grid of 1000 points per decade from 10⁻⁵ to 3 × 10⁴ GeV, evaluate Σ from `Xav_EOS_spline` on the old
table (`pipeline_w_table_ec6bca8.csv`) and the new one, and record the largest |ΔΣ| in each decade. Over
10⁻⁵ to T₀ and over T₀ to 22 GeV the rows are identical, so a difference there comes only from the spline's
global coupling. Record it. Do not bound it. Also record:
- the electroweak peak (T, Σ) on both tables;
- ∫Σ d ln T over 30–100, 100–300 and 300–25 000 GeV on both tables, beside the addendum's figures.

The script is `.documents/electroweak-eos/table/sigma_change.py`, and its output goes to `out/`.

### 1.9 The verification document (P10)

`.documents/electroweak-eos-verification.md` (new), §1: the inputs and their hashes, the build and its
`--check`, the composition, §1.8's measurements, the re-pinned tests (the table of §1.6), the goldens (§1.7),
and the labels.

## 2. What is not wanted

- **No change to code that computes.** Not `Xav_EOS_spline`'s methods, the Saikawa–Shirai classes (g_ρ and
  g_s stay, U4), the integrator, the drain or anything else. Only the CSV the class reads changes.
- **No other rows.** Below T₀ the table is today's, and from T₀ to 100 GeV it is the notebook's. Neither is
  adjusted, smoothed or extended. Laine & Meyer's data is not used as a source of rows.
- **Nothing about provenance** (U7): no request to the author, and no change to
  `[00-kicking-function-table-has-no-provenance-in-the-repository]`.
- **No change to the audit's files** (`.documents/peakewsurf-kicking-function/`) or to any old golden file.
- **No store** but temporary ones in tests. **No Ray.** No figure.
- The physics measurement (the threshold and the sample) is 02's, and the documents and handover are 03's.

## 3. Verification

- The suites, before you change anything and after, per `CLAUDE.md` (ComputeTargets in two halves, tools
  alone). Record both sets of counts. **No count may fall.** `tools/tests` gains this prompt's tests.
- The build's `--check` exits 0 on the committed tree.
- The goldens at their new paths, and `test_store_fingerprint_golden` and `test_fingerprint_format_guard`
  unchanged and passing.
- The Table gate (README §2.1), item by item.
- **The breakage record:** each new test shown to fail when what it guards is broken. That includes the
  build test failing when one byte of the installed CSV changes, and the golden tests failing on the old
  table once their paths point at the new records.
- `black` on the Python files you changed or added.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `CosmologyModels/GenericEOS/Xav_EOS_data.csv` and `Xav_EOS_spline.py` (docstring);
  `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`;
- `tools/build_w_table.py` and `tools/tests/test_build_w_table.py`;
- `config/version.py`;
- the re-pinned tests, each named in the log;
- the three new golden files, and the three golden modules (paths and docstrings);
- `CodeComparisonTools/XavEOS_test.py` (docstring);
- `.documents/electroweak-eos/table/` (scripts, and `out/` as text only), and
  `.documents/electroweak-eos-verification.md`;
- the log `logs/01-install-the-table.md` (template: `dk-and-rpk-migration` README §5.1), with every
  deviation classified, "Observations not acted on", and "State handed to the next prompt". The last
  includes what 02 needs: the commit to use as "new", `ec6bca8` as "old", and the golden's tuned values on
  both;
- the board: row 01, its items, the Table gate, and §3/§4 for any issue opened, with
  `.documents/OPEN_ISSUES.md` in the same commit;
- `CLAUDE.md`'s test counts, unless the orchestrator note says otherwise.

Stage by explicit path. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the
SHA goes; the orchestrator substitutes it.
