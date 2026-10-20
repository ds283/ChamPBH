# Prompt 03 — the sampling axis

**Campaign:** [`README.md`](README.md) · **Board items:** **Z1**–**Z7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R4, the Jordan-frame redshift; R12,
ln(1 + z) in place of z; P8). Its orchestrator note, written after 02's review, governs where it
differs from this file.
**Gate:**
- 02 landed and reviewed, the gate "Rename" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` and
`[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` (`dk-and-rpk-migration` board);
`[05-the-value-factory-compares-stored-phi-against-pi]` (`science-readiness` board).
**Opens:** only what the work finds.
**Recommended model:** **Opus**. A change to the store's shared axis that every value table keys on,
with a new derived column whose identity must hold exactly.

**Read first:**

1. [`README.md`](README.md): §0.3 item 4 (the overflow, and what reads z), §2.2 (03's charter), §5,
   §6.0 R4 and R12, §6.2 P3 and P8.
2. `CLAUDE.md`'s "Redshift" bullet.
3. `CosmologyConcepts/redshift.py` (`redshift`, `redshift_array` and its `truncate` modes);
   `ObjectFactories/redshift.py` (`build`, `read_table`); `config/defaults.py`
   (`DEFAULT_REDSHIFT_PRECISION`, `DEFAULT_REDSHIFT_RELATIVE_PRECISION`, `DEFAULT_FLOAT_PRECISION`).
4. `ComputeTargets/ScalarHistory.py`: the sampling after integration (`final_N`, `largest_z`, the
   grid's truncation and the error when the grid is too short, `N_backward`, the sample loop), and
   `HubblePolicy` (`H_Jordan` can be negative, `:386` before 02).
5. `main.py`'s grid (`num_z_samples`, `convert_to_redshift`, the options
   `--samples-per-log10-z`, `--log10-one-plus-z-high`, `--log10-one-plus-z-low`).
6. The readers of z: `extract_common.py` (`:327`, `:342`, `:349`, `:407` before 02),
   `plot_ScalarHistory.py`, `tools/bbn_from_store.py` (its SQL), `tools/inventory_report.py`,
   `ComputeTargets/AdiabaticHistory.py`, `ComputeTargets/BBNData.py` (both key value rows on the
   redshift).
7. `ObjectFactories/ScalarHistory.py`'s value factory (`sqla_ScalarHistoryValue_factory`): its
   columns and its `build`, where the two φ-against-π issues are.
8. The three issues' entries on their boards.

---

## 1. What is wanted

The stored label of a sample is ln(1 + z_E), the Einstein-frame redshift counted back from the end
of the history, so nothing overflows however long a history runs. Every sample also carries
ln(1 + z_J), the Jordan-frame redshift. The grid's density is today's.

### 1.1 The items

- **Z1 — the concept.** `redshift` holds `log_one_plus_z` (a float, ≥ 0). `redshift.z` remains as a
  derived property, `expm1(log_one_plus_z)`, returning `inf` (not raising) past float range;
  ordering, equality, hashing and `redshift_array`'s sorting and truncation work on
  `log_one_plus_z`. `__float__` returns `log_one_plus_z`; say in the docstring that this changed,
  and find every caller of `float(z)` before you change it.
- **Z2 — the factory.** The table `redshift` stores `log_one_plus_z` (`Float(64)`, indexed) and no
  `z`. `build` matches by absolute difference, `|Δ| < 10⁻¹⁰` (P8; the grid spacing is
  ln 10/250 ≈ 9.2 × 10⁻³), in one branch, with no special case at zero. This closes
  `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`: a test that a lookup at 0 does not
  match a row at a small negative value, and that one at 10⁻¹² does. `read_table` orders by it. The
  spec's leaf follows; `FINGERPRINT_FORMAT` 4, its pin and golden file.
- **Z3 — the grid.** `log_one_plus_z = k Δ`, k = 0 … K, with Δ = ln 10 / S, S =
  `--samples-per-log10-z` (default 250, so the density is today's), and K = round(H S), H =
  `--log10-one-plus-z-high`. **H's default becomes 300** (P8; about 691 e-folds, against histories
  that need 80–200). `--log10-one-plus-z-low` is checked to be 0 or refused, since the axis is
  anchored at the end. Each point is computed as `k * DELTA` from integers, never accumulated, so
  that two runs build the same floats. `exponential.yaml`'s 85 is left as it is (08 decides the
  YAML); say in the log what an 85 now means.
- **Z4 — the sampling.** In `compute_scalar_history`, N_backward is the axis value itself; no `exp`
  or `log` of a label remains. The grid is truncated at `final_N` on the axis, and the "grid too
  short" error compares axis values. Write down in the log every expression you removed.
- **Z5 — ln(1 + z_J) on every sample (R4).** `SampleValues` gains `log_one_plus_z_Jordan`, computed
  at sampling as ln(1 + z_E) + ln A(φ₀) − ln A(φ), with φ₀ the history's final φ (a_J = A a_E). The
  value table gains the column (nullable `False`). It is a column, never a key: H_J < 0 during a
  bounce, so a_J is not monotonic and z_J is not a coordinate. Its docstring says both, and that
  "from the end" means T_J = T_CMB for every stored history once 07 lands (today a history may stop
  at another T_stop).
- **Z6 — the readers.** `extract_common.py`'s z axis offers ln(1 + z) and keeps 1 + z computed from
  it; `plot_ScalarHistory.py`'s frame carries both columns; `tools/bbn_from_store.py`'s SQL orders
  by `log_one_plus_z`; `tools/inventory_report.py` prints it; `AdiabaticHistory` and `BBNData` are
  checked to use the redshift only as a key (say so in the log, with the lines).
- **Z7 — the value factory's issues.** In `sqla_ScalarHistoryValue_factory.build`, compare the
  stored π with the expected π; or, if no caller of that path exists after this prompt (`grep`),
  delete the path and say so. Either closes both `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`
  and `[05-the-value-factory-compares-stored-phi-against-pi]`. If the path stays, a test drives it
  with φ ≠ π.
- **The golden history** (P3) is regenerated in this commit, as `golden_history_03.json` beside 01's
  (not over it). The test reads the newest.
- **The prose this commit makes false:** `CLAUDE.md`'s "Redshift" bullet: the stored label is
  ln(1 + z_E) from the end; ln(1 + z_J) is on each sample; the Jordan-frame temperature is
  `log_T_Jordan`.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 02 handed on; the golden history passes.
2. **The change in values.** Run the golden histories on 02's tree and on yours (a `git worktree`
   in your scratch space, never in the repository) and record, per `SampleValues` field, the
   largest relative difference over matched samples, and whether the sample count changed. The
   expectation is the 10⁻¹⁵ level, from the grid points' last bits. **More than 10⁻¹² in any field
   is a stop.**
3. **The axis without overflow.** A test builds a grid to 400 decades (N ≈ 921), truncates it at
   N = 800, and reads `z` on every point (`inf` where it must be) without raising; and
   `compute_scalar_history`'s truncation logic is exercised on that grid with a synthetic `final_N`
   (no integration to 800 e-folds is needed, nor possible here).
4. **ln(1 + z_J):**
   - equals ln(1 + z_E) + ln A(φ₀) − ln A(φ) at every sample of the golden histories, exactly as
     computed;
   - agrees with ln(1 + `cosmology.z`(T_J)) − ln(1 + `cosmology.z`(T_J,end)) along the golden
     histories away from bounces, to the integration's accuracy (give the largest difference and
     where it occurs). This also checks the T_J equation (the brief's R7).
5. **After:** the five suites; `black --check`.
6. **Deliberate breakages**, each failing a named test:
   - the grid accumulated (`x += DELTA`) instead of `k * DELTA`: a determinism or lookup test
     fails;
   - the lookup's tolerance set to 10⁻²: two grid points collide;
   - ln A(φ₀) dropped from Z5: the identity test fails.

## 3. Acceptance

1. Z1–Z7 as §1.1 says, the golden history regenerated, `CLAUDE.md`'s bullet.
2. §2's checks in the log, the table of item 2 in full.
3. **One commit**, with the log `logs/03-the-sampling-axis.md`. The board: row 03, Z1–Z7, the gate
   "Axis", the three issues closed on their boards (§4) with their rows deleted from
   `.documents/OPEN_ISSUES.md` §1.15 and the count corrected, `### 5.4 After prompt 03 (<date>)`.

## 4. Stop conditions: stop and ask

- §2 item 2 exceeds 10⁻¹² in any field, or the sample count changes.
- A reader uses z numerically in a way that ln(1 + z) cannot serve.
- ln(1 + z_J) disagrees with `cosmology.z` by more than the integration's tolerance away from
  bounces: that would question the T_J equation, which is outside this prompt.

## 5. What this prompt does not do

- It does not change the model, the units, the tuning or the history's key.
- **Files it changes:** `CosmologyConcepts/redshift.py`, `ObjectFactories/redshift.py`,
  `config/defaults.py` (the tolerance), `config/fingerprint.py`, `config/argument_parser.py` (the
  default and the refusal), `main.py` (the grid), `ComputeTargets/ScalarHistory.py` (Z4, Z5),
  `ObjectFactories/ScalarHistory.py` (the value table, Z7), the readers of Z6, the tests, the
  golden data, `CLAUDE.md`, the boards and the index, the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
