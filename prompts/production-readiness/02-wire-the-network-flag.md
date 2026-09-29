# Prompt 02 — Wire the network flag; run the full network; bump the version

**Campaign:** [`README.md`](README.md) · **Board item:** **P2** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and P2.
**Closes:** `[03-small-network-flag-is-never-read-by-prymordial]` on the `review-remediation`
board. **Recommended model:** **Opus**. The diff is a handful of lines. The judgement is in showing
three things:

- the flag now reaches PRyMordial;
- the network it selects is the one the stored label names;
- nothing else moved.

**Read first:**

1. [`README.md`](README.md) §0.2, §0.4, §2 (b), §5, §6.2.
2. `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3, the entry for the issue above, with
   its measurements.
3. `PRyM/PRyM_init.py:100–115` and `PRyM/PRyM_main.py:546, 602, 849, 887, 973–995, 1152–1172`.
   Confirm for yourself that `smallnet_flag` is read at run time, inside the solver's functions,
   and that nothing reads `small_network_flag`.
4. `ComputeTargets/BBNData.py`:
   - `:239–267`, `_configure_PRyMordial`;
   - `:270–290`, `compute_SM_baseline`;
   - `:293–300`, the `compute_BBN_data` signature;
   - `:690–710`, how `BBNData` passes `small_network` from its payload.
5. `main.py:76–84` (the version label and its comment) and `:747–750` (the payload).
6. `plot_by_beta.py:74` and `:860–875`; `tools/bbn_baseline.py:40–70`.
7. `ComputeTargets/tests/prym_fixtures.py:165–220` (`run_prym`),
   `ComputeTargets/tests/test_bbn_callbacks.py:170–185` and `:495–510`.
8. `extract_common.py:121–166`, `add_BBN_info_labels`.

---

## 1. The changes

**N1 — the flag reaches PRyMordial.** `_configure_PRyMordial(small_network)` sets
`PRyMini.smallnet_flag = small_network`.

- Stop setting `small_network_flag`.
- Correct the comment above it. The small network is faster and unreliable for ⁷Li.

**N2 — production runs the full network** (README §0.2). `small_network=False` in all of these:

- `main.py`'s BBN payload;
- the `compute_BBN_data` default;
- `plot_by_beta.py`'s `compute_SM_baseline` call, and the comment above it that says what
  `main.py` passes;
- `tools/bbn_baseline.py`'s default and its help text.

**Keep the switch.** It now works, and a later decision may want it.

**N3 — the fixtures follow.**

- `run_prym` sets and restores `smallnet_flag`, not `small_network_flag`, and its default becomes
  `False`.
  - It may call `_configure_PRyMordial` and then override `NP_thermo_flag`, so that the two cannot
    drift apart again. That is an IMPLEMENTATION CHOICE; record it either way.
  - Correct its docstring, which describes the old defect.
- `test_bbn_callbacks.py`'s saved-flag list and its `assertTrue(baseline["small_network"])`
  follow.
- **No pinned abundance is re-taken.** They were all measured on the full network (README
  §2 (b)), so they must pass unchanged. That they do is the check that nothing else moved.

**N4 — the label display.** In `add_BBN_info_labels`, `small_network is "True"` and
`is "Multiple"` compare string identity. Make them `==`. Change nothing else in that function.

**N5 — the version bump** (README §0.2).

- `VERSION_LABEL` goes from `"2026.2.0"` to `"2026.3.0"` in `main.py` and `plot_by_beta.py`.
- Add a dated sentence to `main.py`'s comment. It says that from 2026.3.0 the stored
  `small_network` describes the network that ran, and that production uses the full one. Keep the
  2026-09-29 sentence; this one is added beneath it. Prompt 03 will add its own reason under the
  same label.
- `plot_by_beta.py`'s label needs no comment beyond what it has.

**N6 — documents, additively.** The following describe the old behaviour. Add a dated note
beside each; do not rewrite them (CLAUDE.md rule 6):

- `.documents/numerical-strategies.md:448–454`, "By default the small reaction network is used
  (`small_network_flag = True`)";
- `.documents/architecture-summary.md:754`.

`.documents/numerical-methods-for-paper.md:235` also records the defect. Add a dated line there
saying it is fixed and which network production uses.

---

## 2. Tests — `ComputeTargets/tests/test_network_flag.py`

**Docstring: one test runs PRyMordial twice, about 15 s.** Save and restore every PRyMordial
module global you touch.

- **(a) The flag arrives. No solve.** After `_configure_PRyMordial(True)`,
  `PRyM.PRyM_init.smallnet_flag is True`; after `_configure_PRyMordial(False)`, it is `False`.
  **This must fail on `HEAD~1`.** Show it in the log.
- **(b) The flag selects the network.** Run the constant 0.08 family (the fixture's) with
  `run_prym(..., small_network=True)` and with `small_network=False`. Then:
  - ⁷Li/H differs by **≥ 5e-3 relative**; the board measured about 1 %;
  - D/H differs by **≤ 1e-3 relative** (board 1.5e-4);
  - Yp differs by **≤ 1e-5 relative** (board 1.5e-6);
  - the `False` run matches the fixture's pinned Yp and D/H to the fixture's own tolerance.

  Quote all four numbers in the log.
- **(c) Production's defaults are the full network.** Assert the `compute_BBN_data` default, and
  `compute_SM_baseline`'s result when called as `plot_by_beta.py` calls it, say
  `small_network = False`. Use `inspect.signature` for the default. Do not run a solve for this.

`ComputeTargets/tests` rises by the number of methods you add; `CosmologyModels/tests` is
unchanged.

---

## 3. What this prompt does not do

- No patch to `PRyM/`, and `PRyM_version` unchanged (README §0.4).
- No change to `BBNData.build()`'s lookup key; it does not filter on `small_network`. Note it
  under "Observations" and tie it to `[00-datastore-lookups-ignore-the-version-column]`. Open
  nothing new for it unless you find something that issue does not cover.
- No change to PRyMordial's tolerances, the callbacks, the spline domain or the sampling.
- No other edit to `extract_common.py`. `add_ScalarModel_labels` is prompt 01's.

## 4. Acceptance

1. README §6.2, every row, with measured values in the log.
2. `grep -rn "small_network_flag" --include='*.py' ComputeTargets/ tools/ main.py plot_by_beta.py`
   is empty, or finds only a comment that names the old attribute as history.
3. Both suites pass with every pin unchanged. `black --check` is clean on the changed files.
4. `grep -n VERSION_LABEL main.py plot_by_beta.py` shows `"2026.3.0"` in both.
5. The board and the index:
   - P2 done;
   - the issue closed: its row deleted from `.documents/OPEN_ISSUES.md`, and a dated **Resolved**
     line added to its `review-remediation` board entry (README §5 rule 4);
   - count and date corrected.

## 5. Stop conditions — stop and ask the user

- **A pinned abundance fails with the full network.** Then some pinned solve did not run the full
  network, or something else moved. Report which pin, and by how much.
- **Test (b) finds ⁷Li/H unmoved.** Then `smallnet_flag` is not effective when set at run time, and
  the fix needs a different mechanism. Report what you found in `PRyM_main.py`.
- The fix appears to need a patch to `PRyM/`.

## 6. The log and the board

`logs/02-wire-the-network-flag.md`, in the README §5.1 template. Beyond the template:

- the four test (b) numbers and the two wall-clocks;
- how test (a) was shown to fail on `HEAD~1`;
- `VERSION_LABEL` before and after, and the new comment verbatim;
- the consequence: **every store made before 2026.3.0 is invalid**. Write it on the board's
  header and on P2's row.
