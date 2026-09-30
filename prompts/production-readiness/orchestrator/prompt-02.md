# Orchestrator — prompt 02, wire the network flag and bump the version

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2, §2 (b), §6.2 first. **You do
not write code.**

**The prompt:** [`../02-wire-the-network-flag.md`](../02-wire-the-network-flag.md)
**Board item:** P2 · **Closes:** `[03-small-network-flag-is-never-read-by-prymordial]`

## 0. What makes this prompt unusual

The diff is small, but it carries the campaign's one version bump, and it is the first prompt to
change physical output. The review is four things:

- the flag arrives, and ⁷Li moves when it is set;
- every pinned abundance holds, which shows nothing else moved;
- `PRyM/` is untouched;
- the label is `"2026.3.0"`.

## 1. Before you dispatch

1. Prompt 01 landed and reviewed; branch `production-readiness`; record `HEAD`; `git status`
   clean.
2. Baselines: both suite counts, and the `ComputeTargets/tests` wall-clock.
3. Record these three greps for comparison:
   ```bash
   grep -rn "small_network_flag\|smallnet_flag" --include='*.py' ComputeTargets/ tools/ main.py plot_by_beta.py
   grep -n "small_network" main.py plot_by_beta.py tools/bbn_baseline.py
   grep -n VERSION_LABEL main.py plot_by_beta.py    # "2026.2.0" in both
   ```
4. `venv/bin/black --check ComputeTargets/BBNData.py main.py plot_by_beta.py tools/bbn_baseline.py extract_common.py ComputeTargets/tests/prym_fixtures.py ComputeTargets/tests/test_bbn_callbacks.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`ComputeTargets/BBNData.py`, `main.py` and `plot_by_beta.py` (the `small_network` values and the
version label and its comment), `tools/bbn_baseline.py`, `extract_common.py`
(`add_BBN_info_labels`' two comparisons only), `ComputeTargets/tests/`, three documents under
`.documents/` (additive notes only), the log, this campaign's board, the `review-remediation`
board (one Resolved line), and `.documents/OPEN_ISSUES.md`. Not `PRyM/`, not `thirdparty/`. Do
not re-pin any abundance."*

## 3. The review — nine checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`. `PRyM/` is not in it.
2. **The flag.** `_configure_PRyMordial` sets `PRyMini.smallnet_flag`. §1 item 3's first grep
   finds no `small_network_flag` assignment, except possibly a comment.
3. **The breakage, run by you.** Use the generic procedure in `README.md` with
   `F = ComputeTargets/BBNData.py` and test (a) alone.
   - On `HEAD~1`, `smallnet_flag` stays `False` after `_configure_PRyMordial(True)`, so the
     assertion fails.
   - Restore, and confirm it passes.
4. **The network moves.** Run test (b) yourself, about 15 s. Quote the ⁷Li/H, D/H and Yp
   differences. They must be ≥ 5e-3, ≤ 1e-3 and ≤ 1e-5 relative.
   *Amended 2026-09-30 after the run (the user; README header): only the ⁷Li/H bound stands. The
   D/H and Yp shifts are quoted, not checked.*
5. **No pin was re-taken.** `git diff HEAD~1 HEAD -- ComputeTargets/tests/` changes no numeric
   pin: not `RES_*` values, not Yp 0.2540937879, D/H 2.671500711, nor the baseline figures. Then
   the whole `ComputeTargets` suite passes.
6. **Production is the full network.** Rerun §1 item 3's second grep. `main.py`'s payload,
   `plot_by_beta.py`'s baseline and `tools/bbn_baseline.py`'s default are all `False`. Read the
   `compute_BBN_data` signature.
7. **The label.** `"2026.3.0"` in both files. `main.py`'s comment keeps the 2026-09-29 sentence
   and adds a dated one beneath it.
8. **Documents are additive.** Each of the three files shows only added lines:
   ```bash
   git diff HEAD~1 HEAD -- .documents/ | grep '^-' | grep -v '^---'
   ```
   The only removals allowed are in `OPEN_ISSUES.md`: the closed issue's row, and the count and
   date lines.
9. **Housekeeping.**
   - Suite counts up; `CosmologyModels/tests` unchanged; `black --check` clean.
   - The board: P2 done, and the header says every store before 2026.3.0 is invalid.
   - The index row gone, count and date corrected.
   - Exactly one Resolved line on the `review-remediation` board.

## 4. Stop and ask the user

- Any of the prompt's §5.
- Check 4 finds ⁷Li/H unmoved, or check 5 finds a re-pinned value.
- The label is anything but `"2026.3.0"`, or was bumped in only one file.

## 5. After it lands

Report:

- the commit;
- the breakage record;
- the four test (b) numbers and the two wall-clocks;
- the label before and after, with the new comment;
- the consequence sentence;
- the suite counts.

Then stop.
