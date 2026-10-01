# Prompt 09 — Close-out verification and handover

**Campaign:** [`README.md`](README.md) · **Board:** `IMPLEMENTATION_STATE.md`. Close it.
**Recommended model:** **Sonnet.** No production code and no test changes. The work is
re-measuring, running the roster, and writing an addendum the science run can act on.

**Read first:**

1. [`README.md`](README.md) §0, §6, §7.
2. `IMPLEMENTATION_STATE.md`, and logs 01–08.
3. `.documents/review-remediation-verification.md` §4, including §4.8–§4.9, the handover this
   addendum amends.
4. `source/campaign_reevaluation_2026-10-01.md` §1–§2: the figures to set the roster against.
5. `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`.

---

## 1. Re-measure

On the final tree, record the commit:

- **All three suites.** Counts, wall-clocks and results, against 18, 67 and 17 at `6aaa706`.
- **Every README §6.2–§6.8 row.** The final-tree value and its witness, run by you. Where a value
  differs from its prompt's log, say by how much. **A row at or better than target in the log but
  not now is a stop.**
- **The roster of §6.9** through `tools/history_and_bbn.py`, one history at a time, unloaded. For
  each, record:
  - RHS and accepted steps;
  - the first bounce (`N`, `T_J`, reflected);
  - the history's wall time, and the BBN wall time;
  - Yp and D/H, and the shifts against the SM baseline;
  - the averaged and point `ratio` windows.

  Set them against the source's §2 tables where they overlap (same β, `M`), saying which network
  each used.
- **The scope check.** `git diff --stat 6aaa706..HEAD`: every file is one some prompt was allowed
  to touch. Name any that is not.
- `grep -rn "VERSION_LABEL =" --include='*.py' .` outside `venv/`, `thirdparty/`: one line,
  `"2026.6.0"`. `grep -n "PRYM_VERSION =" ComputeTargets/BBNData.py`.

## 2. Write

**`.documents/review-remediation-verification.md` §4.10**, after §4.9, headed with today's date and
this campaign's name. It carries README §7's six points, each with its evidence (commit, test or
driver line, number). It supersedes, *by statement and not by edit*:

- every earlier statement that `VERSION_LABEL` is `"2026.5.0"`;
- §4.2's run list, where it implies a datastore made before 2026.6.0 can be reused;
- any statement that BBN uses `NP_thermo_flag` or a pressure callback.

**Nothing above it is changed.**

## 3. Close the board

- The status line gives COMPLETE and the count landed, with the final suites.
- Every §3 issue this campaign opened is either in §4 or stays open with a dated line.
- `.documents/OPEN_ISSUES.md`: the counts and the date. `prompts/INDEX.md`: this campaign's row is
  marked complete.

## 4. Stop conditions — stop and ask the user

- Any roster history fails, in integration or in BBN.
- A §6 row regressed since its prompt's log.
- The scope check names a file no prompt was allowed to touch.

## 5. The log and the board

`logs/09-close-out-verification.md`, in the README §5.1 template.
