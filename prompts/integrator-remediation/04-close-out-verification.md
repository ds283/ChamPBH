# Prompt 04 — Close-out verification and handover

**Campaign:** [`README.md`](README.md) · **Board:** `IMPLEMENTATION_STATE.md`. Close it.
**Recommended model:** **Sonnet**. No production code and no test changes. The work is
re-measuring, running the nine histories, and writing an addendum a science run can act on.

**Read first:**

1. [`README.md`](README.md) §0, §6, §7.
2. `IMPLEMENTATION_STATE.md`, and the logs `logs/01-…`, `logs/02-…`, `logs/03-…`.
3. `.documents/review-remediation-verification.md` §4, including §4.7 (`run-integrity`), the
   handover this addendum amends.
4. `.documents/integrator-audit-2026-09-30/README.md` §9.3 and its `p_full.py`,
   `p_smallM_scan.py`, `p1_sweep.py`, `p3_grazing.py`, `p2_parked.py`.
5. `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`.

---

## 1. Re-measure

On the final tree, record the commit:

- **All three suites.** Counts, wall-clocks and results, against 18, 41 and 17 at `2b89022`.
- **Every README §6.1–§6.3 row.** The final-tree value and its witness: the test that prints it,
  or the grep, run by you. Where a value differs from the one its prompt's log quotes, say by how
  much. **A row at or better than target in the log but not now is a stop.**
- **The nine histories of §6.1 (d)**, twice: once with the audit's
  `p_full.py β M 1e-8 1e-4 kin reflect` (which drives the audit's probe loop, not the production
  code, so it is the reference), and once through the production loop with the scratch driver
  log 01 describes (outside the repository). RHS, steps, wall, reflections (0), substitutions
  (0), first bounce `N` and `T_J`, for each. The two must agree on the first bounce to `1e-5`
  in `N` and on RHS to 10 %; say where they do not.
- **Two physical-`M` histories through the production loop**, β = 0.9 and β = 2 at
  `M = 4.1e-28`, with the default budget: the first completes (audit §3.7: 140 reflections,
  1.9×10⁵ RHS); the second must end as a failure row's payload (`{"failure": True}`) with the
  budget message printed, not run past the budget. Record the wall time of the second; it is the
  cost of a clean failure.
- **The scope check.** `git diff --stat 2b89022..HEAD`. Every file must be one a prompt was
  allowed to touch: check against each prompt's §1 and §3, and the README's §0.4. Name any that
  is not.
- `grep -rn "VERSION_LABEL =" --include='*.py' .` outside `venv/`, `thirdparty/` and
  `claude-context/`: one line, `"2026.5.0"`.
- **The audit's probes against the shipped scheme are history**: `p1_sweep.py a`'s `regions`
  rows and `p3_grazing.py regions` drive the audit's own copy of the fragment loop in
  `harness.py`, not the production code, so they still run and still give the `b1f64d8`
  figures. Say so; do not re-run the ten-minute one.

## 2. Write

**A new section at the end of `.documents/review-remediation-verification.md` §4**, after §4.7,
headed with today's date and this campaign's name. It carries README §7's five points, each with
its evidence: commit, test, number. It supersedes, *by statement and not by edit*:

- every earlier statement that `VERSION_LABEL` is `"2026.4.0"`;
- §4.2's run-list expectations of cost per history, if any are stated there, with the new
  figures;
- any statement in §4.3 about hard reflections as a failure indicator.

**Nothing above it is changed** (CLAUDE.md rule 6).

**`prompts/INDEX.md`:** this campaign's row becomes **complete**, with the date, the final
commit's subject, and the open-issue count from the index.

**`IMPLEMENTATION_STATE.md`:** status `COMPLETE`, the final commit, the suite counts; §3 lists
what remains open and unassigned.

## 3. Tests

None added or changed.

## 4. Acceptance

1. Every §1 measurement in the log with its witness.
2. The addendum, the index row and the board.
3. `git diff --stat HEAD~1 HEAD` touches only `.documents/review-remediation-verification.md`,
   `prompts/INDEX.md`, this campaign's board and log, and `.documents/OPEN_ISSUES.md` if an
   observation opens an issue.

## 5. Stop conditions — stop and ask the user

- Any §6 row misses its target on the final tree.
- Any of the nine histories does not complete through the production loop, or its first bounce
  disagrees with the probe's by more than `1e-5` in `N`.
- The β = 2 physical-`M` history does not stop on the budget.

## 6. The log and the board

`logs/04-close-out-verification.md` in the README §5.1 template; **Result** is `COMPLETE` only
if every row is at or better than target.
