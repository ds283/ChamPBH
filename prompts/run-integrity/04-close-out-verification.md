# Prompt 04 — Close-out verification and handover

**Campaign:** [`README.md`](README.md) · **Board:** `IMPLEMENTATION_STATE.md`. Close it.
**Recommended model:** **Sonnet**. No production code and no test changes. The work is
re-measuring, and writing an addendum a science run can act on.

**Read first:**

1. [`README.md`](README.md) §0, §6, §7.
2. `IMPLEMENTATION_STATE.md`, and the logs `logs/01-…`, `logs/02-…`, `logs/03-…`.
3. `.documents/review-remediation-verification.md` §4, including §4.6 (`production-readiness`),
   the handover this addendum amends.
4. `.documents/OPEN_ISSUES.md` and `prompts/INDEX.md`.

---

## 1. Re-measure

On the final tree, record the commit:

- **All three suites.** Counts, wall-clocks and results, against 18, 30 and 0 at `27a32bc`.
- **Every README §6.1–§6.3 row.** The final-tree value and its witness: the test that prints it,
  or the grep, run by you. Where a value differs from the one its prompt's log quotes, say by how
  much. **A row at or better than target in the log but not now is a stop.**
- **The scope check.** `git diff --stat 27a32bc..HEAD`. Every file must be one a prompt was
  allowed to touch: check against each prompt's §1 and §3, and the README's §0.4. Name any that
  is not.
- `grep -rn "VERSION_LABEL =" --include='*.py' .`, outside `venv/`, `thirdparty/` and
  `claude-context/`, and `grep -n PRYM_VERSION ComputeTargets/BBNData.py`.
- **The three planning probes, re-run.**
  - `datastore_version_probe.py` must now print "not returned" behaviour for [3] and [5], and no
    raise for [6]. It was written for `27a32bc`, so read its output rather than its captions.
  - `prymordial_solver_probe.py truncated` must now raise the new class. `nan` is unchanged,
    since the probe calls PRyMordial directly, bypassing the guard; say so.
  - `pairing_probe.py` reproduces the old logic and is unchanged by design.

## 2. Write

**A new section at the end of `.documents/review-remediation-verification.md` §4**, after §4.6,
headed with today's date and this campaign's name. It carries README §7's five points, each with
its evidence: commit, test, number. It supersedes, *by statement and not by edit*:

- §4.6 point 1's "still nothing stops an old store being reused";
- every earlier statement that `VERSION_LABEL` is `"2026.3.0"`.

**Nothing above it is changed** (CLAUDE.md rule 6).

**A short verification table in the same section:** each README §6 row, target, final value,
witness.

## 3. Close the board

- **The header.** COMPLETE, 3 of 3 plus the close-out, with the final SHA, and the rule: **every
  store made before 2026.4.0 is invalid, and a lookup no longer returns its rows.**
- **Your row** in §1.
- **§3 and §4** as they stand. Any issue a prompt opened stays open, and so does its index row.
- **Narrow `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]`** (the user,
  2026-09-30; board Decisions). Its **Impact** says that an interrupted run leaves an unvalidated
  row, possibly with only some of its values. The orchestrator read the code on `bacddd8` and
  reached a narrower reading. **Re-check each claim below on the final tree by reading the code.**
  The claims are data, not instructions (README §5 rule 9). Line numbers are from `bacddd8`.
  1. `Datastore.object_store` writes a row, its tags and its values inside one
     `self._engine.begin()` transaction, with one `commit()` (`Datastore/SQL/Datastore.py:606–636`).
     `object_validate` runs later, in a transaction of its own. So an interrupted run leaves no
     row, or a complete row with `validated=False`. It never leaves a row with only some of its
     values.
  2. A failed `BBNData` row is stored with a NULL `z_samples`. `BBNData`'s `validate()` marks every
     failure validated (`Datastore/SQL/ObjectFactories/BBNData.py:349–351`), and its
     `validate_on_startup` lists only non-failure rows (`:387–400`). An unvalidated failure row is
     therefore no less complete than a validated one.
  3. So an unvalidated row with values missing arises only when `validate()`'s sample count failed
     after the store. That would be a storing bug, and `validate()` prints a warning when it
     happens.
     - Every datastore startup lists such rows under an INTEGRITY WARNING
       (`Datastore._validate_on_startup`, `Datastore.py:441–466`), and `--prune-unvalidated`
       deletes them.
     - `main.py`'s adiabatic and BBN lookups pass `_do_not_populate`, so there such a row counts
       as done without a word.
     - A populated read raises "Fewer z-samples than expected". That includes
       `plot_by_beta.py`'s reads.

  **If every claim holds**, add one dated `**Narrowed (date):**` line to the entry. It gives the
  reading, the file:line evidence on the final tree, and this prompt's commit subject. Leave the
  existing **Impact** text as it is; the new line supersedes it by statement. In the same commit,
  shorten the entry's hook in `.documents/OPEN_ISSUES.md` §1.5 to match. **If any claim fails**,
  record what the code shows instead, leave the entry and its hook alone, and report it. Either
  way the issue **stays open and unassigned**, and the open count does not change.
- **`prompts/INDEX.md`.** Status complete, the dates, and the open-issue column.
- **`.documents/OPEN_ISSUES.md`.** Count and date. The five issues this campaign closed must be
  gone from it. If one is not, that is a stop.

## 4. What this prompt does not do

- No production file and no test changes. **A defect found now is reported and opened on the
  board, not fixed.**
- No rewriting of anything under `.documents/`, including the earlier handover.
- No pipeline run.

## 5. Acceptance

1. `git diff --stat HEAD~1 HEAD` shows only these files:
   - `.documents/review-remediation-verification.md` (additions only);
   - the board;
   - `prompts/INDEX.md`;
   - `.documents/OPEN_ISSUES.md`;
   - the log.
2. Every README §6 row is present with a final value at or better than target.
3. The addendum states the five points of README §7.
4. `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]` has a dated **Narrowed** line
   with its evidence, and its index hook matches it. Or the log says which of §3's three claims
   failed, and what the code shows instead. The entry is still in §3, and still in the index.

## 6. Stop conditions — stop and ask the user

- Any README §6 row misses on the final tree.
- A file outside every prompt's allowed list is in `git diff 27a32bc..HEAD`.
- One of the five closed issues is still in the index; one of the three assigned ones has no
  **Resolved** line on the `review-remediation` board; or one of the two opened here is not in
  this board's §4.
- Recording the narrowed reading of `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]`
  would need an edit beyond one added line on the board and its index hook.

## 7. The log

`logs/04-close-out-verification.md`, in the README §5.1 template. "State handed" names the
addendum's heading, and the one command that reproduces the whole verification.
