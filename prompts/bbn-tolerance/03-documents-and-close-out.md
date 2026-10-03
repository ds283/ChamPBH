# Prompt 03 — Documents and close-out

**Campaign:** [`README.md`](README.md) · **Board item:** **D** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and D; close the campaign.
**Closes:** nothing new; confirms the board's §3 is empty or names what stays open.
**Recommended model:** **Sonnet.** No production code. Additive text, one re-measurement and a
handover.

**Read first:**

1. [`README.md`](README.md) §0.2 (P5, P9), §2 (f), §5, §6.3, §7.
2. `IMPLEMENTATION_STATE.md` in full.
3. `logs/01-mechanism-and-tolerance-scan.md` and `logs/02-low-T-tolerance-patch.md`: "What
   shipped", "Verification performed", and "State handed to the next prompt".
4. `.documents/numerical-strategies.md` §7.5–§7.6; `.documents/numerical-methods-for-paper.md`
   §4; `.documents/review-remediation-verification.md` §4.10, the pattern for §4.11;
   `.documents/paper-corrections-numerical-section.md` §5.

---

## 1. The documents (additive only; `CLAUDE.md` rule 6)

- **`numerical-strategies.md`, new §7.7, dated.** Cover:
  - the low-T stage's tolerance as upstream ships it, and as patched;
  - log 01's mechanism;
  - the scan's summary table;
  - the residual D/H spread and Yp's floor, as measurements with provenance (P9);
  - the `PRyM/` hunk for an upgrade.

  §7.6's list of `PRyM/` hunks is extended by a note in §7.7, not edited.
- **`numerical-methods-for-paper.md`.** A dated addendum to §4 stating the tolerance, what it
  costs, and the D/H precision the paper may claim for a single history.
- **`paper-corrections-numerical-section.md` §5.** A row if the paper states or implies
  PRyMordial's precision.
- **`review-remediation-verification.md`, new §4.11**, after §4.10, carrying README §7's five
  points. Give the refresh route as commands the user can run. Copy the store to a new stem (17
  files) and run `main.py` with the science run's arguments plus `--drop bbn-data`; take those
  arguments from `full_run_2026.6.0.sh` if it is in the working tree, and otherwise describe them.
  Say what the P6 warning prints on a store that was not refreshed.

## 2. Re-measure (README §6.3)

The tool, `--variant prod`, no override, on the 17-input roster on the final tree. Every figure
must be identical to log 02's. Run serially or in parallel; wall times are not acceptance rows
here.

## 3. Stop conditions — stop and ask the user

- A roster figure differs from log 02's.
- `git diff --numstat` on `.documents/` shows a deletion.

## 4. The log, the board, the index and the campaign index

- `logs/03-documents-and-close-out.md`, in the README §5.1 template.
- The board: D done; the status line reads **COMPLETE** with the final suite counts.
- `.documents/OPEN_ISSUES.md`: correct the header's description of this board.
- `prompts/INDEX.md`: this campaign's row becomes **complete**.

**Allowed files:**
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `paper-corrections-numerical-section.md`, `review-remediation-verification.md` (additive);
- the log; this campaign's board; `.documents/OPEN_ISSUES.md`; `prompts/INDEX.md`.
