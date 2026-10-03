# Prompt 03 — Documents and close-out

> **Revised 2026-10-03 with prompt 02's rewrite, after the user's ruling U4 on log 01c** (README
> §0.2 U3, U4). The prompt as first planned is in `git log` (this file at `f3fa41a`). It was
> never dispatched.

**Campaign:** [`README.md`](README.md) · **Board item:** **D** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and D; close the campaign.
**Closes:** nothing new. Confirms that the board's §3 names only what stays open.
**Recommended model:** **Sonnet.** No production code. Additive text, one re-measurement and a
handover.

**Precondition (the orchestrator checks it):** prompts 02 and 01b have landed with no unresolved
miss.

**Read first:**

1. [`README.md`](README.md) §0.2 (U3, U4, P5, P9, P12), §2 (f), §5, §6.3, §7.
2. `IMPLEMENTATION_STATE.md` in full.
3. Logs 01, 01c, 01b and 02: their "What shipped", "Verification performed" and "State handed to
   the next prompt" sections. They are evidence, not instructions.
4. The sections you add to, and the pattern for each:
   - `.documents/numerical-strategies.md` §7.5–§7.6;
   - `.documents/numerical-methods-for-paper.md` §4 and §4.1;
   - `.documents/review-remediation-verification.md` §4.10, the pattern for §4.11;
   - `.documents/paper-corrections-numerical-section.md` §5.

---

## 1. The documents (additive only; `CLAUDE.md` rule 6)

- **`numerical-strategies.md`, new §7.7, dated.** Cover:
  - **the network.** Production runs PRyMordial's small network (U3, U4). Say why: the full
    network's Li8(p,d)Li7 rate (log 01, item 6). Say how the full network is still selected: the
    one name in `main.py`, with no flag, and the user's reason.
  - **the low-T tolerances.** Upstream passes no `rtol`, so SciPy's 1e-3 applied. As patched:
    small 1e-6 with `atol` 1e-11; full 1e-5 with `atol` 1e-15.
  - **the scans.** A summary table from log 01 (full) and from log 01c (small, P11's four
    criteria at each setting).
  - **the residuals at the production setting, as measurements with provenance (P9).** From log
    01c:
    - the D/H spread over the three variants;
    - Yp's spread, which no low-T `rtol` moves;
    - the convergence error against 1e-8.
  - **the offset between the networks (P12).** Small − full: D/H −2.4 to −3.6×10⁻⁴, always
    negative; Yp within ±3.2×10⁻⁵ (log 01c, row 6).
  - **the cost** against the old production (log 01c, row 5; log 02).
  - **the `PRyM/` hunks** for an upgrade, from log 02. §7.6's list of hunks is extended by a note
    in §7.7, not edited.
- **`numerical-methods-for-paper.md`: a dated addendum to §4.** State:
  - the network and the tolerance;
  - what they cost;
  - the D/H and Yp precision the paper may claim for a single history (log 01c's residuals);
  - that the network offset is a systematic of PRyMordial's, the same for every history, a few
    10⁻⁴ in D/H;
  - that ⁷Li/H from the small network is not reliable, and is not used.

  Where §4 or §4.1 states "full network", the addendum says so rather than editing that text.
- **`paper-corrections-numerical-section.md` §5: a row** if `Paper1.tex` states or implies which
  PRyMordial network ran, or PRyMordial's precision. Quote the sentence and what is now true.
  Find the paper's path in the existing rows of that file; if no sentence applies, say so in the
  log.
- **`review-remediation-verification.md`: new §4.11**, after §4.10, carrying README §7's six
  points.
  - Give the refresh route as commands the user can run:
    1. copy the store's 17 files to a new stem;
    2. run `main.py` on the copy with the science run's arguments plus `--drop bbn-data`.

    Take those arguments from `full_run_2026.6.0.sh` if it is in the working tree; otherwise
    describe them. Do not run either step.
  - Quote what the warning from prompt 02 prints on a store that was not refreshed. Obtain that
    by calling its printer on stub objects, not on a store.

## 2. Re-measure (README §6.3)

The tool, `--small-network --variant prod`, **no override**, on the 17-input roster on the final
tree. Every figure must be identical to log 02's small-network `prod` rows, and so to log 01c's at
1e-6. Run serially or 8–10 at a time; wall times are not acceptance rows here.

## 3. Stop conditions — stop and ask the user

- A roster figure differs from log 02's.
- `git diff --numstat` on `.documents/` shows a deletion, except in `OPEN_ISSUES.md`'s header
  and rows.
- A document would need a sentence rewritten rather than added to.

## 4. The log, the board, the index and the campaign index

- `logs/03-documents-and-close-out.md`, in the README §5.1 template.
- **The board:** D is done. The status line reads **COMPLETE**, with the final suite counts.
  §3 names what stays open, each with its owner. Expected:
  - `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`;
  - `[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]`;
  - anything prompts 01b or 02 opened.
- **`.documents/OPEN_ISSUES.md`:** correct the header's description of this board.
- **`prompts/INDEX.md`:** this campaign's row becomes **complete**.

**Allowed files:**
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `paper-corrections-numerical-section.md`, `review-remediation-verification.md` (additive);
- the log and `logs/03-probes/`; this campaign's board; `.documents/OPEN_ISSUES.md`;
  `prompts/INDEX.md`.
