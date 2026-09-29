# Prompt 06 — close-out verification and the handover to the numerical campaign

**Campaign:** [`README.md`](README.md) · **Board item:** **R4** (verification), the handover ·
**Board:** `IMPLEMENTATION_STATE.md` — update your row, R4, and close the campaign in the header.
**Closes:** the campaign. **Recommended model:** **Opus**. No production code. The deliverable is
a document a stranger can act on.

**Read first:**

1. [`README.md`](README.md) in full, especially §6 and §7.
2. Every log in `logs/`, in order.
3. `.documents/audit-2026-09-29/README.md` — the "now" figures and the suggested order of work.
4. `CLAUDE.md`, "Verification documents are additive."

**It changes no production code and no test.** If a re-run reveals a defect, that is a §4 stop,
not something to fix here.

---

## 1. Re-take every measurement on the final tree

From the repository root, on the campaign's final commit (record its SHA):

1. The three audit scripts. `tlaw_check.py`'s `kappa=1/ln10` column now double-divides (prompt 02
   said so); report it as such, do not edit the script.
2. Both suites, with counts, and the wall-clock of each.
3. `tools/bbn_baseline.py`.
4. `git diff --stat f5896bb..HEAD`, and confirm every file in it is inside the campaign's scope
   (README §0.4, board header "Code").

## 2. Write `.documents/review-remediation-verification.md`

Sections:

1. **Before/after table**: every row of README §6.1–§6.4 with the `f5896bb` value, the target,
   the value on the final tree, and the witness (test name or script). A miss anywhere is a §4
   stop, but the row is still written.
2. **What changed, in one paragraph per prompt**, citing the log.
3. **What was deliberately not changed**, from README §0.4 and the board's §3, with the issue
   names.
4. **The handover** (README §7), written for whoever plans the numerical campaign:
   - the fresh-database rule and `VERSION_LABEL = "2026.2.0"`;
   - the run list: `exponential.yaml` at 1.1 ≤ β ≤ 3 with finer spacing, the same φ*, T*, M,
     Λ as before so the comparison is like for like; the baseline; the review's three cheap tests;
     `density_NP_ratio` at 1 MeV for β = 2; `hard_reflections` per history; a `density_NP_ratio`
     against T_J overlay for two adjacent β;
   - the expected magnitudes, so the campaign can tell a result from a bug: for a parked field
     with A² − 1 = r, D/H moves by ≈ +8.5 % at r = 0.08 (README §2 (f)), and a `density_NP_ratio`
     near 0.08 at 1 MeV for β = 2 is what the review's H1 predicts;
   - the list of paper figures that carry the 1.42-e-fold redshift-label offset (audit §1,
     consequence 3), phrased as a question for the authors where provenance is unknown;
   - the seeded issues the numerical campaign may want first (H5, H8).

## 3. Close the campaign

- Board header: **COMPLETE — 6 of 6**, date, final SHA. R4 → done.
- `prompts/INDEX.md`: status → complete, last update date.
- `.documents/audit-2026-09-29/README.md`: one dated line at the top, additive, pointing at the
  verification document. Nothing else in it changes.
- `.documents/OPEN_ISSUES.md`: count and date; no issue closes here unless a prompt closed it and
  forgot the index, in which case say so in the log.

## 4. Stop conditions — stop and ask the user

- Any §6 row misses on the final tree.
- A file outside the campaign's scope is in the diff.
- A log is missing, or a log's Result is not `COMPLETE` / `COMPLETE WITH DEVIATIONS`.

## 5. The log and the board

`logs/06-close-out-verification.md`. Its "State handed to the next prompt" section is the
handover's summary and names the verification document.
