# Campaign index

One line per campaign under `prompts/`. It says which campaigns are live, when each opened and
was last touched, what it owns, and where its open issues are indexed. It is an index: the
campaign's `README.md` holds the plan and its `IMPLEMENTATION_STATE.md` the status; if this file
and a board disagree, the board is right. Add a line when a campaign folder is created and
change its status when the board closes it. The open-issue column is a snapshot from
[`.documents/OPEN_ISSUES.md`](../.documents/OPEN_ISSUES.md) on the date in the header; the section
pointer is the durable part.

**Last updated:** 2026-09-30 · 2 campaigns: 1 planned, 0 live, 1 closed.

| Campaign | Status | Opened → last board update | Owns | Open issues (§ of `OPEN_ISSUES.md`; rows naming its board) |
|---|---|---|---|---|
| [`review-remediation`](review-remediation/IMPLEMENTATION_STATE.md) | **complete** — 6 of 6 landed; closed 2026-09-30, verification in [`.documents/review-remediation-verification.md`](../.documents/review-remediation-verification.md) | 2026-09-29 → 2026-09-30 | the Jordan temperature law and the EOS derivative convention; the PRyMordial interface (`BBNData.py`, the vendored `PRyM/`); the kicking-function characterisation | §1.1; 11 (+ 4 assigned to `production-readiness`, §1.2) |
| [`production-readiness`](production-readiness/IMPLEMENTATION_STATE.md) | **planned** — 0 of 4 landed; branch `production-readiness` from `204795e` | 2026-09-30 → 2026-09-30 | the hard-reflection count's reporting (`extract_common.py`, `plot_by_beta.py`); the BBN network flag and `VERSION_LABEL` 2026.3.0; the source-response term in `AdiabaticHistory`'s M²_eff and `dw_dlogT` on the EOS classes | §1.2–§1.3; 4 assigned from `review-remediation`, 2 opened |

Planning material that belongs to no campaign yet: the audit at
[`.documents/audit-2026-09-29/`](../.documents/audit-2026-09-29/README.md), whose items 5–9 are
recorded as seeded issues on the `review-remediation` board and are **not** in that campaign's
scope. The numerical campaign that will re-run the scalar histories and the BBN survey on the
corrected code is the user's, and is planned separately. Its starting point is the handover,
§4 of `.documents/review-remediation-verification.md`, as amended by `production-readiness`'s
close-out, which clears four issues before it.
