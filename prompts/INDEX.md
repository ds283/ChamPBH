# Campaign index

One line per campaign under `prompts/`. It says which campaigns are live, when each opened and
was last touched, what it owns, and where its open issues are indexed. It is an index: the
campaign's `README.md` holds the plan and its `IMPLEMENTATION_STATE.md` the status; if this file
and a board disagree, the board is right. Add a line when a campaign folder is created and
change its status when the board closes it. The open-issue column is a snapshot from
[`.documents/OPEN_ISSUES.md`](../.documents/OPEN_ISSUES.md) on the date in the header; the section
pointer is the durable part.

**Last updated:** 2026-09-29 · 1 campaign: 1 planned, 0 live, 0 closed.

| Campaign | Status | Opened → last board update | Owns | Open issues (§ of `OPEN_ISSUES.md`; rows naming its board) |
|---|---|---|---|---|
| [`review-remediation`](review-remediation/IMPLEMENTATION_STATE.md) | **planned** — 6 prompts written, 0 landed | 2026-09-29 | the Jordan temperature law and the EOS derivative convention; the PRyMordial interface (`BBNData.py`, the vendored `PRyM/`); the kicking-function characterisation | §1.1; 7 |

Planning material that belongs to no campaign yet: the audit at
[`.documents/audit-2026-09-29/`](../.documents/audit-2026-09-29/README.md), whose items 5–9 are
recorded as seeded issues on the `review-remediation` board and are **not** in that campaign's
scope. The numerical campaign that will re-run the scalar histories and the BBN survey on the
corrected code is the user's, and is planned separately once this campaign closes.
