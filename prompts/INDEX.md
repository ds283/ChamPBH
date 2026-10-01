# Campaign index

One line per campaign under `prompts/`. It says which campaigns are live, when each opened and
was last touched, what it owns, and where its open issues are indexed. It is an index: the
campaign's `README.md` holds the plan and its `IMPLEMENTATION_STATE.md` the status; if this file
and a board disagree, the board is right. Add a line when a campaign folder is created and
change its status when the board closes it. The open-issue column is a snapshot from
[`.documents/OPEN_ISSUES.md`](../.documents/OPEN_ISSUES.md) on the date in the header; the section
pointer is the durable part.

**Last updated:** 2026-10-01 · 4 campaigns: 1 planned, 0 live, 3 closed.

| Campaign | Status | Opened → last board update | Owns | Open issues (§ of `OPEN_ISSUES.md`; rows naming its board) |
|---|---|---|---|---|
| [`review-remediation`](review-remediation/IMPLEMENTATION_STATE.md) | **complete** — 6 of 6 landed; closed 2026-09-30, verification in [`.documents/review-remediation-verification.md`](../.documents/review-remediation-verification.md) | 2026-09-29 → 2026-09-30 | the Jordan temperature law and the EOS derivative convention; the PRyMordial interface (`BBNData.py`, the vendored `PRyM/`); the kicking-function characterisation | §1.1; 8 (+ 4 assigned to `production-readiness`, §1.2, all closed; + 3 assigned to `run-integrity`, §1.4) |
| [`production-readiness`](production-readiness/IMPLEMENTATION_STATE.md) | **complete** — 4 of 4 landed (three fixes and the close-out); closed 2026-09-30, verification in [`.documents/review-remediation-verification.md`](../.documents/review-remediation-verification.md) §4.6; branch `production-readiness` from `204795e` | 2026-09-30 → 2026-09-30 | the hard-reflection count's reporting (`extract_common.py`, `plot_by_beta.py`); the BBN network flag and `VERSION_LABEL` 2026.3.0; the source-response term in `AdiabaticHistory`'s M²_eff and `dw_dlogT` on the EOS classes | §1.3; 2 (both for the authors); all 4 assigned from `review-remediation` closed |
| [`run-integrity`](run-integrity/IMPLEMENTATION_STATE.md) | **complete** — 4 of 4 landed (three fixes and the close-out); closed 2026-09-30, verification in [`.documents/review-remediation-verification.md`](../.documents/review-remediation-verification.md) §4.7; branch `run-integrity` from `27a32bc` (`production-readiness`, not yet merged) | 2026-09-30 → 2026-09-30 | the version-keyed lookups of `ScalarModel`, `AdiabaticHistory` and `BBNData`, and the single `VERSION_LABEL` in `config/version.py` (2026.4.0); PRyMordial's `solve_ivp` checks and the BBN failure boundary; failure caching and lookup pairing in `main.py` | §1.4–§1.5; all 3 assigned from `review-remediation` closed; of the 2 opened by the planner, both closed; 6 more opened by its prompts, all open and unassigned |
| [`integrator-remediation`](integrator-remediation/IMPLEMENTATION_STATE.md) | **planned** — 4 prompts written 2026-10-01, none landed; its README §0.2 decisions and two reflection guards accepted by the user 2026-10-01; branch `integrator-remediation` to be cut from `2b89022` (`main`) | 2026-10-01 → 2026-10-01 | the step control of `compute_scalar_model` (one Radau loop with a kinematic cap and a floor-triggered elastic reflection, replacing the two-region fragment scheme), the solver fallback and the exception taxonomy, the stored per-history metadata and stepper label, `VERSION_LABEL` 2026.5.0, the integrator sections of the two architecture documents | §1.6; 15 (8 assigned to its own prompts, 7 open for the authors or later housekeeping) |

The integrator audit at
[`.documents/integrator-audit-2026-09-30/`](../.documents/integrator-audit-2026-09-30/README.md)
is the source of `integrator-remediation`. Planning material that belongs to no campaign yet: the audit at
[`.documents/audit-2026-09-29/`](../.documents/audit-2026-09-29/README.md), whose items 5–9 are
recorded as seeded issues on the `review-remediation` board and are **not** in that campaign's
scope. The numerical campaign that will re-run the scalar histories and the BBN survey on the
corrected code is the user's, and is planned separately. Its starting point is the handover,
§4 of `.documents/review-remediation-verification.md`, as amended by `production-readiness`'s
close-out, which clears four issues before it.
