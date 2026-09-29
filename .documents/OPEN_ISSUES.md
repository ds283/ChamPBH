# Open issues — project-wide index

**Last updated:** 2026-09-29 · **8 open** across one campaign.

This file exists so that an issue opened by one campaign is not lost when that campaign closes.
It is an **index, not a record**: one line per issue, pointing at the campaign status board that
holds the measurements, the impact statement and the next step. Never put issue content here — if
the two disagree, the board is right.

> **Maintenance rule.** Whenever you add, narrow or close an entry in a campaign board's
> §3 (Active and unresolved issues) or §4 (Resolved issues), update this file **in the same
> commit**. Add the line, move it between sections here, or delete it, and correct the count and
> the date above. See `CLAUDE.md`.

**Boards.** [`review-remediation`](../prompts/review-remediation/IMPLEMENTATION_STATE.md)

## 1. Open, by owning board

### 1.1 `review-remediation` — [board §3](../prompts/review-remediation/IMPLEMENTATION_STATE.md)

Seven seeded at planning on 2026-09-29 from `.documents/audit-2026-09-29/README.md`. The two
opened after that on 2026-09-29 are resolved (board §4). One opened by prompt 02.

- `[00-adiabaticity-diagnostic-omits-the-source-response-term]` — `AdiabaticHistory.py:103`
  gives zero conformal mass for the exponential coupling; the (Ω′)² response term is missing
  (review H5). Out of this campaign's scope.
- `[00-initial-field-value-is-hard-coded-and-unchecked]` — `main.py:814` fixes φ* = 5 M_P with no
  A*T* ≲ M_P guard (review H8). Out of this campaign's scope.
- `[00-hard-reflection-count-is-stored-but-never-reported]` — `hard_reflections` is on every
  `ScalarModel` row and appears in no plot or caption (review N1).
- `[00-kicking-function-table-has-no-provenance-in-the-repository]` —
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` was added in commit `1759515` without the script
  that built it.
- `[00-datastore-lookups-ignore-the-version-column]` — `ScalarModel`, `AdiabaticHistory` and
  `BBNData` rows carry a `version` but no lookup filters on it, so a numerical fix returns stale
  rows from an old store without complaint.
- `[00-two-files-are-not-black-clean]` — `Datastore/SQL/ObjectFactories/base.py` and
  `CosmologyModels/LambdaCDM/Planck.py` at `f5896bb`; housekeeping.
- `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` — `BBNData.py:47` tabulates down to
  0.1 eV where PRyMordial stops near 0.3 keV; harmless at 250 knots per decade, but it includes
  the matter-era re-delivery oscillations.
- `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` — three wrong comments in
  `CosmologyModels/GenericEOS/` (the base `dG_s_dlogT` docstring, the jax class's "1/GeV", the
  "600 keV" above `SAIKAWA_SHIRAI_T_LO`); comment-only.
